package main

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// workerProgress tracks, for one worker and one source, the LSNs the worker has
// written inside its currently open transaction.
//
// WrittenLSN is the maximum LSN applied in the open transaction; it only
// describes uncommitted work. A value becomes "committed" after the progress
// row written in the same transaction has been read back durably (see
// VerifyProgress), at which point it is reflected in replicationProgress.
type workerProgress struct {
	ref        sourceRef
	WrittenLSN pglogrepl.LSN
}

type (
	Worker struct {
		log         *slog.Logger
		workChannel chan operation
		jobsCounter prometheus.Counter
		// tx is the worker's open destination transaction, or nil.
		tx pgx.Tx
		// mu guards progress and pendingSources.
		mu sync.Mutex
		// progress holds the per-source LSNs of the open transaction.
		progress map[string]workerProgress
		// pendingSources are sources whose commit result could not yet be
		// verified against the durable progress table; they are re-checked on
		// every commit cycle until the read succeeds.
		pendingSources []sourceRef
	}
)

var Workers []Worker

func (w *Worker) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tx = nil
	w.progress = make(map[string]workerProgress)
	w.pendingSources = nil
}

// ResetWorkers clears all worker transaction state and the verified watermarks.
// It runs during a full process restart, before destinations are reopened.
func ResetWorkers() {
	replicationProgress.Reset()
	for i := range Workers {
		Workers[i].Reset()
	}
}

func (w *Worker) writeLSN(ref sourceRef, lsn pglogrepl.LSN) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.progress == nil {
		w.progress = make(map[string]workerProgress)
	}
	id := ref.id()
	status := w.progress[id]
	status.ref = ref
	if lsn > status.WrittenLSN {
		status.WrittenLSN = lsn
	}
	w.progress[id] = status
}

// commitTransaction persists the applied data together with each source's
// confirmable LSN in a single destination transaction, then verifies the
// durable result before any in-memory watermark moves.
//
// On commit failure the transaction is rolled back or treated as having an
// unknown outcome. In both cases the watermark is only advanced through
// VerifyProgress, which trusts the durable progress table rather than the
// commit call's return value.
func (w *Worker) commitTransaction() {
	log := w.log
	if w.tx == nil {
		return
	}
	ctx := context.Background()

	w.mu.Lock()
	tx := w.tx
	snap := make([]workerProgress, 0, len(w.progress))
	for _, status := range w.progress {
		snap = append(snap, status)
	}
	w.mu.Unlock()

	// Persist each source's confirmable LSN in the same transaction as the
	// applied data. If this write fails, roll everything back: nothing may be
	// reported as confirmable without its durable progress row.
	for _, status := range snap {
		if err := SaveProgress(ctx, tx, status.ref.database, status.ref.sid, status.WrittenLSN); err != nil {
			log.Error("cannot persist progress in transaction, rolling back",
				"source", status.ref.id(), "error", err)
			progressErrors.Inc()
			_ = tx.Rollback(ctx)
			w.mu.Lock()
			w.tx = nil
			w.progress = make(map[string]workerProgress)
			w.mu.Unlock()
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		// The commit result is unknown (network failure, ambiguous error, ...).
		// Do not assume either outcome: clear the open state and let
		// VerifyProgress decide what actually landed on disk.
		log.Error("failed to commit transaction, treating outcome as unknown until progress is verified",
			"error", err)
		progressErrors.Inc()
	}
	w.mu.Lock()
	w.tx = nil
	// The open-transaction state is cleared regardless of the commit result:
	// anything not proven durable will be replayed from the slot because its
	// LSN never reaches a standby status update.
	w.progress = make(map[string]workerProgress)
	refs := make([]sourceRef, 0, len(snap))
	for _, status := range snap {
		refs = append(refs, status.ref)
	}
	// Deduplicate: a source whose verification keeps failing must not
	// accumulate entries once per commit cycle while the destination is down.
	pending := mergeSourceRefs(w.pendingSources, refs)
	w.mu.Unlock()

	// Re-read durable progress before advancing any watermark. Failures stay
	// pending and are retried on the next commit cycle; no error is swallowed
	// into an optimistic in-memory advance.
	failed := VerifyProgress(ctx, pending, log)
	w.mu.Lock()
	w.pendingSources = failed
	w.mu.Unlock()
}

// verifyPending re-checks sources whose earlier verification failed, so a
// transient progress-table outage does not stall confirmation forever.
func (w *Worker) verifyPending() {
	w.mu.Lock()
	if len(w.pendingSources) == 0 {
		w.mu.Unlock()
		return
	}
	pending := w.pendingSources
	w.mu.Unlock()
	failed := VerifyProgress(context.Background(), pending, w.log)
	w.mu.Lock()
	w.pendingSources = failed
	w.mu.Unlock()
}

func (w *Worker) work() {
	var op operation
	var err error
	log := w.log
	timer := time.NewTimer(time.Duration(config.App.CommitDelay) * time.Second)
	for {
		select {
		case <-timer.C:
			w.verifyPending()
			if w.tx != nil {
				w.commitTransaction()
			}
			timer.Reset(time.Duration(config.App.CommitDelay) * time.Second)
		case op = <-w.workChannel:
			w.jobsCounter.Inc()
			if w.tx == nil {
				w.tx, err = DestConnectionPool.Begin(context.Background())
				if err != nil {
					log.Error("failed to begin transaction", "error", err)
					progressErrors.Inc()
					continue
				}
			}
			log.Debug("received operation", "op", op)
			switch op.opCode {
			case "ic":
				_ = op.insertClone(w.tx)
			case "uc":
				_ = op.updateClone(w.tx)
			case "dc":
				_ = op.deleteClone(w.tx)
			default:
				log.Error("unhandled opcode", "op", op.opCode)
			}
			w.writeLSN(sourceRef{database: op.database, sid: op.sid}, op.lsn)
			log.Debug("Performed operation", "op", op, "lsn", op.lsn)
		}
	}
}

func SendWork(op operation) {
	Workers[op.id%int64(len(Workers))].workChannel <- op
}

// mergeSourceRefs returns the union of the two source lists, keeping the first
// occurrence of each source so ordering stays stable and no duplicates remain.
func mergeSourceRefs(lists ...[]sourceRef) []sourceRef {
	seen := make(map[string]bool)
	merged := make([]sourceRef, 0)
	for _, list := range lists {
		for _, ref := range list {
			id := ref.id()
			if seen[id] {
				continue
			}
			seen[id] = true
			merged = append(merged, ref)
		}
	}
	return merged
}

func StartWorkers(numWorkers int) {
	Workers = make([]Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		Workers[i].workChannel = make(chan operation)
		Workers[i].jobsCounter = jobsTotal.WithLabelValues(strconv.Itoa(i))
		Workers[i].log = log.With("worker", i)
		Workers[i].progress = make(map[string]workerProgress)
		go Workers[i].work()
	}
}

// GetCommittedLSN returns the highest LSN that can safely be confirmed to the
// source for one source across all workers.
//
// The confirmation may only advance to a contiguous safe boundary: every
// operation up to the returned LSN must be durably applied. Therefore:
//
//  1. Find the lowest LSN that any worker has written but not yet durably
//     committed (lowestDirtyLSN).
//  2. The durable watermark is confirmable only while it stays below that
//     dirty boundary and at or below the LSN the source has itself committed.
//
// Taking the maximum LSN of a fast worker would skip changes still pending in
// a slow worker and is explicitly not done here.
func GetCommittedLSN(database, sid string, sourceCommittedLSN pglogrepl.LSN) pglogrepl.LSN {
	id := sourceID(database, sid)

	// Step 1: lowest dirty LSN across workers.
	lowestDirtyLSN := pglogrepl.LSN(0)
	for i := range Workers {
		Workers[i].mu.Lock()
		if status, ok := Workers[i].progress[id]; ok && status.WrittenLSN != 0 {
			if status.WrittenLSN < lowestDirtyLSN || lowestDirtyLSN == 0 {
				lowestDirtyLSN = status.WrittenLSN
			}
		}
		Workers[i].mu.Unlock()
	}

	// Step 2: the verified durable watermark, bounded by the dirty boundary and
	// by the source's own commit position.
	durable := replicationProgress.Get(id)
	if durable > sourceCommittedLSN {
		durable = sourceCommittedLSN
	}
	if lowestDirtyLSN != 0 && durable >= lowestDirtyLSN {
		// The durable watermark cannot cover LSNs at or above uncommitted work.
		// In steady state this branch is unreachable; withhold confirmation
		// rather than risk an unsafe one.
		log.Error("durable watermark reaches uncommitted boundary, withholding confirmation",
			"source", id, "durable_lsn", durable, "lowest_dirty_lsn", lowestDirtyLSN)
		return 0
	}
	return durable
}
