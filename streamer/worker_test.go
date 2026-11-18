package main

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func lsn(v uint64) pglogrepl.LSN { return pglogrepl.LSN(v) }

// setupWorkers builds n workers with empty open-transaction progress and a
// clean verified watermark map.
func setupWorkers(t *testing.T, n int) {
	t.Helper()
	Workers = make([]Worker, n)
	for i := range Workers {
		Workers[i].progress = make(map[string]workerProgress)
	}
	replicationProgress = &ProgressTracker{verified: map[string]pglogrepl.LSN{}}
}

func TestProgressTrackerSetIsMonotonic(t *testing.T) {
	p := &ProgressTracker{verified: map[string]pglogrepl.LSN{}}
	id := "db-sid"

	p.Set(id, lsn(10))
	if got := p.Get(id); got != lsn(10) {
		t.Fatalf("expected 10, got %d", got)
	}
	// a verified higher value advances the watermark
	p.Set(id, lsn(20))
	if got := p.Get(id); got != lsn(20) {
		t.Fatalf("expected 20, got %d", got)
	}
	// a lower (e.g. ambiguous-commit) read must never move it backwards
	p.Set(id, lsn(5))
	if got := p.Get(id); got != lsn(20) {
		t.Fatalf("watermark regressed to %d", got)
	}
}

func TestGetCommittedLSNContiguousBoundary(t *testing.T) {
	setupWorkers(t, 2)
	const db, sid = "db1", "sid1"
	id := sourceID(db, sid)
	ref := sourceRef{database: db, sid: sid}

	// Worker 0 has durable 100 and an open (uncommitted) write at 150.
	replicationProgress.Set(id, lsn(100))
	Workers[0].writeLSN(ref, lsn(150))
	// Worker 1 is the fast worker: already written 200 in its open transaction.
	Workers[1].writeLSN(ref, lsn(200))

	got := GetCommittedLSN(db, sid, lsn(1000))
	// Lowest dirty LSN is 150; the durable 100 is below it, so 100 is the
	// contiguous safe boundary. The fast worker's 200 must not be reported.
	if got != lsn(100) {
		t.Fatalf("expected safe boundary 100, got %d", got)
	}
}

func TestGetCommittedLSNWaitsForSlowWorker(t *testing.T) {
	setupWorkers(t, 2)
	const db, sid = "db1", "sid1"
	id := sourceID(db, sid)
	ref := sourceRef{database: db, sid: sid}

	// Both workers verified 100. Worker 1 then opens a new transaction with a
	// write at 120 while worker 0 is idle.
	replicationProgress.Set(id, lsn(100))
	Workers[1].writeLSN(ref, lsn(120))

	got := GetCommittedLSN(db, sid, lsn(1000))
	if got != lsn(100) {
		t.Fatalf("confirmation must not pass the slow worker's dirty boundary, got %d", got)
	}
}

func TestGetCommittedLSNNoDirtyWorkers(t *testing.T) {
	setupWorkers(t, 2)
	const db, sid = "db1", "sid1"
	id := sourceID(db, sid)
	replicationProgress.Set(id, lsn(300))

	// With no open transactions anywhere, the full durable watermark is safe.
	if got := GetCommittedLSN(db, sid, lsn(1000)); got != lsn(300) {
		t.Fatalf("expected 300, got %d", got)
	}
}

func TestGetCommittedLSNBoundedBySourceCommit(t *testing.T) {
	setupWorkers(t, 1)
	const db, sid = "db1", "sid1"
	id := sourceID(db, sid)
	replicationProgress.Set(id, lsn(500))

	// The source has not itself committed beyond 250, so confirmation is capped.
	if got := GetCommittedLSN(db, sid, lsn(250)); got != lsn(250) {
		t.Fatalf("expected source cap 250, got %d", got)
	}
}

func TestGetCommittedLSNZeroWhenNothingVerified(t *testing.T) {
	setupWorkers(t, 2)
	const db, sid = "db1", "sid1"
	ref := sourceRef{database: db, sid: sid}
	// Written but never verified durable: nothing may be confirmed.
	Workers[0].writeLSN(ref, lsn(10))
	if got := GetCommittedLSN(db, sid, lsn(1000)); got != 0 {
		t.Fatalf("expected 0 before any verified commit, got %d", got)
	}
}

func TestWriteLSNTracksMaximumPerSource(t *testing.T) {
	setupWorkers(t, 1)
	ref := sourceRef{database: "db1", sid: "sid1"}
	w := &Workers[0]

	w.writeLSN(ref, lsn(10))
	w.writeLSN(ref, lsn(30))
	w.writeLSN(ref, lsn(20)) // out of order, must not lower the max

	status := w.progress[ref.id()]
	if status.WrittenLSN != lsn(30) {
		t.Fatalf("expected max written LSN 30, got %d", status.WrittenLSN)
	}
	if status.ref != ref {
		t.Fatalf("source reference not retained: %+v", status.ref)
	}
}

func TestGetCommittedLSNIndependentAcrossSources(t *testing.T) {
	setupWorkers(t, 2)
	refA := sourceRef{database: "db1", sid: "sidA"}
	refB := sourceRef{database: "db1", sid: "sidB"}
	replicationProgress.Set(refA.id(), lsn(100))
	replicationProgress.Set(refB.id(), lsn(200))
	// A dirty transaction for A must not affect B's safe boundary.
	Workers[0].writeLSN(refA, lsn(150))

	if got := GetCommittedLSN(refA.database, refA.sid, lsn(1000)); got != lsn(100) {
		t.Fatalf("source A expected 100, got %d", got)
	}
	if got := GetCommittedLSN(refB.database, refB.sid, lsn(1000)); got != lsn(200) {
		t.Fatalf("source B expected 200, got %d", got)
	}
}

func TestMergeSourceRefsDeduplicates(t *testing.T) {
	a := sourceRef{database: "db1", sid: "sidA"}
	b := sourceRef{database: "db1", sid: "sidB"}
	merged := mergeSourceRefs([]sourceRef{a, b}, []sourceRef{a}, []sourceRef{b, a})
	if len(merged) != 2 {
		t.Fatalf("expected 2 distinct sources, got %d: %+v", len(merged), merged)
	}
	if merged[0] != a || merged[1] != b {
		t.Fatalf("order/first-occurrence not preserved: %+v", merged)
	}
}

func TestResetWorkersClearsState(t *testing.T) {
	setupWorkers(t, 2)
	ref := sourceRef{database: "db1", sid: "sid1"}
	replicationProgress.Set(ref.id(), lsn(100))
	Workers[0].writeLSN(ref, lsn(150))
	Workers[0].pendingSources = []sourceRef{ref}

	ResetWorkers()

	if got := replicationProgress.Get(ref.id()); got != 0 {
		t.Fatalf("verified watermark not reset, got %d", got)
	}
	for i := range Workers {
		Workers[i].mu.Lock()
		dirty := len(Workers[i].progress)
		pending := len(Workers[i].pendingSources)
		tx := Workers[i].tx
		Workers[i].mu.Unlock()
		if dirty != 0 || pending != 0 || tx != nil {
			t.Fatalf("worker %d not fully reset: dirty=%d pending=%d tx=%v", i, dirty, pending, tx)
		}
	}
}
