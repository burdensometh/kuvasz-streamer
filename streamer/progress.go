package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// progressTable is the destination-side table that durably records, for each
// source (identified by database name and sid), the highest LSN whose changes
// have been applied *and committed* on the destination. The row is updated in
// the same transaction as the applied data, so a durable LSN can never describe
// changes that are not themselves durable.
const progressTable = "kvsz_replication_progress"

// sourceRef identifies one replication source.
type sourceRef struct {
	database string
	sid      string
}

func (s sourceRef) id() string { return sourceID(s.database, s.sid) }

func sourceID(database, sid string) string { return database + "-" + sid }

// ProgressTracker holds the in-memory view of durable progress. Every value
// stored here has either been loaded from the durable progress table at startup
// or been verified against that table after a worker commit. It never contains
// a value inferred from a commit call alone.
type ProgressTracker struct {
	mu       sync.Mutex
	verified map[string]pglogrepl.LSN
}

var replicationProgress = &ProgressTracker{verified: map[string]pglogrepl.LSN{}}

func (p *ProgressTracker) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.verified = map[string]pglogrepl.LSN{}
}

// Set advances the in-memory watermark monotonically. A lower value read from
// the durable table (e.g. a transaction that turned out not to have committed)
// can never move the watermark backwards.
func (p *ProgressTracker) Set(id string, lsn pglogrepl.LSN) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lsn > p.verified[id] {
		p.verified[id] = lsn
	}
}

func (p *ProgressTracker) Get(id string) pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.verified[id]
}

// EnsureProgressTable creates the durable progress table in the destination
// database. It is idempotent and runs as part of destination setup.
func EnsureProgressTable(ctx context.Context) error {
	ddl := fmt.Sprintf(`create table if not exists %s.%s (
		database_name text        not null,
		sid           text        not null,
		applied_lsn   pg_lsn      not null,
		updated_at    timestamptz not null default now(),
		primary key (database_name, sid)
	);`, config.Database.Schema, progressTable)
	if _, err := DestConnectionPool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("cannot create progress table %s.%s: %w", config.Database.Schema, progressTable, err)
	}
	return nil
}

// LoadProgress returns the durable applied LSN recorded for the source, or 0
// when the source has no progress row yet.
func LoadProgress(ctx context.Context, database, sid string) (pglogrepl.LSN, error) {
	var lsnText string
	err := DestConnectionPool.QueryRow(ctx,
		fmt.Sprintf(`select applied_lsn::text from %s.%s where database_name=$1 and sid=$2`,
			config.Database.Schema, progressTable),
		database, sid).Scan(&lsnText)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cannot load durable progress for %s: %w", sourceID(database, sid), err)
	}
	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return 0, fmt.Errorf("cannot parse durable LSN %q for %s: %w", lsnText, sourceID(database, sid), err)
	}
	return lsn, nil
}

// SaveProgress records, inside the worker transaction, the highest LSN applied
// by this transaction for the source. It shares the transaction with the
// applied row changes, so the progress only becomes durable if the data does.
// GREATEST keeps the row monotonic even if an older transaction ends up
// committing after a newer one.
func SaveProgress(ctx context.Context, tx pgx.Tx, database, sid string, lsn pglogrepl.LSN) error {
	query := fmt.Sprintf(`insert into %s.%s as p (database_name, sid, applied_lsn, updated_at)
		values ($1, $2, $3::pg_lsn, now())
		on conflict (database_name, sid) do update
			set applied_lsn = greatest(p.applied_lsn, excluded.applied_lsn),
			    updated_at    = now()`,
		config.Database.Schema, progressTable)
	if _, err := tx.Exec(ctx, query, database, sid, lsn.String()); err != nil {
		return fmt.Errorf("cannot persist replication progress for %s: %w", sourceID(database, sid), err)
	}
	return nil
}

// initProgressRow anchors the durable progress at a known-safe LSN for a source
// that has no progress row yet (first run or upgraded deployment).
func initProgressRow(ctx context.Context, database, sid string, lsn pglogrepl.LSN) error {
	query := fmt.Sprintf(`insert into %s.%s as p (database_name, sid, applied_lsn, updated_at)
		values ($1, $2, $3::pg_lsn, now())
		on conflict (database_name, sid) do update
			set applied_lsn = greatest(p.applied_lsn, excluded.applied_lsn),
			    updated_at    = now()`,
		config.Database.Schema, progressTable)
	if _, err := DestConnectionPool.Exec(ctx, query, database, sid, lsn.String()); err != nil {
		return fmt.Errorf("cannot initialize progress row for %s: %w", sourceID(database, sid), err)
	}
	return nil
}

// ReconcileProgress cross-checks the durable progress against the source slot
// position and initializes the in-memory watermark before streaming starts.
//
// durable < slotLSN is a hard inconsistency rather than a loggable warning: the
// source may already have discarded WAL that the destination still needs, so
// replication must not start and the operator has to resolve the divergence.
// durable == 0 means the source is new (the initial sync just completed), so
// the progress is anchored at the slot position. durable > slotLSN can happen
// after a slot recreation; replayed changes are idempotent, so the higher
// watermark is kept.
func ReconcileProgress(ctx context.Context, logger *slog.Logger, database, sid string, slotLSN pglogrepl.LSN) error {
	id := sourceID(database, sid)
	durable, err := LoadProgress(ctx, database, sid)
	if err != nil {
		return err
	}
	switch {
	case durable == 0:
		if err := initProgressRow(ctx, database, sid, slotLSN); err != nil {
			return err
		}
		durable = slotLSN
		logger.Info("Initialized durable progress at slot position", "source", id, "slot_lsn", slotLSN)
	case durable < slotLSN:
		return fmt.Errorf("durable progress %s is behind replication slot confirmed_flush_lsn %s for %s: "+
			"required WAL may have been discarded, manual intervention is required", durable, slotLSN, id)
	default:
		logger.Info("Resumed from durable progress", "source", id, "durable_lsn", durable, "slot_lsn", slotLSN)
	}
	replicationProgress.Set(id, durable)
	return nil
}

// VerifyProgress re-reads the durable progress after a worker commit and
// advances the in-memory watermark only to values that are provably durable.
// This is what makes a commit with an unknown result safe: until the row is
// observed in the progress table, the watermark does not move and the source
// retains the WAL for replay. Sources whose verification fails are returned so
// the worker can retry on subsequent commit cycles.
func VerifyProgress(ctx context.Context, sources []sourceRef, logger *slog.Logger) []sourceRef {
	failed := make([]sourceRef, 0)
	for _, s := range sources {
		durable, err := LoadProgress(ctx, s.database, s.sid)
		if err != nil {
			logger.Error("cannot verify durable progress, in-memory watermark not advanced",
				"source", s.id(), "error", err)
			progressErrors.Inc()
			failed = append(failed, s)
			continue
		}
		replicationProgress.Set(s.id(), durable)
		logger.Debug("verified durable progress", "source", s.id(), "durable_lsn", durable)
	}
	return failed
}
