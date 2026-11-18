package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	syncChannel struct {
		log             *slog.Logger
		SyncDataChannel chan []byte
		CommandChannel  chan string
		done            chan struct{}
		rowsTotal       prometheus.Counter
		bytesTotal      prometheus.Counter
		finished        bool
	}
)

var size int64

func (s *syncChannel) Read(p []byte) (int, error) {
	if s.finished {
		return 0, io.EOF
	}
	// Always drain buffered rows before honoring the stop command. A plain
	// select would pick randomly between a ready stop and ready data, ending
	// the destination COPY while rows are still buffered and silently losing
	// them (up to the whole channel buffer on every full sync).
	select {
	case row := <-s.SyncDataChannel:
		n := copy(p, row)
		return n, nil
	default:
	}
	select {
	case command := <-s.CommandChannel:
		log.Debug("received command", "command", command)
		s.finished = true
		return 0, io.EOF
	case row := <-s.SyncDataChannel:
		n := copy(p, row)
		return n, nil
	}
}

func (s *syncChannel) Write(p []byte) (int, error) {
	err := lim.Wait(context.Background())
	if err != nil {
		return 0, fmt.Errorf("cannot wait for token, error=%w", err)
	}
	row := slices.Clone(p)
	// If the destination writer died with the buffer full, the reader's
	// context cancellation cannot interrupt a blocking send. The done channel
	// unblocks Write so CopyTo returns instead of hanging the goroutine.
	select {
	case s.SyncDataChannel <- row:
	case <-s.done:
		return 0, io.ErrClosedPipe
	}
	size += int64(len(row))
	s.rowsTotal.Inc()
	s.bytesTotal.Add(float64(len(row)))
	return len(p), nil
}

func writeDestination(log *slog.Logger, tableName string, hasSID bool, columns string, s *syncChannel) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3600)
	defer cancel()
	conn, err := DestConnectionPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("cannot acquire connection to destination database, error=%w", err)
	}
	defer conn.Release()
	var tag pgconn.CommandTag
	if hasSID {
		tag, err = conn.Conn().PgConn().CopyFrom(ctx, s, fmt.Sprintf("COPY %s(sid, %s) FROM STDIN;", tableName, columns))
	} else {
		tag, err = conn.Conn().PgConn().CopyFrom(ctx, s, fmt.Sprintf("COPY %s(%s) FROM STDIN;", tableName, columns))
	}
	if err != nil {
		return fmt.Errorf("cannot COPY FROM, table=%s, error=%w", tableName, err)
	}
	log.Debug("COPY FROM", "tag", tag)
	return nil
}

func syncTable(log *slog.Logger,
	db string,
	sid string,
	sourceTableName string,
	destTableName string,
	tx pgx.Tx) error {
	log = log.With("sourceTable", sourceTableName, "destTable", destTableName)
	ctx, cancelReader := context.WithCancel(context.Background())
	defer cancelReader()
	hasSID := false

	log.Debug("Starting full sync")
	// Prepare channels between reader and writer.
	// SyncDataChannel is buffered so that the source COPY, which keeps calling
	// Write until the whole result set has been pumped, can drain fully even
	// though the destination COPY may not have consumed every row yet. The
	// reader relies on Read returning io.EOF to terminate CopyTo; it must never
	// block waiting for a stop signal that can only be sent after CopyTo
	// returns.
	syncDataChannel := make(chan []byte, 1024)
	commandChannel := make(chan string, 1)
	done := make(chan struct{})
	s := &syncChannel{
		log:             log,
		CommandChannel:  commandChannel,
		SyncDataChannel: syncDataChannel,
		done:            done,
		rowsTotal:       syncRowsTotal.WithLabelValues(db, sid, sourceTableName),
		bytesTotal:      syncBytesTotal.WithLabelValues(db, sid, sourceTableName),
	}

	// Find map entry for source table
	mapentry, err := MappingTable.FindByName(db, sourceTableName)
	if err != nil {
		log.Error("cannot match table", "database", db, "table", sourceTableName)
		return fmt.Errorf("cannot find table: %s", sourceTableName)
	}
	log.Debug("Found mapping entry", "map", MappingTable, "mapentry", mapentry)

	// Prepare column list
	columns := ""
	for c := range DestTables[destTableName].Columns {
		if strings.HasPrefix(c, "kvsz_") {
			continue
		}
		if c == "sid" {
			hasSID = true
			continue
		}
		if _, ok := mapentry.SourceColumns[c]; !ok {
			log.Debug("Target column not found in source table", "column", c, "mapentry.SourceColumns[c]", mapentry.SourceColumns[c])
			continue
		}
		if columns == "" {
			columns = c
		} else {
			columns = fmt.Sprintf("%s, %s", columns, c)
		}
	}
	log.Debug("Target columns", "columns", columns)

	// Start writer. Its error must be surfaced: a half-written table is not a
	// successful initial sync. If the writer dies, close(done) and the reader
	// cancel unblock a possibly full data channel instead of hanging.
	writerErr := make(chan error, 1)
	go func() {
		werr := writeDestination(log, destTableName, hasSID, columns, s)
		if werr != nil {
			cancelReader()
			close(done)
		}
		writerErr <- werr
	}()

	// Start reader on the snapshot transaction, so the table is read at the
	// replication slot's exported consistency point.
	var copyStatement string
	if hasSID {
		copyStatement = fmt.Sprintf("COPY (SELECT '%s', %s FROM %s) TO STDOUT;", sid, columns, sourceTableName)
	} else {
		copyStatement = fmt.Sprintf("COPY (SELECT %s FROM %s) TO STDOUT;", columns, sourceTableName)
	}
	t0 := time.Now()
	size = 0
	tag, readErr := tx.Conn().PgConn().CopyTo(ctx, s, copyStatement)
	// Signal the writer to finish (non-blocking: it may have exited already).
	select {
	case commandChannel <- "stop":
	default:
	}
	writerError := <-writerErr
	if readErr != nil {
		return fmt.Errorf("cannot perform full sync, error reading source=%s, dest=%s, error=%w", sourceTableName, destTableName, readErr)
	}
	if writerError != nil {
		return fmt.Errorf("cannot perform full sync, error writing dest=%s, error=%w", destTableName, writerError)
	}
	log.Info("Finished full sync",
		"tag", tag,
		"duration", time.Since(t0), "size",
		size, "throughput",
		(float64(size) / (time.Since(t0).Seconds()) / 1024 / 1024))
	return nil
}

// openSnapshotTransaction opens a read-only transaction on a separate (non
// replication) connection and imports the snapshot exported by the logical
// replication slot creation. SET TRANSACTION SNAPSHOT cannot run on a walsender
// connection, hence the dedicated connection. Every table read through the
// returned tx sees the database at the slot's single consistency point.
func openSnapshotTransaction(ctx context.Context, sourceURL, snapshotName string) (pgx.Tx, func(), error) {
	parsedConfig, err := pgx.ParseConfig(sourceURL)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse source url for snapshot read, error=%w", err)
	}
	parsedConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, parsedConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot connect for snapshot read, error=%w", err)
	}
	cleanup := func() {
		_ = conn.Close(ctx)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("cannot begin snapshot transaction, error=%w", err)
	}
	if _, err = tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+snapshotName+"'"); err != nil {
		_ = tx.Rollback(ctx)
		cleanup()
		return nil, nil, fmt.Errorf("cannot import exported snapshot=%s, error=%w", snapshotName, err)
	}
	return tx, cleanup, nil
}

// syncAllTables performs the initial full read of every configured table inside
// one read-only transaction importing the slot's exported snapshot. Any failure
// aborts the transaction (nothing is considered initialized) and the error is
// returned to the caller, which keeps the stream in PhaseSnapshot.
func syncAllTables(
	log *slog.Logger,
	db string,
	sid string,
	sourceTables SourceTables,
	sourceURL string,
	snapshotName string) error {
	log.Info("Starting full sync for all tables", "sourceTables", sourceTables, "snapshot", snapshotName)
	ctx := context.Background()
	tx, cleanup, err := openSnapshotTransaction(ctx, sourceURL, snapshotName)
	if err != nil {
		return err
	}
	defer cleanup()

	for sourceTableName := range sourceTables {
		destTableName, err := sourceTables.GetTable(sourceTableName)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		log.Info("Syncing", "sourceTable", sourceTableName, "destTable", destTableName)
		if err = syncTable(log, db, sid, sourceTableName, destTableName, tx); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("cannot commit snapshot transaction, error=%w", err)
	}
	return nil
}

// syncNewTables reads tables added to an already streaming publication. The
// slot's initial snapshot boundary belongs to the past and cannot be re-imported
// here, so each call gets its own repeatable-read transaction: every individual
// table is read at one consistent point, but different tables do not share a
// single cross-table snapshot. This covers the supported single-instance, plain
// table case; cross-database snapshots are out of scope.
func syncNewTables(
	log *slog.Logger,
	db string,
	sid string,
	sourceTables SourceTables,
	newTables []string,
	sourceURL string) error {
	log.Info("Starting full sync for new tables", "sourceTables", sourceTables)
	ctx := context.Background()
	for i := range newTables {
		destTableName, err := sourceTables.GetTable(newTables[i])
		if err != nil {
			return err
		}
		log.Info("Syncing", "sourceTable", newTables[i], "destTable", destTableName)
		parsedConfig, err := pgx.ParseConfig(sourceURL)
		if err != nil {
			return fmt.Errorf("cannot parse source url for new table read, error=%w", err)
		}
		parsedConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		conn, err := pgx.ConnectConfig(ctx, parsedConfig)
		if err != nil {
			return fmt.Errorf("cannot connect for new table read, error=%w", err)
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		})
		if err != nil {
			_ = conn.Close(ctx)
			return fmt.Errorf("cannot begin transaction for new table=%s, error=%w", newTables[i], err)
		}
		if err = syncTable(log, db, sid, newTables[i], destTableName, tx); err != nil {
			_ = tx.Rollback(ctx)
			_ = conn.Close(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			_ = conn.Close(ctx)
			return fmt.Errorf("cannot commit new table transaction, error=%w", err)
		}
		if err = conn.Close(ctx); err != nil {
			return fmt.Errorf("cannot close new table connection, error=%w", err)
		}
	}
	return nil
}

// truncateDestinationTables removes rows previously copied for this stream when
// an unfinished snapshot is redone. Tables carrying a sid column only lose the
// rows of this sid, other streams are untouched; tables without sid are fully
// truncated as they are owned by a single source stream. A destination table
// that does not exist yet (configuration being fixed) is skipped: there is
// nothing to clean, and blocking recovery on it would prevent the very redo
// that can succeed once the table is created.
func truncateDestinationTables(log *slog.Logger, db string, sid string, sourceTables SourceTables) error {
	ctx := context.Background()
	conn, err := DestConnectionPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("cannot acquire destination connection for cleanup, error=%w", err)
	}
	defer conn.Release()
	for sourceTableName := range sourceTables {
		destTableName, err := sourceTables.GetTable(sourceTableName)
		if err != nil {
			// Most often "destination table does not exist": there is nothing
			// to clean yet. Skip instead of blocking recovery; the redo itself
			// will surface the real configuration error.
			log.Warn("Skipping cleanup, destination table not resolvable",
				"sourceTable", sourceTableName, "error", err)
			continue
		}
		if _, ok := DestTables[destTableName].Columns["sid"]; ok {
			log.Info("Deleting rows of unfinished snapshot", "table", destTableName, "sid", sid)
			if _, err = conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE sid=$1", destTableName), sid); err != nil {
				return fmt.Errorf("cannot clean destination table=%s, error=%w", destTableName, err)
			}
		} else {
			log.Info("Truncating table of unfinished snapshot", "table", destTableName)
			if _, err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", destTableName)); err != nil {
				return fmt.Errorf("cannot truncate destination table=%s, error=%w", destTableName, err)
			}
		}
	}
	return nil
}
