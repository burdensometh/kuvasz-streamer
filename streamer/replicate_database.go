package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type Partitions map[string][]string

func pluginArguments(pgVersion int, slotName string) (int, []string) {
	protocolVersion := 2
	arg := make([]string, 0, 10)
	switch pgVersion {
	case 12, 13:
		arg = append(arg, "proto_version '1'")
		protocolVersion = 1
	default:
		arg = append(arg, "proto_version '2'")
	}
	arg = append(arg, fmt.Sprintf("publication_names '%s'", slotName))
	if pgVersion >= 14 {
		arg = append(arg, "binary 'false'")
		arg = append(arg, "messages 'true'")
		arg = append(arg, "streaming 'true'")
	}
	if pgVersion >= 16 {
		arg = append(arg, "origin 'none'")
	}
	log.Debug("calculate args", "arg", arg)
	return protocolVersion, arg
}

// slotCreation is the consistency boundary returned by CREATE_REPLICATION_SLOT.
// Both the initial full read (via the exported snapshot) and the subsequent WAL
// stream (started at SlotLSN) must come from this single boundary.
type slotCreation struct {
	slotName     string
	slotLSN      pglogrepl.LSN
	snapshotName string
}

// createReplicationSlot creates the logical replication slot when it does not
// exist and exports the snapshot that the initial full read must use. When the
// slot already exists its confirmed_flush_lsn is returned instead.
//
// The starting LSN is always taken from the slot itself (the create command's
// consistent point, or confirmed_flush_lsn for an existing slot); it is never
// the IdentifySystem() XLogPos queried before creating the slot, which does not
// share a boundary with any snapshot.
func createReplicationSlot(
	log *slog.Logger,
	conn *pgx.Conn,
	slotName string,
) (created bool, result slotCreation, err error) {
	ctx := context.Background()
	replConn := conn.PgConn()
	log.Info("Checking replication slot", "slotname", slotName)
	var active bool
	var activePid *int
	var existingLSN pglogrepl.LSN
	err = conn.QueryRow(
		ctx,
		"select active, active_pid, confirmed_flush_lsn from pg_replication_slots where slot_name=$1;",
		slotName).Scan(&active, &activePid, &existingLSN)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, slotCreation{}, fmt.Errorf("cannot check existence of replication slot: %w", err)
	}
	if active {
		return false, slotCreation{}, fmt.Errorf("cannot create replication, slot is active, slot_name=%s, active_pid=%d", slotName, *activePid)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Info("Replication slot exists", "lsn", existingLSN)
		return false, slotCreation{slotName: slotName, slotLSN: existingLSN}, nil
	}

	// Export a snapshot so the full read can be taken on the exact consistency
	// point of the slot. The snapshot stays valid as long as the slot exists.
	replicationSlotResult, err := pglogrepl.CreateReplicationSlot(
		ctx,
		replConn,
		slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      false,
			SnapshotAction: "EXPORT_SNAPSHOT",
			Mode:           pglogrepl.LogicalReplication,
		})
	if err != nil {
		return false, slotCreation{}, fmt.Errorf("cannot create replication slot: %w", err)
	}
	slotLSN, err := pglogrepl.ParseLSN(replicationSlotResult.ConsistentPoint)
	if err != nil {
		return false, slotCreation{}, fmt.Errorf("cannot parse slot consistent point=%s, error=%w", replicationSlotResult.ConsistentPoint, err)
	}
	if replicationSlotResult.SnapshotName == "" {
		return false, slotCreation{}, fmt.Errorf("replication slot %s created without an exported snapshot", slotName)
	}
	log.Info("Created replication slot",
		"slotname", replicationSlotResult.SlotName,
		"lsn", slotLSN,
		"snapshot", replicationSlotResult.SnapshotName)
	return true, slotCreation{
		slotName:     replicationSlotResult.SlotName,
		slotLSN:      slotLSN,
		snapshotName: replicationSlotResult.SnapshotName,
	}, nil
}

// dropReplicationSlot removes a slot whose initial snapshot never completed.
// Nothing has been replayed from it yet, so recreating it from scratch loses no
// committed work. A slot that is already gone is treated as success, because
// the desired end state (no slot) is reached.
func dropReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	if _, err := conn.Exec(ctx, "select pg_drop_replication_slot($1)", slotName); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" { // undefined_object
			return nil
		}
		return fmt.Errorf("cannot drop replication slot=%s, error=%w", slotName, err)
	}
	return nil
}

// pgVersion gets postgres server version and checks it is supported.
func pgVersion(log *slog.Logger, conn *pgx.Conn) (int, error) {
	log.Info("Getting server version")
	var versionString string
	err := conn.QueryRow(context.Background(), "show server_version_num;").Scan(&versionString)
	if err != nil {
		return 0, fmt.Errorf("cannot get postgres server version, error=%w", err)
	}
	version := (int(versionString[0])-48)*10 + int(versionString[1]) - 48

	log.Info("Postgres server version", "version", version)
	if version < 12 {
		return version, fmt.Errorf("unsupported postgres version=%d, minversion=12", version)
	}
	if version > 17 {
		log.Warn("New postgres version. may not be supported", "version", version, "maxversion", 17)
	}
	return version, nil
}

//nolint:funlen,gocognit,cyclop,gocyclo // This is is just multiple steps and needs to be in a single function
func ReplicateDatabase(rootContext context.Context, database SourceDatabase, url *SourceURL) error {
	// Create command channel
	url.commandChannel = make(chan string)
	syncContext, syncCancel := context.WithCancel(rootContext)
	defer syncCancel()
	go func() {
		select {
		case <-url.commandChannel:
			syncCancel()
		case <-syncContext.Done():
		case <-rootContext.Done():
		}
	}()

	// Connect to selected source database
	log := log.With("db-sid", database.Name+"-"+url.SID)
	databaseURL := strings.Split(url.URL, "?")[0] + "?replication=database&application_name=kuvasz_" + database.Name
	parsedConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("cannot parse url=%s, error=%w", databaseURL, err)
	}
	parsedConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	// dbName := parsedConfig.Database
	log.Info("Connecting", "databaseURL", databaseURL)
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, parsedConfig)
	if err != nil {
		return fmt.Errorf("cannot start replication connection, error=%w", err)
	}
	defer conn.Close(ctx)
	replConn := conn.PgConn()

	// Validate server version
	log.Info("Validating server version")
	ver, err := pgVersion(log, conn)
	if err != nil {
		return fmt.Errorf("cannot get postgres server version, error=%w", err)
	}

	// Get server informatioon
	log.Info("Identifying system")
	sysident, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		return fmt.Errorf("cannot identify system, error=%w", err)
	}
	log.Info("System identification",
		"SystemID", sysident.SystemID,
		"Timeline", sysident.Timeline,
		"XLogPos", sysident.XLogPos,
		"DBName", sysident.DBName)

	// Check for correct wal_level
	log.Info("Checking wal_level is logical")
	var walLevel string
	err = conn.QueryRow(ctx, "show wal_level").Scan(&walLevel)
	if err != nil {
		return fmt.Errorf("cannot get wal_level, error=%w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("wal_level must be logical, got %s, change configuration in postgresql.conf and restart server", walLevel)
	}

	// Check existing publication and create if needed, drop replication slot if required
	slotName := "kuvasz_" + database.Name + "_" + url.SID
	slotName = strings.ReplaceAll(slotName, "-", "_")
	var publication, slot int
	err = conn.QueryRow(context.Background(), `with publication as (
							select count(*) as publication
							from pg_publication
							where pubname=$1),
						slot as (select count(*) as slot
							from pg_replication_slots
							where slot_name=$1)
						select publication.publication, slot.slot from publication,slot;`, slotName).Scan(&publication, &slot)
	if err != nil {
		return fmt.Errorf("cannot check publication and slot, error=%w", err)
	}
	// Check existing replication slot and existing consumer
	// Publication=0, Slot=0 => Fresh config, create publication
	// Publication=0, Slot=1 => Anomaly, slot created without publication, drop it and create publication
	// Publication=1, Slot=0 => Anomaly, Publication created without slot, drop it and re-create it
	// Publication=1, Slot=1 => Normal case, tables may have been added or removed, sync publication
	var newTables []string
	//nolint:nestif // this cannot be really simplified
	if publication == 1 && slot == 1 {
		newTables, err = SyncPublications(log, conn, database, url.SID)
		if err != nil {
			return fmt.Errorf("cannot sync publications: %w", err)
		}
	} else {
		if publication == 0 && slot == 1 { // slot without publication, drop it
			if err = dropReplicationSlot(context.Background(), conn, slotName); err != nil {
				return err
			}
			// any local state belonging to the dropped slot is stale
			_ = removeSlotState(log, database.Name, url.SID)
		}
		if publication == 1 && slot == 0 { // publication may have been created by mistake, remove it
			_, err = conn.Exec(context.Background(), "drop publication "+slotName)
			if err != nil {
				return fmt.Errorf("cannot drop publication, error=%w", err)
			}
		}
		q := "create publication " + slotName + makePublication(database)
		log.Debug("Creating publication", "publication", slotName, "q", q)
		_, err = conn.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("cannot create publication, error=%w", err)
		}
	}

	// Recover persisted state. It distinguishes a stream whose initial snapshot
	// never finished from one that already entered incremental WAL replay.
	state, stateErr := loadSlotState(log, database.Name, url.SID)
	if stateErr != nil && !errors.Is(stateErr, errStateNotFound) {
		return fmt.Errorf("cannot load replication state, error=%w", stateErr)
	}
	if stateErr == nil && state.Phase == PhaseSnapshot {
		// The previous attempt died (or was restarted) while the initial full
		// read was in progress. The half-copied data cannot be treated as a
		// completed initialisation: drop the slot and redo the whole snapshot
		// against a freshly exported boundary.
		log.Warn("Found unfinished initial snapshot, dropping slot and redoing full sync",
			"slotname", state.SlotName, "lsn", state.SlotLSN)
		if err = dropReplicationSlot(context.Background(), conn, state.SlotName); err != nil {
			return err
		}
		if err = truncateDestinationTables(log, database.Name, url.SID, database.Tables); err != nil {
			return fmt.Errorf("cannot clean destination before snapshot redo, error=%w", err)
		}
		if err = removeSlotState(log, database.Name, url.SID); err != nil {
			return err
		}
		state = nil
	} else if stateErr == nil && state.Phase == PhaseStreaming {
		// State claims streaming already started. If the slot vanished on the
		// server (e.g. manual drop), the destination must be rebuilt from
		// scratch rather than resumed from a guessed position.
		var slotExists bool
		if err = conn.QueryRow(ctx,
			"select exists(select 1 from pg_replication_slots where slot_name=$1)",
			state.SlotName).Scan(&slotExists); err != nil {
			return fmt.Errorf("cannot check replication slot existence, error=%w", err)
		}
		if !slotExists {
			log.Warn("Streaming state exists but slot is missing on server, rebuilding from scratch",
				"slotname", state.SlotName)
			if err = truncateDestinationTables(log, database.Name, url.SID, database.Tables); err != nil {
				return fmt.Errorf("cannot clean destination before rebuild, error=%w", err)
			}
			if err = removeSlotState(log, database.Name, url.SID); err != nil {
				return err
			}
			state = nil
		}
	}

	// Create slot if it does not exist; an existing slot means we already
	// entered incremental replay and must resume from its confirmed_flush_lsn.
	created, slotInfo, err := createReplicationSlot(log, conn, slotName)
	if err != nil {
		return fmt.Errorf("cannot create replication slot, error=%w", err)
	}

	if created {
		// Persist the snapshot boundary BEFORE reading any data: a crash from
		// this point on is recovered as "unfinished snapshot", never as a
		// completed initialisation.
		state = &SlotState{
			SlotName:     slotInfo.slotName,
			Phase:        PhaseSnapshot,
			SlotLSN:      slotInfo.slotLSN,
			SnapshotName: slotInfo.snapshotName,
		}
		if err = writeSlotState(log, database.Name, url.SID, state); err != nil {
			return err
		}

		// Full read of every configured table in a separate read transaction
		// that imports the snapshot exported by the slot creation. All tables
		// are therefore seen at the single slot consistency point.
		err = syncAllTables(log, database.Name, url.SID, database.Tables, url.URL, slotInfo.snapshotName)
		if err != nil {
			return fmt.Errorf("cannot perform initial sync, error=%w", err)
		}
		log.Info("Finished full table sync", "snapshot", slotInfo.snapshotName)

		// Snapshot fully read and committed: only now may we enter streaming.
		state.Phase = PhaseStreaming
		state.SnapshotName = ""
		if err = writeSlotState(log, database.Name, url.SID, state); err != nil {
			return err
		}
		time.Sleep(time.Duration(config.Maintenance.StartDelay) * time.Second)
	} else {
		// Existing slot: the initial snapshot was completed in a previous run
		// (or the slot was created outside this state file). Newly added tables
		// are synced on their own read transaction; see the limitation note in
		// syncNewTables.
		if state == nil {
			log.Warn("Replication slot exists without local state, assuming initial snapshot was completed",
				"slotname", slotInfo.slotName, "lsn", slotInfo.slotLSN)
			state = &SlotState{
				SlotName: slotInfo.slotName,
				Phase:    PhaseStreaming,
				SlotLSN:  slotInfo.slotLSN,
			}
			if err = writeSlotState(log, database.Name, url.SID, state); err != nil {
				return err
			}
		}
		if state.Phase != PhaseStreaming {
			return fmt.Errorf("unexpected replication state phase=%s for existing slot=%s", state.Phase, slotName)
		}
		// Resume from the slot's confirmed_flush_lsn (slotInfo.slotLSN), which
		// is authoritative on the server and always shares a boundary with the
		// WAL stream; never fall back to a position guessed before slot
		// creation. The persisted LSN is kept as a sanity record.
		log.Info("Resuming streaming from existing slot",
			"slotname", slotInfo.slotName, "confirmed_flush_lsn", slotInfo.slotLSN,
			"persisted_lsn", state.SlotLSN)
		err = syncNewTables(log, database.Name, url.SID, database.Tables, newTables, url.URL)
		if err != nil {
			return fmt.Errorf("cannot perform initial sync for new tables, error=%w", err)
		}
		log.Debug("Finished full table sync for new tables")
		time.Sleep(time.Duration(config.Maintenance.StartDelay) * time.Second)
	}
	lsn := slotInfo.slotLSN

	// Start replication
	log.Debug("Starting replication slot")
	protocolVersion, args := pluginArguments(ver, slotName)
	err = pglogrepl.StartReplication(
		ctx,
		replConn,
		slotName,
		lsn+1,
		pglogrepl.StartReplicationOptions{
			Timeline:   0,
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: args})
	if err != nil {
		return fmt.Errorf("cannot start replication, error=%w", err)
	}
	log.Info("Started logical replication slot", "slotname", slotName, "lsn", lsn)

	// Start streaming and processing messages
	standbyMessageTimeout := time.Second * 1
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := PGRelations{}
	typeMap := pgtype.NewMap()

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false
	transactionLSN := pglogrepl.LSN(0)
	committedTransactionLSN := lsn
	SetCommittedLSN(database.Name, url.SID, lsn)

	for {
		urlHeartbeat.WithLabelValues(database.Name, url.SID).Set(float64(time.Now().Unix()))

		if time.Now().After(nextStandbyMessageDeadline) {
			clientXLogPos := GetCommittedLSN(database.Name, url.SID, committedTransactionLSN)
			if clientXLogPos != 0 {
				err = pglogrepl.SendStandbyStatusUpdate(
					ctx,
					replConn,
					pglogrepl.StandbyStatusUpdate{
						WALWritePosition: clientXLogPos,
						WALFlushPosition: clientXLogPos,
						WALApplyPosition: clientXLogPos,
						ClientTime:       time.Now(),
						ReplyRequested:   false,
					})
				if err != nil {
					return fmt.Errorf("cannot send SendStandbyStatusUpdate, error=%w", err)
				}
				log.Debug("Sent Standby status message", "pos", clientXLogPos.String())
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		timerCtx, cancel := context.WithDeadline(syncContext, nextStandbyMessageDeadline)
		rawMsg, err := replConn.ReceiveMessage(timerCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if errors.Is(err, context.Canceled) {
				log.Info("Got restart message, restarting replication")
				return nil
			}
			return fmt.Errorf("cannot ReceiveMessage, error=%w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error=%v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Info("Received unexpected message", "message", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Error("ParsePrimaryKeepaliveMessage failed", "error", err)
			}
			log.Debug("PrimaryKeepalive",
				"ServerWALEnd", pkm.ServerWALEnd,
				"ServerTime", pkm.ServerTime,
				"ReplyRequested", pkm.ReplyRequested)
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("cannot ParseXLogData, error=%w", err)
			}

			log.Debug("XLogData", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime", xld.ServerTime)
			processMessage(log, database, *url, protocolVersion, xld, relations, typeMap, &transactionLSN, &committedTransactionLSN, &inStream)
		}
	}
}

func DoReplicateDatabase(rootContext context.Context, database SourceDatabase, url *SourceURL) {
	defer wg.Done()
	for {
		err := ReplicateDatabase(rootContext, database, url)
		if err == nil {
			log.Error("Interrupted", "db-sid", database.Name+"-"+url.SID, "url", url.URL)
			return
		}

		log.Error("cannot start replication", "error", err, "db-sid", database.Name+"-"+url.SID, "url", url.URL)
		URLError[url.URL] = err.Error()
		time.Sleep(60 * time.Second)
	}
}
