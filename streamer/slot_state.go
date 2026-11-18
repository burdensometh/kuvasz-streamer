package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jackc/pglogrepl"
)

var errStateNotFound = errors.New("slot state file not found")

// Phases of a replication stream. The phase is persisted on disk so that a
// restart can tell an unfinished initial snapshot from a stream that has
// already entered logical (WAL based) replication.
const (
	// PhaseSnapshot: the replication slot exists and its exported snapshot is
	// being (or must be) used for the initial full read. Incremental replay has
	// not started.
	PhaseSnapshot = "snapshot"
	// PhaseStreaming: the initial snapshot was fully read and committed, WAL
	// replay is the source of truth from SlotLSN onwards.
	PhaseStreaming = "streaming"
)

// SlotState is the on-disk state tying a replication slot to its consistency
// boundary. A full read and the subsequent WAL stream must share the exact
// same boundary, which is the LSN and exported snapshot returned by
// CREATE_REPLICATION_SLOT. It is never derived from an IdentifySystem()
// position queried before the slot was created.
type SlotState struct {
	SlotName     string        `json:"slot_name"`
	Phase        string        `json:"phase"`
	SlotLSN      pglogrepl.LSN `json:"slot_lsn"`
	SnapshotName string        `json:"snapshot_name"`
}

// stateFileName builds the deterministic state file name for a given stream.
// Only a single source instance/database + sid combination is supported per
// stream, there is no cross-database snapshot involved.
func stateFileName(db, sid string) string {
	return fmt.Sprintf("slot-state-%s-%s.json", db, sid)
}

func stateFilePath(db, sid string) string {
	return filepath.Join(config.App.StateDirectory, stateFileName(db, sid))
}

// loadSlotState reads the persisted state. errStateNotFound is returned when no
// state exists, meaning this stream never recorded its slot locally.
func loadSlotState(log *slog.Logger, db, sid string) (*SlotState, error) {
	path := stateFilePath(db, sid)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errStateNotFound
		}
		return nil, fmt.Errorf("cannot read slot state file=%s, error=%w", path, err)
	}
	state := SlotState{}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("cannot parse slot state file=%s, error=%w", path, err)
	}
	log.Debug("Loaded slot state", "path", path, "state", state)
	return &state, nil
}

// writeSlotState atomically writes the state (write to a temp file then rename)
// so a crash can never leave a half-written state file behind.
func writeSlotState(log *slog.Logger, db, sid string, state *SlotState) error {
	if err := os.MkdirAll(config.App.StateDirectory, 0o750); err != nil {
		return fmt.Errorf("cannot create state directory=%s, error=%w", config.App.StateDirectory, err)
	}
	finalPath := stateFilePath(db, sid)
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal slot state, error=%w", err)
	}
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("cannot write slot state file=%s, error=%w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("cannot rename slot state file=%s, error=%w", finalPath, err)
	}
	log.Debug("Saved slot state", "path", finalPath, "state", state)
	return nil
}

func removeSlotState(log *slog.Logger, db, sid string) error {
	path := stateFilePath(db, sid)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot remove slot state file=%s, error=%w", path, err)
	}
	log.Debug("Removed slot state", "path", path)
	return nil
}
