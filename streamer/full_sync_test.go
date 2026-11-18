package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"golang.org/x/time/rate"
)

func TestMain(m *testing.M) {
	log = slog.New(slog.NewTextHandler(io.Discard, nil))
	os.Exit(m.Run())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSyncChannelDrainsBeforeStop guards the consistency boundary between the
// source COPY writer and the destination COPY reader: once the stop command is
// queued while rows are still buffered, every buffered row must be delivered
// before Read returns io.EOF. A random select between the two channels would
// silently lose up to the whole buffer worth of rows per full sync.
func TestSyncChannelDrainsBeforeStop(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		data := make(chan []byte, 16)
		cmd := make(chan string, 1)
		s := &syncChannel{SyncDataChannel: data, CommandChannel: cmd}
		const rows = 10
		for i := 0; i < rows; i++ {
			data <- []byte{byte(i)}
		}
		cmd <- "stop"

		got := 0
		for {
			p := make([]byte, 64)
			n, err := s.Read(p)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("iteration %d: unexpected read error=%v", iter, err)
			}
			if n != 1 {
				t.Fatalf("iteration %d: read %d bytes, want 1", iter, n)
			}
			got++
		}
		if got != rows {
			t.Fatalf("iteration %d: lost buffered rows, got=%d want=%d", iter, got, rows)
		}
		// Subsequent reads keep returning EOF.
		if _, err := s.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("iteration %d: expected persistent EOF, got err=%v", iter, err)
		}
	}
}

// TestSyncChannelWriteReadRoundTrip checks the writer side delivers every
// payload through the channel and rate limiting does not drop data.
func TestSyncChannelWriteReadRoundTrip(t *testing.T) {
	lim = rate.NewLimiter(rate.Inf, 1)
	data := make(chan []byte, 4)
	cmd := make(chan string, 1)
	s := &syncChannel{
		SyncDataChannel: data,
		CommandChannel:  cmd,
		done:            make(chan struct{}),
		rowsTotal:       syncRowsTotal.WithLabelValues("t", "s", "tbl"),
		bytesTotal:      syncBytesTotal.WithLabelValues("t", "s", "tbl"),
	}
	payloads := [][]byte{[]byte("row-1"), []byte("row-2"), []byte("row-3")}
	done := make(chan struct{})
	go func() {
		for _, p := range payloads {
			if n, err := s.Write(p); err != nil || n != len(p) {
				t.Errorf("write returned n=%d err=%v", n, err)
			}
		}
		close(done)
	}()
	for i, want := range payloads {
		p := make([]byte, len(want))
		n, err := s.Read(p)
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(p[:n]) != string(want) {
			t.Fatalf("read %d got=%q want=%q", i, p[:n], want)
		}
	}
	<-done
}

// TestSyncChannelWriteUnblocksOnDone ensures that when the destination writer
// dies with a full data channel, Write returns an error instead of blocking the
// source COPY goroutine forever.
func TestSyncChannelWriteUnblocksOnDone(t *testing.T) {
	lim = rate.NewLimiter(rate.Inf, 1)
	data := make(chan []byte, 1)
	cmd := make(chan string, 1)
	done := make(chan struct{})
	s := &syncChannel{
		SyncDataChannel: data,
		CommandChannel:  cmd,
		done:            done,
		rowsTotal:       syncRowsTotal.WithLabelValues("t", "s", "tbl2"),
		bytesTotal:      syncBytesTotal.WithLabelValues("t", "s", "tbl2"),
	}
	// Fill the single-slot buffer so the next Write has nowhere to send.
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.Write([]byte("blocked"))
		result <- err
	}()
	select {
	case <-result:
		t.Fatal("Write returned before done was closed")
	default:
	}
	close(done)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected error after done closed, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not unblock after done was closed")
	}
}

// TestSlotStateRoundTrip verifies the persisted phase/boundary survives a
// reload and that a missing state file reports errStateNotFound, which is what
// the recovery logic branches on.
func TestSlotStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	config.App.StateDirectory = dir
	l := testLogger()

	if _, err := loadSlotState(l, "db1", "sid1"); !errors.Is(err, errStateNotFound) {
		t.Fatalf("expected errStateNotFound, got=%v", err)
	}

	want := &SlotState{
		SlotName:     "kuvasz_db1_sid1",
		Phase:        PhaseSnapshot,
		SlotLSN:      pglogrepl.LSN(0x1574A08),
		SnapshotName: "00000003-000000B2-1",
	}
	if err := writeSlotState(l, "db1", "sid1", want); err != nil {
		t.Fatalf("writeSlotState failed: %v", err)
	}
	got, err := loadSlotState(l, "db1", "sid1")
	if err != nil {
		t.Fatalf("loadSlotState failed: %v", err)
	}
	if *got != *want {
		t.Fatalf("state mismatch got=%+v want=%+v", got, want)
	}

	// A different stream keeps an independent state file.
	if _, err := loadSlotState(l, "db1", "other"); !errors.Is(err, errStateNotFound) {
		t.Fatalf("expected errStateNotFound for other stream, got=%v", err)
	}

	if err := removeSlotState(l, "db1", "sid1"); err != nil {
		t.Fatalf("removeSlotState failed: %v", err)
	}
	if _, err := loadSlotState(l, "db1", "sid1"); !errors.Is(err, errStateNotFound) {
		t.Fatalf("expected errStateNotFound after removal, got=%v", err)
	}
	// Removing again is not an error.
	if err := removeSlotState(l, "db1", "sid1"); err != nil {
		t.Fatalf("idempotent remove failed: %v", err)
	}
}

// TestSlotStatePhaseTransition documents the legal phases used by recovery:
// snapshot while the initial full read may still be incomplete, streaming only
// after it committed.
func TestSlotStatePhaseTransition(t *testing.T) {
	if PhaseSnapshot == PhaseStreaming {
		t.Fatal("phase constants must differ")
	}
	st := SlotState{Phase: PhaseSnapshot}
	st.Phase = PhaseStreaming
	if st.Phase != PhaseStreaming {
		t.Fatal("phase transition not recorded")
	}
}
