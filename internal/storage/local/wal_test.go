package local_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func openWAL(t *testing.T) *local.WAL {
	t.Helper()
	dir := t.TempDir()
	w, err := local.OpenWAL(filepath.Join(dir, "wal.dat"))
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func makeMsg(t *testing.T) *types.Message {
	t.Helper()
	return &types.Message{
		ID:          node.MustNewID(),
		Namespace:   "test",
		Queue:       "default",
		Body:        []byte(`{"k":"v"}`),
		PublishedAt: time.Now().UnixMilli(),
		Attempt:     1,
		MaxRetries:  3,
	}
}

// ─── Tests ───────────────────────────────────────────────────────────────────

// TestWAL_WriteAndCommit verifies that a written-then-committed entry does NOT
// appear in Replay output.
func TestWAL_WriteAndCommit(t *testing.T) {
	w := openWAL(t)
	msg := makeMsg(t)

	seq, err := w.Write(msg)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if seq == 0 {
		t.Fatal("expected non-zero seq")
	}

	if err := w.Commit(seq); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 uncommitted entries after commit, got %d", len(entries))
	}
}

// TestWAL_ReplayReturnsUncommitted verifies that a written-but-not-committed
// entry is returned by Replay (simulates crash after WAL write).
func TestWAL_ReplayReturnsUncommitted(t *testing.T) {
	w := openWAL(t)
	msg := makeMsg(t)

	_, err := w.Write(msg)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Deliberately do NOT call Commit.

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 uncommitted entry, got %d", len(entries))
	}
	if entries[0].MsgID != msg.ID {
		t.Fatalf("expected MsgID %s, got %s", msg.ID, entries[0].MsgID)
	}
	if entries[0].Msg == nil {
		t.Fatal("expected non-nil Msg in WALEntry")
	}
	if string(entries[0].Msg.Body) != string(msg.Body) {
		t.Fatalf("body mismatch: want %s got %s", msg.Body, entries[0].Msg.Body)
	}
}

// TestWAL_MultipleWritesPartialCommit verifies that only uncommitted entries
// are returned when some writes were committed and others were not.
func TestWAL_MultipleWritesPartialCommit(t *testing.T) {
	w := openWAL(t)

	msgs := make([]*types.Message, 5)
	seqs := make([]uint64, 5)
	for i := range msgs {
		msgs[i] = makeMsg(t)
		seq, err := w.Write(msgs[i])
		if err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
		seqs[i] = seq
	}

	// Commit only even-indexed messages.
	for i := 0; i < 5; i += 2 {
		if err := w.Commit(seqs[i]); err != nil {
			t.Fatalf("Commit[%d]: %v", i, err)
		}
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(entries) != 2 { // indices 1 and 3
		t.Fatalf("expected 2 uncommitted entries, got %d", len(entries))
	}

	// Verify the uncommitted IDs are the odd-indexed messages.
	uncommittedIDs := map[string]struct{}{}
	for _, e := range entries {
		uncommittedIDs[e.MsgID] = struct{}{}
	}
	for i := 1; i < 5; i += 2 {
		if _, ok := uncommittedIDs[msgs[i].ID]; !ok {
			t.Errorf("expected msgs[%d] (ID %s) in uncommitted set", i, msgs[i].ID)
		}
	}
}

// TestWAL_TruncateClearsEntries verifies that Truncate removes all entries
// and subsequent Replay returns nothing.
func TestWAL_TruncateClearsEntries(t *testing.T) {
	w := openWAL(t)
	msg := makeMsg(t)

	if _, err := w.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := w.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay after Truncate: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after Truncate, got %d", len(entries))
	}
}

// TestWAL_PersistAcrossReopen verifies that uncommitted entries survive a
// close + reopen cycle (the primary crash-safety guarantee).
func TestWAL_PersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.dat")

	msg := makeMsg(t)

	// Write without commit, then close.
	{
		w, err := local.OpenWAL(path)
		if err != nil {
			t.Fatalf("OpenWAL first open: %v", err)
		}
		if _, err := w.Write(msg); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen and replay — should see the uncommitted entry.
	{
		w, err := local.OpenWAL(path)
		if err != nil {
			t.Fatalf("OpenWAL second open: %v", err)
		}
		defer w.Close()

		entries, err := w.Replay()
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry across reopen, got %d", len(entries))
		}
		if entries[0].MsgID != msg.ID {
			t.Fatalf("ID mismatch: want %s got %s", msg.ID, entries[0].MsgID)
		}
	}
}

// TestWAL_InvalidMagicHeader verifies that OpenWAL rejects a file that doesn't
// start with the expected magic bytes.
func TestWAL_InvalidMagicHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.dat")

	// Write garbage magic.
	if err := os.WriteFile(path, []byte("JUNK_NOT_WAL"), 0o640); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	_, err := local.OpenWAL(path)
	if err == nil {
		t.Fatal("expected error for invalid magic header, got nil")
	}
}

// TestWAL_SeqIncrements verifies that each Write returns a strictly increasing
// sequence number.
func TestWAL_SeqIncrements(t *testing.T) {
	w := openWAL(t)
	var prevSeq uint64
	for i := 0; i < 10; i++ {
		seq, err := w.Write(makeMsg(t))
		if err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
		if seq <= prevSeq {
			t.Fatalf("seq not increasing: got %d after %d", seq, prevSeq)
		}
		prevSeq = seq
	}
}
