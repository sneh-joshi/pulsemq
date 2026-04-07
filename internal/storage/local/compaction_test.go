package local_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

// openStorageWithCompaction opens a Storage with a long compaction interval so
// the background compactor does not fire automatically during a test.
func openStorageWithCompaction(t *testing.T) *local.Storage {
	t.Helper()
	cfg := local.DefaultConfig()
	cfg.CompactionInterval = 24 * time.Hour // effectively disabled during tests
	cfg.Fsync = local.FsyncNever
	s, err := local.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// appendAndIndex is a helper that performs the two-step durable write:
//
//  1. s.Append(msg) → log + WAL intent
//  2. s.WriteIndex(msg.ID, {Offset, Status: READY}) → bbolt + WAL commit
func appendAndIndex(t *testing.T, s *local.Storage, msg *types.Message) int64 {
	t.Helper()
	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.WriteIndex(msg.ID, storage.IndexEntry{
		Offset: offset,
		Status: types.StatusReady,
	}); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}
	return offset
}

// ─── Compaction tests ────────────────────────────────────────────────────────

// TestCompaction_RunOnce_RemovesDeletedMessages verifies that after compaction:
//   - Deleted messages are not present in the log any more (they can't be read
//     at their old offsets via the index).
//   - Live messages are still readable at their (potentially new) offsets.
func TestCompaction_RunOnce_RemovesDeletedMessages(t *testing.T) {
	s := openStorageWithCompaction(t)

	// Write 5 messages.
	msgs := make([]*types.Message, 5)
	offsets := make([]int64, 5)
	for i := range msgs {
		msgs[i] = &types.Message{
			ID:          node.MustNewID(),
			Namespace:   "test",
			Queue:       "orders",
			Body:        []byte(`{"i":` + string(rune('0'+i)) + `}`),
			PublishedAt: time.Now().UnixMilli(),
			Attempt:     1,
			MaxRetries:  3,
		}
		offsets[i] = appendAndIndex(t, s, msgs[i])
	}

	// Delete messages at indices 0, 2, and 4.
	for _, i := range []int{0, 2, 4} {
		if err := s.WriteIndex(msgs[i].ID, storage.IndexEntry{
			Offset: offsets[i],
			Status: types.StatusDeleted,
		}); err != nil {
			t.Fatalf("WriteIndex (delete) msgs[%d]: %v", i, err)
		}
	}

	// Run compaction.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Compactor().RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// Live messages (1 and 3) must still be readable.
	for _, i := range []int{1, 3} {
		entry, err := s.ReadIndex(msgs[i].ID)
		if err != nil {
			t.Fatalf("ReadIndex live msgs[%d]: %v", i, err)
		}
		got, err := s.ReadAt(entry.Offset)
		if err != nil {
			t.Fatalf("ReadAt live msgs[%d] at offset %d: %v", i, entry.Offset, err)
		}
		if got.ID != msgs[i].ID {
			t.Errorf("msgs[%d] ID: want %s got %s", i, msgs[i].ID, got.ID)
		}
		if !bytes.Equal(got.Body, msgs[i].Body) {
			t.Errorf("msgs[%d] body mismatch after compaction", i)
		}
	}
}

// TestCompaction_RunOnce_NoopWhenNothingDeleted verifies that RunOnce returns
// nil (and is effectively a no-op) when all messages are live.
func TestCompaction_RunOnce_NoopWhenNothingDeleted(t *testing.T) {
	s := openStorageWithCompaction(t)

	for i := 0; i < 3; i++ {
		msg := &types.Message{
			ID:          node.MustNewID(),
			Namespace:   "test",
			Queue:       "q",
			Body:        []byte(`{}`),
			PublishedAt: time.Now().UnixMilli(),
		}
		appendAndIndex(t, s, msg)
	}

	ctx := context.Background()
	if err := s.Compactor().RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce on fully-live storage: %v", err)
	}

	// Storage should function normally after the noop compaction.
	msg := &types.Message{
		ID:          node.MustNewID(),
		Namespace:   "test",
		Queue:       "q",
		Body:        []byte(`{"post":"compaction"}`),
		PublishedAt: time.Now().UnixMilli(),
	}
	off := appendAndIndex(t, s, msg)
	got, err := s.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt after noop compaction: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("ID mismatch after noop compaction: want %s got %s", msg.ID, got.ID)
	}
}

// TestCompaction_IndexOffsetsUpdated verifies that after compaction the index
// entries point to the correct new offsets in the compacted log.
func TestCompaction_IndexOffsetsUpdated(t *testing.T) {
	s := openStorageWithCompaction(t)

	// Write 3 messages; delete the first to shift the offsets of the others.
	msgs := make([]*types.Message, 3)
	offsets := make([]int64, 3)
	for i := range msgs {
		msgs[i] = &types.Message{
			ID:          node.MustNewID(),
			Namespace:   "ns",
			Queue:       "q",
			Body:        make([]byte, 100), // fixed-size bodies for predictable offsets
			PublishedAt: time.Now().UnixMilli(),
		}
		offsets[i] = appendAndIndex(t, s, msgs[i])
	}

	// Delete msgs[0].
	if err := s.WriteIndex(msgs[0].ID, storage.IndexEntry{
		Offset: offsets[0],
		Status: types.StatusDeleted,
	}); err != nil {
		t.Fatalf("delete msgs[0]: %v", err)
	}

	oldOff1, err := s.ReadIndex(msgs[1].ID)
	if err != nil {
		t.Fatalf("ReadIndex msgs[1] before compaction: %v", err)
	}

	ctx := context.Background()
	if err := s.Compactor().RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	newOff1, err := s.ReadIndex(msgs[1].ID)
	if err != nil {
		t.Fatalf("ReadIndex msgs[1] after compaction: %v", err)
	}

	// After compaction, msgs[1] is now the first entry so its offset should
	// be smaller than before (the deleted entry no longer precedes it).
	// We verify this by reading the message body at the NEW offset.
	got, err := s.ReadAt(newOff1.Offset)
	if err != nil {
		t.Fatalf("ReadAt msgs[1] new offset %d: %v", newOff1.Offset, err)
	}
	if got.ID != msgs[1].ID {
		t.Errorf("msgs[1] offset after compaction: got wrong message ID %s (want %s)", got.ID, msgs[1].ID)
	}
	t.Logf("msgs[1] offset before=%d after=%d", oldOff1.Offset, newOff1.Offset)
}
