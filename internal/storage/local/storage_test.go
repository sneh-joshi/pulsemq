package local_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

// ---- helpers ----------------------------------------------------------------

func newTestMsg(t *testing.T, ns, q string, body []byte) *types.Message {
	t.Helper()
	return &types.Message{
		ID:          node.MustNewID(),
		Namespace:   ns,
		Queue:       q,
		Body:        body,
		DeliverAt:   0,
		PublishedAt: time.Now().UnixMilli(),
		Attempt:     1,
		MaxRetries:  3,
		NodeID:      node.MustNewID(),
	}
}

func openStorage(t *testing.T) *local.Storage {
	t.Helper()
	s, err := local.Open(t.TempDir())
	if err != nil {
		t.Fatalf("local.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// ---- Log tests --------------------------------------------------------------

func TestLog_AppendAndReadAt(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "payments", "orders", []byte(`{"orderId":1}`))

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	assertMessagesEqual(t, msg, got)
}

func TestLog_MultipleAppends_CorrectOffsets(t *testing.T) {
	s := openStorage(t)

	msgs := make([]*types.Message, 5)
	offsets := make([]int64, 5)
	for i := range msgs {
		msgs[i] = newTestMsg(t, "ns", "q", []byte(fmt.Sprintf(`{"i":%d}`, i)))
		var err error
		offsets[i], err = s.Append(msgs[i])
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	// Each offset must be unique and messages must round-trip correctly.
	seen := make(map[int64]bool)
	for i, off := range offsets {
		if seen[off] {
			t.Errorf("duplicate offset %d at index %d", off, i)
		}
		seen[off] = true

		got, err := s.ReadAt(off)
		if err != nil {
			t.Fatalf("ReadAt[%d] offset=%d: %v", i, off, err)
		}
		assertMessagesEqual(t, msgs[i], got)
	}
}

func TestLog_WithMetadata(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "ns", "q", []byte("hello"))
	msg.Metadata = map[string]string{
		"trace-id":     "abc123",
		"content-type": "application/json",
	}

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if got.Metadata["trace-id"] != "abc123" {
		t.Errorf("metadata trace-id: got %q want %q", got.Metadata["trace-id"], "abc123")
	}
	if got.Metadata["content-type"] != "application/json" {
		t.Errorf("metadata content-type: got %q want %q", got.Metadata["content-type"], "application/json")
	}
}

func TestLog_WithScheduledDeliverAt(t *testing.T) {
	s := openStorage(t)
	deliverAt := time.Now().Add(2 * time.Hour).UnixMilli()
	msg := newTestMsg(t, "jobs", "emails", []byte("send email"))
	msg.DeliverAt = deliverAt

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if got.DeliverAt != deliverAt {
		t.Errorf("DeliverAt: got %d want %d", got.DeliverAt, deliverAt)
	}
}

func TestLog_WithWALFields(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "ns", "q", []byte("body"))
	msg.Term = 7
	// LogIndex is set by the log itself on Append, but Term is preserved.

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadAt(offset)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if got.Term != 7 {
		t.Errorf("Term: got %d want 7", got.Term)
	}
	// LogIndex must be >= 1 (set by the log layer).
	if got.LogIndex < 1 {
		t.Errorf("LogIndex should be >= 1, got %d", got.LogIndex)
	}
}

func TestLog_ReadAt_InvalidOffset_ReturnsError(t *testing.T) {
	s := openStorage(t)
	// Offset 999999 does not exist in an empty log.
	_, err := s.ReadAt(999999)
	if err == nil {
		t.Fatal("expected error for out-of-range offset")
	}
}

func TestLog_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	msg := newTestMsg(t, "payments", "orders", []byte("persistent"))

	var offset int64
	// Write with first instance.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("first Open: %v", err)
		}
		offset, err = s.Append(msg)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Read with second instance (simulates restart).
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("second Open: %v", err)
		}
		defer s.Close()

		got, err := s.ReadAt(offset)
		if err != nil {
			t.Fatalf("ReadAt after reopen: %v", err)
		}
		assertMessagesEqual(t, msg, got)
	}
}

// ---- Index tests ------------------------------------------------------------

func TestIndex_WriteAndRead(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "ns", "q", []byte("x"))

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	entry := storage.IndexEntry{
		Offset: offset,
		Status: types.StatusReady,
	}
	if err := s.WriteIndex(msg.ID, entry); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	got, err := s.ReadIndex(msg.ID)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if got.Offset != offset {
		t.Errorf("Offset: got %d want %d", got.Offset, offset)
	}
	if got.Status != types.StatusReady {
		t.Errorf("Status: got %v want ready", got.Status)
	}
}

func TestIndex_UpdateStatus(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "ns", "q", []byte("x"))
	offset, _ := s.Append(msg)

	// Write initial status.
	_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady})

	// Update to in-flight.
	receipt := node.MustNewID()
	deadline := time.Now().Add(30 * time.Second).UnixMilli()
	updated := storage.IndexEntry{
		Offset:               offset,
		Status:               types.StatusInFlight,
		ReceiptHandle:        receipt,
		VisibilityDeadlineMs: deadline,
	}
	if err := s.WriteIndex(msg.ID, updated); err != nil {
		t.Fatalf("WriteIndex (update): %v", err)
	}

	got, err := s.ReadIndex(msg.ID)
	if err != nil {
		t.Fatalf("ReadIndex after update: %v", err)
	}

	if got.Status != types.StatusInFlight {
		t.Errorf("Status: got %v want in_flight", got.Status)
	}
	if got.ReceiptHandle != receipt {
		t.Errorf("ReceiptHandle: got %q want %q", got.ReceiptHandle, receipt)
	}
	if got.VisibilityDeadlineMs != deadline {
		t.Errorf("VisibilityDeadlineMs: got %d want %d", got.VisibilityDeadlineMs, deadline)
	}
}

func TestIndex_ReadNotFound(t *testing.T) {
	s := openStorage(t)
	_, err := s.ReadIndex("nonexistent-id")
	if err == nil {
		t.Fatal("expected ErrNotFound for unknown ID")
	}
}

func TestIndex_Delete(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "ns", "q", []byte("x"))
	offset, _ := s.Append(msg)

	_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady})
	if err := s.DeleteIndex(msg.ID); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}

	_, err := s.ReadIndex(msg.ID)
	if err == nil {
		t.Fatal("expected ErrNotFound after delete")
	}
}

func TestIndex_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	msg := newTestMsg(t, "ns", "q", []byte("x"))

	{
		s, _ := local.Open(dir)
		offset, _ := s.Append(msg)
		_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady})
		_ = s.Close()
	}

	{
		s, _ := local.Open(dir)
		defer s.Close()

		entry, err := s.ReadIndex(msg.ID)
		if err != nil {
			t.Fatalf("ReadIndex after reopen: %v", err)
		}
		if entry.Status != types.StatusReady {
			t.Errorf("Status after reopen: got %v want ready", entry.Status)
		}
	}
}

// ---- Full round-trip tests --------------------------------------------------

func TestStorage_AppendWriteIndexReadBack(t *testing.T) {
	s := openStorage(t)
	msg := newTestMsg(t, "payments", "orders", []byte(`{"amount":99}`))

	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady})

	// Simulate consumer reading by looking up index then fetching from log.
	entry, err := s.ReadIndex(msg.ID)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	got, err := s.ReadAt(entry.Offset)
	if err != nil {
		t.Fatalf("ReadAt via index: %v", err)
	}

	assertMessagesEqual(t, msg, got)
}

func TestStorage_Close_IsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := local.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic (may return error, that's fine).
	_ = s.Close()
}

// ---- assertion helper -------------------------------------------------------

func assertMessagesEqual(t *testing.T, want, got *types.Message) {
	t.Helper()
	if want.ID != got.ID {
		t.Errorf("ID: want %q got %q", want.ID, got.ID)
	}
	if want.Namespace != got.Namespace {
		t.Errorf("Namespace: want %q got %q", want.Namespace, got.Namespace)
	}
	if want.Queue != got.Queue {
		t.Errorf("Queue: want %q got %q", want.Queue, got.Queue)
	}
	if !bytes.Equal(want.Body, got.Body) {
		t.Errorf("Body: want %q got %q", want.Body, got.Body)
	}
	if want.DeliverAt != got.DeliverAt {
		t.Errorf("DeliverAt: want %d got %d", want.DeliverAt, got.DeliverAt)
	}
	if want.PublishedAt != got.PublishedAt {
		t.Errorf("PublishedAt: want %d got %d", want.PublishedAt, got.PublishedAt)
	}
	if want.Attempt != got.Attempt {
		t.Errorf("Attempt: want %d got %d", want.Attempt, got.Attempt)
	}
	if want.MaxRetries != got.MaxRetries {
		t.Errorf("MaxRetries: want %d got %d", want.MaxRetries, got.MaxRetries)
	}
	if want.Term != got.Term {
		t.Errorf("Term: want %d got %d", want.Term, got.Term)
	}
}

// ─── Crash recovery tests ────────────────────────────────────────────────────

// TestStorage_CrashRecovery_WALReplayedOnReopen simulates a crash that occurs
// after Append (WAL write + log write) but before WriteIndex.
// On reopen, crash recovery must detect the uncommitted WAL entry and add the
// message to the index so it is not lost.
func TestStorage_CrashRecovery_WALReplayedOnReopen(t *testing.T) {
	dir := t.TempDir()
	msg := newTestMsg(t, "payments", "orders", []byte(`{"crash":"test"}`))

	// Step 1: open storage and write WAL+log but intentionally skip WriteIndex.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		if _, err := s.Append(msg); err != nil {
			t.Fatalf("Append: %v", err)
		}
		// Deliberately skip WriteIndex to simulate crash.
		// Close will flush the WAL file but WAL entry remains uncommitted.
		if err := s.Close(); err != nil {
			t.Fatalf("Close (first): %v", err)
		}
	}

	// Step 2: reopen — crash recovery must reconstruct the index entry.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		defer s.Close()

		entry, err := s.ReadIndex(msg.ID)
		if err != nil {
			t.Fatalf("ReadIndex after crash recovery: message not recovered: %v", err)
		}
		if entry.Status != types.StatusReady {
			t.Errorf("expected StatusReady after recovery, got %v", entry.Status)
		}

		got, err := s.ReadAt(entry.Offset)
		if err != nil {
			t.Fatalf("ReadAt recovered message: %v", err)
		}
		if got.ID != msg.ID {
			t.Errorf("ID mismatch after recovery: want %s got %s", msg.ID, got.ID)
		}
		if !bytes.Equal(got.Body, msg.Body) {
			t.Errorf("Body mismatch after recovery")
		}
	}
}

// TestStorage_CrashRecovery_CommittedMessageSurvives verifies that messages
// that were fully committed (Append + WriteIndex both called) survive a normal
// close+reopen cycle.
func TestStorage_CrashRecovery_CommittedMessageSurvives(t *testing.T) {
	dir := t.TempDir()
	msg := newTestMsg(t, "ns", "q", []byte(`{"committed":true}`))

	var originalOffset int64

	// Write and commit.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		offset, err := s.Append(msg)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		originalOffset = offset
		if err := s.WriteIndex(msg.ID, storage.IndexEntry{
			Offset: offset,
			Status: types.StatusReady,
		}); err != nil {
			t.Fatalf("WriteIndex: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen and verify.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		defer s.Close()

		entry, err := s.ReadIndex(msg.ID)
		if err != nil {
			t.Fatalf("ReadIndex after reopen: %v", err)
		}
		if entry.Offset != originalOffset {
			t.Errorf("Offset: want %d got %d", originalOffset, entry.Offset)
		}
		got, err := s.ReadAt(entry.Offset)
		if err != nil {
			t.Fatalf("ReadAt after reopen: %v", err)
		}
		assertMessagesEqual(t, msg, got)
	}
}

// TestStorage_CrashRecovery_MultipleUncommittedMessages verifies that recovery
// replays all uncommitted WAL entries, not just the first one.
func TestStorage_CrashRecovery_MultipleUncommittedMessages(t *testing.T) {
	dir := t.TempDir()

	msgs := make([]*types.Message, 4)
	for i := range msgs {
		msgs[i] = newTestMsg(t, "ns", "q", []byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	// Append all messages without WriteIndex.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		for i, m := range msgs {
			if _, err := s.Append(m); err != nil {
				t.Fatalf("Append[%d]: %v", i, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen — all 4 messages should be recovered.
	{
		s, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		defer s.Close()

		for i, m := range msgs {
			entry, err := s.ReadIndex(m.ID)
			if err != nil {
				t.Errorf("ReadIndex[%d] not recovered: %v", i, err)
				continue
			}
			got, err := s.ReadAt(entry.Offset)
			if err != nil {
				t.Errorf("ReadAt[%d]: %v", i, err)
				continue
			}
			if got.ID != m.ID {
				t.Errorf("msgs[%d] ID mismatch after recovery", i)
			}
		}
	}
}
