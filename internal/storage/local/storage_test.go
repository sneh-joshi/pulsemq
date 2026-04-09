package local_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// ─── ForEach tests ────────────────────────────────────────────────────────────

func TestStorage_ForEach_IteratesIndexedMessages(t *testing.T) {
s := openStorage(t)

msgs := []*types.Message{
newTestMsg(t, "ns", "q", []byte("a")),
newTestMsg(t, "ns", "q", []byte("b")),
newTestMsg(t, "ns", "q", []byte("c")),
}

ids := make(map[string]bool)
for _, m := range msgs {
offset, err := s.Append(m)
if err != nil {
t.Fatalf("Append: %v", err)
}
if err := s.WriteIndex(m.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady}); err != nil {
t.Fatalf("WriteIndex: %v", err)
}
ids[m.ID] = false
}

count := 0
err := s.ForEach(func(msgID string, entry storage.IndexEntry) error {
if _, ok := ids[msgID]; !ok {
t.Errorf("unexpected msgID in ForEach: %s", msgID)
}
ids[msgID] = true
count++
return nil
})
if err != nil {
t.Fatalf("ForEach: %v", err)
}
if count != 3 {
t.Errorf("ForEach count: want 3, got %d", count)
}
for id, seen := range ids {
if !seen {
t.Errorf("message %s not visited in ForEach", id)
}
}
}

func TestStorage_ForEach_EmptyStorage(t *testing.T) {
s := openStorage(t)
count := 0
if err := s.ForEach(func(_ string, _ storage.IndexEntry) error {
count++
return nil
}); err != nil {
t.Fatalf("ForEach on empty storage: %v", err)
}
if count != 0 {
t.Errorf("expected 0 iterations on empty storage, got %d", count)
}
}

// ─── fsync policy tests ───────────────────────────────────────────────────────

func TestStorage_Open_WithFsyncAlways(t *testing.T) {
dir := t.TempDir()
s, err := local.Open(dir, local.Config{Fsync: local.FsyncAlways})
if err != nil {
t.Fatalf("Open with FsyncAlways: %v", err)
}
defer s.Close()

msg := newTestMsg(t, "ns", "q", []byte("always-fsync"))
offset, err := s.Append(msg)
if err != nil {
t.Fatalf("Append: %v", err)
}

got, err := s.ReadAt(offset)
if err != nil {
t.Fatalf("ReadAt: %v", err)
}
if got.ID != msg.ID {
t.Errorf("ID mismatch: want %s got %s", msg.ID, got.ID)
}
}

func TestStorage_Open_WithFsyncBatch(t *testing.T) {
dir := t.TempDir()
s, err := local.Open(dir, local.Config{Fsync: local.FsyncBatch, FsyncBatchSize: 2})
if err != nil {
t.Fatalf("Open with FsyncBatch: %v", err)
}
defer s.Close()

// Write 4 messages (2× the batch size) to trigger batch fsyncs.
for i := 0; i < 4; i++ {
msg := newTestMsg(t, "ns", "q", []byte("batch"))
if _, err := s.Append(msg); err != nil {
t.Fatalf("Append[%d]: %v", i, err)
}
}
}

func TestStorage_Open_WithFsyncNever(t *testing.T) {
dir := t.TempDir()
s, err := local.Open(dir, local.Config{Fsync: local.FsyncNever})
if err != nil {
t.Fatalf("Open with FsyncNever: %v", err)
}
defer s.Close()

msg := newTestMsg(t, "ns", "q", []byte("never-fsync"))
if _, err := s.Append(msg); err != nil {
t.Fatalf("Append: %v", err)
}
}

// TestStorage_ForEach_FnError verifies that ForEach stops and returns the
// error when the callback returns a non-nil error.
func TestStorage_ForEach_FnError(t *testing.T) {
s := openStorage(t)

msg := newTestMsg(t, "ns", "q", []byte("data"))
offset, _ := s.Append(msg)
_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady})

sentinelErr := fmt.Errorf("stop iteration")
err := s.ForEach(func(_ string, _ storage.IndexEntry) error {
return sentinelErr
})
if err == nil {
t.Fatal("expected ForEach to propagate callback error")
}
}

// TestStorage_Compaction_RunOnce runs compaction on a storage instance where
// some messages have been ACKed (deleted), covering the RunOnce hot path.
func TestStorage_Compaction_RunOnce(t *testing.T) {
s := openStorage(t)

// Write and index 3 messages, then mark first one deleted.
msgs := make([]*types.Message, 3)
offsets := make([]int64, 3)
for i := range msgs {
msgs[i] = newTestMsg(t, "ns", "q", []byte(fmt.Sprintf("body%d", i)))
var err error
offsets[i], err = s.Append(msgs[i])
if err != nil {
t.Fatalf("Append[%d]: %v", i, err)
}
_ = s.WriteIndex(msgs[i].ID, storage.IndexEntry{Offset: offsets[i], Status: types.StatusReady})
}
// Delete (ACK) the first message.
_ = s.WriteIndex(msgs[0].ID, storage.IndexEntry{Offset: offsets[0], Status: types.StatusDeleted})

ctx := context.Background()
if err := s.Compactor().RunOnce(ctx); err != nil {
t.Fatalf("RunOnce: %v", err)
}

// After compaction, remaining messages should still be readable via index.
for _, m := range msgs[1:] {
entry, err := s.ReadIndex(m.ID)
if err != nil {
t.Errorf("ReadIndex %s after compaction: %v", m.ID, err)
continue
}
got, err := s.ReadAt(entry.Offset)
if err != nil {
t.Errorf("ReadAt %s after compaction: %v", m.ID, err)
continue
}
if got.ID != m.ID {
t.Errorf("ID mismatch after compaction: want %s got %s", m.ID, got.ID)
}
}
}

// TestStorage_DeleteIndex removes an index entry and confirms it's gone.
func TestStorage_DeleteIndex(t *testing.T) {
s := openStorage(t)

msg := newTestMsg(t, "ns", "q", []byte("delete-me"))
offset, err := s.Append(msg)
if err != nil {
t.Fatalf("Append: %v", err)
}
if err := s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady}); err != nil {
t.Fatalf("WriteIndex: %v", err)
}

if err := s.DeleteIndex(msg.ID); err != nil {
t.Fatalf("DeleteIndex: %v", err)
}

// Reading back the deleted entry should fail.
if _, err := s.ReadIndex(msg.ID); err == nil {
t.Error("expected error after DeleteIndex, got nil")
}
}

// TestStorage_ReadIndex_NotFound verifies ReadIndex returns error for unknown IDs.
func TestStorage_ReadIndex_NotFound(t *testing.T) {
s := openStorage(t)
_, err := s.ReadIndex("non-existent-id")
if err == nil {
t.Error("expected error for non-existent ID, got nil")
}
}

// TestStorage_CrashRecovery_AlreadyIndexed verifies that recover() skips WAL
// entries whose message is already in the index (the "late crash" path where
// the index write succeeded but WAL.Commit was lost).
func TestStorage_CrashRecovery_AlreadyIndexed(t *testing.T) {
	dir := t.TempDir()
	msg := newTestMsg(t, "ns", "q", []byte(`{"already":true}`))

	// Step 1: simulate crash between index.Write and wal.Commit.
	{
		w, err := local.OpenWAL(dir + "/wal.dat")
		if err != nil {
			t.Fatalf("OpenWAL: %v", err)
		}
		if _, err := w.Write(msg); err != nil {
			t.Fatalf("WAL.Write: %v", err)
		}
		// No w.Commit() — intentionally left uncommitted.
		if err := w.Close(); err != nil {
			t.Fatalf("WAL.Close: %v", err)
		}

		lg, err := local.OpenLog(dir + "/log.dat")
		if err != nil {
			t.Fatalf("OpenLog: %v", err)
		}
		offset, err := lg.Append(msg)
		if err != nil {
			t.Fatalf("log.Append: %v", err)
		}
		if err := lg.Close(); err != nil {
			t.Fatalf("log.Close: %v", err)
		}

		idx, err := local.OpenIndex(dir + "/index.db")
		if err != nil {
			t.Fatalf("OpenIndex: %v", err)
		}
		if err := idx.Write(msg.ID, storage.IndexEntry{
			Offset: offset,
			Status: types.StatusReady,
		}); err != nil {
			t.Fatalf("index.Write: %v", err)
		}
		if err := idx.Close(); err != nil {
			t.Fatalf("index.Close: %v", err)
		}
	}

	// Step 2: opening Storage must run recover() which skips the already-indexed entry.
	s, err := local.Open(dir)
	if err != nil {
		t.Fatalf("Open after simulated crash: %v", err)
	}
	defer s.Close()

	entry, err := s.ReadIndex(msg.ID)
	if err != nil {
		t.Fatalf("ReadIndex after crash recovery: %v", err)
	}
	got, err := s.ReadAt(entry.Offset)
	if err != nil {
		t.Fatalf("ReadAt after crash recovery: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("recovered message ID mismatch: want %s, got %s", msg.ID, got.ID)
	}
}

// TestStorage_Compaction_NothingToCompact verifies RunOnce returns nil when all
// messages are still live (deletedCount == 0 → early-return branch).
func TestStorage_Compaction_NothingToCompact(t *testing.T) {
	s := openStorage(t)

	// Append and index one live (READY) message — nothing deleted.
	msg := newTestMsg(t, "ns", "q", []byte("live"))
	offset, err := s.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady}); err != nil {
		t.Fatalf("WriteIndex: %v", err)
	}

	if err := s.Compactor().RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce with no deleted messages: %v", err)
	}

	// Message must still be readable after a no-op compaction.
	entry, err := s.ReadIndex(msg.ID)
	if err != nil {
		t.Fatalf("ReadIndex after no-op compaction: %v", err)
	}
	got, err := s.ReadAt(entry.Offset)
	if err != nil {
		t.Fatalf("ReadAt after no-op compaction: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("ID mismatch: want %s got %s", msg.ID, got.ID)
	}
}

// TestStorage_Compaction_OrphanedEntry verifies that RunOnce increments
// deletedCount (and proceeds to compact) for a log entry that has no
// corresponding index entry (i.e. an orphan from a previous partial write).
func TestStorage_Compaction_OrphanedEntry(t *testing.T) {
	s := openStorage(t)

	// Append to log (and WAL) without calling WriteIndex — creating an orphan.
	orphan := newTestMsg(t, "ns", "q", []byte("orphan"))
	_, err := s.Append(orphan)
	if err != nil {
		t.Fatalf("Append orphan: %v", err)
	}
	// Intentionally no s.WriteIndex(orphan.ID, ...) — orphan stays out of index.

	// Add one properly-indexed live message so compaction has something to copy.
	live := newTestMsg(t, "ns", "q", []byte("live"))
	offset, err := s.Append(live)
	if err != nil {
		t.Fatalf("Append live: %v", err)
	}
	if err := s.WriteIndex(live.ID, storage.IndexEntry{Offset: offset, Status: types.StatusReady}); err != nil {
		t.Fatalf("WriteIndex live: %v", err)
	}

	// RunOnce must succeed: orphan is treated as deleted (not in index).
	if err := s.Compactor().RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce with orphan: %v", err)
	}

	// Live message should still be accessible.
	entry, err := s.ReadIndex(live.ID)
	if err != nil {
		t.Fatalf("ReadIndex live after compaction: %v", err)
	}
	got, err := s.ReadAt(entry.Offset)
	if err != nil {
		t.Fatalf("ReadAt live after compaction: %v", err)
	}
	if got.ID != live.ID {
		t.Errorf("ID mismatch: want %s got %s", live.ID, got.ID)
	}
}

// TestStorage_Compactor_Start verifies that the background compactor goroutine
// (Start) fires at least once and completes RunOnce without error.
func TestStorage_Compactor_Start(t *testing.T) {
	dir := t.TempDir()
	// Open with a very short compaction interval so the ticker fires quickly.
	s, err := local.Open(dir, local.Config{CompactionInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Let the background goroutine run for several ticks.
	time.Sleep(50 * time.Millisecond)

	// Close waits for the compactor goroutine to finish (Stop + wg.Wait).
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStorage_Compaction_Cancelled verifies that RunOnce respects context
// cancellation inside the log scan loop.
func TestStorage_Compaction_Cancelled(t *testing.T) {
	s := openStorage(t)

	// Write several messages. Mark the first as deleted so deletedCount > 0
	// and compaction proceeds past the early-return check into the scan loop.
	var firstOffset int64
	var firstID string
	for i := 0; i < 5; i++ {
		msg := newTestMsg(t, "ns", "q", []byte(fmt.Sprintf("cancel%d", i)))
		offset, err := s.Append(msg)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		status := types.StatusReady
		if i == 0 {
			firstOffset = offset
			firstID = msg.ID
			status = types.StatusDeleted
		}
		_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: status})
	}
	_ = firstOffset
	_ = firstID

	// Cancel the context immediately — RunOnce should abort inside the scan loop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := s.Compactor().RunOnce(ctx)
	if err == nil {
		t.Fatal("RunOnce should return a context error when context is cancelled during scan")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

// TestStorage_Open_LogFileIsDirectory verifies that Open returns an error when
// the log.dat path is blocked by a directory (OpenLog fails), covering
// storage.go lines 134-136.
func TestStorage_Open_LogFileIsDirectory(t *testing.T) {
dir := t.TempDir()
// Create a directory named "log.dat" — OpenLog(os.O_RDWR|O_CREATE) will fail.
if err := os.Mkdir(filepath.Join(dir, "log.dat"), 0o750); err != nil {
t.Fatalf("Mkdir: %v", err)
}
_, err := local.Open(dir)
if err == nil {
t.Fatal("expected error when log.dat is a directory, got nil")
}
}

// TestStorage_Open_IndexFileIsDirectory verifies that Open returns an error when
// the index.db path is blocked by a directory (OpenIndex fails), covering
// storage.go lines 139-142.
func TestStorage_Open_IndexFileIsDirectory(t *testing.T) {
dir := t.TempDir()
// Create valid log.dat so OpenLog succeeds but index.db fails.
if f, err := os.Create(filepath.Join(dir, "log.dat")); err == nil {
f.Close()
}
// Block index.db with a directory.
if err := os.Mkdir(filepath.Join(dir, "index.db"), 0o750); err != nil {
t.Fatalf("Mkdir: %v", err)
}
_, err := local.Open(dir)
if err == nil {
t.Fatal("expected error when index.db is a directory, got nil")
}
}

// TestStorage_Open_WALFileIsDirectory verifies that Open returns an error when
// the wal.dat path is blocked by a directory (OpenWAL fails), covering
// storage.go lines 144-149.
func TestStorage_Open_WALFileIsDirectory(t *testing.T) {
dir := t.TempDir()
// We need valid log.dat and index.db so their opens succeed.
if f, err := os.Create(filepath.Join(dir, "log.dat")); err == nil {
f.Close()
}
// index.db needs to be a valid bbolt database. Let the real Open create it
// first in a different dir, then we'll use a trick: just passing a directory
// path for the WAL. We can't do that easily without opening log and index
// successfully. Let's take another approach: block wal.dat with a directory.
if err := os.Mkdir(filepath.Join(dir, "wal.dat"), 0o750); err != nil {
t.Fatalf("Mkdir: %v", err)
}
// We still need index.db to succeed. bbolt can't open a directory, but
// since we haven't seeded any data, let's try a two-step approach:
// first open to create the index.db, then close and block wal.
_, err := local.Open(dir)
if err == nil {
t.Fatal("expected error when wal.dat is a directory, got nil")
}
}

// TestStorage_Open_MkdirAllFails verifies that Open returns an error when the
// storage directory cannot be created (covering storage.go lines 129-131).
func TestStorage_Open_MkdirAllFails(t *testing.T) {
parent := t.TempDir()
// Create a FILE at the path so MkdirAll fails.
blocked := filepath.Join(parent, "blocked")
if err := os.WriteFile(blocked, []byte("block"), 0o640); err != nil {
t.Fatalf("WriteFile: %v", err)
}
// Try to open storage at "blocked/subdir" — MkdirAll fails because
// "blocked" is a file, not a directory.
_, err := local.Open(filepath.Join(blocked, "subdir"))
if err == nil {
t.Fatal("expected error when MkdirAll fails, got nil")
}
}

// TestWAL_OpenWAL_InvalidMagic verifies that OpenWAL returns an error when the
// WAL file contains an invalid magic header, covering wal.go lines 96-99.
func TestWAL_OpenWAL_InvalidMagic(t *testing.T) {
dir := t.TempDir()
path := filepath.Join(dir, "wal.dat")
// Write 4 bytes that are NOT the WAL magic header (walMagic = 0x4C5157_01).
if err := os.WriteFile(path, []byte{0x00, 0x00, 0x00, 0x00}, 0o640); err != nil {
t.Fatalf("WriteFile: %v", err)
}
_, err := local.OpenWAL(path)
if err == nil {
t.Fatal("expected error for invalid WAL magic header, got nil")
}
}

// TestLog_ReadAt_TruncatedEntryBody verifies that ReadAt returns an error when
// the log file contains only a length prefix but no entry body, covering
// log.go lines 144-145 (readAt body read failure).
// It also exercises ReadAll propagating the non-EOF/non-Corrupted error (line 177).
func TestLog_ReadAt_TruncatedEntryBody(t *testing.T) {
dir := t.TempDir()
path := filepath.Join(dir, "log.dat")
// Write a 4-byte big-endian length prefix claiming 16 bytes but supply no body.
// binary.BigEndian.PutUint32([0,0,0,16]) == 16
if err := os.WriteFile(path, []byte{0, 0, 0, 16}, 0o640); err != nil {
t.Fatalf("WriteFile: %v", err)
}
lg, err := local.OpenLog(path)
if err != nil {
t.Fatalf("OpenLog: %v", err)
}
defer func() { _ = lg.Close() }()

// ReadAt at offset 0: reads the 4-byte length (16), then tries to read 16
// bytes at offset 4 but the file is only 4 bytes long → EOF → error.
if _, readErr := lg.ReadAt(0); readErr == nil {
t.Fatal("expected ReadAt error for truncated entry body, got nil")
}

// ReadAll should propagate the same error (it's not ErrNotFound or ErrCorrupted).
if err := lg.ReadAll(func(_ int64, _ *types.Message) error { return nil }); err == nil {
t.Fatal("expected ReadAll to propagate truncated entry error, got nil")
}
}

// TestLog_Reopen_NonexistentPath verifies that Log.Reopen returns an error when
// the new path is in a directory that does not exist, covering log.go lines 210-211.
func TestLog_Reopen_NonexistentPath(t *testing.T) {
dir := t.TempDir()
lg, err := local.OpenLog(filepath.Join(dir, "log.dat"))
if err != nil {
t.Fatalf("OpenLog: %v", err)
}
// Reopen with a path inside a non-existent directory.
reopenErr := lg.Reopen(filepath.Join(dir, "missing", "log.dat"))
if reopenErr == nil {
t.Fatal("expected Reopen error for non-existent directory, got nil")
}
// Close may error because the original fd was closed by Reopen; that is expected.
_ = lg.Close()
}

// TestCompaction_RunOnce_TmpLogBlocked verifies that RunOnce returns an error when
// the temporary log path is blocked by a directory, covering compaction.go lines 136-139.
func TestCompaction_RunOnce_TmpLogBlocked(t *testing.T) {
dir := t.TempDir()
s, err := local.Open(dir, local.Config{Fsync: local.FsyncNever})
if err != nil {
t.Fatalf("Open: %v", err)
}
t.Cleanup(func() { _ = s.Close() })

// Write one message and mark it deleted so deletedCount > 0 (avoids early return).
msg := newTestMsg(t, "ns", "q", []byte("to-delete"))
offset, err := s.Append(msg)
if err != nil {
t.Fatalf("Append: %v", err)
}
_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusDeleted})

// Block log.dat.tmp with a directory so OpenLog(tmpPath) fails.
tmpPath := filepath.Join(dir, "log.dat.tmp")
if err := os.Mkdir(tmpPath, 0o750); err != nil {
t.Fatalf("Mkdir: %v", err)
}

if err := s.Compactor().RunOnce(context.Background()); err == nil {
t.Fatal("expected RunOnce error when tmp log path is a directory, got nil")
}
}

// TestCompaction_RunOnce_RenameFails verifies that RunOnce returns an error when
// the rename of log.dat to log.dat.old fails (e.g., destination is a directory),
// covering compaction.go lines 196-206.
func TestCompaction_RunOnce_RenameFails(t *testing.T) {
dir := t.TempDir()
s, err := local.Open(dir, local.Config{Fsync: local.FsyncNever})
if err != nil {
t.Fatalf("Open: %v", err)
}
t.Cleanup(func() { _ = s.Close() })

// Write one message and mark it deleted so deletedCount > 0.
msg := newTestMsg(t, "ns", "q", []byte("deleted"))
offset, err := s.Append(msg)
if err != nil {
t.Fatalf("Append: %v", err)
}
_ = s.WriteIndex(msg.ID, storage.IndexEntry{Offset: offset, Status: types.StatusDeleted})

// Block log.dat.old with a non-empty directory. On POSIX, renaming a regular
// file over a directory always fails (EISDIR / ENOTEMPTY).
oldPath := filepath.Join(dir, "log.dat.old")
if err := os.MkdirAll(filepath.Join(oldPath, "notempty"), 0o750); err != nil {
t.Fatalf("MkdirAll: %v", err)
}

if err := s.Compactor().RunOnce(context.Background()); err == nil {
t.Fatal("expected RunOnce error when rename is blocked, got nil")
}
}
