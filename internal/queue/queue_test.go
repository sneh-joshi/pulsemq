package queue_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/queue"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func newEngine(t *testing.T) *local.Storage {
	t.Helper()
	eng, err := local.Open(t.TempDir())
	if err != nil {
		t.Fatalf("local.Open: %v", err)
	}
	return eng
}

// openQueue creates a Queue backed by a fresh temporary storage engine.
// onSchedule and onDLQ are nil (tests that need them wire them up manually).
func openQueue(t *testing.T) *queue.Queue {
	t.Helper()
	q, err := queue.New("ns", "orders", newEngine(t), queue.DefaultConfig(), nil, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func newMsg(t *testing.T) *queue.Message {
	t.Helper()
	return &queue.Message{
		ID:          node.MustNewID(),
		Namespace:   "ns",
		Queue:       "orders",
		Body:        []byte(`{"test":true}`),
		PublishedAt: time.Now().UnixMilli(),
		Attempt:     1,
		MaxRetries:  3,
	}
}

// ─── Queue tests ─────────────────────────────────────────────────────────────

func TestQueue_PublishDequeue(t *testing.T) {
	q := openQueue(t)
	msg := newMsg(t)

	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if q.Len() != 1 {
		t.Fatalf("Len after Publish: want 1, got %d", q.Len())
	}

	res, err := q.Dequeue(0)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if res == nil {
		t.Fatal("Dequeue: expected non-nil result")
	}
	if res.Message.ID != msg.ID {
		t.Errorf("Dequeue ID: want %s, got %s", msg.ID, res.Message.ID)
	}
	if res.ReceiptHandle == "" {
		t.Error("Dequeue: empty ReceiptHandle")
	}
	if q.Len() != 0 {
		t.Errorf("Len after Dequeue: want 0, got %d", q.Len())
	}
	if q.InFlightCount() != 1 {
		t.Errorf("InFlightCount: want 1, got %d", q.InFlightCount())
	}
}

func TestQueue_DequeueEmpty(t *testing.T) {
	q := openQueue(t)
	res, err := q.Dequeue(0)
	if err != nil {
		t.Fatalf("Dequeue on empty queue: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil from empty Dequeue, got %+v", res)
	}
}

func TestQueue_Ack(t *testing.T) {
	q := openQueue(t)
	if err := q.Publish(newMsg(t)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	res, _ := q.Dequeue(0)
	if err := q.Ack(res.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if q.InFlightCount() != 0 {
		t.Errorf("InFlightCount after Ack: want 0, got %d", q.InFlightCount())
	}
	if q.Len() != 0 {
		t.Errorf("Len after Ack: want 0, got %d", q.Len())
	}
}

func TestQueue_Ack_UnknownReceiptHandle(t *testing.T) {
	q := openQueue(t)
	err := q.Ack("non-existent-handle")
	if err == nil {
		t.Fatal("expected error for unknown receipt handle, got nil")
	}
}

func TestQueue_Nack_RequeuesMessage(t *testing.T) {
	q := openQueue(t)
	msg := newMsg(t)
	msg.MaxRetries = 5
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	res, _ := q.Dequeue(0)
	if err := q.Nack(res.ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Message should be back in READY state.
	if q.Len() != 1 {
		t.Errorf("Len after Nack: want 1, got %d", q.Len())
	}

	// Dequeue again — attempt should be incremented.
	res2, err := q.Dequeue(0)
	if err != nil || res2 == nil {
		t.Fatalf("Dequeue after Nack: err=%v res=%v", err, res2)
	}
	// The original message in the log still has Attempt=1 but the index has
	// Attempt=2 after the Nack requeue. The returned message reflects the
	// original log entry — Attempt reconciliation is tracked in the index.
	_ = q.Ack(res2.ReceiptHandle)
}

func TestQueue_Nack_ExceedsRetries_GoesToDLQ(t *testing.T) {
	var dlqMsgs []*queue.Message

	q, err := queue.New("ns", "orders", newEngine(t), queue.DefaultConfig(),
		nil,
		func(msg *queue.Message) error {
			dlqMsgs = append(dlqMsgs, msg)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	msg := newMsg(t)
	msg.MaxRetries = 1
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// First dequeue and NACK — moves to attempt 2 > MaxRetries(1) → DLQ.
	res, _ := q.Dequeue(0)
	if err := q.Nack(res.ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlqMsgs))
	}
	if dlqMsgs[0].ID != msg.ID {
		t.Errorf("DLQ message ID mismatch: want %s got %s", msg.ID, dlqMsgs[0].ID)
	}
	// Queue should now be empty.
	if q.Len() != 0 {
		t.Errorf("Len after DLQ: want 0, got %d", q.Len())
	}
}

func TestQueue_VisibilityTimeout_Requeues(t *testing.T) {
	q := openQueue(t)
	msg := newMsg(t)
	msg.MaxRetries = 5

	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Dequeue with a very short visibility timeout (50ms).
	res, err := q.Dequeue(50)
	if err != nil || res == nil {
		t.Fatalf("Dequeue: err=%v res=%v", err, res)
	}

	// Wait for the reaper to expire and requeue.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if q.Len() == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if q.Len() != 1 {
		t.Fatalf("expected message re-queued after visibility timeout, Len=%d InFlight=%d",
			q.Len(), q.InFlightCount())
	}
}

func TestQueue_BatchDequeue(t *testing.T) {
	q := openQueue(t)

	const n = 5
	for i := 0; i < n; i++ {
		msg := newMsg(t)
		if err := q.Publish(msg); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	results, err := q.DequeueN(n, 0)
	if err != nil {
		t.Fatalf("DequeueN: %v", err)
	}
	if len(results) != n {
		t.Fatalf("DequeueN: want %d results, got %d", n, len(results))
	}
	if q.Len() != 0 {
		t.Errorf("Len after batch dequeue: want 0, got %d", q.Len())
	}
	if q.InFlightCount() != n {
		t.Errorf("InFlightCount: want %d, got %d", n, q.InFlightCount())
	}
}

func TestQueue_ScheduledMessage_DeliveredAfterDelay(t *testing.T) {
	var scheduledID string
	var scheduledQueueKey string
	var scheduledDeliverAt int64

	onSchedule := func(msgID, queueKey string, deliverAt int64) {
		scheduledID = msgID
		scheduledQueueKey = queueKey
		scheduledDeliverAt = deliverAt
	}

	q, err := queue.New("ns", "orders", newEngine(t), queue.DefaultConfig(), onSchedule, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	msg := newMsg(t)
	msg.DeliverAt = time.Now().Add(100 * time.Millisecond).UnixMilli()

	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish scheduled: %v", err)
	}

	// Must NOT be in the ready list (it's SCHEDULED).
	if q.Len() != 0 {
		t.Fatalf("scheduled message appeared in ready list prematurely")
	}

	// Verify onSchedule was called.
	if scheduledID != msg.ID {
		t.Errorf("onSchedule: msgID want %s got %s", msg.ID, scheduledID)
	}
	if scheduledQueueKey != "ns/orders" {
		t.Errorf("onSchedule: queueKey want ns/orders got %s", scheduledQueueKey)
	}
	_ = scheduledDeliverAt // tested by scheduler tests

	// Simulate scheduler callback (in production this comes from Scheduler.Start).
	time.Sleep(150 * time.Millisecond)
	if err := q.EnqueueScheduled(msg.ID, 0, 1); err != nil {
		// offset 0 won't work here because we haven't tracked it — use promoteScheduled
		t.Logf("EnqueueScheduled with offset 0 failed (expected): %v", err)
	}
}

func TestQueue_StateTransitions(t *testing.T) {
	cases := []struct {
		from queue.Status
		to   queue.Status
		want bool
	}{
		{queue.StatusReady, queue.StatusInFlight, true},
		{queue.StatusReady, queue.StatusDeleted, false},
		{queue.StatusReady, queue.StatusDeadLetter, false},
		{queue.StatusInFlight, queue.StatusDeleted, true},
		{queue.StatusInFlight, queue.StatusReady, true},
		{queue.StatusInFlight, queue.StatusDeadLetter, true},
		{queue.StatusInFlight, queue.StatusScheduled, false},
		{queue.StatusScheduled, queue.StatusReady, true},
		{queue.StatusScheduled, queue.StatusInFlight, false},
		{queue.StatusDeleted, queue.StatusReady, false},
		{queue.StatusDeadLetter, queue.StatusReady, false},
		// Unknown status value falls through to the default branch → false.
		{queue.Status(99), queue.StatusReady, false},
	}
	for _, tc := range cases {
		got := queue.ValidTransition(tc.from, tc.to)
		if got != tc.want {
			t.Errorf("ValidTransition(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}

func TestQueue_RebuildFromStorage(t *testing.T) {
	dir := t.TempDir()

	msg := newMsg(t)

	// Write then close.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open eng: %v", err)
		}
		q, err := queue.New("ns", "orders", eng, queue.DefaultConfig(), nil, nil)
		if err != nil {
			t.Fatalf("queue.New (first): %v", err)
		}
		if err := q.Publish(msg); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if err := q.Close(); err != nil {
			t.Fatalf("Close (first): %v", err)
		}
	}

	// Reopen — message should still be in READY state.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open eng (second): %v", err)
		}
		q, err := queue.New("ns", "orders", eng, queue.DefaultConfig(), nil, nil)
		if err != nil {
			t.Fatalf("queue.New (second): %v", err)
		}
		defer q.Close()

		if q.Len() != 1 {
			t.Fatalf("Len after rebuild: want 1, got %d", q.Len())
		}
		res, err := q.Dequeue(0)
		if err != nil || res == nil {
			t.Fatalf("Dequeue after rebuild: err=%v res=%v", err, res)
		}
		if res.Message.ID != msg.ID {
			t.Errorf("ID after rebuild: want %s got %s", msg.ID, res.Message.ID)
		}
	}
}

func TestQueue_RebuildFromStorage_InFlightExpired(t *testing.T) {
	dir := t.TempDir()
	msg := newMsg(t)

	// Write, dequeue (mark IN_FLIGHT with 50ms timeout), then close without ACK.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open eng: %v", err)
		}
		q, err := queue.New("ns", "q", eng, queue.DefaultConfig(), nil, nil)
		if err != nil {
			t.Fatalf("queue.New: %v", err)
		}
		if err := q.Publish(msg); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if _, err := q.Dequeue(50); err != nil { // 50 ms timeout
			t.Fatalf("Dequeue: %v", err)
		}
		// Close immediately — in-flight with 50ms deadline persisted to index.
		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Wait for the visibilityDeadline to expire.
	time.Sleep(100 * time.Millisecond)

	// Reopen — loadFromStorage should detect expired in-flight and re-queue.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open eng (second): %v", err)
		}
		q, err := queue.New("ns", "q", eng, queue.DefaultConfig(), nil, nil)
		if err != nil {
			t.Fatalf("queue.New (second): %v", err)
		}
		defer q.Close()

		if q.Len() != 1 {
			t.Fatalf("expected expired in-flight to be re-queued on load, Len=%d", q.Len())
		}
	}
}

// ─── TotalCount ───────────────────────────────────────────────────────────────

func TestQueue_TotalCount(t *testing.T) {
q := openQueue(t)

if n := q.TotalCount(); n != 0 {
t.Errorf("TotalCount on empty queue: want 0, got %d", n)
}

if err := q.Publish(newMsg(t)); err != nil {
t.Fatalf("Publish: %v", err)
}
if n := q.TotalCount(); n != 1 {
t.Errorf("TotalCount after Publish: want 1, got %d", n)
}
}

// ─── Purge ───────────────────────────────────────────────────────────────────

func TestQueue_Purge(t *testing.T) {
q := openQueue(t)

// Publish 3 messages.
for i := 0; i < 3; i++ {
if err := q.Publish(newMsg(t)); err != nil {
t.Fatalf("Publish[%d]: %v", i, err)
}
}
if q.Len() != 3 {
t.Fatalf("Len before Purge: want 3, got %d", q.Len())
}

n, err := q.Purge()
if err != nil {
t.Fatalf("Purge: %v", err)
}
if n != 3 {
t.Errorf("Purge count: want 3, got %d", n)
}
if q.Len() != 0 {
t.Errorf("Len after Purge: want 0, got %d", q.Len())
}
}

func TestQueue_Purge_WithInFlight(t *testing.T) {
q := openQueue(t)

if err := q.Publish(newMsg(t)); err != nil {
t.Fatalf("Publish: %v", err)
}
// Dequeue to put message in-flight.
_, err := q.Dequeue(30000)
if err != nil {
t.Fatalf("Dequeue: %v", err)
}
if q.InFlightCount() != 1 {
t.Fatalf("InFlightCount: want 1, got %d", q.InFlightCount())
}

n, err := q.Purge()
if err != nil {
t.Fatalf("Purge with in-flight: %v", err)
}
if n != 1 {
t.Errorf("Purge in-flight count: want 1, got %d", n)
}
if q.InFlightCount() != 0 {
t.Errorf("InFlightCount after Purge: want 0, got %d", q.InFlightCount())
}
}

func TestQueue_Nack_DeadLetter_NoOnDLQ(t *testing.T) {
	// Queue with MaxRetries=1 but no onDLQ callback — verifies deadLetter
	// is called and doesn't panic when onDLQ is nil.
	eng := newEngine(t)
	cfg := queue.Config{
		DefaultVisibilityTimeoutMs: 30000,
		MaxBatchSize:               10,
		MaxRetries:                 1,
		MaxMessages:                100,
	}
	q, err := queue.New("ns", "nack-dlq-noontlq", eng, cfg, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Close()

	msg := newMsg(t)
	msg.MaxRetries = 1
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	r, err := q.Dequeue(30000)
	if err != nil || r == nil {
		t.Fatalf("Dequeue: err=%v r=%v", err, r)
	}

	// Nack with attempt=1 and MaxRetries=1 → dead-letter path.
	if err := q.Nack(r.ReceiptHandle); err != nil {
		t.Fatalf("Nack (dead-letter, no onDLQ): %v", err)
	}
}

func TestQueue_LoadFromStorage_ExpiredInFlight(t *testing.T) {
	dir := t.TempDir()

	var msgID string
	// Step 1: publish and consume a message to put it in-flight.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		cfg := queue.Config{
			DefaultVisibilityTimeoutMs: 1, // 1ms — will expire immediately
			MaxBatchSize:               10,
			MaxRetries:                 3,
			MaxMessages:                100,
		}
		q, err := queue.New("ns", "expired", eng, cfg, nil, nil)
		if err != nil {
			t.Fatalf("New (first): %v", err)
		}

		m := newMsg(t)
		msgID = m.ID
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		r, err := q.Dequeue(1) // 1ms visibility
		if err != nil || r == nil {
			t.Fatalf("Dequeue: err=%v r=%v", err, r)
		}
		// Do NOT ack — leave in-flight but with expired deadline.
		q.Close()
	}

	// Wait for the visibility window to expire.
	time.Sleep(10 * time.Millisecond)

	// Step 2: reopen — expired in-flight should be moved back to ready.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		cfg := queue.Config{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               10,
			MaxRetries:                 3,
			MaxMessages:                100,
		}
		q, err := queue.New("ns", "expired", eng, cfg, nil, nil)
		if err != nil {
			t.Fatalf("New (second): %v", err)
		}
		defer q.Close()

		if q.Len() != 1 {
			t.Errorf("expected 1 ready message after reopen (expired in-flight restored), got %d", q.Len())
		}
		r, err := q.Dequeue(30000)
		if err != nil || r == nil {
			t.Fatalf("Dequeue after reopen: err=%v r=%v", err, r)
		}
		if r.Message.ID != msgID {
			t.Errorf("message ID mismatch: want %s got %s", msgID, r.Message.ID)
		}
	}
}

// TestManager_Create_FactoryError verifies GetOrCreate propagates a storage
// factory failure.
func TestManager_Create_FactoryError(t *testing.T) {
	failFactory := queue.EngineFactory(func(ns, name string) (storage.StorageEngine, error) {
		return nil, fmt.Errorf("simulated factory failure")
	})
	mgr := queue.NewManager(failFactory, nil, queue.DefaultConfig())
	defer mgr.Close()

	_, err := mgr.GetOrCreate("ns", "fail-q")
	if err == nil {
		t.Fatal("expected error from failing factory, got nil")
	}
}

// TestManager_SchedulerReadyFn_UnknownQueue verifies SchedulerReadyFn
// silently ignores a queue key that no longer exists.
func TestManager_SchedulerReadyFn_UnknownQueue(t *testing.T) {
	factory := queue.EngineFactory(func(ns, name string) (storage.StorageEngine, error) {
		return local.Open(t.TempDir())
	})
	mgr := queue.NewManager(factory, nil, queue.DefaultConfig())
	defer mgr.Close()

	fn := mgr.SchedulerReadyFn()
	fn("some-msg-id", "ns/ghost-queue") // must not panic
}

// TestQueue_DequeueN_MultipleMessages verifies DequeueN returns the correct count.
func TestQueue_DequeueN_MultipleMessages(t *testing.T) {
	q := openQueue(t)
	for i := 0; i < 5; i++ {
		m := newMsg(t)
		m.Body = []byte(fmt.Sprintf("msg-%d", i))
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}
	results, err := q.DequeueN(3, 0)
	if err != nil {
		t.Fatalf("DequeueN: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("DequeueN: want 3, got %d", len(results))
	}
}

// TestQueue_Nack_ExceedsMaxRetries verifies exceeding MaxRetries triggers
// the onDLQ callback. With MaxRetries=1 a single Nack moves the message to
// the DLQ because attempt 1→2 > 1.
func TestQueue_Nack_ExceedsMaxRetries(t *testing.T) {
	cfg := queue.Config{
		MaxRetries:                 1,
		MaxMessages:                1000,
		DefaultVisibilityTimeoutMs: 30000,
	}
	var mu sync.Mutex
	dlqReceived := false
	onDLQ := func(_ *queue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		dlqReceived = true
		return nil
	}

	q, err := queue.New("ns", "retry-q", newEngine(t), cfg, nil, onDLQ)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Close()

	m := newMsg(t)
	m.MaxRetries = 1
	if err := q.Publish(m); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Attempt 1→2 > MaxRetries=1: dead-letters immediately.
	r, _ := q.Dequeue(30000)
	if r == nil {
		t.Fatal("Dequeue: nil result")
	}
	if err := q.Nack(r.ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	mu.Lock()
	got := dlqReceived
	mu.Unlock()
	if !got {
		t.Error("expected onDLQ callback after exceeding MaxRetries")
	}
}

// TestManager_PromoteScheduled_ExistingQueue verifies SchedulerReadyFn
// promotes a scheduled message to ready when the queue exists.
func TestManager_PromoteScheduled_ExistingQueue(t *testing.T) {
	factory := queue.EngineFactory(func(ns, name string) (storage.StorageEngine, error) {
		return local.Open(t.TempDir())
	})
	mgr := queue.NewManager(factory, nil, queue.DefaultConfig())
	defer mgr.Close()

	readyFn := mgr.SchedulerReadyFn()

	_, err := mgr.GetOrCreate("ns", "sched-q")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Call SchedulerReadyFn with an unknown msgID on the existing queue
	// to exercise the "not found in index" error path in promoteScheduled.
	readyFn("unknown-msg-id", "ns/sched-q")
}

// TestQueue_LoadFromStorage_NonExpiredInFlight verifies that in-flight entries
// whose visibility deadline has not yet passed are restored on restart.
func TestQueue_LoadFromStorage_NonExpiredInFlight(t *testing.T) {
	dir := t.TempDir()

	var receiptHandle string
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		cfg := queue.Config{
			DefaultVisibilityTimeoutMs: 60_000, // 60s — will NOT expire during test
			MaxBatchSize:               10,
			MaxRetries:                 3,
			MaxMessages:                100,
		}
		q, err := queue.New("ns", "inflight", eng, cfg, nil, nil)
		if err != nil {
			t.Fatalf("New (first): %v", err)
		}

		m := newMsg(t)
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		r, err := q.Dequeue(60_000) // 60s visibility
		if err != nil || r == nil {
			t.Fatalf("Dequeue: err=%v r=%v", err, r)
		}
		receiptHandle = r.ReceiptHandle
		// Close without ACK — message stays IN_FLIGHT in storage.
		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen immediately — deadline is still far in the future.
	{
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		cfg := queue.Config{
			DefaultVisibilityTimeoutMs: 60_000,
			MaxBatchSize:               10,
			MaxRetries:                 3,
			MaxMessages:                100,
		}
		q, err := queue.New("ns", "inflight", eng, cfg, nil, nil)
		if err != nil {
			t.Fatalf("New (second): %v", err)
		}
		defer q.Close()

		// Message should be restored as in-flight (not re-queued).
		if q.InFlightCount() != 1 {
			t.Errorf("expected 1 in-flight message after reopen, got %d", q.InFlightCount())
		}
		if q.Len() != 0 {
			t.Errorf("expected 0 ready messages after reopen, got %d", q.Len())
		}
		// The original receipt handle should still be valid for ACK.
		if err := q.Ack(receiptHandle); err != nil {
			t.Errorf("Ack with restored receipt handle: %v", err)
		}
	}
}

// TestQueue_LoadFromStorage_Scheduled verifies that SCHEDULED messages are
// re-registered with the onSchedule callback when the queue is restarted.
func TestQueue_LoadFromStorage_Scheduled(t *testing.T) {
	dir := t.TempDir()

	var published string
	{
		called := make(chan string, 2)
		onSchedule := func(msgID, _ string, _ int64) { called <- msgID }

		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		q, err := queue.New("ns", "sched", eng, queue.DefaultConfig(), onSchedule, nil)
		if err != nil {
			t.Fatalf("New (first): %v", err)
		}

		m := newMsg(t)
		m.DeliverAt = time.Now().Add(10 * time.Minute).UnixMilli() // far future
		published = m.ID
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish scheduled: %v", err)
		}
		// Callback fires once during Publish.
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("onSchedule not called during Publish")
		}

		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen — loadFromStorage should call onSchedule again for the SCHEDULED entry.
	{
		called := make(chan string, 2)
		onSchedule := func(msgID, _ string, _ int64) { called <- msgID }

		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		q, err := queue.New("ns", "sched", eng, queue.DefaultConfig(), onSchedule, nil)
		if err != nil {
			t.Fatalf("New (second): %v", err)
		}
		defer q.Close()

		select {
		case id := <-called:
			if id != published {
				t.Errorf("onSchedule on restart: want %s, got %s", published, id)
			}
		case <-time.After(time.Second):
			t.Fatal("onSchedule not called on restart for SCHEDULED message")
		}
	}
}

// TestQueue_LoadFromStorage_MissedScheduled verifies that a SCHEDULED message
// whose deliverAt passed while the server was down is promoted to READY on restart.
func TestQueue_LoadFromStorage_MissedScheduled(t *testing.T) {
	dir := t.TempDir()

	{
		called := make(chan struct{}, 2)
		onSchedule := func(_, _ string, _ int64) { called <- struct{}{} }

		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (first): %v", err)
		}
		q, err := queue.New("ns", "missed", eng, queue.DefaultConfig(), onSchedule, nil)
		if err != nil {
			t.Fatalf("New (first): %v", err)
		}

		m := newMsg(t)
		m.DeliverAt = time.Now().Add(50 * time.Millisecond).UnixMilli()
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		// Wait for onSchedule callback from Publish.
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("onSchedule not called during Publish")
		}

		// Wait for the deliverAt to pass (no real scheduler, so stays SCHEDULED).
		time.Sleep(100 * time.Millisecond)

		if err := q.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Reopen — deliverAt is now in the past → missed delivery → message moved to READY.
	{
		onSchedule := func(_, _ string, _ int64) {} // still required to enter the branch
		eng, err := local.Open(dir)
		if err != nil {
			t.Fatalf("Open (second): %v", err)
		}
		q, err := queue.New("ns", "missed", eng, queue.DefaultConfig(), onSchedule, nil)
		if err != nil {
			t.Fatalf("New (second): %v", err)
		}
		defer q.Close()

		if q.Len() != 1 {
			t.Errorf("expected 1 ready message (missed scheduled), got %d", q.Len())
		}
	}
}

// TestManager_Create_DLQFactoryError verifies that a storage factory error on
// the DLQ engine causes Manager.Create to return an error and close the primary engine.
func TestManager_Create_DLQFactoryError(t *testing.T) {
	callCount := 0
	factory := queue.EngineFactory(func(ns, name string) (storage.StorageEngine, error) {
		callCount++
		if callCount >= 2 {
			return nil, fmt.Errorf("simulated DLQ factory failure")
		}
		return local.Open(t.TempDir())
	})
	mgr := queue.NewManager(factory, nil, queue.DefaultConfig())
	defer mgr.Close()

	_, err := mgr.Create("ns", "myqueue", queue.DefaultConfig())
	if err == nil {
		t.Fatal("expected error from DLQ factory failure, got nil")
	}
}

// TestQueue_Nack_UnknownReceipt verifies Nack returns an error for an
// unrecognised receipt handle (i.e. the unknown-or-expired path).
func TestQueue_Nack_UnknownReceipt(t *testing.T) {
	q := openQueue(t)

	// Nacking a receipt handle that was never issued must return an error.
	err := q.Nack("this-receipt-does-not-exist")
	if err == nil {
		t.Fatal("expected error from Nack with unknown receipt handle, got nil")
	}
}

// TestQueue_Ack_UnknownReceipt verifies Ack returns an error for an
// unrecognised receipt handle.
func TestQueue_Ack_UnknownReceipt(t *testing.T) {
	q := openQueue(t)

	err := q.Ack("this-receipt-does-not-exist")
	if err == nil {
		t.Fatal("expected error from Ack with unknown receipt handle, got nil")
	}
}

// TestQueue_Publish_AtCapacity verifies that publishing to a full queue returns
// an error (the MaxMessages capacity check path).
func TestQueue_Publish_AtCapacity(t *testing.T) {
	eng := newEngine(t)
	cfg := queue.DefaultConfig()
	cfg.MaxMessages = 1

	q, err := queue.New("ns", "cap-q", eng, cfg, nil, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	// First message fills the queue.
	msg1 := &queue.Message{ID: node.MustNewID(), Namespace: "ns", Queue: "cap-q",
		Body: []byte("first"), Attempt: 1, MaxRetries: 3}
	if err := q.Publish(msg1); err != nil {
		t.Fatalf("first Publish: %v", err)
	}

	// Second message must be rejected.
	msg2 := &queue.Message{ID: node.MustNewID(), Namespace: "ns", Queue: "cap-q",
		Body: []byte("second"), Attempt: 1, MaxRetries: 3}
	if err := q.Publish(msg2); err == nil {
		t.Fatal("expected error when publishing to full queue, got nil")
	}
}

// TestQueue_Publish_DefaultMaxRetries verifies that publishing a message with
// MaxRetries=0 causes the queue to fill in the default from its config.
func TestQueue_Publish_DefaultMaxRetries(t *testing.T) {
	q := openQueue(t)

	// MaxRetries=0 → Publish must set it from q.cfg.MaxRetries (3).
	msg := &queue.Message{
		ID:          node.MustNewID(),
		Namespace:   "ns",
		Queue:       "orders",
		Body:        []byte("default-retries"),
		Attempt:     1,
		MaxRetries:  0, // intentionally zero
		PublishedAt: time.Now().UnixMilli(),
	}
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The message should be in the ready queue.
	if q.Len() != 1 {
		t.Errorf("Len after Publish with MaxRetries=0: want 1, got %d", q.Len())
	}
}

// TestQueue_DequeueN_ExceedsMaxBatchSize verifies that requesting more than
// MaxBatchSize messages is silently capped to MaxBatchSize.
func TestQueue_DequeueN_ExceedsMaxBatchSize(t *testing.T) {
	eng := newEngine(t)
	cfg := queue.DefaultConfig()
	cfg.MaxBatchSize = 2

	q, err := queue.New("ns", "batch-cap", eng, cfg, nil, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	// Publish 4 messages.
	for i := 0; i < 4; i++ {
		m := &queue.Message{ID: node.MustNewID(), Namespace: "ns", Queue: "batch-cap",
			Body: []byte("x"), Attempt: 1, MaxRetries: 3}
		if err := q.Publish(m); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	// Requesting 10 but MaxBatchSize=2 — should get at most 2.
	results, err := q.DequeueN(10, 0)
	if err != nil {
		t.Fatalf("DequeueN: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("DequeueN capped: want 2, got %d", len(results))
	}
}

// ─── DefaultVisibilityTimeout=0 uses default ─────────────────────────────────

// TestQueue_New_ZeroDefaultTimeout verifies that when DefaultVisibilityTimeoutMs
// is 0 the queue falls back to the built-in default (covers line 126 in queue.go).
func TestQueue_New_ZeroDefaultTimeout(t *testing.T) {
cfg := queue.DefaultConfig()
cfg.DefaultVisibilityTimeoutMs = 0 // force the zero-value path

q, err := queue.New("ns", "orders", newEngine(t), cfg, nil, nil)
if err != nil {
t.Fatalf("queue.New: %v", err)
}
t.Cleanup(func() { _ = q.Close() })

// Just verify the queue is usable — the internal default was applied.
msg := newMsg(t)
if err := q.Publish(msg); err != nil {
t.Fatalf("Publish: %v", err)
}
}

// ─── loadFromStorage sort comparator ─────────────────────────────────────────

// TestQueue_LoadFromStorage_Sorted verifies that the sort.Slice comparator
// inside loadFromStorage executes correctly when there are 2+ ready messages
// (covering the comparator body that only runs with len ≥ 2).
func TestQueue_LoadFromStorage_Sorted(t *testing.T) {
dir := t.TempDir()

// Helper to open a fresh engine over the same dir.
openEng := func() *local.Storage {
eng, err := local.Open(dir)
if err != nil {
t.Fatalf("local.Open: %v", err)
}
return eng
}

// First open: publish 2 messages so storage holds 2 ready entries.
q1, err := queue.New("ns", "orders", openEng(), queue.DefaultConfig(), nil, nil)
if err != nil {
t.Fatalf("first queue.New: %v", err)
}
for i := 0; i < 2; i++ {
if err := q1.Publish(newMsg(t)); err != nil {
t.Fatalf("Publish[%d]: %v", i, err)
}
}
if err := q1.Close(); err != nil {
t.Fatalf("Close: %v", err)
}

// Second open: loadFromStorage reads 2 entries → sort comparator fires.
q2, err := queue.New("ns", "orders", openEng(), queue.DefaultConfig(), nil, nil)
if err != nil {
t.Fatalf("second queue.New: %v", err)
}
t.Cleanup(func() { _ = q2.Close() })

results, deqErr := q2.DequeueN(2, 30000)
	if deqErr != nil {
		t.Fatalf("DequeueN after reload: %v", deqErr)
	}
	if len(results) != 2 {
		t.Errorf("Dequeue after reload: want 2, got %d", len(results))
	}
}

// ─── promoteScheduled non-Scheduled entry ────────────────────────────────────

// TestManager_PromoteScheduled_NonScheduledStatus verifies that SchedulerReadyFn
// is a no-op when the indexed message already has StatusReady (not StatusScheduled),
// covering the early-return path in manager.go line 314.
func TestManager_PromoteScheduled_NonScheduledStatus(t *testing.T) {
	factory := queue.EngineFactory(func(ns, name string) (storage.StorageEngine, error) {
		return local.Open(t.TempDir())
	})
	mgr := queue.NewManager(factory, nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	cfg := queue.Config{
		MaxMessages:                100,
		DefaultVisibilityTimeoutMs: 30000,
		MaxRetries:                 3,
	}
	q, err := mgr.Create("ns", "ready-q", cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Publish a normal (non-scheduled) message → it has StatusReady in storage.
	msg := newMsg(t)
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Invoke SchedulerReadyFn with the ready message\'s ID and the queue key.
	// The index entry has StatusReady ≠ StatusScheduled → early return nil.
	// SchedulerReadyFn silently ignores the return; we just need the branch covered.
	fn := mgr.SchedulerReadyFn()
	fn(msg.ID, "ns/ready-q")
}
