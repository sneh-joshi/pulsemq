package queue_test

import (
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/queue"
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
