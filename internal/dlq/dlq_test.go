package dlq_test

import (
	"path/filepath"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/dlq"
	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/queue"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
)

func newFactory(baseDir string) queue.EngineFactory {
	return func(ns, name string) (storage.StorageEngine, error) {
		return local.Open(filepath.Join(baseDir, ns, name))
	}
}

// exhaustRetries publishes msg, dequeues it, and NACKs MaxRetries times so that
// it lands in the DLQ. Returns when the message has been dead-lettered.
func exhaustRetries(t *testing.T, q *queue.Queue, msg *queue.Message) {
	t.Helper()
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	res, err := q.Dequeue(0)
	if err != nil || res == nil {
		t.Fatalf("Dequeue: err=%v res=%v", err, res)
	}
	// One NACK is enough when MaxRetries=1.
	if err := q.Nack(res.ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}
}

func TestDLQ_Len_AfterDeadLetter(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	dm := dlq.NewManager(mgr)

	// Nothing in DLQ yet (queue doesn't even exist).
	if n := dm.Len("ns", "orders"); n != 0 {
		t.Errorf("Len before any message: want 0, got %d", n)
	}

	q, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	msg := &queue.Message{
		ID:         node.MustNewID(),
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte(`dead`),
		MaxRetries: 1,
	}
	exhaustRetries(t, q, msg)

	if n := dm.Len("ns", "orders"); n != 1 {
		t.Errorf("Len after dead-letter: want 1, got %d", n)
	}
}

func TestDLQ_Drain(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	dm := dlq.NewManager(mgr)

	q, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	msg := &queue.Message{
		ID:         node.MustNewID(),
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte(`dead`),
		MaxRetries: 1,
	}
	exhaustRetries(t, q, msg)

	results, err := dm.Drain("ns", "orders", 10)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Drain: want 1 result, got %d", len(results))
	}
	if results[0].Message.ID != msg.ID {
		t.Errorf("Drain message ID: want %s got %s", msg.ID, results[0].Message.ID)
	}
}

func TestDLQ_Replay(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	dm := dlq.NewManager(mgr)

	q, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	msg := &queue.Message{
		ID:         node.MustNewID(),
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte(`replay-me`),
		MaxRetries: 1,
	}
	exhaustRetries(t, q, msg)

	// Confirm it landed in DLQ and is gone from primary.
	if q.Len() != 0 {
		t.Fatalf("primary queue not empty after DLQ: Len=%d", q.Len())
	}
	if dm.Len("ns", "orders") != 1 {
		t.Fatalf("DLQ Len: want 1, got %d", dm.Len("ns", "orders"))
	}

	// Replay back to primary.
	n, err := dm.Replay("ns", "orders", 10)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if n != 1 {
		t.Fatalf("Replay: want 1 replayed, got %d", n)
	}

	// Message should now be back in the primary queue.
	if q.Len() != 1 {
		t.Fatalf("primary queue Len after Replay: want 1, got %d", q.Len())
	}

	// DLQ should be empty (ACKed during Replay).
	if dm.Len("ns", "orders") != 0 {
		t.Errorf("DLQ Len after Replay: want 0, got %d", dm.Len("ns", "orders"))
	}

	// Dequeue replayed message.
	res, err := q.Dequeue(0)
	if err != nil || res == nil {
		t.Fatalf("Dequeue replayed: err=%v res=%v", err, res)
	}
	if res.Message.ID != msg.ID {
		t.Errorf("replayed message ID: want %s got %s", msg.ID, res.Message.ID)
	}
}

func TestDLQ_Replay_EmptyDLQ(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	dm := dlq.NewManager(mgr)
	_, _ = mgr.GetOrCreate("ns", "orders")

	n, err := dm.Replay("ns", "orders", 10)
	if err != nil {
		t.Fatalf("Replay on empty DLQ: %v", err)
	}
	if n != 0 {
		t.Errorf("replayed=%d, want 0", n)
	}
}

// TestDLQ_Replay_PrimaryNotFound verifies error when primary queue missing.
func TestDLQ_Replay_PrimaryNotFound(t *testing.T) {
mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
t.Cleanup(func() { _ = mgr.Close() })

dm := dlq.NewManager(mgr)
// No queue created — both primary and DLQ are absent.
_, err := dm.Replay("ns", "ghost-queue", 10)
if err == nil {
t.Fatal("expected error when primary queue not found, got nil")
}
}


// TestDLQ_Drain_DLQNotFound verifies error when neither primary nor DLQ exists.
func TestDLQ_Drain_DLQNotFound(t *testing.T) {
mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
t.Cleanup(func() { _ = mgr.Close() })

dm := dlq.NewManager(mgr)
// Neither primary nor DLQ exists — Drain should return an error.
_, err := dm.Drain("ns", "no-such-queue", 10)
if err == nil {
t.Fatal("expected error when DLQ not found, got nil")
}
}

// TestDLQ_Replay_PublishFailure verifies that Replay skips messages whose
// re-publish to the primary queue fails (primary is at capacity).
func TestDLQ_Replay_PublishFailure(t *testing.T) {
	// Use a primary queue with capacity 1 so it fills up immediately.
	primaryCfg := queue.DefaultConfig()
	primaryCfg.MaxMessages = 1

	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	dm := dlq.NewManager(mgr)

	// Create primary with MaxMessages=1.
	primaryQueue, err := mgr.Create("ns", "orders", primaryCfg)
	if err != nil {
		t.Fatalf("Create primary: %v", err)
	}

	// Dead-letter a message using MaxRetries=1.
	msg := &queue.Message{
		ID:         node.MustNewID(),
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte("will-die"),
		MaxRetries: 1,
	}
	exhaustRetries(t, primaryQueue, msg)

	// Confirm the message landed in the DLQ.
	if dm.Len("ns", "orders") != 1 {
		t.Fatalf("DLQ Len after dead-letter: want 1, got %d", dm.Len("ns", "orders"))
	}

	// Fill the primary queue so Publish fails during Replay.
	filler := &queue.Message{
		ID:         node.MustNewID(),
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte("filler"),
		MaxRetries: 1,
	}
	if err := primaryQueue.Publish(filler); err != nil {
		t.Fatalf("Publish filler: %v", err)
	}

	// Replay must return 0 replayed (Publish fails, message left in-flight on DLQ).
	n, err := dm.Replay("ns", "orders", 10)
	if err != nil {
		t.Fatalf("Replay: unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("Replay: want 0 replayed (primary full), got %d", n)
	}
}
