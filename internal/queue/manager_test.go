package queue_test

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/queue"
	"github.com/sneh-joshi/pulsemq/internal/scheduler"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
)

// newFactory returns an EngineFactory that stores queue data under baseDir.
func newFactory(baseDir string) queue.EngineFactory {
	return func(ns, name string) (storage.StorageEngine, error) {
		dir := filepath.Join(baseDir, ns, name)
		return local.Open(dir)
	}
}

// ─── GetOrCreate ─────────────────────────────────────────────────────────────

func TestManager_GetOrCreate_CreatesQueue(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	q, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if q == nil {
		t.Fatal("GetOrCreate returned nil queue")
	}
}

func TestManager_GetOrCreate_ReturnsSameInstance(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	q1, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("first GetOrCreate: %v", err)
	}
	q2, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("second GetOrCreate: %v", err)
	}
	if q1 != q2 {
		t.Error("expected same *Queue pointer on repeated GetOrCreate")
	}
}

func TestManager_GetOrCreate_CreatesDLQ(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	_, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// The paired DLQ should also have been created.
	keys := mgr.List()
	sort.Strings(keys)
	wantDLQ := "ns/" + queue.DLQName("orders")
	found := false
	for _, k := range keys {
		if k == wantDLQ {
			found = true
		}
	}
	if !found {
		t.Errorf("DLQ %q not found in key list %v", wantDLQ, keys)
	}
}

// ─── Create ──────────────────────────────────────────────────────────────────

func TestManager_Create_CustomConfig(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	cfg := queue.DefaultConfig()
	cfg.MaxRetries = 7

	q, err := mgr.Create("ns", "jobs", cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if q == nil {
		t.Fatal("Create returned nil")
	}
}

func TestManager_Create_DuplicateReturnsError(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	if _, err := mgr.Create("ns", "orders", queue.DefaultConfig()); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := mgr.Create("ns", "orders", queue.DefaultConfig())
	if err == nil {
		t.Fatal("expected error on duplicate Create, got nil")
	}
	if !errors.Is(err, queue.ErrQueueExists) {
		t.Errorf("expected ErrQueueExists, got %v", err)
	}
}

// ─── Get ─────────────────────────────────────────────────────────────────────

func TestManager_Get_AfterCreate(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	_, _ = mgr.GetOrCreate("ns", "orders")

	q, err := mgr.Get("ns", "orders")
	if err != nil || q == nil {
		t.Fatalf("Get: err=%v q=%v", err, q)
	}
}

func TestManager_Get_NotFound(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	_, err := mgr.Get("ns", "nope")
	if err == nil {
		t.Fatal("expected ErrQueueNotFound, got nil")
	}
	if !errors.Is(err, queue.ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, got %v", err)
	}
}

// ─── Delete ──────────────────────────────────────────────────────────────────

func TestManager_Delete_RemovesQueueAndDLQ(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	if _, err := mgr.GetOrCreate("ns", "orders"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	if err := mgr.Delete("ns", "orders"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := mgr.Get("ns", "orders"); !errors.Is(err, queue.ErrQueueNotFound) {
		t.Errorf("Get after Delete: expected ErrQueueNotFound, got %v", err)
	}
}

func TestManager_Delete_NotFound(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	err := mgr.Delete("ns", "ghost")
	if !errors.Is(err, queue.ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, got %v", err)
	}
}

// ─── List ────────────────────────────────────────────────────────────────────

func TestManager_List(t *testing.T) {
	mgr := queue.NewManager(newFactory(t.TempDir()), nil, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	_, _ = mgr.GetOrCreate("ns", "a")
	_, _ = mgr.GetOrCreate("ns", "b")

	keys := mgr.List()
	// Expect 4 entries: a, __dlq__a, b, __dlq__b
	if len(keys) != 4 {
		t.Errorf("List: want 4 keys, got %d: %v", len(keys), keys)
	}
}

// ─── Scheduler integration ───────────────────────────────────────────────────

func TestManager_WithScheduler_PromotesScheduledMessage(t *testing.T) {
	sched := scheduler.New()
	base := t.TempDir()
	mgr := queue.NewManager(newFactory(base), sched, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	ctx := t.Context()
	sched.Start(ctx, mgr.SchedulerReadyFn())

	q, err := mgr.GetOrCreate("ns", "orders")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// Publish a message scheduled 100ms in the future.
	msg := &queue.Message{
		ID:          "test-sched-01",
		Namespace:   "ns",
		Queue:       "orders",
		Body:        []byte("hello"),
		PublishedAt: time.Now().UnixMilli(),
		DeliverAt:   time.Now().Add(100 * time.Millisecond).UnixMilli(),
		MaxRetries:  3,
	}
	if err := q.Publish(msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Message must not be in READY list immediately.
	if q.Len() != 0 {
		t.Fatalf("message appeared in ready queue before deliverAt")
	}

	// Wait for the scheduler to promote it (up to 2s).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if q.Len() == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if q.Len() != 1 {
		t.Fatalf("message not promoted after deliverAt, Len=%d", q.Len())
	}

	res, err := q.Dequeue(0)
	if err != nil || res == nil {
		t.Fatalf("Dequeue: err=%v res=%v", err, res)
	}
	if res.Message.ID != msg.ID {
		t.Errorf("promoted message ID: want %s got %s", msg.ID, res.Message.ID)
	}
}

// TestManager_AllStats_IncludesScheduled verifies that AllStats reports the
// Scheduled counter for messages that are in the Min-Heap awaiting delivery.
func TestManager_AllStats_IncludesScheduled(t *testing.T) {
	sched := scheduler.New()
	base := t.TempDir()
	mgr := queue.NewManager(newFactory(base), sched, queue.DefaultConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	ctx := t.Context()
	sched.Start(ctx, mgr.SchedulerReadyFn())

	q, err := mgr.GetOrCreate("ns", "jobs")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	// One immediate message + one 60 s future message.
	if err := q.Publish(&queue.Message{
		ID: "now-01", Namespace: "ns", Queue: "jobs",
		Body: []byte("now"), PublishedAt: time.Now().UnixMilli(), MaxRetries: 3,
	}); err != nil {
		t.Fatalf("Publish immediate: %v", err)
	}
	if err := q.Publish(&queue.Message{
		ID: "fut-01", Namespace: "ns", Queue: "jobs",
		Body: []byte("future"), PublishedAt: time.Now().UnixMilli(),
		DeliverAt: time.Now().Add(60 * time.Second).UnixMilli(), MaxRetries: 3,
	}); err != nil {
		t.Fatalf("Publish future: %v", err)
	}

	snaps := mgr.AllStats()

	// Find the ns/jobs snapshot (skip DLQ entries).
	var snap *queue.QueueSnapshot
	for i := range snaps {
		if snaps[i].Key == "ns/jobs" {
			snap = &snaps[i]
			break
		}
	}
	if snap == nil {
		t.Fatal("ns/jobs not found in AllStats")
	}
	if snap.Ready != 1 {
		t.Errorf("Ready: want 1, got %d", snap.Ready)
	}
	if snap.Scheduled != 1 {
		t.Errorf("Scheduled: want 1, got %d", snap.Scheduled)
	}
}

// ─── IsDLQ ───────────────────────────────────────────────────────────────────

func TestIsDLQ(t *testing.T) {
	if !queue.IsDLQ(queue.DLQName("orders")) {
		t.Error("IsDLQ: DLQName(orders) should return true")
	}
	if queue.IsDLQ("orders") {
		t.Error("IsDLQ: 'orders' should return false")
	}
	if queue.IsDLQ("") {
		t.Error("IsDLQ: empty string should return false")
	}
}
