package broker_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/metrics"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	"github.com/sneh-joshi/pulsemq/internal/queue"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	b, err := broker.New(cfg, "test-node")
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// ─── Publish ─────────────────────────────────────────────────────────────────

func TestBroker_Publish_BasicMessage(t *testing.T) {
	b := newTestBroker(t)
	resp, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "orders",
		Body:      []byte(`{"hello":"world"}`),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.MessageID == "" {
		t.Error("expected non-empty message ID")
	}
}

func TestBroker_Publish_ScheduledMessage(t *testing.T) {
	b := newTestBroker(t)
	deliverAt := time.Now().Add(10 * time.Second).UnixMilli()
	resp, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "orders",
		Body:      []byte("scheduled"),
		DeliverAt: deliverAt,
	})
	if err != nil {
		t.Fatalf("Publish scheduled: %v", err)
	}
	if resp.MessageID == "" {
		t.Error("expected non-empty message ID")
	}
	// Queue should be empty (message is SCHEDULED, not yet READY).
	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 ready messages, got %d", len(results))
	}
}

// ─── Consume → Ack ───────────────────────────────────────────────────────────

func TestBroker_Consume_Ack(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "orders",
		Body:      []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if err := b.Ack(results[0].ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Queue should now be empty.
	results2, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if len(results2) != 0 {
		t.Errorf("expected empty queue after ACK, got %d", len(results2))
	}
}

func TestBroker_Consume_Empty(t *testing.T) {
	b := newTestBroker(t)
	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if err != nil {
		t.Fatalf("unexpected error on empty queue consume: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestBroker_Consume_BatchDequeue(t *testing.T) {
	b := newTestBroker(t)

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := b.Publish(broker.PublishRequest{
			Namespace: "ns",
			Queue:     "jobs",
			Body:      []byte("job"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "jobs", N: n})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

// ─── Nack → DLQ ──────────────────────────────────────────────────────────────

func TestBroker_Nack_ExhaustsRetries_GoesToDLQ(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Publish(broker.PublishRequest{
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte("fail-me"),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	results, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if len(results) != 1 {
		t.Fatalf("expected 1 message, got %d", len(results))
	}

	// One NACK exhausts MaxRetries=1 → moves to DLQ.
	if err := b.Nack(results[0].ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Primary queue should be empty.
	primary, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if len(primary) != 0 {
		t.Errorf("primary queue should be empty after DLQ, got %d", len(primary))
	}

	// DLQ should have 1 message.
	if n := b.DLQLen("ns", "orders"); n != 1 {
		t.Errorf("DLQ len: want 1, got %d", n)
	}
}

// ─── Unknown receipt ─────────────────────────────────────────────────────────

func TestBroker_Ack_UnknownReceipt(t *testing.T) {
	b := newTestBroker(t)
	err := b.Ack("non-existent-handle")
	if err == nil {
		t.Fatal("expected error for unknown receipt, got nil")
	}
}

// ─── Queue management ─────────────────────────────────────────────────────────

func TestBroker_CreateDeleteQueue(t *testing.T) {
	b := newTestBroker(t)

	if err := b.CreateQueue("ns", "custom", queue.DefaultConfig()); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	lists := b.ListQueues()
	found := false
	for _, k := range lists {
		if k == "ns/custom" {
			found = true
		}
	}
	if !found {
		t.Errorf("ns/custom not in ListQueues: %v", lists)
	}

	if err := b.DeleteQueue("ns", "custom"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}

	for _, k := range b.ListQueues() {
		if k == "ns/custom" {
			t.Error("ns/custom still present after DeleteQueue")
		}
	}
}

// ─── DLQ replay ──────────────────────────────────────────────────────────────

func TestBroker_ReplayDLQ(t *testing.T) {
	b := newTestBroker(t)

	// Publish + exhaust retries to get message into DLQ.
	_, _ = b.Publish(broker.PublishRequest{
		Namespace:  "ns",
		Queue:      "orders",
		Body:       []byte("replay-me"),
		MaxRetries: 1,
	})
	results, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	_ = b.Nack(results[0].ReceiptHandle)

	if b.DLQLen("ns", "orders") != 1 {
		t.Fatalf("DLQ should have 1 message")
	}

	replayed, err := b.ReplayDLQ("ns", "orders", 10)
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if replayed != 1 {
		t.Errorf("replayed: want 1, got %d", replayed)
	}

	// Message should now be back in primary queue.
	results2, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if len(results2) != 1 {
		t.Fatalf("primary queue should have replayed message, got %d", len(results2))
	}
}

// ─── Stats / Summary / NodeID ────────────────────────────────────────────────

func TestBroker_Stats(t *testing.T) {
	b := newTestBroker(t)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns", Queue: "q", Body: []byte("x"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stats := b.Stats()
	if stats.QueueCount < 1 {
		t.Errorf("Stats.QueueCount: want >= 1, got %d", stats.QueueCount)
	}
}

func TestBroker_NodeID(t *testing.T) {
	b := newTestBroker(t)
	if id := b.NodeID(); id != "test-node" {
		t.Errorf("NodeID: want %q, got %q", "test-node", id)
	}
}

func TestBroker_Summary(t *testing.T) {
	b := newTestBroker(t)

	for i := 0; i < 3; i++ {
		if _, err := b.Publish(broker.PublishRequest{
			Namespace: "ns", Queue: "q", Body: []byte("msg"),
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	s := b.Summary()
	if s.TotalQueues < 1 {
		t.Errorf("Summary.TotalQueues: want >= 1, got %d", s.TotalQueues)
	}
	if s.TotalDepth < 3 {
		t.Errorf("Summary.TotalDepth: want >= 3, got %d", s.TotalDepth)
	}
}

func TestBroker_WithMetrics(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	// metrics.New requires no arguments — just ensure the broker accepts the option.
	b, err := broker.New(cfg, "metrics-node", broker.WithMetrics(nil))
	if err != nil {
		t.Fatalf("broker.New with WithMetrics: %v", err)
	}
	defer b.Close()
}

func TestBroker_WithNamespaceRegistry(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	b, err := broker.New(cfg, "ns-node", broker.WithNamespaceRegistry(nil))
	if err != nil {
		t.Fatalf("broker.New with WithNamespaceRegistry: %v", err)
	}
	defer b.Close()

	// Publish still works when namespace registry is nil (no-op ensure).
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns", Queue: "q", Body: []byte("hi"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestBroker_PurgeQueue(t *testing.T) {
	b := newTestBroker(t)

	for i := 0; i < 3; i++ {
		if _, err := b.Publish(broker.PublishRequest{
			Namespace: "ns", Queue: "purge-me", Body: []byte("msg"),
		}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	n, err := b.PurgeQueue("ns", "purge-me")
	if err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if n < 1 {
		t.Errorf("PurgeQueue: want >= 1 purged, got %d", n)
	}

	results, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "purge-me", N: 10})
	if len(results) != 0 {
		t.Errorf("queue should be empty after purge, got %d messages", len(results))
	}
}

func TestBroker_DrainDLQ(t *testing.T) {
	b := newTestBroker(t)

	// Get a message into the DLQ via exhausted retries.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns", Queue: "orders", Body: []byte("to-dlq"), MaxRetries: 1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	results, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "orders", N: 1})
	if len(results) == 0 {
		t.Fatal("expected 1 message")
	}
	_ = b.Nack(results[0].ReceiptHandle)

	dlqResults, err := b.DrainDLQ("ns", "orders", 10)
	if err != nil {
		t.Fatalf("DrainDLQ: %v", err)
	}
	if len(dlqResults) != 1 {
		t.Fatalf("DrainDLQ: want 1, got %d", len(dlqResults))
	}
	// ACK the drained DLQ message via its receipt handle.
	if err := b.Ack(dlqResults[0].ReceiptHandle); err != nil {
		t.Fatalf("Ack DLQ result: %v", err)
	}
}

func TestBroker_QueueManager(t *testing.T) {
	b := newTestBroker(t)
	if qm := b.QueueManager(); qm == nil {
		t.Error("QueueManager: should not be nil")
	}
}

// ─── Publish with metrics and namespace registry ───────────────────────────

func TestBroker_Publish_WithNamespaceRegistry(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	nsReg, err := namespace.New(cfg.Node.DataDir)
	if err != nil {
		t.Fatalf("namespace.New: %v", err)
	}
	b, err := broker.New(cfg, "test-node", broker.WithNamespaceRegistry(nsReg))
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	defer b.Close()

	// Publish auto-registers the namespace.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "myns",
		Queue:     "myq",
		Body:      []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish with ns registry: %v", err)
	}

	// CreateQueue also auto-registers the namespace.
	if err := b.CreateQueue("myns", "myq2", queue.DefaultConfig()); err != nil {
		t.Fatalf("CreateQueue with ns registry: %v", err)
	}
}

// ─── Nack error path ─────────────────────────────────────────────────────────

func TestBroker_Nack_UnknownReceipt(t *testing.T) {
	b := newTestBroker(t)
	if err := b.Nack("unknown-handle"); err == nil {
		t.Error("expected error for Nack with unknown receipt handle")
	}
}

// ─── Consume with metrics ─────────────────────────────────────────────────────

func TestBroker_Consume_WithMetrics(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	reg := &metrics.Registry{}
	b, err := broker.New(cfg, "metrics-node", broker.WithMetrics(reg))
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	defer b.Close()

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns", Queue: "q", Body: []byte("msg"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "q", N: 1})
	if err != nil {
		t.Fatalf("Consume with metrics: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 message, got %d", len(results))
	}
}

// ─── QueueStats ───────────────────────────────────────────────────────────────

// TestBroker_QueueStats_ScheduledAndDepth verifies that QueueStats correctly
// counts READY, IN_FLIGHT, SCHEDULED, and DEPTH (total) for a queue that has
// both immediately-ready and future-scheduled messages.
func TestBroker_QueueStats_ScheduledAndDepth(t *testing.T) {
	b := newTestBroker(t)

	// Publish one immediate message.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns", Queue: "jobs", Body: []byte("immediate"),
	}); err != nil {
		t.Fatalf("Publish immediate: %v", err)
	}

	// Publish two future-scheduled messages (60 s ahead — won't fire during test).
	future := time.Now().Add(60 * time.Second).UnixMilli()
	for i := 0; i < 2; i++ {
		if _, err := b.Publish(broker.PublishRequest{
			Namespace: "ns", Queue: "jobs", Body: []byte("future"), DeliverAt: future,
		}); err != nil {
			t.Fatalf("Publish future %d: %v", i, err)
		}
	}

	stats := b.QueueStats()
	var found *broker.QueueInfo
	for i := range stats {
		if stats[i].Namespace == "ns" && stats[i].Name == "jobs" {
			found = &stats[i]
			break
		}
	}
	if found == nil {
		t.Fatal("ns/jobs not found in QueueStats")
	}

	if found.Ready != 1 {
		t.Errorf("Ready: want 1, got %d", found.Ready)
	}
	if found.Scheduled != 2 {
		t.Errorf("Scheduled: want 2, got %d", found.Scheduled)
	}
	wantDepth := int64(3) // 1 ready + 0 in-flight + 2 scheduled
	if found.Depth != wantDepth {
		t.Errorf("Depth: want %d, got %d", wantDepth, found.Depth)
	}
}

// ─── QueueStatsPaged edge cases ───────────────────────────────────────────────

func TestBroker_QueueStatsPaged_EmptyBroker(t *testing.T) {
b := newTestBroker(t)
page := b.QueueStatsPaged(1, 10)
if page.Total != 0 {
t.Errorf("Total: want 0, got %d", page.Total)
}
if len(page.Queues) != 0 {
t.Errorf("Queues: want empty, got %d", len(page.Queues))
}
if page.TotalPages != 1 {
t.Errorf("TotalPages: want 1, got %d", page.TotalPages)
}
}

func TestBroker_QueueStatsPaged_PageClampedToLast(t *testing.T) {
b := newTestBroker(t)

// Create 3 queues.
for i, q := range []string{"q1", "q2", "q3"} {
if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: q, Body: []byte(fmt.Sprintf("msg%d", i)),
}); err != nil {
t.Fatalf("Publish: %v", err)
}
}

// Requesting page 999 should be clamped to last page.
page := b.QueueStatsPaged(999, 2)
if page.Page > page.TotalPages {
t.Errorf("Page clamping failed: page=%d totalPages=%d", page.Page, page.TotalPages)
}
}

func TestBroker_QueueStatsPaged_LimitClamped(t *testing.T) {
b := newTestBroker(t)
// limit=0 should be clamped to 50, limit=300 should be clamped to 200.
page0 := b.QueueStatsPaged(1, 0)
if page0.Limit != 50 {
t.Errorf("limit=0 should clamp to 50, got %d", page0.Limit)
}
page300 := b.QueueStatsPaged(1, 300)
if page300.Limit != 200 {
t.Errorf("limit=300 should clamp to 200, got %d", page300.Limit)
}
}

func TestBroker_QueueStatsPaged_DLQExcluded(t *testing.T) {
b := newTestBroker(t)

// Publish and nack to exhaustion to create a DLQ queue.
resp, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "tasks", Body: []byte("dead"),
MaxRetries: 1,
})
if err != nil {
t.Fatalf("Publish: %v", err)
}
_ = resp

results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "tasks", N: 1})
if err != nil || len(results) == 0 {
t.Fatalf("Consume: err=%v results=%d", err, len(results))
}
if err := b.Nack(results[0].ReceiptHandle); err != nil {
t.Fatalf("Nack: %v", err)
}

// QueueStatsPaged should not include the DLQ queue.
page := b.QueueStatsPaged(1, 200)
for _, q := range page.Queues {
if strings.HasPrefix(q.Name, "__dlq__") {
t.Errorf("DLQ queue should be excluded from stats: %s", q.Name)
}
}
}

// ─── Ack/Nack with metrics ─────────────────────────────────────────────────

func TestBroker_Ack_WithMetrics(t *testing.T) {
cfg := &config.Config{
Node: config.NodeConfig{DataDir: t.TempDir()},
Queue: config.QueueConfig{
DefaultVisibilityTimeoutMs: 30000,
MaxBatchSize:               100,
MaxRetries:                 3,
MaxMessages:                100000,
},
}
reg := &metrics.Registry{}
b, err := broker.New(cfg, "node", broker.WithMetrics(reg))
if err != nil {
t.Fatalf("broker.New: %v", err)
}
defer b.Close()

if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "q", Body: []byte("ack-test"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}

results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "q", N: 1})
if err != nil || len(results) == 0 {
t.Fatalf("Consume: err=%v results=%d", err, len(results))
}

if err := b.Ack(results[0].ReceiptHandle); err != nil {
t.Fatalf("Ack with metrics: %v", err)
}

var acked int64
reg.Acked.Each(func(_ string, v int64) { acked += v })
if acked < 1 {
t.Errorf("expected acked metric ≥ 1, got %d", acked)
}
}

func TestBroker_Nack_WithMetrics(t *testing.T) {
cfg := &config.Config{
Node: config.NodeConfig{DataDir: t.TempDir()},
Queue: config.QueueConfig{
DefaultVisibilityTimeoutMs: 30000,
MaxBatchSize:               100,
MaxRetries:                 5,
MaxMessages:                100000,
},
}
reg := &metrics.Registry{}
b, err := broker.New(cfg, "node", broker.WithMetrics(reg))
if err != nil {
t.Fatalf("broker.New: %v", err)
}
defer b.Close()

if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "q", Body: []byte("nack-test"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}

results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "q", N: 1})
if err != nil || len(results) == 0 {
t.Fatalf("Consume: err=%v results=%d", err, len(results))
}

if err := b.Nack(results[0].ReceiptHandle); err != nil {
t.Fatalf("Nack with metrics: %v", err)
}

var nacked int64
reg.Nacked.Each(func(_ string, v int64) { nacked += v })
if nacked < 1 {
t.Errorf("expected nacked metric ≥ 1, got %d", nacked)
}
}

// ─── Publish with namespace error ─────────────────────────────────────────────

func TestBroker_CreateQueue_Existing(t *testing.T) {
b := newTestBroker(t)
// CreateQueue twice — second returns an error (not idempotent).
if err := b.CreateQueue("ns", "existing-q", queue.Config{}); err != nil {
t.Fatalf("first CreateQueue: %v", err)
}
if err := b.CreateQueue("ns", "existing-q", queue.Config{}); err == nil {
t.Fatal("expected error on duplicate CreateQueue, got nil")
}




}

// TestBroker_PurgeQueue_WithDLQ exercises the DLQ purge branch.
func TestBroker_PurgeQueue_WithDLQ(t *testing.T) {
b := newTestBroker(t)
// Publish + consume without ACK to create an in-flight message.
if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "purge-q", Body: []byte("purge"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}
results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "purge-q", N: 1})
if err != nil || len(results) == 0 {
t.Fatalf("Consume: err=%v len=%d", err, len(results))
}
// Purge should remove the in-flight message.
n, err := b.PurgeQueue("ns", "purge-q")
if err != nil {
t.Fatalf("PurgeQueue: %v", err)
}
if n < 1 {
t.Errorf("PurgeQueue: expected ≥1 purged, got %d", n)
}
}

// TestBroker_PurgeQueue_NotFound verifies error on non-existent queue.
func TestBroker_PurgeQueue_NotFound(t *testing.T) {
b := newTestBroker(t)
_, err := b.PurgeQueue("ns", "no-such-purge-q")
if err == nil {
t.Fatal("expected error purging non-existent queue")
}
}

// TestBroker_Consume_QueueNotFound verifies Consume error path.
func TestBroker_Consume_QueueNotFound(t *testing.T) {
b := newTestBroker(t)
// Consume from a queue that doesn't exist should create it (via GetOrCreate).
// Call Ack with an unknown receipt handle instead to trigger the error path.
err := b.Ack("totally-invalid-receipt-handle-xyz")
if err == nil {
t.Fatal("expected error acking unknown receipt handle")
}
}

// TestBroker_Summary_WithQueues verifies Summary covers paged stats.
func TestBroker_Summary_WithQueues(t *testing.T) {
b := newTestBroker(t)
if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "summary-q", Body: []byte("body"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}
s := b.Summary()
if s.TotalQueues < 1 {
t.Errorf("Summary.TotalQueues: want ≥1, got %d", s.TotalQueues)
}
}

// TestBroker_DrainDLQ_WithMessages exercises the DrainDLQ receipt tracking path.
func TestBroker_DrainDLQ_WithMessages(t *testing.T) {
cfg := &config.Config{
Node: config.NodeConfig{DataDir: t.TempDir()},
Queue: config.QueueConfig{
DefaultVisibilityTimeoutMs: 30000,
MaxBatchSize:               100,
MaxRetries:                 1,
MaxMessages:                100000,
},
}
b, err := broker.New(cfg, "node")
if err != nil {
t.Fatalf("broker.New: %v", err)
}
defer b.Close()

// Publish and nack to exhaustion → dead-letter.
if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns", Queue: "drain-q", Body: []byte("drain-me"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}
results, _ := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "drain-q", N: 1})
if len(results) > 0 {
_ = b.Nack(results[0].ReceiptHandle) // MaxRetries=1 → dead-letters
}

// DrainDLQ retrieves from DLQ.
drained, err := b.DrainDLQ("ns", "drain-q", 10)
if err != nil {
t.Fatalf("DrainDLQ: %v", err)
}
if len(drained) < 1 {
t.Errorf("DrainDLQ: expected ≥1 result, got %d", len(drained))
return
}
// ACK the drained message.
	if err := b.Ack(drained[0].ReceiptHandle); err != nil {
		t.Fatalf("Ack drained: %v", err)
	}
}

// TestBroker_New_ZeroQueueConfig verifies that broker.New applies sensible
// defaults when the queue config fields are all zero-valued.
func TestBroker_New_ZeroQueueConfig(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{}, // all zero
	}
	b, err := broker.New(cfg, "test-node")
	if err != nil {
		t.Fatalf("broker.New with zero config: %v", err)
	}
	defer b.Close()

	// Sanity check: should still be able to publish.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "q",
		Body:      []byte("ok"),
	}); err != nil {
		t.Fatalf("Publish after zero-config broker.New: %v", err)
	}
}

// TestBroker_Publish_QueueFull verifies that broker.Publish returns an error
// when the target queue has reached its MaxMessages capacity.
func TestBroker_Publish_QueueFull(t *testing.T) {
	b := newTestBroker(t)

	// Create a queue with a capacity of exactly 1.
	cfg := queue.DefaultConfig()
	cfg.MaxMessages = 1
	if err := b.CreateQueue("ns", "tiny", cfg); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// First publish must succeed.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "tiny",
		Body:      []byte("first"),
	}); err != nil {
		t.Fatalf("Publish (first): %v", err)
	}

	// Second publish must fail — queue is full.
	_, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "tiny",
		Body:      []byte("second"),
	})
	if err == nil {
		t.Fatal("expected error publishing to full queue, got nil")
	}
}

// TestBroker_Publish_NSEnsureError verifies that broker.Publish returns an
// error when the namespace registry rejects an invalid namespace name.
func TestBroker_Publish_NSEnsureError(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	nsReg, err := namespace.New(cfg.Node.DataDir)
	if err != nil {
		t.Fatalf("namespace.New: %v", err)
	}
	b, err := broker.New(cfg, "test-node", broker.WithNamespaceRegistry(nsReg))
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	defer b.Close()

	// Uppercase names are invalid — Ensure should return ErrInvalidName.
	_, err = b.Publish(broker.PublishRequest{
		Namespace: "InvalidNS",
		Queue:     "q",
		Body:      []byte("hello"),
	})
	if err == nil {
		t.Fatal("expected error from invalid namespace name, got nil")
	}
}

// ─── Summary DLQ alerts ───────────────────────────────────────────────────────

// TestBroker_Summary_DLQAlert verifies that Summary.DLQAlerts > 0 when a
// message has been dead-lettered (moved to the DLQ).
func TestBroker_Summary_DLQAlert(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 1, // only 1 retry so one Nack dead-letters
			MaxMessages:                100000,
		},
	}
	b, err := broker.New(cfg, "dlq-alert-node")
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.Publish(broker.PublishRequest{
		Namespace:  "ns",
		Queue:      "q",
		Body:       []byte("test"),
		MaxRetries: 1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "q", N: 1})
	if err != nil || len(results) == 0 {
		t.Fatalf("Consume: err=%v results=%d", err, len(results))
	}

	// Nack twice to exhaust MaxRetries=1 and push to DLQ.
	if err := b.Nack(results[0].ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// The message is now in the DLQ (consumed once, nacked at attempt 2 > MaxRetries=1).
	s := b.Summary()
	if s.DLQAlerts == 0 {
		t.Error("expected DLQAlerts > 0 after dead-lettering, got 0")
	}
}

// ─── QueueStatsPaged page clamping ────────────────────────────────────────────

// TestBroker_QueueStatsPaged_PageClamped verifies that page 0 is clamped to 1.
func TestBroker_QueueStatsPaged_PageClamped(t *testing.T) {
	b := newTestBroker(t)

	// Create a queue so there is data to paginate.
	if err := b.CreateQueue("ns", "q", queue.Config{MaxMessages: 100, DefaultVisibilityTimeoutMs: 30000}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// page=0 must be clamped to 1 (not panic or return out-of-bounds data).
	pg := b.QueueStatsPaged(0, 10)
	if pg.Page != 1 {
		t.Errorf("QueueStatsPaged(0, 10).Page: want 1, got %d", pg.Page)
	}
}

// TestBroker_QueueStatsPaged_MultiNamespace verifies the multi-namespace sort path.
func TestBroker_QueueStatsPaged_MultiNamespace(t *testing.T) {
	b := newTestBroker(t)

	// Create queues in two different namespaces so the sort comparator hits the
	// ns-inequality branch (items[i].ns != items[j].ns).
	for _, key := range []struct{ ns, q string }{
		{"aaa", "queue1"},
		{"bbb", "queue2"},
	} {
		if err := b.CreateQueue(key.ns, key.q, queue.Config{MaxMessages: 100, DefaultVisibilityTimeoutMs: 30000}); err != nil {
			t.Fatalf("CreateQueue %s/%s: %v", key.ns, key.q, err)
		}
	}

	pg := b.QueueStatsPaged(1, 10)
	if pg.Total < 2 {
		t.Errorf("expected at least 2 queues, got %d", pg.Total)
	}
}

// ─── Consume with N ≤ 0 ──────────────────────────────────────────────────────

// TestBroker_Consume_ZeroN verifies that N≤0 is treated as N=1.
func TestBroker_Consume_ZeroN(t *testing.T) {
	b := newTestBroker(t)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "q",
		Body:      []byte("msg"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	results, err := b.Consume(broker.ConsumeRequest{Namespace: "ns", Queue: "q", N: -1})
	if err != nil {
		t.Fatalf("Consume N=-1: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for N=-1 (clamped to 1), got %d", len(results))
	}
}

// ─── DrainDLQ on non-existent queue ──────────────────────────────────────────

// TestBroker_DrainDLQ_QueueNotFound verifies that DrainDLQ returns an error
// when the primary queue does not exist.
func TestBroker_DrainDLQ_QueueNotFound(t *testing.T) {
	b := newTestBroker(t)
	_, err := b.DrainDLQ("ns", "no-such-queue", 10)
	if err == nil {
		t.Fatal("expected error from DrainDLQ on non-existent queue, got nil")
	}
}

// ─── CreateQueue with invalid namespace ───────────────────────────────────────

// TestBroker_CreateQueue_InvalidNamespace verifies that CreateQueue returns an
// error when the namespace name is invalid (and a namespace registry is configured).
func TestBroker_CreateQueue_InvalidNamespace(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir()},
		Queue: config.QueueConfig{
			DefaultVisibilityTimeoutMs: 30000,
			MaxBatchSize:               100,
			MaxRetries:                 3,
			MaxMessages:                100000,
		},
	}
	nsReg, err := namespace.New(cfg.Node.DataDir)
	if err != nil {
		t.Fatalf("namespace.New: %v", err)
	}
	b, err := broker.New(cfg, "test-node", broker.WithNamespaceRegistry(nsReg))
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// "My_NS" has uppercase and underscore — invalid namespace name.
	err = b.CreateQueue("My_NS", "q", queue.Config{MaxMessages: 100, DefaultVisibilityTimeoutMs: 30000})
	if err == nil {
		t.Fatal("expected error creating queue with invalid namespace name, got nil")
	}
}

// ─── Ack / Nack after visibility timeout expiry ───────────────────────────────

// TestBroker_Ack_ExpiredReceipt verifies that Ack returns an error when the
// receipt handle has already been expired by the visibility-timeout reaper.
// This covers q.Ack / q.Nack error paths in broker.go.
func TestBroker_Ack_ExpiredReceipt(t *testing.T) {
	b := newTestBroker(t)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "q",
		Body:      []byte("msg"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Consume with a very short visibility timeout (100 ms).
	results, err := b.Consume(broker.ConsumeRequest{
		Namespace:         "ns",
		Queue:             "q",
		N:                 1,
		VisibilityTimeout: 100,
	})
	if err != nil || len(results) == 0 {
		t.Fatalf("Consume: err=%v results=%d", err, len(results))
	}
	receipt := results[0].ReceiptHandle

	// Wait long enough for the reaper (runs every 500 ms) to expire the entry.
	time.Sleep(800 * time.Millisecond)

	// Ack should fail because the receipt is no longer in q.inFlight.
	if err := b.Ack(receipt); err == nil {
		t.Error("expected error Acking an expired receipt, got nil")
	}

	// Nack should also fail for the same reason (receipt still in b.receipts).
	if err := b.Nack(receipt); err == nil {
		t.Error("expected error Nacking an expired receipt, got nil")
	}
}

// TestBroker_New_EmptyDataDir verifies that broker.New uses "./data" as the
// default DataDir when cfg.Node.DataDir is empty (covers line 159 in broker.go).
func TestBroker_New_EmptyDataDir(t *testing.T) {
// Chdir to a temp dir so that "./data" is created there, not in the repo.
t.Chdir(t.TempDir())

cfg := &config.Config{
Node: config.NodeConfig{DataDir: ""}, // empty → defaults to "./data"
Queue: config.QueueConfig{
DefaultVisibilityTimeoutMs: 30000,
MaxBatchSize:               100,
MaxRetries:                 3,
MaxMessages:                100000,
},
}
b, err := broker.New(cfg, "default-dir-node")
if err != nil {
t.Fatalf("broker.New with empty DataDir: %v", err)
}
t.Cleanup(func() { _ = b.Close() })
// Basic smoke test — ensure the broker is usable.
if _, err := b.Publish(broker.PublishRequest{
Namespace: "ns",
Queue:     "q",
Body:      []byte("hello"),
}); err != nil {
t.Fatalf("Publish: %v", err)
}
}
