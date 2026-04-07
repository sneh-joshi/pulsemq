package broker_test

import (
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
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
