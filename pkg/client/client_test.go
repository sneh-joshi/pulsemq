package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
	"github.com/sneh-joshi/pulsemq/internal/metrics"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	transphttp "github.com/sneh-joshi/pulsemq/internal/transport/http"
	"github.com/sneh-joshi/pulsemq/pkg/client"
)

// ─── test server helpers ──────────────────────────────────────────────────────

// newTestEnv spins up a real PulseMQ stack (broker + HTTP) backed by httptest.Server.
// All resources are cleaned up in t.Cleanup.
func newTestEnv(t *testing.T) *client.Client {
	t.Helper()

	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir(), Host: "127.0.0.1", Port: 9999},
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
	metricsReg := &metrics.Registry{}

	b, err := broker.New(cfg, "test-node",
		broker.WithNamespaceRegistry(nsReg),
		broker.WithMetrics(metricsReg),
	)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	cm := consumer.NewManager(b)
	t.Cleanup(cm.Close)

	srv := transphttp.New(b, cm, cfg, nsReg, metricsReg)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return client.New(ts.URL)
}

// ctx is a convenience context for tests.
func ctx() context.Context { return context.Background() }

// ─── Namespace tests ──────────────────────────────────────────────────────────

func TestNamespace_CreateListDelete(t *testing.T) {
	c := newTestEnv(t)

	if err := c.CreateNamespace(ctx(), "payments"); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	nsList, err := c.ListNamespaces(ctx())
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	found := false
	for _, ns := range nsList {
		if ns.Name == "payments" {
			found = true
			if ns.CreatedAt.IsZero() {
				t.Error("CreatedAt should not be zero")
			}
		}
	}
	if !found {
		t.Fatalf("namespace 'payments' not in list: %v", nsList)
	}

	if err := c.DeleteNamespace(ctx(), "payments"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	nsList, _ = c.ListNamespaces(ctx())
	for _, ns := range nsList {
		if ns.Name == "payments" {
			t.Fatal("namespace should have been deleted")
		}
	}
}

func TestNamespace_DuplicateReturnsConflict(t *testing.T) {
	c := newTestEnv(t)

	_ = c.CreateNamespace(ctx(), "dup")
	err := c.CreateNamespace(ctx(), "dup")
	if !client.IsConflict(err) {
		t.Fatalf("want IsConflict, got %v", err)
	}
}

func TestNamespace_DeleteNotFound(t *testing.T) {
	c := newTestEnv(t)
	err := c.DeleteNamespace(ctx(), "ghost")
	if !client.IsNotFound(err) {
		t.Fatalf("want IsNotFound, got %v", err)
	}
}

// ─── Queue management tests ───────────────────────────────────────────────────

func TestQueue_CreateListDelete(t *testing.T) {
	c := newTestEnv(t)

	if err := c.CreateQueue(ctx(), "payments", "invoices"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	queues, err := c.ListQueues(ctx(), "payments")
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	found := false
	for _, q := range queues {
		if q == "payments/invoices" {
			found = true
		}
	}
	if !found {
		t.Fatalf("queue not found in list: %v", queues)
	}

	if err := c.DeleteQueue(ctx(), "payments", "invoices"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
}

func TestQueue_CreateWithOptions(t *testing.T) {
	c := newTestEnv(t)

	err := c.CreateQueue(ctx(), "ops", "jobs",
		client.WithQueueVisibilityTimeout(60*time.Second),
		client.WithQueueMaxRetries(5),
		client.WithQueueMaxBatchSize(50),
	)
	if err != nil {
		t.Fatalf("CreateQueue with options: %v", err)
	}
}

// ─── Publish / Consume / Ack tests ───────────────────────────────────────────

func TestPublish_Immediate(t *testing.T) {
	c := newTestEnv(t)

	id, err := c.Publish(ctx(), "ns", "q", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestPublish_WithMetadata(t *testing.T) {
	c := newTestEnv(t)

	id, err := c.Publish(ctx(), "ns", "q", []byte(`data`),
		client.WithMetadata(map[string]string{"source": "test", "priority": "high"}),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id == "" {
		t.Fatal("expected message ID")
	}
}

func TestPublishAndConsume_RoundTrip(t *testing.T) {
	c := newTestEnv(t)

	payload := []byte(`{"order":"123","amount":99}`)
	_, err := c.Publish(ctx(), "shop", "orders", payload)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgs, err := c.Consume(ctx(), "shop", "orders", 1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	m := msgs[0]
	if string(m.Body) != string(payload) {
		t.Fatalf("body mismatch: got %q, want %q", m.Body, payload)
	}
	if m.Namespace != "shop" || m.Queue != "orders" {
		t.Fatalf("namespace/queue mismatch: %s/%s", m.Namespace, m.Queue)
	}
	if m.ReceiptHandle == "" {
		t.Fatal("receipt handle must not be empty")
	}
	if m.PublishedAt.IsZero() {
		t.Fatal("PublishedAt must not be zero")
	}

	if err := c.Ack(ctx(), m.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// After ACK, the message should not be returned again.
	msgs2, err := c.Consume(ctx(), "shop", "orders", 1)
	if err != nil {
		t.Fatalf("second Consume: %v", err)
	}
	if len(msgs2) != 0 {
		t.Fatalf("expected 0 messages after ACK, got %d", len(msgs2))
	}
}

func TestConsume_EmptyQueue(t *testing.T) {
	c := newTestEnv(t)
	msgs, err := c.Consume(ctx(), "empty", "queue", 10)
	if err != nil {
		t.Fatalf("Consume empty queue: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(msgs))
	}
}

func TestPublishBatch(t *testing.T) {
	c := newTestEnv(t)

	bodies := [][]byte{
		[]byte(`msg1`),
		[]byte(`msg2`),
		[]byte(`msg3`),
	}
	ids, err := c.PublishBatch(ctx(), "batch", "q", bodies)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	for i, id := range ids {
		if id == "" {
			t.Errorf("ID[%d] is empty", i)
		}
	}
}

func TestNack_RequeuesMessage(t *testing.T) {
	c := newTestEnv(t)

	_, err := c.Publish(ctx(), "retry", "q", []byte(`retry-me`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msgs, err := c.Consume(ctx(), "retry", "q", 1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected 1 message")
	}

	if err := c.Nack(ctx(), msgs[0].ReceiptHandle); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// After NACK the message should become available again.
	msgs2, err := c.Consume(ctx(), "retry", "q", 1)
	if err != nil {
		t.Fatalf("second Consume: %v", err)
	}
	if len(msgs2) == 0 {
		t.Fatal("expected message to be requeued after NACK")
	}
}

// ─── Scheduled delivery ───────────────────────────────────────────────────────

func TestPublish_Scheduled_NotVisibleImmediately(t *testing.T) {
	c := newTestEnv(t)

	// Schedule 10 seconds in the future.
	_, err := c.Publish(ctx(), "sched", "q", []byte(`future`),
		client.WithDelay(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Publish scheduled: %v", err)
	}

	// Should get 0 messages immediately.
	msgs, err := c.Consume(ctx(), "sched", "q", 1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("scheduled message should not be visible yet, got %d", len(msgs))
	}
}

func TestPublish_WithDeliverAt_ImmediateIfPast(t *testing.T) {
	c := newTestEnv(t)

	past := time.Now().Add(-time.Minute)
	id, err := c.Publish(ctx(), "ns", "q", []byte(`past`),
		client.WithDeliverAt(past),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id == "" {
		t.Fatal("expected ID")
	}

	msgs, err := c.Consume(ctx(), "ns", "q", 1)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("past-delivered message not visible: got %d", len(msgs))
	}
}

// ─── DLQ tests ────────────────────────────────────────────────────────────────

func TestDLQ_DrainAndReplay(t *testing.T) {
	c := newTestEnv(t)

	// Create the queue with maxRetries=1 so the message goes to DLQ on first NACK
	// (attempt 1 → nextAttempt 2 > maxRetries 1).
	if err := c.CreateQueue(ctx(), "dlq-ns", "primary", client.WithQueueMaxRetries(1)); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// Publish two messages so one can be drained (verify) and one replayed.
	for i := 0; i < 2; i++ {
		if _, err := c.Publish(ctx(), "dlq-ns", "primary", []byte(`fail-me`)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Consume both and NACK them — both go to DLQ (maxRetries=1).
	msgs, err := c.Consume(ctx(), "dlq-ns", "primary", 2)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	for _, m := range msgs {
		if err := c.Nack(ctx(), m.ReceiptHandle); err != nil {
			t.Fatalf("Nack: %v", err)
		}
	}

	// Drain 1 message to verify the DLQ is populated.
	dlqMsgs, err := c.DrainDLQ(ctx(), "dlq-ns", "primary", 1)
	if err != nil {
		t.Fatalf("DrainDLQ: %v", err)
	}
	if len(dlqMsgs) == 0 {
		t.Fatal("expected message in DLQ")
	}

	// Replay the remaining message back to the primary queue.
	replayed, err := c.ReplayDLQ(ctx(), "dlq-ns", "primary", 10)
	if err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if replayed < 1 {
		t.Fatalf("expected at least 1 replayed, got %d", replayed)
	}
}

// ─── Health / Stats tests ─────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	c := newTestEnv(t)
	h, err := c.Health(ctx())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if h.Status != "ok" {
		t.Fatalf("status = %q, want ok", h.Status)
	}
	if h.NodeID == "" {
		t.Fatal("NodeID must not be empty")
	}
}

func TestStats(t *testing.T) {
	c := newTestEnv(t)

	_, _ = c.Publish(ctx(), "stats-ns", "jobs", []byte(`a`))
	_, _ = c.Publish(ctx(), "stats-ns", "jobs", []byte(`b`))

	stats, err := c.Stats(ctx())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	found := false
	for _, s := range stats {
		if s.Namespace == "stats-ns" && s.Name == "jobs" {
			found = true
			if s.Ready < 2 {
				t.Errorf("expected ready ≥ 2, got %d", s.Ready)
			}
		}
	}
	if !found {
		t.Fatalf("expected stats-ns/jobs in stats, got %v", stats)
	}
}

func TestStatsPaged(t *testing.T) {
	c := newTestEnv(t)

	_, _ = c.Publish(ctx(), "paged-ns", "tasks", []byte(`x`))
	_, _ = c.Publish(ctx(), "paged-ns", "tasks", []byte(`y`))

	// Page 1 with limit 50
	queues, total, err := c.StatsPaged(ctx(), 1, 50)
	if err != nil {
		t.Fatalf("StatsPaged: %v", err)
	}
	if total < 1 {
		t.Errorf("expected total ≥ 1, got %d", total)
	}
	if len(queues) < 1 {
		t.Errorf("expected at least 1 queue returned, got %d", len(queues))
	}
	found := false
	for _, s := range queues {
		if s.Namespace == "paged-ns" && s.Name == "tasks" {
			found = true
			if s.Ready < 2 {
				t.Errorf("expected ready ≥ 2, got %d", s.Ready)
			}
		}
	}
	if !found {
		t.Errorf("paged-ns/tasks not found in StatsPaged result")
	}

	// Requesting a high page number is clamped to last page — total remains consistent
	_, total2, err := c.StatsPaged(ctx(), 999, 50)
	if err != nil {
		t.Fatalf("StatsPaged page 999: %v", err)
	}
	if total2 != total {
		t.Errorf("total should be stable across pages: got %d vs %d", total2, total)
	}
}

// ─── APIError tests ───────────────────────────────────────────────────────────

func TestAPIError_IsNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /missing", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := client.New(ts.URL)
	err := c.DeleteNamespace(ctx(), "phantom")

	var ae *client.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.StatusCode != http.StatusNotFound {
		t.Fatalf("StatusCode = %d, want 404", ae.StatusCode)
	}
	if !client.IsNotFound(err) {
		t.Fatal("IsNotFound should return true")
	}
}

// ─── Client options tests ─────────────────────────────────────────────────────

func TestWithAPIKey_Passed(t *testing.T) {
	// Minimal server that requires X-Api-Key.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "mysecret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok", "node_id": "test", "queues": 0, "uptime_ms": 0, "version": "1.0",
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Without key → 401
	c1 := client.New(ts.URL)
	if _, err := c1.Health(ctx()); err == nil {
		t.Fatal("expected auth error without API key")
	}

	// With key → success
	c2 := client.New(ts.URL, client.WithAPIKey("mysecret"))
	if _, err := c2.Health(ctx()); err != nil {
		t.Fatalf("Health with API key: %v", err)
	}
}

func TestWithTimeout(t *testing.T) {
	c := client.New("http://localhost:1", client.WithTimeout(50*time.Millisecond))
	_, err := c.Health(ctx())
	if err == nil {
		t.Fatal("expected error on unreachable server")
	}
}

// ─── Subscribe / Unsubscribe ──────────────────────────────────────────────────

func TestSubscribe_AndUnsubscribe(t *testing.T) {
c := newTestEnv(t)

if err := c.CreateNamespace(ctx(), "events"); err != nil {
t.Fatalf("CreateNamespace: %v", err)
}
if err := c.CreateQueue(ctx(), "events", "clicks"); err != nil {
t.Fatalf("CreateQueue: %v", err)
}

id, err := c.Subscribe(ctx(), "events", "clicks", "http://localhost:9999/webhook", "")
if err != nil {
t.Fatalf("Subscribe: %v", err)
}
if id == "" {
t.Fatal("expected non-empty subscription ID")
}

if err := c.Unsubscribe(ctx(), id); err != nil {
t.Fatalf("Unsubscribe: %v", err)
}
}

// ─── APIError.Error ───────────────────────────────────────────────────────────

func TestAPIError_Error_String(t *testing.T) {
ae := &client.APIError{StatusCode: 404, Message: "not found"}
got := ae.Error()
if got != "pulsemq: server returned 404: not found" {
t.Errorf("Error(): got %q", got)
}
}

func TestAPIError_IsConflict(t *testing.T) {
ae := &client.APIError{StatusCode: 409, Message: "already exists"}
if !client.IsConflict(ae) {
t.Error("IsConflict should return true for 409")
}
if client.IsNotFound(ae) {
t.Error("IsNotFound should return false for 409")
}
}

// ─── Option functions ─────────────────────────────────────────────────────────

func TestWithHTTPClient(t *testing.T) {
custom := &http.Client{Timeout: 5 * time.Second}
c := client.New("http://localhost:1", client.WithHTTPClient(custom))
// The custom client has a very short timeout; connecting to :1 should fail.
_, err := c.Health(ctx())
if err == nil {
t.Fatal("expected error with custom http client on unreachable server")
}
}

func TestWithMaxRetries(t *testing.T) {
c := newTestEnv(t)

if err := c.CreateNamespace(ctx(), "retries"); err != nil {
t.Fatalf("CreateNamespace: %v", err)
}
if err := c.CreateQueue(ctx(), "retries", "work"); err != nil {
t.Fatalf("CreateQueue: %v", err)
}

id, err := c.Publish(ctx(), "retries", "work", []byte("payload"),
client.WithMaxRetries(5))
if err != nil {
t.Fatalf("Publish with WithMaxRetries: %v", err)
}
if id == "" {
t.Fatal("expected non-empty message ID")
}
}

func TestWithVisibilityTimeout(t *testing.T) {
c := newTestEnv(t)

if err := c.CreateNamespace(ctx(), "vt"); err != nil {
t.Fatalf("CreateNamespace: %v", err)
}
if err := c.CreateQueue(ctx(), "vt", "jobs"); err != nil {
t.Fatalf("CreateQueue: %v", err)
}
if _, err := c.Publish(ctx(), "vt", "jobs", []byte("body")); err != nil {
t.Fatalf("Publish: %v", err)
}

msgs, err := c.Consume(ctx(), "vt", "jobs", 1,
client.WithVisibilityTimeout(60*time.Second))
if err != nil {
t.Fatalf("Consume with WithVisibilityTimeout: %v", err)
}
if len(msgs) != 1 {
t.Fatalf("expected 1 message, got %d", len(msgs))
}
}

func TestWithQueueMaxMessages(t *testing.T) {
c := newTestEnv(t)

if err := c.CreateNamespace(ctx(), "maxmsg"); err != nil {
t.Fatalf("CreateNamespace: %v", err)
}
err := c.CreateQueue(ctx(), "maxmsg", "capped",
client.WithQueueMaxMessages(100))
if err != nil {
t.Fatalf("CreateQueue with WithQueueMaxMessages: %v", err)
}
}

// ─── Attempt field in consumed messages ──────────────────────────────────────

func TestConsume_AttemptFieldPopulated(t *testing.T) {
c := newTestEnv(t)

if err := c.CreateNamespace(ctx(), "atns"); err != nil {
t.Fatalf("CreateNamespace: %v", err)
}
if err := c.CreateQueue(ctx(), "atns", "atq"); err != nil {
t.Fatalf("CreateQueue: %v", err)
}
if _, err := c.Publish(ctx(), "atns", "atq", []byte("data")); err != nil {
t.Fatalf("Publish: %v", err)
}

msgs, err := c.Consume(ctx(), "atns", "atq", 1)
if err != nil {
t.Fatalf("Consume: %v", err)
}
if len(msgs) != 1 {
t.Fatalf("expected 1 message, got %d", len(msgs))
}
if msgs[0].Attempt < 1 {
t.Errorf("Attempt: want ≥1, got %d", msgs[0].Attempt)
}
}

// ─── toMessage non-base64 body fallback ─────────────────────────────────────

// TestConsume_NonBase64Body verifies the client gracefully handles a server
// returning a raw (non-base64) body in the consume response.
func TestConsume_NonBase64Body(t *testing.T) {
rawBody := "raw-text-not-base64!@#"
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
resp := map[string]any{
"messages": []map[string]any{
{
"id":             "01ABCDEFGHIJKLMNOPQRSTUVWX",
"body":           rawBody,
"receipt_handle": "rh-1",
"namespace":      "ns",
"queue":          "q",
"attempt":        1,
"published_at":   0,
},
},
}
_ = json.NewEncoder(w).Encode(resp)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
msgs, err := c.Consume(context.Background(), "ns", "q", 1)
if err != nil {
t.Fatalf("Consume: %v", err)
}
if len(msgs) != 1 {
t.Fatalf("expected 1 message, got %d", len(msgs))
}
if string(msgs[0].Body) != rawBody {
t.Errorf("body: want %q, got %q", rawBody, string(msgs[0].Body))
}
}

// TestListQueues_ServerError verifies ListQueues propagates server errors.
func TestListQueues_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"boom"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
if _, err := c.ListQueues(context.Background(), "ns"); err == nil {
t.Fatal("expected error from ListQueues on 500, got nil")
}
}

// TestPublishBatch_ServerError verifies PublishBatch propagates server errors.
func TestPublishBatch_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"batch failed"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
if _, err := c.PublishBatch(context.Background(), "ns", "q", [][]byte{[]byte("body")}); err == nil {
t.Fatal("expected error from PublishBatch on 500, got nil")
}
}

// TestDrainDLQ_ServerError verifies DrainDLQ propagates server errors.
func TestDrainDLQ_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"drain failed"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
if _, err := c.DrainDLQ(context.Background(), "ns", "q", 10); err == nil {
t.Fatal("expected error from DrainDLQ on 500, got nil")
}
}

// TestReplayDLQ_ServerError verifies ReplayDLQ propagates server errors.
func TestReplayDLQ_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"replay failed"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
if _, err := c.ReplayDLQ(context.Background(), "ns", "q", 10); err == nil {
t.Fatal("expected error from ReplayDLQ on 500, got nil")
}
}

// ─── Error path coverage (mock servers returning 500) ─────────────────────────

// TestPublish_ServerError verifies that Publish propagates server errors.
func TestPublish_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, err := c.Publish(context.Background(), "ns", "q", []byte("body"))
if err == nil {
t.Fatal("expected error from Publish on 500, got nil")
}
}

// TestPublishBatch_WithOptions verifies that the opts loop in PublishBatch
// executes when PublishOptions are supplied (covers the loop body).
func TestPublishBatch_WithOptions(t *testing.T) {
c := newTestEnv(t)
if err := c.CreateQueue(context.Background(), "ns", "batch-opts-q"); err != nil {
t.Fatalf("CreateQueue: %v", err)
}
ids, err := c.PublishBatch(context.Background(), "ns", "batch-opts-q",
[][]byte{[]byte("msg1"), []byte("msg2")},
client.WithDeliverAt(time.Now().Add(10*time.Second)),
)
if err != nil {
t.Fatalf("PublishBatch with opts: %v", err)
}
if len(ids) != 2 {
t.Errorf("PublishBatch with opts: want 2 IDs, got %d", len(ids))
}
}

// TestConsume_ServerError verifies that Consume propagates server errors.
func TestConsume_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, err := c.Consume(context.Background(), "ns", "q", 1)
if err == nil {
t.Fatal("expected error from Consume on 500, got nil")
}
}

// TestListNamespaces_ServerError verifies that ListNamespaces propagates 500.
func TestListNamespaces_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, err := c.ListNamespaces(context.Background())
if err == nil {
t.Fatal("expected error from ListNamespaces on 500, got nil")
}
}

// TestSubscribe_ServerError verifies that Subscribe propagates server errors.
func TestSubscribe_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, err := c.Subscribe(context.Background(), "ns", "q", "http://example.com/hook", "")
if err == nil {
t.Fatal("expected error from Subscribe on 500, got nil")
}
}

// TestStats_ServerError verifies that Stats propagates server errors.
func TestStats_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, err := c.Stats(context.Background())
if err == nil {
t.Fatal("expected error from Stats on 500, got nil")
}
}

// TestStatsPaged_ServerError verifies that StatsPaged propagates server errors.
func TestStatsPaged_ServerError(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
_, _, err := c.StatsPaged(context.Background(), 1, 10)
if err == nil {
t.Fatal("expected error from StatsPaged on 500, got nil")
}
}

// TestDo_InvalidJSONResponse verifies that the `do` method returns a JSON
// decode error when the server sends 200 with non-JSON body.
func TestDo_InvalidJSONResponse(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_, _ = w.Write([]byte("not valid json{{{"))
}))
t.Cleanup(srv.Close)

c := client.New(srv.URL)
// Stats decodes a JSON array — invalid body must cause a decode error.
_, err := c.Stats(context.Background())
if err == nil {
t.Fatal("expected JSON decode error from invalid response body, got nil")
}
}
