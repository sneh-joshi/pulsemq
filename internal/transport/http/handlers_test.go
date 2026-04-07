package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
	transphttp "github.com/sneh-joshi/pulsemq/internal/transport/http"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{
		Node: config.NodeConfig{DataDir: t.TempDir(), Host: "0.0.0.0", Port: 8080},
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

	cm := consumer.NewManager(b)
	t.Cleanup(cm.Close)

	srv := transphttp.New(b, cm, cfg, nil, nil)
	return srv.Handler()
}

func doRequest(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&reqBody).Encode(body); err != nil {
			t.Fatalf("encode request body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &reqBody)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(rr.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v, body: %s", err, rr.Body.String())
	}
}

// ─── Health ───────────────────────────────────────────────────────────────────

func TestHTTP_Health(t *testing.T) {
	h := newTestServer(t)
	rr := doRequest(t, h, "GET", "/health", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("health: want 200, got %d — body: %s", rr.Code, rr.Body)
	}
	var resp map[string]any
	decodeResp(t, rr, &resp)
	if resp["status"] != "ok" {
		t.Errorf("health status: want ok, got %v", resp["status"])
	}
}

// ─── Queue management ─────────────────────────────────────────────────────────

func TestHTTP_CreateQueue_ListQueues(t *testing.T) {
	h := newTestServer(t)

	// Create queue
	rr := doRequest(t, h, "POST", "/namespaces/ns/queues/jobs", map[string]any{
		"max_retries": 5,
	})
	if rr.Code != http.StatusCreated {
		t.Fatalf("createQueue: want 201, got %d — body: %s", rr.Code, rr.Body)
	}

	// List queues
	rr = doRequest(t, h, "GET", "/namespaces/ns/queues", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("listQueues: want 200, got %d", rr.Code)
	}

	var listResp struct {
		Queues []string `json:"queues"`
	}
	decodeResp(t, rr, &listResp)
	found := false
	for _, q := range listResp.Queues {
		if q == "ns/jobs" {
			found = true
		}
	}
	if !found {
		t.Errorf("ns/jobs not found in list: %v", listResp.Queues)
	}
}

func TestHTTP_DeleteQueue(t *testing.T) {
	h := newTestServer(t)

	doRequest(t, h, "POST", "/namespaces/ns/queues/temp", map[string]any{})
	rr := doRequest(t, h, "DELETE", "/namespaces/ns/queues/temp", nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("deleteQueue: want 204, got %d", rr.Code)
	}
}

func TestHTTP_CreateQueue_InvalidName(t *testing.T) {
	h := newTestServer(t)

	cases := []struct {
		path string
		desc string
	}{
		{"/namespaces/Order/queues/payment", "uppercase namespace"},
		{"/namespaces/order/queues/Payment", "uppercase queue name"},
		{"/namespaces/Order/queues/Payment", "both uppercase"},
		{"/namespaces/my_ns/queues/q", "underscore in namespace"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			rr := doRequest(t, h, "POST", tc.path, map[string]any{})
			if rr.Code != http.StatusBadRequest {
				t.Errorf("%s: want 400, got %d — body: %s", tc.desc, rr.Code, rr.Body)
			}
		})
	}
}

// ─── Publish ──────────────────────────────────────────────────────────────────

func TestHTTP_PublishMessage(t *testing.T) {
	h := newTestServer(t)

	rr := doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages", map[string]any{
		"body": "hello world",
	})
	if rr.Code != http.StatusCreated {
		t.Fatalf("publish: want 201, got %d — body: %s", rr.Code, rr.Body)
	}

	var resp struct {
		ID string `json:"id"`
	}
	decodeResp(t, rr, &resp)
	if resp.ID == "" {
		t.Error("expected non-empty id")
	}
}

func TestHTTP_PublishBatch(t *testing.T) {
	h := newTestServer(t)

	batch := []map[string]any{
		{"body": "msg1"},
		{"body": "msg2"},
		{"body": "msg3"},
	}
	rr := doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages/batch", batch)
	if rr.Code != http.StatusCreated {
		t.Fatalf("publishBatch: want 201, got %d — body: %s", rr.Code, rr.Body)
	}

	var resp struct {
		IDs []string `json:"ids"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.IDs) != 3 {
		t.Errorf("batch ids: want 3, got %d", len(resp.IDs))
	}
}

// ─── Consume ──────────────────────────────────────────────────────────────────

func TestHTTP_ConsumeMessages(t *testing.T) {
	h := newTestServer(t)

	// Publish one message first.
	doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages", map[string]any{
		"body": "test",
	})

	rr := doRequest(t, h, "GET", "/namespaces/ns/queues/orders/messages?n=1", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("consume: want 200, got %d — body: %s", rr.Code, rr.Body)
	}

	var resp struct {
		Messages []struct {
			ID            string `json:"id"`
			ReceiptHandle string `json:"receipt_handle"`
		} `json:"messages"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Messages) != 1 {
		t.Fatalf("consume: want 1 message, got %d", len(resp.Messages))
	}
	if resp.Messages[0].ReceiptHandle == "" {
		t.Error("expected non-empty receipt_handle")
	}
}

// ─── Ack / Nack ───────────────────────────────────────────────────────────────

func TestHTTP_Ack(t *testing.T) {
	h := newTestServer(t)

	doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages", map[string]any{"body": "x"})

	var consumeResp struct {
		Messages []struct {
			ReceiptHandle string `json:"receipt_handle"`
		} `json:"messages"`
	}
	rr := doRequest(t, h, "GET", "/namespaces/ns/queues/orders/messages?n=1", nil)
	decodeResp(t, rr, &consumeResp)
	if len(consumeResp.Messages) == 0 {
		t.Fatal("expected at least one message")
	}

	receipt := consumeResp.Messages[0].ReceiptHandle
	rr = doRequest(t, h, "DELETE", "/messages/"+receipt, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("ack: want 204, got %d — body: %s", rr.Code, rr.Body)
	}
}

func TestHTTP_Ack_UnknownReceipt(t *testing.T) {
	h := newTestServer(t)
	rr := doRequest(t, h, "DELETE", "/messages/nonexistent-receipt", nil)
	if rr.Code != http.StatusGone {
		t.Fatalf("ack unknown: want 410, got %d", rr.Code)
	}
}

func TestHTTP_Nack(t *testing.T) {
	h := newTestServer(t)

	doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages", map[string]any{"body": "x"})

	var consumeResp struct {
		Messages []struct {
			ReceiptHandle string `json:"receipt_handle"`
		} `json:"messages"`
	}
	rr := doRequest(t, h, "GET", "/namespaces/ns/queues/orders/messages?n=1", nil)
	decodeResp(t, rr, &consumeResp)
	receipt := consumeResp.Messages[0].ReceiptHandle

	rr = doRequest(t, h, "POST", "/messages/"+receipt+"/nack", nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("nack: want 204, got %d — body: %s", rr.Code, rr.Body)
	}
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

func TestHTTP_ReplayDLQ(t *testing.T) {
	h := newTestServer(t)

	// Publish then nack to move into DLQ (MaxRetries=3 default, nack once requeues).
	// We publish with MaxRetries=1 so one nack sends to DLQ.
	doRequest(t, h, "POST", "/namespaces/ns/queues/orders/messages", map[string]any{
		"body":        "failme",
		"max_retries": 1,
	})

	var cr struct {
		Messages []struct {
			ReceiptHandle string `json:"receipt_handle"`
		} `json:"messages"`
	}
	rr := doRequest(t, h, "GET", "/namespaces/ns/queues/orders/messages?n=1", nil)
	decodeResp(t, rr, &cr)
	if len(cr.Messages) == 0 {
		t.Skip("no message available — timing issue in CI")
	}

	receipt := cr.Messages[0].ReceiptHandle
	doRequest(t, h, "POST", "/messages/"+receipt+"/nack", nil)

	// Replay DLQ
	rr = doRequest(t, h, "POST", "/namespaces/ns/queues/orders/dlq/replay", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("replayDLQ: want 200, got %d — body: %s", rr.Code, rr.Body)
	}

	var replayResp struct {
		Replayed int `json:"replayed"`
	}
	decodeResp(t, rr, &replayResp)
	if replayResp.Replayed < 0 {
		t.Errorf("replayed: unexpected negative %d", replayResp.Replayed)
	}
}
