// Tests for the websocket package.
// Uses the same package name (not _test suffix) to access unexported helpers.
package websocket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gorillaws "github.com/gorilla/websocket"
	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
)

// ─── parseHost ────────────────────────────────────────────────────────────────

func TestParseHost_Valid(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"http://localhost:8080", "localhost:8080"},
		{"https://example.com", "example.com"},
		{"http://127.0.0.1:3000", "127.0.0.1:3000"},
	}
	for _, tc := range cases {
		got, err := parseHost(tc.raw)
		if err != nil {
			t.Errorf("parseHost(%q): unexpected error: %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseHost(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestParseHost_Invalid(t *testing.T) {
	cases := []string{
		"not-a-url",
		"://bad",
		"",
	}
	for _, raw := range cases {
		_, err := parseHost(raw)
		if err == nil {
			t.Errorf("parseHost(%q): expected error, got nil", raw)
		}
	}
}

// ─── upgrader.CheckOrigin ─────────────────────────────────────────────────────

func TestCheckOrigin_NoOrigin(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	if !upgrader.CheckOrigin(req) {
		t.Error("CheckOrigin with no Origin header should return true")
	}
}

func TestCheckOrigin_SameOrigin(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://localhost:8080")
	if !upgrader.CheckOrigin(req) {
		t.Error("CheckOrigin same-origin should return true")
	}
}

func TestCheckOrigin_CrossOrigin(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://evil.com")
	if upgrader.CheckOrigin(req) {
		t.Error("CheckOrigin cross-origin should return false")
	}
}

func TestCheckOrigin_BadOriginHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "://bad-url")
	if upgrader.CheckOrigin(req) {
		t.Error("CheckOrigin with unparsable Origin should return false")
	}
}

// ─── Handler.ServeHTTP ────────────────────────────────────────────────────────

func newTestBrokerForWS(t *testing.T) *broker.Broker {
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
	b, err := broker.New(cfg, "ws-test-node")
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestHandler_ServeHTTP_Connect(t *testing.T) {
	b := newTestBrokerForWS(t)
	h := &Handler{Broker: b}

	mux := http.NewServeMux()
	mux.Handle("GET /namespaces/{ns}/queues/{name}/ws", h)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/namespaces/ns/queues/orders/ws"
	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	conn.Close()
}

func TestHandler_ServeHTTP_PushAndAck(t *testing.T) {
	b := newTestBrokerForWS(t)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "orders",
		Body:      []byte("ws-payload"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	h := &Handler{Broker: b}
	mux := http.NewServeMux()
	mux.Handle("GET /namespaces/{ns}/queues/{name}/ws", h)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/namespaces/ns/queues/orders/ws"
	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	defer conn.Close()

	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	var frame serverFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("unmarshal server frame: %v", err)
	}
	if frame.Type != "message" {
		t.Errorf("frame type: want %q, got %q", "message", frame.Type)
	}
	if frame.ReceiptHandle == "" {
		t.Error("frame receipt_handle should not be empty")
	}

	ack := clientFrame{Type: "ack", ReceiptHandle: frame.ReceiptHandle}
	data, _ := json.Marshal(ack)
	if err := conn.WriteMessage(gorillaws.TextMessage, data); err != nil {
		t.Fatalf("send ack: %v", err)
	}
}

func TestHandler_ServeHTTP_Nack(t *testing.T) {
	b := newTestBrokerForWS(t)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace:  "ns",
		Queue:      "nackit",
		Body:       []byte("nack me"),
		MaxRetries: 3,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	h := &Handler{Broker: b}
	mux := http.NewServeMux()
	mux.Handle("GET /namespaces/{ns}/queues/{name}/ws", h)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/namespaces/ns/queues/nackit/ws"
	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	defer conn.Close()

	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	var frame serverFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}

	nack := clientFrame{Type: "nack", ReceiptHandle: frame.ReceiptHandle}
	data, _ := json.Marshal(nack)
	if err := conn.WriteMessage(gorillaws.TextMessage, data); err != nil {
		t.Fatalf("send nack: %v", err)
	}
}

// TestHandler_ServeHTTP_UpgradeFailed verifies that a plain HTTP request
// (without WS upgrade headers) is rejected gracefully by ServeHTTP.
func TestHandler_ServeHTTP_UpgradeFailed(t *testing.T) {
	b := newTestBrokerForWS(t)
	h := &Handler{Broker: b}

	// Non-WebSocket request — upgrader.Upgrade will fail and ServeHTTP must return.
	req := httptest.NewRequest("GET", "/namespaces/ns/queues/orders/ws", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)
	// The response should be 400 (upgrade failure) or any non-panic result.
	// We just confirm no panic occurs.
}

// TestHandler_ServeHTTP_ConsumeError verifies that a Consume error (queue
// not found) is handled gracefully without crashing the WS loop.
func TestHandler_ServeHTTP_ConsumeError(t *testing.T) {
	b := newTestBrokerForWS(t)
	h := &Handler{Broker: b}

	mux := http.NewServeMux()
	mux.Handle("GET /namespaces/{ns}/queues/{name}/ws", h)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Connect to a queue that does not exist — Consume returns error every tick.
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/namespaces/ns/queues/no-such-queue/ws"
	conn, _, err := gorillaws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	// Close immediately; ServeHTTP will return when controlCh closes.
	_ = conn.Close()
}
