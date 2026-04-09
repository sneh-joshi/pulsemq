package consumer_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
)

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

func TestManager_NewManager(t *testing.T) {
	b := newTestBroker(t)
	m := consumer.NewManager(b)
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	m.Close()
}

func TestManager_Register_Deregister(t *testing.T) {
	b := newTestBroker(t)
	m := consumer.NewManager(b)
	t.Cleanup(m.Close)

	// Use a dummy URL — delivery loop won't deliver since queue is empty.
	id, err := m.Register("ns", "orders", "http://localhost:9999/webhook", "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if id == "" {
		t.Fatal("Register returned empty id")
	}

	if err := m.Deregister(id); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
}

func TestManager_Deregister_NotFound(t *testing.T) {
	b := newTestBroker(t)
	m := consumer.NewManager(b)
	t.Cleanup(m.Close)

	err := m.Deregister("nonexistent-id")
	if err == nil {
		t.Fatal("expected error deregistering unknown subscription, got nil")
	}
}

func TestManager_Close(t *testing.T) {
	b := newTestBroker(t)
	m := consumer.NewManager(b)

	_, _ = m.Register("ns", "q", "http://localhost:9999/webhook", "")
	// Close should not panic even with active subscriptions.
	m.Close()
}

func TestManager_DeliveryLoop_Success(t *testing.T) {
	// Start a test HTTP server to receive webhook deliveries.
	received := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBroker(t)
	m := consumer.NewManager(b)
	t.Cleanup(m.Close)

	// Publish a message.
	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "orders",
		Body:      []byte("hello webhook"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Register the webhook subscription pointing at the test server.
	_, err := m.Register("ns", "orders", ts.URL, "")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Wait for delivery (delivery loop polls every 500ms).
	select {
	case <-received:
		// Allow the delivery goroutine to finish its Ack before ts.Close()
		// cancels the HTTP context (which would turn the Ack into a Nack).
		time.Sleep(200 * time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("webhook delivery not received within 5s")
	}
}

func TestManager_DeliveryLoop_WithSecret(t *testing.T) {
	received := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig := r.Header.Get("X-PulseMQ-Signature")
		if sig == "" {
			http.Error(w, "missing signature", http.StatusBadRequest)
			return
		}
		received <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBroker(t)
	m := consumer.NewManager(b)
	t.Cleanup(m.Close)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace: "ns",
		Queue:     "secure",
		Body:      []byte("secret payload"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if _, err := m.Register("ns", "secure", ts.URL, "mysecret"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	select {
	case <-received:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("signed webhook not received within 5s")
	}
}

func TestManager_DeliveryLoop_EndpointFailure(t *testing.T) {
	// Use a server that returns 500 — the manager should NACK and continue.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	b := newTestBroker(t)
	m := consumer.NewManager(b)
	t.Cleanup(m.Close)

	if _, err := b.Publish(broker.PublishRequest{
		Namespace:  "ns",
		Queue:      "failing",
		Body:       []byte("will fail"),
		MaxRetries: 5,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if _, err := m.Register("ns", "failing", ts.URL, ""); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Give the loop a moment to attempt delivery (should nack, not panic).
	time.Sleep(1200 * time.Millisecond)
}
