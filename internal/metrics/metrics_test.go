package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/metrics"
)

// ─── labelCounter ─────────────────────────────────────────────────────────────

func TestRegistry_MessageCounters(t *testing.T) {
	var reg metrics.Registry

	key := metrics.QueueKey("payments", "orders")
	reg.Published.Inc(key)
	reg.Published.Inc(key)
	reg.Published.Add(key, 3)

	got := int64(0)
	reg.Published.Each(func(k string, v int64) {
		if k == key {
			got = v
		}
	})
	if got != 5 {
		t.Fatalf("Published count = %d, want 5", got)
	}
}

func TestRegistry_HTTPCounters(t *testing.T) {
	var reg metrics.Registry

	reqKey := metrics.HTTPKey("POST", "/queues/orders/publish", "200")
	durKey := metrics.HTTPDurKey("POST", "/queues/orders/publish")

	reg.HTTPReqs.Inc(reqKey)
	reg.HTTPReqs.Inc(reqKey)
	reg.HTTPDurMs.Add(durKey, 42)
	reg.HTTPDurMs.Add(durKey, 18)
	reg.HTTPDurCnt.Inc(durKey)
	reg.HTTPDurCnt.Inc(durKey)

	reqCount := int64(0)
	reg.HTTPReqs.Each(func(k string, v int64) {
		if k == reqKey {
			reqCount = v
		}
	})
	if reqCount != 2 {
		t.Fatalf("HTTPReqs count = %d, want 2", reqCount)
	}

	durSum := int64(0)
	reg.HTTPDurMs.Each(func(k string, v int64) {
		if k == durKey {
			durSum = v
		}
	})
	if durSum != 60 {
		t.Fatalf("HTTPDurMs sum = %d, want 60", durSum)
	}
}

// ─── Prometheus output format ─────────────────────────────────────────────────

func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestHandler_ContentType(t *testing.T) {
	var reg metrics.Registry
	reg.Published.Inc(metrics.QueueKey("ns", "q"))

	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
}

func TestHandler_EmptyRegistry(t *testing.T) {
	var reg metrics.Registry
	body := scrape(t, &reg)
	if body != "" {
		t.Fatalf("expected empty body for empty registry, got:\n%s", body)
	}
}

func TestHandler_PublishedCounter(t *testing.T) {
	var reg metrics.Registry

	reg.Published.Inc(metrics.QueueKey("payments", "invoices"))
	reg.Published.Add(metrics.QueueKey("payments", "invoices"), 4)
	reg.Published.Inc(metrics.QueueKey("analytics", "events"))

	body := scrape(t, &reg)

	mustContain(t, body, "# HELP pulsemq_messages_published_total")
	mustContain(t, body, "# TYPE pulsemq_messages_published_total counter")
	mustContain(t, body, `namespace="payments"`)
	mustContain(t, body, `queue="invoices"`)
	mustContain(t, body, `namespace="analytics"`)
}

func TestHandler_HTTPCounters(t *testing.T) {
	var reg metrics.Registry

	reg.HTTPReqs.Inc(metrics.HTTPKey("GET", "/health", "200"))
	reg.HTTPDurMs.Add(metrics.HTTPDurKey("GET", "/health"), 5)
	reg.HTTPDurCnt.Inc(metrics.HTTPDurKey("GET", "/health"))

	body := scrape(t, &reg)

	mustContain(t, body, "# HELP pulsemq_http_requests_total")
	mustContain(t, body, `method="GET"`)
	mustContain(t, body, `path="/health"`)
	mustContain(t, body, `status="200"`)
	mustContain(t, body, "pulsemq_http_request_duration_milliseconds_sum")
	mustContain(t, body, "pulsemq_http_request_duration_milliseconds_count")
}

func TestHandler_MultipleMetricFamilies(t *testing.T) {
	var reg metrics.Registry

	k := metrics.QueueKey("ops", "jobs")
	reg.Published.Add(k, 10)
	reg.Consumed.Add(k, 8)
	reg.Acked.Add(k, 7)
	reg.Nacked.Add(k, 1)
	reg.DLQRouted.Add(k, 1)

	body := scrape(t, &reg)

	mustContain(t, body, "pulsemq_messages_published_total")
	mustContain(t, body, "pulsemq_messages_consumed_total")
	mustContain(t, body, "pulsemq_messages_acked_total")
	mustContain(t, body, "pulsemq_messages_nacked_total")
	mustContain(t, body, "pulsemq_messages_dlq_routed_total")
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func mustContain(t *testing.T, body, substr string) {
	t.Helper()
	if !strings.Contains(body, substr) {
		t.Errorf("expected body to contain %q\nbody:\n%s", substr, body)
	}
}

// ─── Concurrent safety ────────────────────────────────────────────────────────

func TestRegistry_ConcurrentInc(t *testing.T) {
	var reg metrics.Registry
	key := metrics.QueueKey("load", "test")

	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			reg.Published.Inc(key)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}

	got := int64(0)
	reg.Published.Each(func(k string, v int64) {
		if k == key {
			got = v
		}
	})
	if got != 100 {
		t.Fatalf("concurrent Inc: got %d, want 100", got)
	}
}

// ─── splitTwo tests ───────────────────────────────────────────────────────────

func TestSplitTwo_WithTab(t *testing.T) {
key := metrics.QueueKey("payments", "orders")
if key != "payments\torders" {
t.Errorf("QueueKey: want %q, got %q", "payments\torders", key)
}
}

func TestSplitTwo_MetricsCountByQueue(t *testing.T) {
var reg metrics.Registry
reg.Published.Inc(metrics.QueueKey("ns", "q"))
reg.Published.Inc(metrics.QueueKey("ns", "q"))

var total int64
reg.Published.Each(func(k string, v int64) {
total += v
})
if total != 2 {
t.Errorf("want 2 published, got %d", total)
}
}
