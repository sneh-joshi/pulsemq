// Package metrics provides a lightweight Prometheus-compatible metrics
// registry for PulseMQ. It deliberately avoids the prometheus/client_golang
// package so the server binary stays small with no additional dependencies.
//
// # Counter naming convention
//
// Every counter uses a tab-separated string as its label key so that a single
// sync.Map can hold all label combinations without additional map nesting.
//
//	Published / Consumed / Acked / Nacked / DLQRouted  →  key = "namespace\tqueue"
//	HTTPReqs                                            →  key = "method\tpath\tstatus"
//	HTTPDurMs / HTTPDurCnt                              →  key = "method\tpath"
//
// # Prometheus text output
//
// Calling Registry.Handler() returns an http.Handler that renders all counters
// in the Prometheus exposition format (text/plain; version=0.0.4).
package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// ─── labelCounter ─────────────────────────────────────────────────────────────

// labelCounter is a lock-free, label-keyed counter map backed by sync.Map and
// atomic.Int64 values.
type labelCounter struct {
	vals sync.Map // key string → *atomic.Int64
}

func (lc *labelCounter) get(key string) *atomic.Int64 {
	v, _ := lc.vals.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// Inc increments the counter for key by 1.
func (lc *labelCounter) Inc(key string) { lc.get(key).Add(1) }

// Add increments the counter for key by n.
func (lc *labelCounter) Add(key string, n int64) { lc.get(key).Add(n) }

// Each calls fn for every key/value pair. The order is non-deterministic.
func (lc *labelCounter) Each(fn func(key string, val int64)) {
	lc.vals.Range(func(k, v any) bool {
		fn(k.(string), v.(*atomic.Int64).Load())
		return true
	})
}

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry holds all PulseMQ application metrics.
type Registry struct {
	// Message-level counters.  key = "namespace\tqueue"
	Published labelCounter
	Consumed  labelCounter
	Acked     labelCounter
	Nacked    labelCounter
	DLQRouted labelCounter

	// HTTP-level counters.  key = "method\tpath\tstatus" (Reqs) or "method\tpath" (Dur*)
	HTTPReqs   labelCounter
	HTTPDurMs  labelCounter // sum of request durations in milliseconds
	HTTPDurCnt labelCounter // number of requests (same key as HTTPDurMs, for avg)
}

// ─── Prometheus text serialisation ────────────────────────────────────────────

// Handler returns an http.Handler that renders all metrics in the Prometheus
// plain-text exposition format (text/plain; version=0.0.4).
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		var b strings.Builder

		// ── message counters ──────────────────────────────────────────────────
		writeFamily(&b, "pulsemq_messages_published_total",
			"Total messages published", "counter",
			func(fn func(labels, val string)) {
				r.Published.Each(func(key string, val int64) {
					ns, q := splitTwo(key)
					fn(fmt.Sprintf(`namespace=%q,queue=%q`, ns, q),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_messages_consumed_total",
			"Total messages delivered to consumers", "counter",
			func(fn func(labels, val string)) {
				r.Consumed.Each(func(key string, val int64) {
					ns, q := splitTwo(key)
					fn(fmt.Sprintf(`namespace=%q,queue=%q`, ns, q),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_messages_acked_total",
			"Total messages acknowledged by consumers", "counter",
			func(fn func(labels, val string)) {
				r.Acked.Each(func(key string, val int64) {
					ns, q := splitTwo(key)
					fn(fmt.Sprintf(`namespace=%q,queue=%q`, ns, q),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_messages_nacked_total",
			"Total messages negatively acknowledged (requeued or DLQ'd)", "counter",
			func(fn func(labels, val string)) {
				r.Nacked.Each(func(key string, val int64) {
					ns, q := splitTwo(key)
					fn(fmt.Sprintf(`namespace=%q,queue=%q`, ns, q),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_messages_dlq_routed_total",
			"Total messages routed to a dead-letter queue", "counter",
			func(fn func(labels, val string)) {
				r.DLQRouted.Each(func(key string, val int64) {
					ns, q := splitTwo(key)
					fn(fmt.Sprintf(`namespace=%q,queue=%q`, ns, q),
						fmt.Sprintf("%d", val))
				})
			})

		// ── HTTP counters ─────────────────────────────────────────────────────
		writeFamily(&b, "pulsemq_http_requests_total",
			"Total HTTP requests by method, path, and status code", "counter",
			func(fn func(labels, val string)) {
				r.HTTPReqs.Each(func(key string, val int64) {
					method, path, status := splitThree(key)
					fn(fmt.Sprintf(`method=%q,path=%q,status=%q`, method, path, status),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_http_request_duration_milliseconds_sum",
			"Sum of HTTP request durations in milliseconds", "counter",
			func(fn func(labels, val string)) {
				r.HTTPDurMs.Each(func(key string, val int64) {
					method, path := splitTwo(key)
					fn(fmt.Sprintf(`method=%q,path=%q`, method, path),
						fmt.Sprintf("%d", val))
				})
			})

		writeFamily(&b, "pulsemq_http_request_duration_milliseconds_count",
			"Count of observed HTTP request durations", "counter",
			func(fn func(labels, val string)) {
				r.HTTPDurCnt.Each(func(key string, val int64) {
					method, path := splitTwo(key)
					fn(fmt.Sprintf(`method=%q,path=%q`, method, path),
						fmt.Sprintf("%d", val))
				})
			})

		fmt.Fprint(w, b.String())
	})
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// writeFamily writes a single Prometheus metric family to b.
// fill is called with a writer function that appends individual label+value lines.
func writeFamily(
	b *strings.Builder,
	name, help, typ string,
	fill func(fn func(labels, val string)),
) {
	// Buffer individual metric lines so we can skip the header when empty.
	var lines []string
	fill(func(labels, val string) {
		lines = append(lines, fmt.Sprintf("%s{%s} %s\n", name, labels, val))
	})
	if len(lines) == 0 {
		return
	}
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s %s\n", name, typ)
	for _, l := range lines {
		b.WriteString(l)
	}
}

// splitTwo splits a tab-delimited key of the form "a\tb" into (a, b).
// If there is no tab, the whole string is returned as the first component.
func splitTwo(key string) (string, string) {
	i := strings.IndexByte(key, '\t')
	if i < 0 {
		return key, ""
	}
	return key[:i], key[i+1:]
}

// splitThree splits a tab-delimited key "a\tb\tc" into (a, b, c).
func splitThree(key string) (string, string, string) {
	a, rest := splitTwo(key)
	b, c := splitTwo(rest)
	return a, b, c
}

// ─── Convenience key builders ─────────────────────────────────────────────────

// QueueKey builds the label key used by Published/Consumed/Acked/Nacked/DLQRouted.
func QueueKey(namespace, queue string) string {
	return namespace + "\t" + queue
}

// HTTPKey builds the label key used by HTTPReqs.
func HTTPKey(method, path, status string) string {
	return method + "\t" + path + "\t" + status
}

// HTTPDurKey builds the label key used by HTTPDurMs / HTTPDurCnt.
func HTTPDurKey(method, path string) string {
	return method + "\t" + path
}
