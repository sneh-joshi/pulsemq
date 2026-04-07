// Package http provides the HTTP transport layer for PulseMQ.
//
// Routes (Go 1.22+ method-qualified patterns):
//
//	GET    /health
//	POST   /namespaces
//	GET    /namespaces
//	DELETE /namespaces/{ns}
//	POST   /namespaces/{ns}/queues/{name}
//	GET    /namespaces/{ns}/queues
//	DELETE /namespaces/{ns}/queues/{name}
//	POST   /namespaces/{ns}/queues/{name}/messages
//	POST   /namespaces/{ns}/queues/{name}/messages/batch
//	GET    /namespaces/{ns}/queues/{name}/messages
//	DELETE /messages/{receipt}
//	POST   /messages/{receipt}/nack
//	GET    /namespaces/{ns}/queues/{name}/dlq
//	POST   /namespaces/{ns}/queues/{name}/dlq/replay
//	GET    /namespaces/{ns}/queues/{name}/ws
//	POST   /namespaces/{ns}/queues/{name}/subscriptions
//	DELETE /subscriptions/{id}
//	GET    /metrics
//	GET    /dashboard
//	GET    /api/stats
package http

import (
	"context"
	_ "embed"
	"net/http"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
	"github.com/sneh-joshi/pulsemq/internal/metrics"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	transportws "github.com/sneh-joshi/pulsemq/internal/transport/websocket"
)

//go:embed static/index.html
var dashboardHTML []byte

//go:embed static/playground.html
var playgroundHTML []byte

// Server wraps the stdlib HTTP server with PulseMQ route wiring.
type Server struct {
	inner *http.Server
}

// New builds a Server from a Broker.
// The caller is responsible for calling ListenAndServe / Shutdown.
func New(b *broker.Broker, cm *consumer.Manager, cfg *config.Config, ns *namespace.Registry, reg *metrics.Registry) *Server {
	h := &Handler{broker: b, consumer: cm, ns: ns}
	ws := &transportws.Handler{Broker: b}

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("GET /health", h.health)

	// Namespace management
	mux.HandleFunc("POST /namespaces", h.createNamespace)
	mux.HandleFunc("GET /namespaces", h.listNamespaces)
	mux.HandleFunc("DELETE /namespaces/{ns}", h.deleteNamespace)

	// Queue management
	mux.HandleFunc("POST /namespaces/{ns}/queues/{name}", h.createQueue)
	mux.HandleFunc("GET /namespaces/{ns}/queues", h.listQueues)
	mux.HandleFunc("DELETE /namespaces/{ns}/queues/{name}", h.deleteQueue)

	// Messages
	mux.HandleFunc("POST /namespaces/{ns}/queues/{name}/messages", h.publishMessage)
	mux.HandleFunc("POST /namespaces/{ns}/queues/{name}/messages/batch", h.publishBatch)
	mux.HandleFunc("GET /namespaces/{ns}/queues/{name}/messages", h.consumeMessages)
	mux.HandleFunc("DELETE /namespaces/{ns}/queues/{name}/messages", h.purgeQueue)
	mux.HandleFunc("DELETE /messages/{receipt}", h.ackMessage)
	mux.HandleFunc("POST /messages/{receipt}/nack", h.nackMessage)

	// DLQ
	mux.HandleFunc("GET /namespaces/{ns}/queues/{name}/dlq", h.getDLQ)
	mux.HandleFunc("POST /namespaces/{ns}/queues/{name}/dlq/replay", h.replayDLQ)

	// WebSocket push
	mux.Handle("GET /namespaces/{ns}/queues/{name}/ws", ws)

	// Webhook subscriptions
	mux.HandleFunc("POST /namespaces/{ns}/queues/{name}/subscriptions", h.createSubscription)
	mux.HandleFunc("DELETE /subscriptions/{id}", h.deleteSubscription)

	// Metrics (Prometheus text format)
	if reg != nil {
		mux.Handle("GET /metrics", reg.Handler())
	}

	// Dashboard
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})

	// Interactive playground (publish + real-time consume via WebSocket)
	mux.HandleFunc("GET /playground", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(playgroundHTML)
	})

	// Stats API (used by the dashboard)
	mux.HandleFunc("GET /api/stats/summary", h.statsAPISummary)
	mux.HandleFunc("GET /api/stats", h.statsAPI)

	// Build middleware chain: logging → auth → rate-limit
	rps := 100.0
	burst := 200
	if r := cfg.Node; r.DataDir != "" { /* placeholder for future config */
		_ = r
	}

	authEnabled := cfg.Auth.Enabled
	apiKey := cfg.Auth.APIKey

	var handler http.Handler = mux
	handler = chain(handler,
		CORSMiddleware,
		MaxBodyMiddleware,
		LoggingMiddleware,
		AuthMiddleware(apiKey, authEnabled),
		RateLimitMiddleware(rps, burst),
	)

	return &Server{
		inner: &http.Server{
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// Handler returns the composed http.Handler (useful for testing).
func (s *Server) Handler() http.Handler { return s.inner.Handler }

// ListenAndServe starts the server on the given address (e.g. ":8080").
// It returns when the server stops or encounters an error.
func (s *Server) ListenAndServe(addr string) error {
	s.inner.Addr = addr
	return s.inner.ListenAndServe()
}

// Shutdown gracefully stops the server, waiting up to ctx's deadline for
// in-flight requests to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}
