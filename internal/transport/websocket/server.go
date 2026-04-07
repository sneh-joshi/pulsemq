// Package websocket provides WebSocket-based push delivery for PulseMQ.
//
// Clients open a WebSocket connection to:
//
//	GET /namespaces/{ns}/queues/{name}/ws
//
// The server polls the queue every 200 ms and pushes any available messages.
// Clients respond with ACK or NACK frames.
//
// Server → client message frame:
//
//	{"type":"message","id":"<ULID>","body":"<base64>","receipt_handle":"<ULID>","namespace":"...","queue":"...","published_at":...}
//
// Client → server control frame:
//
//	{"type":"ack",  "receipt_handle":"<ULID>"}
//	{"type":"nack", "receipt_handle":"<ULID>"}
package websocket

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	gorillaws "github.com/gorilla/websocket"
	"github.com/sneh-joshi/pulsemq/internal/broker"
)

// urlParse is an alias so the upgrader closure can call it without shadowing
// the url package import.
var urlParse = url.Parse

var upgrader = gorillaws.Upgrader{
	// CheckOrigin rejects cross-origin WebSocket upgrade requests.
	// A request is considered same-origin when its Origin header matches the
	// Host header (scheme-agnostic).  Requests without an Origin header
	// (e.g. from native clients/curl) are always allowed.
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // non-browser client, allow
		}
		// Parse and compare host portions only so that ws:// and http:// are
		// treated as the same origin.
		parsed, err := parseHost(origin)
		if err != nil {
			return false
		}
		return parsed == r.Host
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// parseHost returns the host:port (or just host) portion of a URL string.
func parseHost(rawURL string) (string, error) {
	u, err := urlParse(rawURL)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("invalid origin %q", rawURL)
	}
	return u.Host, nil
}

// Handler serves the WebSocket endpoint for a specific queue.
// It is mounted by the HTTP server and reads ns/name from r.PathValue.
type Handler struct {
	Broker *broker.Broker
}

// serverFrame is the JSON structure the server sends to the client.
type serverFrame struct {
	Type          string `json:"type"` // "message"
	ID            string `json:"id"`
	Body          string `json:"body"` // base64
	ReceiptHandle string `json:"receipt_handle"`
	Namespace     string `json:"namespace"`
	Queue         string `json:"queue"`
	PublishedAt   int64  `json:"published_at"`
}

// clientFrame is the JSON structure the client sends to the server.
type clientFrame struct {
	Type          string `json:"type"` // "ack" | "nack"
	ReceiptHandle string `json:"receipt_handle"`
}

// ServeHTTP upgrades the connection and starts the push loop.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("websocket upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	// Start a goroutine to read control frames from the client.
	controlCh := make(chan clientFrame, 64)
	go func() {
		defer close(controlCh)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var cf clientFrame
			if jsonErr := json.Unmarshal(raw, &cf); jsonErr == nil {
				controlCh <- cf
			}
		}
	}()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case cf, ok := <-controlCh:
			if !ok {
				return // client disconnected
			}
			switch cf.Type {
			case "ack":
				if err := h.Broker.Ack(cf.ReceiptHandle); err != nil {
					slog.Warn("ws ack failed", "receipt", cf.ReceiptHandle, "err", err)
				}
			case "nack":
				if err := h.Broker.Nack(cf.ReceiptHandle); err != nil {
					slog.Warn("ws nack failed", "receipt", cf.ReceiptHandle, "err", err)
				}
			}

		case <-ticker.C:
			results, err := h.Broker.Consume(broker.ConsumeRequest{
				Namespace: ns,
				Queue:     name,
				N:         10,
			})
			if err != nil {
				slog.Warn("ws consume failed", "ns", ns, "queue", name, "err", err)
				continue
			}
			for _, res := range results {
				frame := serverFrame{
					Type:          "message",
					ReceiptHandle: res.ReceiptHandle,
				}
				if res.Message != nil {
					frame.ID = res.Message.ID
					frame.Body = base64.StdEncoding.EncodeToString(res.Message.Body)
					frame.Namespace = res.Message.Namespace
					frame.Queue = res.Message.Queue
					frame.PublishedAt = res.Message.PublishedAt
				}
				data, _ := json.Marshal(frame)
				if writeErr := conn.WriteMessage(gorillaws.TextMessage, data); writeErr != nil {
					return
				}
			}
		}
	}
}
