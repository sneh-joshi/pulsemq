package http

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/broker"
	"github.com/sneh-joshi/pulsemq/internal/consumer"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	"github.com/sneh-joshi/pulsemq/internal/queue"
)

// Compile-time check that queue is used (avoid "imported and not used" error
// if the blank import below is removed).
var _ = queue.DefaultConfig

// maxBatchPublish is the maximum number of messages accepted in a single
// batch-publish request.  This matches the plan spec of 100.
const maxBatchPublish = 100

// Metadata limits — enforced on every publish path.
const (
	metaMaxKeys     = 16  // max number of key/value pairs
	metaMaxKeyBytes = 64  // max bytes per key
	metaMaxValBytes = 512 // max bytes per value
)

// validateMetadata returns a non-nil error if m violates any metadata limit.
func validateMetadata(m map[string]string) error {
	if len(m) > metaMaxKeys {
		return fmt.Errorf("metadata: too many keys (max %d)", metaMaxKeys)
	}
	for k, v := range m {
		if len(k) == 0 {
			return errors.New("metadata: key must not be empty")
		}
		if len(k) > metaMaxKeyBytes {
			return fmt.Errorf("metadata: key too long (max %d bytes)", metaMaxKeyBytes)
		}
		if len(v) > metaMaxValBytes {
			return fmt.Errorf("metadata: value too long (max %d bytes)", metaMaxValBytes)
		}
	}
	return nil
}

// validName returns true when name is safe to use as a path component.
// It rejects strings that are empty, too long, or that could be used for
// path-traversal (e.g. "..", "../foo", leading "/" or "\").
func validName(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	// Reject any component that contains a path separator or null byte.
	if strings.ContainsAny(s, "/\\\x00") {
		return false
	}
	// Reject bare "." or ".." segments.
	if s == "." || s == ".." {
		return false
	}
	return true
}

// Handler groups all HTTP request handlers around a Broker.
type Handler struct {
	broker   *broker.Broker
	consumer *consumer.Manager
	ns       *namespace.Registry // may be nil if namespaces are disabled
}

// ─── DTOs ─────────────────────────────────────────────────────────────────────

type publishReq struct {
	Body       string            `json:"body"`        // base64-encoded
	DeliverAt  int64             `json:"deliver_at"`  // unix ms; 0 = now
	MaxRetries int               `json:"max_retries"` // 0 = queue default
	Metadata   map[string]string `json:"metadata"`
}

type publishResp struct {
	ID string `json:"id"`
}

type consumedMessage struct {
	ID            string            `json:"id"`
	Body          string            `json:"body"` // base64
	ReceiptHandle string            `json:"receipt_handle"`
	Namespace     string            `json:"namespace"`
	Queue         string            `json:"queue"`
	Attempt       int               `json:"attempt"`
	PublishedAt   int64             `json:"published_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type consumeResp struct {
	Messages []consumedMessage `json:"messages"`
}

type createQueueReq struct {
	VisibilityTimeoutMs int64 `json:"visibility_timeout_ms"`
	MaxMessages         int64 `json:"max_messages"`
	MaxRetries          int   `json:"max_retries"`
	MaxBatchSize        int   `json:"max_batch_size"`
}

type queueListResp struct {
	Queues []string `json:"queues"`
}

type subscribeReq struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}

type subscribeResp struct {
	ID string `json:"id"`
}

type dlqResp struct {
	Messages []consumedMessage `json:"messages"`
}

type replayResp struct {
	Replayed int `json:"replayed"`
}

type healthResp struct {
	Status   string `json:"status"`
	NodeID   string `json:"node_id"`
	Queues   int    `json:"queues"`
	Uptime   string `json:"uptime"`
	UptimeMs int64  `json:"uptime_ms"`
	Version  string `json:"version"`
	DataDir  string `json:"data_dir"`
}

// ─── Health ───────────────────────────────────────────────────────────────────

var startTime = time.Now()

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	stats := h.broker.Stats()
	elapsed := time.Since(startTime)
	writeJSON(w, http.StatusOK, healthResp{
		Status:   "ok",
		NodeID:   h.broker.NodeID(),
		Queues:   stats.QueueCount,
		Uptime:   elapsed.Round(time.Second).String(),
		UptimeMs: elapsed.Milliseconds(),
		Version:  "1.0.0",
	})
}

// ─── Queue management ─────────────────────────────────────────────────────────

func (h *Handler) createQueue(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}
	// Enforce lowercase-alphanumeric naming rules at the HTTP layer regardless
	// of whether the broker has a namespace registry attached.
	if !namespace.ValidateName(ns) || !namespace.ValidateName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "namespace and queue names must be lowercase alphanumeric with optional hyphens (a-z, 0-9, -)",
		})
		return
	}

	var req createQueueReq
	if !decodeJSON(w, r, &req) {
		return
	}

	cfg := queue.DefaultConfig()
	if req.VisibilityTimeoutMs > 0 {
		cfg.DefaultVisibilityTimeoutMs = req.VisibilityTimeoutMs
	}
	if req.MaxMessages > 0 {
		cfg.MaxMessages = req.MaxMessages
	}
	if req.MaxRetries > 0 {
		cfg.MaxRetries = req.MaxRetries
	}
	if req.MaxBatchSize > 0 {
		cfg.MaxBatchSize = req.MaxBatchSize
	}

	if err := h.broker.CreateQueue(ns, name, cfg); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, namespace.ErrInvalidName) {
			code = http.StatusBadRequest
		}
		writeError(w, code, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created"})
}

func (h *Handler) listQueues(w http.ResponseWriter, r *http.Request) {
	queues := h.broker.ListQueues()
	if queues == nil {
		queues = []string{}
	}
	writeJSON(w, http.StatusOK, queueListResp{Queues: queues})
}

func (h *Handler) deleteQueue(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}
	if err := h.broker.DeleteQueue(ns, name); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Publish ──────────────────────────────────────────────────────────────────

func (h *Handler) publishMessage(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	var req publishReq
	if !decodeJSON(w, r, &req) {
		return
	}

	if err := validateMetadata(req.Metadata); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	body, err := base64.StdEncoding.DecodeString(req.Body)
	if err != nil {
		// Treat non-base64 as raw UTF-8 bytes.
		body = []byte(req.Body)
	}

	resp, err := h.broker.Publish(broker.PublishRequest{
		Namespace:  ns,
		Queue:      name,
		Body:       body,
		DeliverAt:  req.DeliverAt,
		MaxRetries: req.MaxRetries,
		Metadata:   req.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, publishResp{ID: resp.MessageID})
}

func (h *Handler) publishBatch(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	var reqs []publishReq
	if !decodeJSON(w, r, &reqs) {
		return
	}
	if len(reqs) > maxBatchPublish {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "batch exceeds maximum of 100 messages"})
		return
	}

	ids := make([]string, 0, len(reqs))
	for _, req := range reqs {
		if err := validateMetadata(req.Metadata); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		body, err := base64.StdEncoding.DecodeString(req.Body)
		if err != nil {
			body = []byte(req.Body)
		}
		resp, err := h.broker.Publish(broker.PublishRequest{
			Namespace:  ns,
			Queue:      name,
			Body:       body,
			DeliverAt:  req.DeliverAt,
			MaxRetries: req.MaxRetries,
			Metadata:   req.Metadata,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		ids = append(ids, resp.MessageID)
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ids": ids})
}

// ─── Consume ──────────────────────────────────────────────────────────────────

func (h *Handler) consumeMessages(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	n := 1
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}

	var visTms int64
	if v := r.URL.Query().Get("visibility_timeout_ms"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			visTms = parsed
		}
	}

	results, err := h.broker.Consume(broker.ConsumeRequest{
		Namespace:         ns,
		Queue:             name,
		N:                 n,
		VisibilityTimeout: visTms,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, consumeResp{Messages: mapDequeueResults(results)})
}

// ─── Purge Queue ────────────────────────────────────────────────────────────────

func (h *Handler) purgeQueue(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}
	n, err := h.broker.PurgeQueue(ns, name)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"purged": n})
}

// ─── Ack / Nack ───────────────────────────────────────────────────────────────

func (h *Handler) ackMessage(w http.ResponseWriter, r *http.Request) {
	receipt := r.PathValue("receipt")
	if err := h.broker.Ack(receipt); err != nil {
		writeError(w, http.StatusGone, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) nackMessage(w http.ResponseWriter, r *http.Request) {
	receipt := r.PathValue("receipt")
	if err := h.broker.Nack(receipt); err != nil {
		writeError(w, http.StatusGone, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

func (h *Handler) getDLQ(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	results, err := h.broker.DrainDLQ(ns, name, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, dlqResp{Messages: mapDequeueResults(results)})
}

func (h *Handler) replayDLQ(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	replayed, err := h.broker.ReplayDLQ(ns, name, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, replayResp{Replayed: replayed})
}

// ─── Namespace management ─────────────────────────────────────────────────────

type createNsReq struct {
	Name string `json:"name"`
}

type nsItem struct {
	Name      string `json:"name"`
	CreatedAt int64  `json:"created_at"`
}

type nsListResp struct {
	Namespaces []nsItem `json:"namespaces"`
}

func (h *Handler) createNamespace(w http.ResponseWriter, r *http.Request) {
	if h.ns == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "namespace registry not configured"})
		return
	}
	var req createNsReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	if err := h.ns.Create(req.Name); err != nil {
		if errors.Is(err, namespace.ErrAlreadyExists) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		if errors.Is(err, namespace.ErrInvalidName) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created", "name": req.Name})
}

func (h *Handler) listNamespaces(w http.ResponseWriter, r *http.Request) {
	if h.ns == nil {
		writeJSON(w, http.StatusOK, nsListResp{Namespaces: []nsItem{}})
		return
	}
	all := h.ns.List()
	items := make([]nsItem, 0, len(all))
	for _, ns := range all {
		items = append(items, nsItem{Name: ns.Name, CreatedAt: ns.CreatedAt})
	}
	writeJSON(w, http.StatusOK, nsListResp{Namespaces: items})
}

func (h *Handler) deleteNamespace(w http.ResponseWriter, r *http.Request) {
	if h.ns == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "namespace registry not configured"})
		return
	}
	name := r.PathValue("ns")
	if err := h.ns.Delete(name); err != nil {
		if errors.Is(err, namespace.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Stats API (for the dashboard) ───────────────────────────────────────────

// statsAPISummary returns aggregated metrics for the dashboard summary cards.
// It is O(N) over the in-memory snapshot with no per-queue storage I/O.
func (h *Handler) statsAPISummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.broker.Summary())
}

// statsAPI returns a paginated page of queue stats.
// Query params: page (default 1), limit (default 50, max 200).
func (h *Handler) statsAPI(w http.ResponseWriter, r *http.Request) {
	page := parseIntParam(r, "page", 1)
	limit := parseIntParam(r, "limit", 50)
	writeJSON(w, http.StatusOK, h.broker.QueueStatsPaged(page, limit))
}

func parseIntParam(r *http.Request, key string, def int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return def
	}
	return v
}

// ─── Subscriptions (webhook) ──────────────────────────────────────────────────

func (h *Handler) createSubscription(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if !validName(ns) || !validName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid namespace or queue name"})
		return
	}

	var req subscribeReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.URL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url is required"})
		return
	}
	if !validWebhookURL(req.URL) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url must be an http or https URL"})
		return
	}

	id, err := h.consumer.Register(ns, name, req.URL, req.Secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, subscribeResp{ID: id})
}

func (h *Handler) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.consumer.Deregister(id); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func mapDequeueResults(results []*queue.DequeueResult) []consumedMessage {
	out := make([]consumedMessage, 0, len(results))
	for _, r := range results {
		m := consumedMessage{ReceiptHandle: r.ReceiptHandle}
		if r.Message != nil {
			m.ID = r.Message.ID
			m.Body = base64.StdEncoding.EncodeToString(r.Message.Body)
			m.Namespace = r.Message.Namespace
			m.Queue = r.Message.Queue
			m.PublishedAt = r.Message.PublishedAt
			m.Metadata = r.Message.Metadata
		}
		out = append(out, m)
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
		return false
	}
	return true
}

// validWebhookURL checks that the target URL is a plain http or https address.
// This prevents SSRF via other URI schemes (file://, ftp://, gopher://, etc.).
// Note: it does not block private RFC-1918 ranges because PulseMQ is a
// self-hosted server where the operator controls what endpoints are reachable.
// Operators who need SSRF protection at the network level should use firewall
// rules or an egress proxy.
func validWebhookURL(raw string) bool {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	return scheme == "http" || scheme == "https"
}
