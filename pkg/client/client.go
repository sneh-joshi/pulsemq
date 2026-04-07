// Package client is the official Go SDK for PulseMQ.
//
// # Quick start
//
//	c := client.New("http://localhost:8080")
//
//	// Publish immediately
//	id, err := c.Publish(ctx, "payments", "invoices", []byte(`{"amount":42}`))
//
//	// Publish in 1 hour
//	id, err := c.Publish(ctx, "payments", "invoices", []byte(`…`),
//	    client.WithDelay(time.Hour))
//
//	// Consume
//	msgs, err := c.Consume(ctx, "payments", "invoices", 10)
//	for _, m := range msgs {
//	    process(m)
//	    c.Ack(ctx, m.ReceiptHandle)
//	}
//
// # Error handling
//
// All methods return an *APIError when the server responds with a non-2xx
// status code. Check errors.As(err, &client.APIError{}) to inspect the HTTP
// status and server message.
//
// # Connection reuse
//
// Client is safe for concurrent use. It shares a single http.Client internally
// so connections are reused across goroutines.
package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ─── Error type ───────────────────────────────────────────────────────────────

// APIError is returned when the PulseMQ server responds with a non-2xx status.
type APIError struct {
	StatusCode int    // HTTP status code
	Message    string // "error" field from the JSON response body
}

func (e *APIError) Error() string {
	return fmt.Sprintf("pulsemq: server returned %d: %s", e.StatusCode, e.Message)
}

// IsNotFound reports whether the error is a 404 from the server.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode == http.StatusNotFound
}

// IsConflict reports whether the error is a 409 (already exists) from the server.
func IsConflict(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.StatusCode == http.StatusConflict
}

// ─── Client options ───────────────────────────────────────────────────────────

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAPIKey sets the API key sent in every request as the X-Api-Key header.
// Required when the server has auth.enabled = true.
func WithAPIKey(key string) ClientOption {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient replaces the default http.Client.
// Use this to configure TLS, proxies, or request tracing.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.http = hc }
}

// WithTimeout sets the per-request timeout.
// The default is 30 seconds.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.http.Timeout = d }
}

// ─── Client ───────────────────────────────────────────────────────────────────

// Client is the PulseMQ API client. It is safe for concurrent use.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// New creates a new Client that connects to the PulseMQ server at baseURL.
//
//	c := client.New("http://localhost:8080")
//	c := client.New("http://pulsemq.example.com", client.WithAPIKey("secret"))
func New(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ─── Publish options ──────────────────────────────────────────────────────────

// PublishOption configures a single Publish call.
type PublishOption func(*publishPayload)

// WithDeliverAt schedules delivery at the given absolute time.
//
//	client.WithDeliverAt(time.Now().Add(time.Hour))
func WithDeliverAt(t time.Time) PublishOption {
	return func(p *publishPayload) { p.DeliverAt = t.UnixMilli() }
}

// WithDelay schedules delivery after a relative delay from now.
//
//	client.WithDelay(24 * time.Hour)
func WithDelay(d time.Duration) PublishOption {
	return func(p *publishPayload) { p.DeliverAt = time.Now().Add(d).UnixMilli() }
}

// WithMaxRetries overrides the queue-default maximum delivery attempts.
// Set to 0 to use the queue default.
func WithMaxRetries(n int) PublishOption {
	return func(p *publishPayload) { p.MaxRetries = n }
}

// WithMetadata attaches user-defined key/value pairs to the message.
func WithMetadata(m map[string]string) PublishOption {
	return func(p *publishPayload) { p.Metadata = m }
}

// ─── Consume options ──────────────────────────────────────────────────────────

// ConsumeOption configures a single Consume call.
type ConsumeOption func(*consumeParams)

// WithVisibilityTimeout overrides the queue-default visibility timeout.
// The message will become visible again if not ACKed within this window.
func WithVisibilityTimeout(d time.Duration) ConsumeOption {
	return func(p *consumeParams) { p.visibilityTimeoutMs = d.Milliseconds() }
}

// ─── Queue options ────────────────────────────────────────────────────────────

// QueueOption configures CreateQueue.
type QueueOption func(*createQueuePayload)

// WithQueueVisibilityTimeout sets the per-queue default visibility timeout.
func WithQueueVisibilityTimeout(d time.Duration) QueueOption {
	return func(p *createQueuePayload) { p.VisibilityTimeoutMs = d.Milliseconds() }
}

// WithQueueMaxMessages sets the hard message cap for the queue.
func WithQueueMaxMessages(n int64) QueueOption {
	return func(p *createQueuePayload) { p.MaxMessages = n }
}

// WithQueueMaxRetries sets the maximum delivery attempts before DLQ.
func WithQueueMaxRetries(n int) QueueOption {
	return func(p *createQueuePayload) { p.MaxRetries = n }
}

// WithQueueMaxBatchSize sets the maximum messages a consumer can receive per call.
func WithQueueMaxBatchSize(n int) QueueOption {
	return func(p *createQueuePayload) { p.MaxBatchSize = n }
}

// ─── Domain types ─────────────────────────────────────────────────────────────

// Message is a message received from a consume call.
type Message struct {
	// ID is the ULID assigned at publish time.
	ID string

	// Body is the raw message payload decoded from base64.
	Body []byte

	// ReceiptHandle must be passed to Ack or Nack.
	// It expires when the visibility timeout elapses.
	ReceiptHandle string

	// Namespace and Queue identify where the message came from.
	Namespace string
	Queue     string

	// Attempt is the 1-based delivery attempt number.
	Attempt int

	// PublishedAt is when the message was originally published (UTC).
	PublishedAt time.Time

	// Metadata holds the user-defined key/value pairs set at publish time.
	Metadata map[string]string
}

// Namespace is a logical grouping of queues.
type Namespace struct {
	Name      string
	CreatedAt time.Time
}

// HealthInfo contains the data returned by the /health endpoint.
type HealthInfo struct {
	Status  string
	NodeID  string
	Queues  int
	Uptime  time.Duration
	Version string
}

// QueueInfo is the depth snapshot of a queue returned by Stats.
type QueueInfo struct {
	Namespace string
	Name      string
	Ready     int64
	InFlight  int64
	DLQDepth  int64
}

// ─── Message operations ───────────────────────────────────────────────────────

// Publish sends a single message to the named queue and returns its ULID.
//
//	id, err := c.Publish(ctx, "payments", "invoices", []byte(`{"amount":99}`))
//
// To schedule delivery in the future:
//
//	id, err := c.Publish(ctx, "payments", "invoices", body, client.WithDelay(time.Hour))
func (c *Client) Publish(ctx context.Context, namespace, queue string, body []byte, opts ...PublishOption) (string, error) {
	p := &publishPayload{
		Body: base64.StdEncoding.EncodeToString(body),
	}
	for _, o := range opts {
		o(p)
	}

	var resp struct {
		ID string `json:"id"`
	}
	path := fmt.Sprintf("/namespaces/%s/queues/%s/messages", namespace, queue)
	if err := c.do(ctx, http.MethodPost, path, p, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// PublishBatch sends multiple messages to the named queue in a single request.
// Each message body is encoded separately. Returns the list of assigned ULIDs.
//
//	ids, err := c.PublishBatch(ctx, "ns", "q", bodies)
func (c *Client) PublishBatch(ctx context.Context, namespace, queue string, bodies [][]byte, opts ...PublishOption) ([]string, error) {
	payloads := make([]publishPayload, len(bodies))
	for i, b := range bodies {
		payloads[i] = publishPayload{Body: base64.StdEncoding.EncodeToString(b)}
		for _, o := range opts {
			o(&payloads[i])
		}
	}

	var resp struct {
		IDs []string `json:"ids"`
	}
	path := fmt.Sprintf("/namespaces/%s/queues/%s/messages/batch", namespace, queue)
	if err := c.do(ctx, http.MethodPost, path, payloads, &resp); err != nil {
		return nil, err
	}
	return resp.IDs, nil
}

// Consume retrieves up to n messages from the named queue.
// Returns an empty slice (not an error) when the queue is empty.
//
//	msgs, err := c.Consume(ctx, "payments", "invoices", 10)
//	for _, m := range msgs {
//	    handle(m)
//	    _ = c.Ack(ctx, m.ReceiptHandle)
//	}
func (c *Client) Consume(ctx context.Context, namespace, queue string, n int, opts ...ConsumeOption) ([]*Message, error) {
	p := &consumeParams{}
	for _, o := range opts {
		o(p)
	}

	q := url.Values{}
	if n > 0 {
		q.Set("n", strconv.Itoa(n))
	}
	if p.visibilityTimeoutMs > 0 {
		q.Set("visibility_timeout_ms", strconv.FormatInt(p.visibilityTimeoutMs, 10))
	}

	path := fmt.Sprintf("/namespaces/%s/queues/%s/messages", namespace, queue)
	if len(q) > 0 {
		path += "?" + q.Encode()
	}

	var resp struct {
		Messages []wireMessage `json:"messages"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}

	out := make([]*Message, 0, len(resp.Messages))
	for i := range resp.Messages {
		m, err := resp.Messages[i].toMessage()
		if err != nil {
			return nil, fmt.Errorf("pulsemq: decode message %d: %w", i, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// Ack acknowledges successful processing of a message and removes it from the
// queue. The receiptHandle comes from Message.ReceiptHandle.
func (c *Client) Ack(ctx context.Context, receiptHandle string) error {
	path := fmt.Sprintf("/messages/%s", url.PathEscape(receiptHandle))
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// Nack signals failed processing. The message will be requeued (if retries
// remain) or moved to the dead-letter queue.
func (c *Client) Nack(ctx context.Context, receiptHandle string) error {
	path := fmt.Sprintf("/messages/%s/nack", url.PathEscape(receiptHandle))
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// ─── Queue management ─────────────────────────────────────────────────────────

// CreateQueue creates a queue with optional configuration.
// It is idempotent in practice — the server will error on duplicate, but you
// can check IsConflict(err) if you want to ignore that case.
func (c *Client) CreateQueue(ctx context.Context, namespace, name string, opts ...QueueOption) error {
	p := &createQueuePayload{}
	for _, o := range opts {
		o(p)
	}
	path := fmt.Sprintf("/namespaces/%s/queues/%s", namespace, name)
	return c.do(ctx, http.MethodPost, path, p, nil)
}

// ListQueues returns all queue keys for a namespace in the form "ns/name".
func (c *Client) ListQueues(ctx context.Context, namespace string) ([]string, error) {
	var resp struct {
		Queues []string `json:"queues"`
	}
	path := fmt.Sprintf("/namespaces/%s/queues", namespace)
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Queues, nil
}

// DeleteQueue permanently removes a queue and all its messages.
func (c *Client) DeleteQueue(ctx context.Context, namespace, name string) error {
	path := fmt.Sprintf("/namespaces/%s/queues/%s", namespace, name)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// ─── Namespace management ─────────────────────────────────────────────────────

// CreateNamespace registers a new namespace.
// Returns an *APIError with StatusCode 409 if the namespace already exists.
func (c *Client) CreateNamespace(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPost, "/namespaces",
		map[string]string{"name": name}, nil)
}

// ListNamespaces returns all registered namespaces sorted by name.
func (c *Client) ListNamespaces(ctx context.Context) ([]*Namespace, error) {
	var resp struct {
		Namespaces []struct {
			Name      string `json:"name"`
			CreatedAt int64  `json:"created_at"`
		} `json:"namespaces"`
	}
	if err := c.do(ctx, http.MethodGet, "/namespaces", nil, &resp); err != nil {
		return nil, err
	}
	out := make([]*Namespace, len(resp.Namespaces))
	for i, ns := range resp.Namespaces {
		out[i] = &Namespace{
			Name:      ns.Name,
			CreatedAt: time.UnixMilli(ns.CreatedAt).UTC(),
		}
	}
	return out, nil
}

// DeleteNamespace removes a namespace from the registry.
func (c *Client) DeleteNamespace(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/namespaces/"+url.PathEscape(name), nil, nil)
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

// DrainDLQ retrieves up to limit messages from the dead-letter queue for the
// named primary queue. The caller can inspect the messages and then ACK them
// to delete them permanently, or NACK to re-route.
func (c *Client) DrainDLQ(ctx context.Context, namespace, queue string, limit int) ([]*Message, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	path := fmt.Sprintf("/namespaces/%s/queues/%s/dlq?%s", namespace, queue, q.Encode())

	var resp struct {
		Messages []wireMessage `json:"messages"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}

	out := make([]*Message, 0, len(resp.Messages))
	for i := range resp.Messages {
		m, err := resp.Messages[i].toMessage()
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// ReplayDLQ moves up to limit messages from the dead-letter queue back into
// the primary queue for re-processing. Returns the number of messages moved.
func (c *Client) ReplayDLQ(ctx context.Context, namespace, queue string, limit int) (int, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	path := fmt.Sprintf("/namespaces/%s/queues/%s/dlq/replay?%s", namespace, queue, q.Encode())

	var resp struct {
		Replayed int `json:"replayed"`
	}
	if err := c.do(ctx, http.MethodPost, path, nil, &resp); err != nil {
		return 0, err
	}
	return resp.Replayed, nil
}

// ─── Webhook subscriptions ────────────────────────────────────────────────────

// Subscribe registers a webhook URL for the named queue.
// The server will POST messages to url as they become ready.
// secret is used to sign the request body with HMAC-SHA256 (X-PulseMQ-Signature).
// Set secret to "" to disable signing.
// Returns the subscription ID needed to call Unsubscribe.
func (c *Client) Subscribe(ctx context.Context, namespace, queue, webhookURL, secret string) (string, error) {
	payload := map[string]string{"url": webhookURL, "secret": secret}
	path := fmt.Sprintf("/namespaces/%s/queues/%s/subscriptions", namespace, queue)

	var resp struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http.MethodPost, path, payload, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Unsubscribe removes a webhook subscription by its ID.
func (c *Client) Unsubscribe(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/subscriptions/"+url.PathEscape(id), nil, nil)
}

// ─── Observability ────────────────────────────────────────────────────────────

// Health checks the server's /health endpoint and returns the node's status.
func (c *Client) Health(ctx context.Context) (*HealthInfo, error) {
	var resp struct {
		Status   string `json:"status"`
		NodeID   string `json:"node_id"`
		Queues   int    `json:"queues"`
		UptimeMs int64  `json:"uptime_ms"`
		Version  string `json:"version"`
	}
	if err := c.do(ctx, http.MethodGet, "/health", nil, &resp); err != nil {
		return nil, err
	}
	return &HealthInfo{
		Status:  resp.Status,
		NodeID:  resp.NodeID,
		Queues:  resp.Queues,
		Uptime:  time.Duration(resp.UptimeMs) * time.Millisecond,
		Version: resp.Version,
	}, nil
}

// Stats returns a depth snapshot for every queue on the server (first page,
// up to 200 queues). The response is decoded from the paginated QueuePage
// envelope returned by GET /api/stats.
func (c *Client) Stats(ctx context.Context) ([]*QueueInfo, error) {
	var page struct {
		Queues []struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			Ready     int64  `json:"ready"`
			InFlight  int64  `json:"in_flight"`
			DLQDepth  int64  `json:"dlq_depth"`
		} `json:"queues"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/stats?limit=200", nil, &page); err != nil {
		return nil, err
	}
	out := make([]*QueueInfo, len(page.Queues))
	for i, r := range page.Queues {
		out[i] = &QueueInfo{
			Namespace: r.Namespace,
			Name:      r.Name,
			Ready:     r.Ready,
			InFlight:  r.InFlight,
			DLQDepth:  r.DLQDepth,
		}
	}
	return out, nil
}

// StatsPaged returns one page of queue depth snapshots. Use page=1 and
// increase page for subsequent pages. limit controls page size (max 200).
func (c *Client) StatsPaged(ctx context.Context, page, limit int) ([]*QueueInfo, int, error) {
	var resp struct {
		Queues []struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			Ready     int64  `json:"ready"`
			InFlight  int64  `json:"in_flight"`
			DLQDepth  int64  `json:"dlq_depth"`
		} `json:"queues"`
		Total      int `json:"total"`
		TotalPages int `json:"total_pages"`
	}
	url := fmt.Sprintf("/api/stats?page=%d&limit=%d", page, limit)
	if err := c.do(ctx, http.MethodGet, url, nil, &resp); err != nil {
		return nil, 0, err
	}
	out := make([]*QueueInfo, len(resp.Queues))
	for i, r := range resp.Queues {
		out[i] = &QueueInfo{
			Namespace: r.Namespace,
			Name:      r.Name,
			Ready:     r.Ready,
			InFlight:  r.InFlight,
			DLQDepth:  r.DLQDepth,
		}
	}
	return out, resp.Total, nil
}

// ─── HTTP transport ───────────────────────────────────────────────────────────

// do performs a single HTTP request.
// body is encoded as JSON when non-nil, resp is decoded from JSON when non-nil.
// A 204 No Content response is treated as success with no body.
func (c *Client) do(ctx context.Context, method, path string, body, resp any) error {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("pulsemq: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("pulsemq: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	req.Header.Set("Accept", "application/json")

	httpResp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("pulsemq: request %s %s: %w", method, path, err)
	}
	defer httpResp.Body.Close()

	// Success without body
	if httpResp.StatusCode == http.StatusNoContent {
		return nil
	}

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Errorf("pulsemq: read response body: %w", err)
	}

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(respBody, &errResp)
		msg := errResp.Error
		if msg == "" {
			msg = http.StatusText(httpResp.StatusCode)
		}
		return &APIError{StatusCode: httpResp.StatusCode, Message: msg}
	}

	if resp != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, resp); err != nil {
			return fmt.Errorf("pulsemq: decode response: %w", err)
		}
	}
	return nil
}

// ─── Internal wire types ──────────────────────────────────────────────────────

type publishPayload struct {
	Body       string            `json:"body"`
	DeliverAt  int64             `json:"deliver_at,omitempty"`
	MaxRetries int               `json:"max_retries,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type consumeParams struct {
	visibilityTimeoutMs int64
}

type createQueuePayload struct {
	VisibilityTimeoutMs int64 `json:"visibility_timeout_ms,omitempty"`
	MaxMessages         int64 `json:"max_messages,omitempty"`
	MaxRetries          int   `json:"max_retries,omitempty"`
	MaxBatchSize        int   `json:"max_batch_size,omitempty"`
}

type wireMessage struct {
	ID            string            `json:"id"`
	Body          string            `json:"body"` // base64
	ReceiptHandle string            `json:"receipt_handle"`
	Namespace     string            `json:"namespace"`
	Queue         string            `json:"queue"`
	Attempt       int               `json:"attempt"`
	PublishedAt   int64             `json:"published_at"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func (w *wireMessage) toMessage() (*Message, error) {
	body, err := base64.StdEncoding.DecodeString(w.Body)
	if err != nil {
		// Fall back to treating the body as raw UTF-8 bytes.
		body = []byte(w.Body)
	}
	return &Message{
		ID:            w.ID,
		Body:          body,
		ReceiptHandle: w.ReceiptHandle,
		Namespace:     w.Namespace,
		Queue:         w.Queue,
		Attempt:       w.Attempt,
		PublishedAt:   time.UnixMilli(w.PublishedAt).UTC(),
		Metadata:      w.Metadata,
	}, nil
}
