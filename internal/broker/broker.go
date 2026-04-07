// Package broker is the central orchestrator for PulseMQ.
//
// All application code (HTTP handlers, WebSocket, webhook consumer) talks to
// the Broker — never directly to the queue or storage layer. This enforces the
// layered architecture from the design plan and keeps coupling low.
//
// Data flow:
//
//	Producer → Broker.Publish → queue.Queue.Publish → StorageEngine
//	Consumer → Broker.Consume → queue.Queue.DequeueN → StorageEngine
//	          → Broker.Ack    → queue.Queue.Ack
//	          → Broker.Nack   → queue.Queue.Nack
package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/config"
	"github.com/sneh-joshi/pulsemq/internal/dlq"
	"github.com/sneh-joshi/pulsemq/internal/metrics"
	"github.com/sneh-joshi/pulsemq/internal/namespace"
	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/queue"
	"github.com/sneh-joshi/pulsemq/internal/scheduler"
	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/storage/local"
)

// ─── Error sentinels ──────────────────────────────────────────────────────────

var (
	// ErrUnknownReceipt is returned when Ack/Nack is called with an unrecognised
	// receipt handle. This can happen if the message already expired or was
	// already ACKed.
	ErrUnknownReceipt = errors.New("broker: unknown receipt handle")
)

// ─── Request / Response types ─────────────────────────────────────────────────

// PublishRequest carries everything needed to publish one message.
type PublishRequest struct {
	Namespace  string
	Queue      string
	Body       []byte
	DeliverAt  int64             // 0 = deliver immediately
	MaxRetries int               // 0 = use queue default
	Metadata   map[string]string // optional producer-set key/value pairs
}

// PublishResponse is returned after a successful Publish.
type PublishResponse struct {
	MessageID string
}

// ConsumeRequest carries parameters for a consume (dequeue) call.
type ConsumeRequest struct {
	Namespace         string
	Queue             string
	N                 int   // max messages to return; 0 uses queue default (1)
	VisibilityTimeout int64 // ms; 0 = use queue default
}

// Stats is a lightweight snapshot of broker-wide state.
type Stats struct {
	QueueCount int
}

// QueueInfo carries the depth snapshot for a single queue, used by the dashboard.
type QueueInfo struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key"`
	Ready     int64  `json:"ready"`
	InFlight  int64  `json:"in_flight"`
	Scheduled int64  `json:"scheduled"`
	Depth     int64  `json:"depth"` // total: ready + in_flight + scheduled
	DLQDepth  int64  `json:"dlq_depth"`
}

// QueueSummary is a cheap aggregated snapshot for the dashboard summary cards.
// It is computed in a single O(N) pass with no per-queue storage I/O.
type QueueSummary struct {
	TotalQueues    int   `json:"total_queues"`
	Namespaces     int   `json:"namespaces"`
	TotalDepth     int64 `json:"total_depth"`
	TotalScheduled int64 `json:"total_scheduled"`
	DLQAlerts      int   `json:"dlq_alerts"` // queues whose DLQ has ≥ 1 message
}

// QueuePage is a single page of QueueInfo returned by QueueStatsPaged.
type QueuePage struct {
	Queues     []QueueInfo `json:"queues"`
	Total      int         `json:"total"`
	Page       int         `json:"page"`
	Limit      int         `json:"limit"`
	TotalPages int         `json:"total_pages"`
}

// ─── Option / functional options ─────────────────────────────────────────────

// Option is a functional option for the Broker.
type Option func(*Broker)

// WithMetrics attaches a metrics.Registry to the broker so that every
// Publish/Consume/Ack/Nack call increments the relevant counter.
func WithMetrics(reg *metrics.Registry) Option {
	return func(b *Broker) { b.metrics = reg }
}

// WithNamespaceRegistry attaches a namespace.Registry so that namespaces are
// automatically registered on first use during Publish / CreateQueue.
func WithNamespaceRegistry(reg *namespace.Registry) Option {
	return func(b *Broker) { b.ns = reg }
}

// ─── Broker ───────────────────────────────────────────────────────────────────

// Broker wires together the queue manager, scheduler, and DLQ manager into a
// single façade used by every transport layer.
//
// All methods are safe for concurrent use.
type Broker struct {
	cfg    *config.Config
	nodeID string

	qm     *queue.Manager
	sched  *scheduler.Scheduler
	dlqMgr *dlq.Manager

	cancel context.CancelFunc

	// Optional integrations (set via functional options).
	metrics *metrics.Registry
	ns      *namespace.Registry

	// receipts maps receiptHandle → "ns/name" (queueKey).
	// This allows Ack/Nack to route to the correct queue from just a receipt
	// handle, as required by the HTTP API (DELETE /messages/{receipt}).
	mu       sync.RWMutex
	receipts map[string]string
}

// New creates and starts a Broker.
//
// The broker creates a StorageEngine factory that stores queue data under
// cfg.Node.DataDir/namespaces/{ns}/{name}/.
// The Scheduler is started immediately with a background context.
func New(cfg *config.Config, nodeID string, opts ...Option) (*Broker, error) {
	dataDir := cfg.Node.DataDir
	if dataDir == "" {
		dataDir = "./data"
	}

	factory := func(ns, name string) (storage.StorageEngine, error) {
		dir := filepath.Join(dataDir, "namespaces", ns, name)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create queue dir %s: %w", dir, err)
		}
		return local.Open(dir)
	}

	defCfg := queue.Config{
		DefaultVisibilityTimeoutMs: int64(cfg.Queue.DefaultVisibilityTimeoutMs),
		MaxMessages:                int64(cfg.Queue.MaxMessages),
		MaxRetries:                 cfg.Queue.MaxRetries,
		MaxBatchSize:               cfg.Queue.MaxBatchSize,
	}
	// Apply defaults if config fields are zero.
	qDefaults := queue.DefaultConfig()
	if defCfg.DefaultVisibilityTimeoutMs <= 0 {
		defCfg.DefaultVisibilityTimeoutMs = qDefaults.DefaultVisibilityTimeoutMs
	}
	if defCfg.MaxBatchSize <= 0 {
		defCfg.MaxBatchSize = qDefaults.MaxBatchSize
	}
	if defCfg.MaxRetries <= 0 {
		defCfg.MaxRetries = qDefaults.MaxRetries
	}

	sched := scheduler.New()
	qm := queue.NewManager(factory, sched, defCfg)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx, qm.SchedulerReadyFn())

	b := &Broker{
		cfg:      cfg,
		nodeID:   nodeID,
		qm:       qm,
		sched:    sched,
		dlqMgr:   dlq.NewManager(qm),
		cancel:   cancel,
		receipts: make(map[string]string),
	}
	for _, o := range opts {
		o(b)
	}
	return b, nil
}

// Close stops the scheduler and closes all queues.
func (b *Broker) Close() error {
	b.cancel()
	return b.qm.Close()
}

// Stats returns a lightweight snapshot of broker state.
func (b *Broker) Stats() Stats {
	return Stats{QueueCount: len(b.qm.List())}
}

// Summary returns aggregated metrics in a single O(N) pass over the in-memory
// snapshot — no per-queue storage I/O. Used by the dashboard summary cards.
func (b *Broker) Summary() QueueSummary {
	snaps := b.qm.AllStats()
	nsSet := make(map[string]struct{}, 16)
	var s QueueSummary
	for _, snap := range snaps {
		ns, name, err := splitKey(snap.Key)
		if err != nil {
			continue
		}
		if strings.HasPrefix(name, "__dlq__") {
			// A non-empty DLQ is a DLQ alert on the owning queue.
			if snap.Ready+snap.InFlight > 0 {
				s.DLQAlerts++
			}
			continue
		}
		s.TotalQueues++
		s.TotalDepth += snap.Ready + snap.InFlight + snap.Scheduled
		s.TotalScheduled += snap.Scheduled
		nsSet[ns] = struct{}{}
	}
	s.Namespaces = len(nsSet)
	return s
}

// QueueStatsPaged returns one page of primary-queue snapshots, sorted
// alphabetically by namespace then name. DLQ depth is looked up only for
// the queues that appear on the requested page — not all queues.
//
//	page  – 1-based; clamped to valid range
//	limit – page size; clamped to [1, 200]
func (b *Broker) QueueStatsPaged(page, limit int) QueuePage {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	type candidate struct {
		snap queue.QueueSnapshot
		ns   string
		name string
	}
	snaps := b.qm.AllStats()
	items := make([]candidate, 0, len(snaps))
	for _, s := range snaps {
		ns, name, err := splitKey(s.Key)
		if err != nil || strings.HasPrefix(name, "__dlq__") {
			continue
		}
		items = append(items, candidate{snap: s, ns: ns, name: name})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ns != items[j].ns {
			return items[i].ns < items[j].ns
		}
		return items[i].name < items[j].name
	})

	total := len(items)
	totalPages := (total + limit - 1) / limit
	if totalPages == 0 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * limit
	if start >= total {
		return QueuePage{Queues: []QueueInfo{}, Total: total, Page: page, Limit: limit, TotalPages: totalPages}
	}
	end := start + limit
	if end > total {
		end = total
	}

	out := make([]QueueInfo, 0, end-start)
	for _, it := range items[start:end] {
		s := it.snap
		out = append(out, QueueInfo{
			Namespace: it.ns,
			Name:      it.name,
			Key:       s.Key,
			Ready:     s.Ready,
			InFlight:  s.InFlight,
			Scheduled: s.Scheduled,
			Depth:     s.Ready + s.InFlight + s.Scheduled,
			DLQDepth:  b.dlqMgr.Len(it.ns, it.name),
		})
	}
	return QueuePage{Queues: out, Total: total, Page: page, Limit: limit, TotalPages: totalPages}
}

// QueueStats returns a depth snapshot for every queue — kept for backward
// compatibility with tests. For the dashboard, prefer QueueStatsPaged.
func (b *Broker) QueueStats() []QueueInfo {
	page := b.QueueStatsPaged(1, 200)
	return page.Queues
}

// NodeID returns the node identity string used in published messages.
func (b *Broker) NodeID() string { return b.nodeID }

// ─── Publish ──────────────────────────────────────────────────────────────────

// Publish durably stores a message and enqueues it (or schedules it).
func (b *Broker) Publish(req PublishRequest) (*PublishResponse, error) {
	// Auto-register the namespace on first use.
	if b.ns != nil {
		if err := b.ns.Ensure(req.Namespace); err != nil {
			return nil, fmt.Errorf("broker: ensure namespace %s: %w", req.Namespace, err)
		}
	}

	q, err := b.qm.GetOrCreate(req.Namespace, req.Queue)
	if err != nil {
		return nil, fmt.Errorf("broker: get queue %s/%s: %w", req.Namespace, req.Queue, err)
	}

	msgID, err := node.NewID()
	if err != nil {
		return nil, fmt.Errorf("broker: generate message ID: %w", err)
	}

	msg := &queue.Message{
		ID:          msgID,
		Namespace:   req.Namespace,
		Queue:       req.Queue,
		Body:        req.Body,
		DeliverAt:   req.DeliverAt,
		PublishedAt: time.Now().UnixMilli(),
		MaxRetries:  req.MaxRetries,
		Metadata:    req.Metadata,
		NodeID:      b.nodeID,
	}

	if err := q.Publish(msg); err != nil {
		return nil, fmt.Errorf("broker: publish to %s/%s: %w", req.Namespace, req.Queue, err)
	}

	if b.metrics != nil {
		b.metrics.Published.Inc(metrics.QueueKey(req.Namespace, req.Queue))
	}

	return &PublishResponse{MessageID: msgID}, nil
}

// ─── Consume ──────────────────────────────────────────────────────────────────

// Consume dequeues up to req.N messages from the named queue.
// Receipt handles are registered in the broker's routing table so that
// Ack/Nack can be called without specifying the queue.
func (b *Broker) Consume(req ConsumeRequest) ([]*queue.DequeueResult, error) {
	q, err := b.qm.GetOrCreate(req.Namespace, req.Queue)
	if err != nil {
		return nil, fmt.Errorf("broker: get queue %s/%s: %w", req.Namespace, req.Queue, err)
	}

	n := req.N
	if n <= 0 {
		n = 1
	}

	results, err := q.DequeueN(n, req.VisibilityTimeout)
	if err != nil {
		return nil, err
	}

	// Register receipt handles → queueKey for ACK/NACK routing.
	if len(results) > 0 {
		b.mu.Lock()
		for _, r := range results {
			b.receipts[r.ReceiptHandle] = q.Key
		}
		b.mu.Unlock()
	}

	if b.metrics != nil && len(results) > 0 {
		b.metrics.Consumed.Add(metrics.QueueKey(req.Namespace, req.Queue), int64(len(results)))
	}

	return results, nil
}

// ─── Ack / Nack ───────────────────────────────────────────────────────────────

// Ack marks a message as successfully processed and removes it from the queue.
func (b *Broker) Ack(receiptHandle string) error {
	q, err := b.queueForReceipt(receiptHandle)
	if err != nil {
		return err
	}
	if ackErr := q.Ack(receiptHandle); ackErr != nil {
		return ackErr
	}
	b.mu.Lock()
	delete(b.receipts, receiptHandle)
	b.mu.Unlock()

	if b.metrics != nil {
		ns, name, _ := splitKey(q.Key)
		b.metrics.Acked.Inc(metrics.QueueKey(ns, name))
	}
	return nil
}

// Nack signals failed processing. The message will be requeued or dead-lettered
// depending on its retry count.
func (b *Broker) Nack(receiptHandle string) error {
	q, err := b.queueForReceipt(receiptHandle)
	if err != nil {
		return err
	}
	if nackErr := q.Nack(receiptHandle); nackErr != nil {
		return nackErr
	}
	b.mu.Lock()
	delete(b.receipts, receiptHandle)
	b.mu.Unlock()

	if b.metrics != nil {
		ns, name, _ := splitKey(q.Key)
		b.metrics.Nacked.Inc(metrics.QueueKey(ns, name))
	}
	return nil
}

// queueForReceipt looks up the queue that owns receiptHandle.
func (b *Broker) queueForReceipt(receiptHandle string) (*queue.Queue, error) {
	b.mu.RLock()
	key, ok := b.receipts[receiptHandle]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownReceipt, receiptHandle)
	}

	ns, name, err := splitKey(key)
	if err != nil {
		return nil, err
	}
	return b.qm.Get(ns, name)
}

// ─── Queue management ─────────────────────────────────────────────────────────

// CreateQueue explicitly creates a queue with a custom config.
func (b *Broker) CreateQueue(ns, name string, cfg queue.Config) error {
	// Auto-register the namespace on explicit queue creation.
	if b.ns != nil {
		if err := b.ns.Ensure(ns); err != nil {
			return fmt.Errorf("broker: ensure namespace %s: %w", ns, err)
		}
	}
	_, err := b.qm.Create(ns, name, cfg)
	return err
}

// DeleteQueue removes a queue and its DLQ.
// PurgeQueue removes all READY and IN_FLIGHT messages from a queue without
// deleting the queue itself. Returns the number of messages purged.
func (b *Broker) PurgeQueue(ns, name string) (int, error) {
	q, err := b.qm.Get(ns, name)
	if err != nil {
		return 0, fmt.Errorf("broker: purge queue %s/%s: %w", ns, name, err)
	}
	return q.Purge()
}

func (b *Broker) DeleteQueue(ns, name string) error {
	return b.qm.Delete(ns, name)
}

// ListQueues returns a sorted list of all live queue keys.
func (b *Broker) ListQueues() []string {
	return b.qm.List()
}

// QueueManager exposes the underlying queue.Manager (for WebSocket push
// handlers that need direct access to queue state).
func (b *Broker) QueueManager() *queue.Manager {
	return b.qm
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

// DrainDLQ dequeues up to limit messages from the DLQ for ns/primaryQueue.
// Receipt handles are registered so callers can ACK/NACK them.
func (b *Broker) DrainDLQ(ns, primaryQueue string, limit int) ([]*queue.DequeueResult, error) {
	results, err := b.dlqMgr.Drain(ns, primaryQueue, limit)
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		dlqKey := ns + "/" + queue.DLQName(primaryQueue)
		b.mu.Lock()
		for _, r := range results {
			b.receipts[r.ReceiptHandle] = dlqKey
		}
		b.mu.Unlock()
	}
	return results, nil
}

// ReplayDLQ moves up to limit messages from the DLQ back to the primary queue.
func (b *Broker) ReplayDLQ(ns, primaryQueue string, limit int) (int, error) {
	return b.dlqMgr.Replay(ns, primaryQueue, limit)
}

// DLQLen returns the number of messages currently in the DLQ.
func (b *Broker) DLQLen(ns, primaryQueue string) int64 {
	return b.dlqMgr.Len(ns, primaryQueue)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// splitKey splits "ns/name" into ("ns", "name").
func splitKey(key string) (ns, name string, err error) {
	idx := strings.IndexByte(key, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("broker: malformed queue key %q", key)
	}
	return key[:idx], key[idx+1:], nil
}
