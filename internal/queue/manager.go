package queue

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sneh-joshi/pulsemq/internal/scheduler"
	"github.com/sneh-joshi/pulsemq/internal/storage"
)

// ─── DLQ naming ──────────────────────────────────────────────────────────────

const dlqPrefix = "__dlq__"

// DLQName returns the internal queue name for the dead-letter queue paired
// with the given queue name.
func DLQName(queueName string) string { return dlqPrefix + queueName }

// IsDLQ reports whether name is a dead-letter queue name.
func IsDLQ(name string) bool { return strings.HasPrefix(name, dlqPrefix) }

// ─── EngineFactory ────────────────────────────────────────────────────────────

// EngineFactory is a constructor for per-queue StorageEngines.
// It is called by the Manager when a queue (or its DLQ) is first created.
//
// The factory is responsible for creating or reopening the on-disk storage
// for the given namespace/queue combination.
//
// Typical implementation:
//
//	func(ns, name string) (storage.StorageEngine, error) {
//	    dir := filepath.Join(dataDir, "namespaces", ns, name)
//	    return local.Open(dir)
//	}
type EngineFactory func(ns, name string) (storage.StorageEngine, error)

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager owns the lifecycle of all Queue instances.
//
// Responsibilities:
//   - Create queues on demand (GetOrCreate).
//   - Wire each queue to its paired DLQ.
//   - Register/unregister queues with the shared Scheduler.
//   - Tear everything down cleanly on Close.
//
// All methods are safe for concurrent use.
type Manager struct {
	mu      sync.RWMutex
	queues  map[string]*Queue // queueKey → *Queue
	factory EngineFactory
	sched   *scheduler.Scheduler
	defCfg  Config // applied to every newly created queue unless overridden
}

// NewManager creates a Manager.
//
// factory is called to create a per-queue StorageEngine.
// sched is the shared Min-Heap Scheduler; pass nil to disable scheduled delivery.
// defCfg is the default Config applied to every queue created via GetOrCreate.
func NewManager(factory EngineFactory, sched *scheduler.Scheduler, defCfg Config) *Manager {
	return &Manager{
		queues:  make(map[string]*Queue),
		factory: factory,
		sched:   sched,
		defCfg:  defCfg,
	}
}

// GetOrCreate returns the live Queue for ns/name, creating it first if needed.
// The DLQ for this queue is also created lazily.
func (m *Manager) GetOrCreate(ns, name string) (*Queue, error) {
	key := ns + "/" + name

	m.mu.RLock()
	q, ok := m.queues[key]
	m.mu.RUnlock()
	if ok {
		return q, nil
	}

	return m.create(ns, name, m.defCfg)
}

// Create explicitly creates a queue with a custom Config.
// Returns ErrQueueExists if the queue already exists.
func (m *Manager) Create(ns, name string, cfg Config) (*Queue, error) {
	key := ns + "/" + name

	m.mu.RLock()
	_, exists := m.queues[key]
	m.mu.RUnlock()
	if exists {
		return nil, fmt.Errorf("%w: %s", ErrQueueExists, key)
	}

	return m.create(ns, name, cfg)
}

// Delete stops and removes the queue for ns/name (and its DLQ if present).
// Returns ErrQueueNotFound if the queue has never been created.
func (m *Manager) Delete(ns, name string) error {
	key := ns + "/" + name

	m.mu.Lock()
	q, ok := m.queues[key]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrQueueNotFound, key)
	}
	delete(m.queues, key)

	// Also remove the DLQ entry if present.
	dlqKey := ns + "/" + DLQName(name)
	dlq := m.queues[dlqKey]
	delete(m.queues, dlqKey)
	m.mu.Unlock()

	if err := q.Close(); err != nil {
		return fmt.Errorf("delete %s: close queue: %w", key, err)
	}
	if dlq != nil {
		_ = dlq.Close()
	}
	return nil
}

// Get returns the live Queue for ns/name, or ErrQueueNotFound.
func (m *Manager) Get(ns, name string) (*Queue, error) {
	key := ns + "/" + name
	m.mu.RLock()
	q, ok := m.queues[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrQueueNotFound, key)
	}
	return q, nil
}

// List returns a snapshot of all currently live queue keys.
func (m *Manager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.queues))
	for k := range m.queues {
		keys = append(keys, k)
	}
	return keys
}

// QueueSnapshot is a point-in-time view of a single queue's depth counters.
type QueueSnapshot struct {
	Key       string
	Ready     int64
	InFlight  int64
	Scheduled int64
}

// AllStats returns a snapshot of ready, in-flight, and scheduled counts for
// every live queue. DLQ queues (prefixed with __dlq__) are included because
// callers may use them to compute DLQ depths.
func (m *Manager) AllStats() []QueueSnapshot {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	out := make([]QueueSnapshot, 0, len(qs))
	for _, q := range qs {
		var scheduled int64
		if m.sched != nil {
			scheduled = m.sched.CountByQueue(q.Key)
		}
		out = append(out, QueueSnapshot{
			Key:       q.Key,
			Ready:     int64(q.Len()),
			InFlight:  int64(q.InFlightCount()),
			Scheduled: scheduled,
		})
	}
	return out
}

// Close stops the Scheduler (if any) and closes all queues.
func (m *Manager) Close() error {
	if m.sched != nil {
		m.sched.Stop()
	}

	m.mu.Lock()
	queues := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		queues = append(queues, q)
	}
	m.queues = make(map[string]*Queue)
	m.mu.Unlock()

	var firstErr error
	for _, q := range queues {
		if err := q.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ─── errors ──────────────────────────────────────────────────────────────────

var (
	ErrQueueExists   = errors.New("queue already exists")
	ErrQueueNotFound = errors.New("queue not found")
)

// ─── internal create ─────────────────────────────────────────────────────────

// create builds the Queue (and its DLQ) under the write lock, wires them
// together, registers them with the Scheduler, and returns the primary queue.
func (m *Manager) create(ns, name string, cfg Config) (*Queue, error) {
	key := ns + "/" + name

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check under write lock (avoid TOCTOU between RLock check and WLock).
	if q, ok := m.queues[key]; ok {
		return q, nil
	}

	// ── 1. Create or reopen the primary queue's storage engine ───────────────
	eng, err := m.factory(ns, name)
	if err != nil {
		return nil, fmt.Errorf("create queue %s: engine: %w", key, err)
	}

	// ── 2. Create or reopen the DLQ's storage engine ─────────────────────────
	dlqName := DLQName(name)
	dlqEng, err := m.factory(ns, dlqName)
	if err != nil {
		_ = eng.Close()
		return nil, fmt.Errorf("create dlq %s/%s: engine: %w", ns, dlqName, err)
	}

	// ── 3. Build the DLQ queue (no onSchedule, no nested DLQ) ────────────────
	dlqKey := ns + "/" + dlqName
	dlq, err := New(ns, dlqName, dlqEng, cfg, nil, nil)
	if err != nil {
		_ = eng.Close()
		_ = dlqEng.Close()
		return nil, fmt.Errorf("create dlq queue %s: %w", dlqKey, err)
	}
	m.queues[dlqKey] = dlq

	// ── 4. Build the primary queue ────────────────────────────────────────────
	// onSchedule: forwards to the global Scheduler (nil if no scheduler).
	var onSchedule func(msgID, queueKey string, deliverAt int64)
	if m.sched != nil {
		sched := m.sched
		onSchedule = func(msgID, queueKey string, deliverAt int64) {
			sched.Schedule(msgID, queueKey, deliverAt)
		}
	}

	// onDLQ: publishes the dead-lettered message to the DLQ queue.
	onDLQ := func(msg *Message) error {
		return dlq.Publish(msg)
	}

	q, err := New(ns, name, eng, cfg, onSchedule, onDLQ)
	if err != nil {
		_ = eng.Close()
		_ = dlq.Close()
		delete(m.queues, dlqKey)
		return nil, fmt.Errorf("create queue %s: %w", key, err)
	}
	m.queues[key] = q

	return q, nil
}

// ─── Scheduler integration ────────────────────────────────────────────────────

// SchedulerReadyFn returns the function that should be passed to Scheduler.Start().
// When the Scheduler fires a message's deliverAt, this function looks up the
// queue and promotes the message from SCHEDULED → READY.
func (m *Manager) SchedulerReadyFn() func(msgID, queueKey string) {
	return func(msgID, queueKey string) {
		m.mu.RLock()
		q, ok := m.queues[queueKey]
		m.mu.RUnlock()
		if !ok {
			return
		}
		_ = q.promoteScheduled(msgID)
	}
}

// ─── Storage engine helpers (package-internal) ───────────────────────────────

// promoteScheduled transitions a message from SCHEDULED → READY by reading
// its index entry and calling EnqueueScheduled. Exported to the manager via
// package-internal access (same package).
func (q *Queue) promoteScheduled(msgID string) error {
	entry, err := q.eng.ReadIndex(msgID)
	if err != nil {
		return fmt.Errorf("promote scheduled %s: read index: %w", msgID, err)
	}
	if entry.Status != StatusScheduled {
		// Already promoted (race) or cancelled — nothing to do.
		return nil
	}
	return q.EnqueueScheduled(msgID, entry.Offset, entry.Attempt)
}

// Ensure Manager satisfies a basic sanity check.
var _ interface {
	GetOrCreate(ns, name string) (*Queue, error)
	Close() error
} = (*Manager)(nil)

// Ensure unused import is referenced. storage is used by manager.go only via
// the EngineFactory return type — we need the import to compile.
var _ storage.StorageEngine
