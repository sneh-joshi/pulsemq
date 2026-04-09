package queue

import (
	"container/list"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/node"
	"github.com/sneh-joshi/pulsemq/internal/storage"
)

// ─── Per-queue config ─────────────────────────────────────────────────────────

// Config holds tunable parameters for a single queue instance.
// All zero-values are valid; use DefaultConfig() for production-safe defaults.
type Config struct {
	// DefaultVisibilityTimeoutMs is how long a consumer has to ACK before the
	// message becomes re-visible. Applies when the caller passes 0.
	DefaultVisibilityTimeoutMs int64

	// MaxMessages is the maximum number of READY+IN_FLIGHT messages allowed.
	// 0 = unlimited.
	MaxMessages int64

	// MaxRetries is how many delivery attempts are allowed before DLQ.
	// 0 = unlimited retries (never dead-letter).
	MaxRetries int

	// MaxMessageSizeKB caps the body size of a single message.
	// 0 = unlimited.
	MaxMessageSizeKB int

	// MaxBatchSize is the maximum number of messages per DequeueN call.
	MaxBatchSize int
}

// DefaultConfig returns a Config with production-safe defaults.
func DefaultConfig() Config {
	return Config{
		DefaultVisibilityTimeoutMs: 30_000, // 30 seconds
		MaxRetries:                 5,
		MaxMessages:                100_000,
		MaxMessageSizeKB:           256,
		MaxBatchSize:               100,
	}
}

// ─── In-memory data structures ────────────────────────────────────────────────

// ReadyEntry is stored in the in-memory FIFO ready list.
type ReadyEntry struct {
	MsgID  string
	Offset int64
}

// InFlightEntry tracks a message that has been delivered to a consumer but
// not yet ACKed. Kept in memory so the reaper can expire it efficiently.
type InFlightEntry struct {
	MsgID         string
	Offset        int64
	Attempt       int // current attempt number (persisted in IndexEntry.Attempt)
	MaxRetries    int // copied from the message so we can dead-letter without re-reading log
	ReceiptHandle string
	DeadlineMs    int64 // UTC ms after which the message becomes visible again
}

// DequeueResult bundles a delivered message with its receipt handle.
type DequeueResult struct {
	Message       *Message
	ReceiptHandle string
}

// ─── Queue ────────────────────────────────────────────────────────────────────

// Queue is the heart of PulseMQ: an in-memory + on-disk FIFO queue with
// visibility timeouts, durable writes, and optional scheduling support.
//
// Architecture:
//   - "ready" is a linked list of ReadyEntry values (FIFO order, cheap pop-front).
//   - "inFlight" is a map of receipt handle → InFlightEntry for O(1) Ack/Nack.
//   - The background reaper goroutine runs every 500 ms to expire in-flight
//     messages whose visibility deadline has passed.
//
// All public methods are safe for concurrent use.
type Queue struct {
	Namespace string
	Name      string
	Key       string // "namespace/name" — used as a routing key throughout
	cfg       Config
	eng       storage.StorageEngine

	mu       sync.Mutex
	ready    *list.List                // elements are *ReadyEntry (FIFO)
	inFlight map[string]*InFlightEntry // receiptHandle → entry
	msgCount int64                     // READY + IN_FLIGHT count

	// onSchedule is called by Publish when msg.DeliverAt is in the future.
	// Nil disables scheduling (e.g. on DLQ queues — no scheduling needed there).
	onSchedule func(msgID, queueKey string, deliverAt int64)

	// onDLQ is called when a message exhausts its retries.
	// Nil means exhausted messages are silently dropped (useful for the DLQ itself
	// to avoid infinite DLQ→DLQ loops).
	onDLQ func(msg *Message) error

	reaperDone chan struct{}
	reaperWG   sync.WaitGroup
}

// New creates a Queue, rebuilds its in-memory state from storage, and starts
// the background visibility-timeout reaper.
//
// onSchedule may be nil (disables scheduled delivery for this queue).
// onDLQ may be nil (drops messages that exhaust retries instead of DLQ-ing them).
//
// Call Close() when the queue is no longer needed.
func New(
	ns, name string,
	eng storage.StorageEngine,
	cfg Config,
	onSchedule func(msgID, queueKey string, deliverAt int64),
	onDLQ func(msg *Message) error,
) (*Queue, error) {
	if cfg.DefaultVisibilityTimeoutMs <= 0 {
		cfg.DefaultVisibilityTimeoutMs = DefaultConfig().DefaultVisibilityTimeoutMs
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = DefaultConfig().MaxBatchSize
	}

	q := &Queue{
		Namespace:  ns,
		Name:       name,
		Key:        ns + "/" + name,
		cfg:        cfg,
		eng:        eng,
		ready:      list.New(),
		inFlight:   make(map[string]*InFlightEntry),
		onSchedule: onSchedule,
		onDLQ:      onDLQ,
		reaperDone: make(chan struct{}),
	}

	if err := q.loadFromStorage(); err != nil {
		return nil, fmt.Errorf("queue %s: load state: %w", q.Key, err)
	}

	q.startReaper()
	return q, nil
}

// ─── Publish ──────────────────────────────────────────────────────────────────

// Publish durably stores msg and places it in the appropriate queue.
//
//   - If msg.DeliverAt == 0 or deliverAt <= now → immediately READY.
//   - If msg.DeliverAt > now → SCHEDULED; onSchedule callback fires to
//     register the message with the Scheduler.
//
// Publish sets msg.Attempt to 1 if not already set, and copies MaxRetries
// from cfg if the message carries 0.
func (q *Queue) Publish(msg *Message) error {
	if msg.Attempt == 0 {
		msg.Attempt = 1
	}
	if msg.MaxRetries == 0 {
		msg.MaxRetries = q.cfg.MaxRetries
	}

	// Capacity check.
	q.mu.Lock()
	over := q.cfg.MaxMessages > 0 && q.msgCount >= q.cfg.MaxMessages
	q.mu.Unlock()
	if over {
		return fmt.Errorf("queue %s: at capacity (%d messages)", q.Key, q.cfg.MaxMessages)
	}

	// Persist the message body to the append-only log.
	offset, err := q.eng.Append(msg)
	if err != nil {
		return fmt.Errorf("publish: append: %w", err)
	}

	now := time.Now().UnixMilli()
	scheduled := msg.DeliverAt > 0 && msg.DeliverAt > now

	var status Status
	if scheduled {
		status = StatusScheduled
	} else {
		status = StatusReady
	}

	// Persist the index entry (commits the WAL entry too via WriteIndex).
	ie := storage.IndexEntry{
		Offset:  offset,
		Status:  status,
		Attempt: msg.Attempt,
	}
	if err := q.eng.WriteIndex(msg.ID, ie); err != nil {
		return fmt.Errorf("publish: write index: %w", err)
	}

	if scheduled {
		// Hand off to the Scheduler — it will call EnqueueScheduled when ready.
		if q.onSchedule != nil {
			q.onSchedule(msg.ID, q.Key, msg.DeliverAt)
		}
		// Don't bump msgCount for scheduled messages — they aren't consuming
		// ready/in-flight quota yet.
	} else {
		q.mu.Lock()
		q.ready.PushBack(&ReadyEntry{MsgID: msg.ID, Offset: offset})
		q.msgCount++
		q.mu.Unlock()
	}

	return nil
}

// EnqueueScheduled is called by the Scheduler when a message's deliverAt has
// arrived. It transitions the message from SCHEDULED → READY in storage and
// adds it to the in-memory ready list.
func (q *Queue) EnqueueScheduled(msgID string, offset int64, attempt int) error {
	ie := storage.IndexEntry{
		Offset:  offset,
		Status:  StatusReady,
		Attempt: attempt,
	}
	if err := q.eng.WriteIndex(msgID, ie); err != nil {
		return fmt.Errorf("enqueue scheduled %s: write index: %w", msgID, err)
	}

	q.mu.Lock()
	q.ready.PushBack(&ReadyEntry{MsgID: msgID, Offset: offset})
	q.msgCount++
	q.mu.Unlock()
	return nil
}

// ─── Dequeue ─────────────────────────────────────────────────────────────────

// Dequeue pops one message from the ready list.
// Returns nil, nil if the queue is empty.
func (q *Queue) Dequeue(visTimeoutMs int64) (*DequeueResult, error) {
	results, err := q.DequeueN(1, visTimeoutMs)
	if err != nil || len(results) == 0 {
		return nil, err
	}
	return results[0], nil
}

// DequeueN pops up to n messages from the ready list and marks them IN_FLIGHT.
// visTimeoutMs <= 0 uses the queue's default visibility timeout.
// n is capped at cfg.MaxBatchSize.
func (q *Queue) DequeueN(n int, visTimeoutMs int64) ([]*DequeueResult, error) {
	if n <= 0 || n > q.cfg.MaxBatchSize {
		n = q.cfg.MaxBatchSize
	}
	if visTimeoutMs <= 0 {
		visTimeoutMs = q.cfg.DefaultVisibilityTimeoutMs
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var results []*DequeueResult
	now := time.Now().UnixMilli()

	for i := 0; i < n && q.ready.Len() > 0; i++ {
		front := q.ready.Front()
		q.ready.Remove(front)
		re := front.Value.(*ReadyEntry)

		msg, err := q.eng.ReadAt(re.Offset)
		if err != nil {
			// Entry is corrupt/missing — skip and release quota.
			q.msgCount--
			continue
		}

		receiptHandle := node.MustNewID()
		deadlineMs := now + visTimeoutMs

		ie := storage.IndexEntry{
			Offset:               re.Offset,
			Status:               StatusInFlight,
			ReceiptHandle:        receiptHandle,
			VisibilityDeadlineMs: deadlineMs,
			Attempt:              msg.Attempt,
		}
		if err := q.eng.WriteIndex(msg.ID, ie); err != nil {
			// Roll back: put the entry back at the front of the ready list.
			q.ready.PushFront(re)
			return results, fmt.Errorf("dequeue: write index %s: %w", msg.ID, err)
		}

		q.inFlight[receiptHandle] = &InFlightEntry{
			MsgID:         msg.ID,
			Offset:        re.Offset,
			Attempt:       msg.Attempt,
			MaxRetries:    msg.MaxRetries,
			ReceiptHandle: receiptHandle,
			DeadlineMs:    deadlineMs,
		}

		results = append(results, &DequeueResult{
			Message:       msg,
			ReceiptHandle: receiptHandle,
		})
	}
	return results, nil
}

// ─── ACK / NACK ──────────────────────────────────────────────────────────────

// Ack marks the message identified by receiptHandle as successfully processed.
// The message status is set to Deleted in storage.
func (q *Queue) Ack(receiptHandle string) error {
	q.mu.Lock()
	entry, ok := q.inFlight[receiptHandle]
	if !ok {
		q.mu.Unlock()
		return fmt.Errorf("ack: unknown or expired receipt handle %q", receiptHandle)
	}
	delete(q.inFlight, receiptHandle)
	q.msgCount--
	q.mu.Unlock()

	return q.eng.WriteIndex(entry.MsgID, storage.IndexEntry{
		Offset: entry.Offset,
		Status: StatusDeleted,
	})
}

// Nack negatively-acknowledges a message.
// The message is re-queued as READY. If it has exhausted MaxRetries, it is
// instead dead-lettered via the onDLQ callback.
func (q *Queue) Nack(receiptHandle string) error {
	q.mu.Lock()
	entry, ok := q.inFlight[receiptHandle]
	if !ok {
		q.mu.Unlock()
		return fmt.Errorf("nack: unknown or expired receipt handle %q", receiptHandle)
	}
	delete(q.inFlight, receiptHandle)
	q.mu.Unlock()

	return q.requeue(entry)
}

// ─── Introspection ───────────────────────────────────────────────────────────

// Len returns the number of READY messages in the queue.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ready.Len()
}

// InFlightCount returns the number of messages currently held by consumers.
func (q *Queue) InFlightCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inFlight)
}

// TotalCount returns the combined READY + IN_FLIGHT message count.
func (q *Queue) TotalCount() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.msgCount
}

// ─── Close ───────────────────────────────────────────────────────────────────

// Close stops the background reaper and closes the underlying storage engine.
func (q *Queue) Close() error {
	close(q.reaperDone)
	q.reaperWG.Wait()
	return q.eng.Close()
}

// Purge removes all READY and IN_FLIGHT messages from the queue immediately
// without waiting for visibility timeouts or retries.
// Scheduled messages (not yet promoted to READY) are unaffected.
// Returns the number of messages purged.
func (q *Queue) Purge() (int, error) {
	q.mu.Lock()

	// Drain the ready list.
	readyEntries := make([]*ReadyEntry, 0, q.ready.Len())
	for e := q.ready.Front(); e != nil; e = e.Next() {
		readyEntries = append(readyEntries, e.Value.(*ReadyEntry))
	}
	q.ready.Init()

	// Collect all in-flight entries.
	inFlightEntries := make([]*InFlightEntry, 0, len(q.inFlight))
	for _, entry := range q.inFlight {
		inFlightEntries = append(inFlightEntries, entry)
	}
	q.inFlight = make(map[string]*InFlightEntry)

	q.msgCount = 0
	q.mu.Unlock()

	// Persist DELETED status for all cleared messages (best-effort).
	for _, re := range readyEntries {
		_ = q.eng.WriteIndex(re.MsgID, storage.IndexEntry{
			Offset: re.Offset,
			Status: StatusDeleted,
		})
	}
	for _, entry := range inFlightEntries {
		_ = q.eng.WriteIndex(entry.MsgID, storage.IndexEntry{
			Offset: entry.Offset,
			Status: StatusDeleted,
		})
	}

	return len(readyEntries) + len(inFlightEntries), nil
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// requeue decides whether to re-enqueue or dead-letter the message.
// Must be called WITHOUT q.mu held (it acquires it when re-adding to ready).
func (q *Queue) requeue(entry *InFlightEntry) error {
	nextAttempt := entry.Attempt + 1

	// Check dead-letter condition.
	if entry.MaxRetries > 0 && nextAttempt > entry.MaxRetries {
		return q.deadLetter(entry, nextAttempt)
	}

	// Update storage: READY with incremented attempt.
	ie := storage.IndexEntry{
		Offset:  entry.Offset,
		Status:  StatusReady,
		Attempt: nextAttempt,
	}
	if err := q.eng.WriteIndex(entry.MsgID, ie); err != nil {
		return fmt.Errorf("requeue %s: write index: %w", entry.MsgID, err)
	}

	q.mu.Lock()
	q.ready.PushBack(&ReadyEntry{MsgID: entry.MsgID, Offset: entry.Offset})
	q.mu.Unlock()
	return nil
}

// deadLetter moves a message to the DLQ and marks it DEAD_LETTER in storage.
func (q *Queue) deadLetter(entry *InFlightEntry, attempt int) error {
	// Mark as dead-lettered in the primary queue's storage.
	if err := q.eng.WriteIndex(entry.MsgID, storage.IndexEntry{
		Offset:  entry.Offset,
		Status:  StatusDeadLetter,
		Attempt: attempt,
	}); err != nil {
		return fmt.Errorf("dead letter %s: write index: %w", entry.MsgID, err)
	}

	q.mu.Lock()
	q.msgCount--
	q.mu.Unlock()

	// Forward to the DLQ queue if one is wired up.
	if q.onDLQ != nil {
		msg, err := q.eng.ReadAt(entry.Offset)
		if err != nil {
			// Log-and-continue: message is already marked dead_letter in storage.
			return nil
		}
		msg.Attempt = 1 // reset attempt for DLQ delivery tracking
		return q.onDLQ(msg)
	}
	return nil
}

// loadFromStorage scans the index and rebuilds the in-memory ready list and
// inFlight map. SCHEDULED messages are handed to onSchedule for re-registration
// with the Scheduler (so they survive a server restart).
//
// READY entries are sorted by msgID (ULID lex ≈ creation time) to preserve
// approximate FIFO ordering after restart.
func (q *Queue) loadFromStorage() error {
	now := time.Now().UnixMilli()

	type rawReady struct {
		msgID  string
		offset int64
	}
	// pendingWrites collects index updates that must be applied AFTER ForEach
	// completes. bbolt does not allow opening a write transaction while a read
	// transaction is already open on the same DB (inside the same goroutine),
	// so we must defer all writes until after the scan is done.
	type pendingWrite struct {
		msgID string
		entry storage.IndexEntry
	}

	var readyList []rawReady
	var writes []pendingWrite

	err := q.eng.ForEach(func(msgID string, entry storage.IndexEntry) error {
		switch entry.Status {
		case StatusReady:
			readyList = append(readyList, rawReady{msgID, entry.Offset})
			q.msgCount++

		case StatusInFlight:
			if entry.VisibilityDeadlineMs <= now {
				// Expired in-flight — will be re-queued; write deferred.
				writes = append(writes, pendingWrite{
					msgID: msgID,
					entry: storage.IndexEntry{
						Offset:  entry.Offset,
						Status:  StatusReady,
						Attempt: entry.Attempt,
					},
				})
				readyList = append(readyList, rawReady{msgID, entry.Offset})
				q.msgCount++
			} else {
				// Still valid in-flight — restore tracking.
				// MaxRetries is not stored in the index; read the message to restore it.
				var maxRetries int
				if msg, err := q.eng.ReadAt(entry.Offset); err == nil {
					maxRetries = msg.MaxRetries
				}
				q.inFlight[entry.ReceiptHandle] = &InFlightEntry{
					MsgID:         msgID,
					Offset:        entry.Offset,
					Attempt:       entry.Attempt,
					MaxRetries:    maxRetries,
					ReceiptHandle: entry.ReceiptHandle,
					DeadlineMs:    entry.VisibilityDeadlineMs,
				}
				q.msgCount++
			}

		case StatusScheduled:
			// Re-register scheduled messages with the Scheduler.
			if q.onSchedule != nil {
				msg, err := q.eng.ReadAt(entry.Offset)
				if err == nil {
					if msg.DeliverAt > now {
						q.onSchedule(msgID, q.Key, msg.DeliverAt)
					} else {
						// Missed delivery window (server was down) — deliver now.
						writes = append(writes, pendingWrite{
							msgID: msgID,
							entry: storage.IndexEntry{
								Offset:  entry.Offset,
								Status:  StatusReady,
								Attempt: entry.Attempt,
							},
						})
						readyList = append(readyList, rawReady{msgID, entry.Offset})
						q.msgCount++
					}
				}
			}

			// StatusDeleted, StatusDeadLetter → nothing to restore.
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Apply deferred index writes now that the ForEach read transaction is closed.
	for _, pw := range writes {
		if err := q.eng.WriteIndex(pw.msgID, pw.entry); err != nil {
			return fmt.Errorf("load: write index %s: %w", pw.msgID, err)
		}
	}

	// Sort READY entries by msgID (ULID = time-sortable) for FIFO ordering.
	sort.Slice(readyList, func(i, j int) bool {
		return readyList[i].msgID < readyList[j].msgID
	})
	for _, r := range readyList {
		q.ready.PushBack(&ReadyEntry{MsgID: r.msgID, Offset: r.offset})
	}
	return nil
}

// ─── Visibility timeout reaper ────────────────────────────────────────────────

func (q *Queue) startReaper() {
	q.reaperWG.Add(1)
	go q.reaperLoop()
}

func (q *Queue) reaperLoop() {
	defer q.reaperWG.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-q.reaperDone:
			return
		case <-ticker.C:
			q.reapExpiredInFlight()
		}
	}
}

// reapExpiredInFlight collects expired in-flight entries and requeues them.
// Runs on the reaper goroutine every 500 ms.
func (q *Queue) reapExpiredInFlight() {
	now := time.Now().UnixMilli()

	q.mu.Lock()
	var expired []*InFlightEntry
	for rh, e := range q.inFlight {
		if e.DeadlineMs <= now {
			expired = append(expired, e)
			delete(q.inFlight, rh)
		}
	}
	q.mu.Unlock()

	for _, e := range expired {
		_ = q.requeue(e)
	}
}
