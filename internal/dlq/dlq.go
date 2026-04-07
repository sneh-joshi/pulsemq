// Package dlq provides utilities for inspecting and replaying messages that
// have been moved to a dead-letter queue.
//
// A dead-letter queue (DLQ) in PulseMQ is just a regular Queue whose name
// follows the convention "__dlq__<primaryQueueName>". The Manager wires each
// primary queue to its DLQ automatically via the onDLQ callback.
//
// This package wraps the queue.Manager to provide DLQ-specific helpers:
//
//   - Peek:   read (but don't consume) the next N dead-lettered messages.
//   - Drain:  destructively consume and return the next N messages.
//   - Replay: move messages back to the primary queue for reprocessing.
package dlq

import (
	"fmt"

	"github.com/sneh-joshi/pulsemq/internal/queue"
)

// Manager provides dead-letter queue operations on top of a queue.Manager.
type Manager struct {
	qm *queue.Manager
}

// NewManager wraps the given queue.Manager.
func NewManager(qm *queue.Manager) *Manager {
	return &Manager{qm: qm}
}

// Drain dequeues up to limit messages from the DLQ for ns/primaryQueueName.
// The caller is responsible for ACKing (or NACKing) the returned results.
// Returns an empty slice if the DLQ is empty.
func (m *Manager) Drain(ns, primaryQueueName string, limit int) ([]*queue.DequeueResult, error) {
	dlqName := queue.DLQName(primaryQueueName)
	dlqQueue, err := m.qm.Get(ns, dlqName)
	if err != nil {
		return nil, fmt.Errorf("dlq.Drain: %w", err)
	}
	return dlqQueue.DequeueN(limit, 0)
}

// Replay moves up to limit messages from the DLQ back to the primary queue.
// Each replayed message is published to the primary queue and then ACKed from
// the DLQ. Returns the number of messages successfully replayed.
func (m *Manager) Replay(ns, primaryQueueName string, limit int) (int, error) {
	// Get both queues upfront.
	primaryQueue, err := m.qm.Get(ns, primaryQueueName)
	if err != nil {
		return 0, fmt.Errorf("dlq.Replay: primary queue: %w", err)
	}
	dlqName := queue.DLQName(primaryQueueName)
	dlqQueue, err := m.qm.Get(ns, dlqName)
	if err != nil {
		return 0, fmt.Errorf("dlq.Replay: dlq: %w", err)
	}

	results, err := dlqQueue.DequeueN(limit, 0)
	if err != nil {
		return 0, fmt.Errorf("dlq.Replay: dequeue from DLQ: %w", err)
	}

	replayed := 0
	for _, r := range results {
		// Build a fresh message for the primary queue, preserving body + metadata.
		fresh := &queue.Message{
			ID:          r.Message.ID, // keep same ID for idempotency
			Namespace:   ns,
			Queue:       primaryQueueName,
			Body:        r.Message.Body,
			PublishedAt: r.Message.PublishedAt,
			MaxRetries:  r.Message.MaxRetries,
			Metadata:    r.Message.Metadata,
			// Attempt and DeliverAt are intentionally reset for a clean retry.
		}

		if pubErr := primaryQueue.Publish(fresh); pubErr != nil {
			// Best-effort: leave this message in-flight on the DLQ so the
			// caller can retry. Skip to the next message.
			continue
		}

		// ACK from DLQ only after successful publish to primary.
		if ackErr := dlqQueue.Ack(r.ReceiptHandle); ackErr == nil {
			replayed++
		}
	}
	return replayed, nil
}

// Len returns the approximate number of messages currently in the DLQ for
// ns/primaryQueueName. Returns 0 if the DLQ has not been created yet.
func (m *Manager) Len(ns, primaryQueueName string) int64 {
	dlqQueue, err := m.qm.Get(ns, queue.DLQName(primaryQueueName))
	if err != nil {
		return 0
	}
	return dlqQueue.TotalCount()
}
