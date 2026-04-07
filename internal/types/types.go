// Package types contains the core domain types shared across all PulseMQ
// internal packages. It deliberately has zero imports of other PulseMQ packages
// so that both the storage layer and the queue layer can import from it without
// creating import cycles.
package types

// Status is the lifecycle state of a message inside a queue.
type Status uint8

const (
	// StatusReady means the message is available for consumption.
	StatusReady Status = iota
	// StatusInFlight means the message has been delivered to a consumer and is
	// awaiting an ACK within the visibility timeout window.
	StatusInFlight
	// StatusDeleted means the consumer has ACKed the message. It is logically
	// gone but may still exist in the log file until compaction runs.
	StatusDeleted
	// StatusDeadLetter means the message exceeded max retries and has been
	// moved to the dead-letter queue.
	StatusDeadLetter
	// StatusScheduled means the message has a future deliverAt and is sitting
	// in the scheduler's Min-Heap, not yet visible to consumers.
	StatusScheduled
)

// String returns a human-readable representation of the status.
func (s Status) String() string {
	switch s {
	case StatusReady:
		return "ready"
	case StatusInFlight:
		return "in_flight"
	case StatusDeleted:
		return "deleted"
	case StatusDeadLetter:
		return "dead_letter"
	case StatusScheduled:
		return "scheduled"
	default:
		return "unknown"
	}
}

// Message is the canonical, immutable unit of data in PulseMQ.
//
// Design rules:
//   - Message format is final. Only optional fields may be added. Never rename
//     or remove a field — existing persisted messages must always be readable.
//   - All timestamps are UTC milliseconds since Unix epoch.
//   - IDs are ULID strings: time-sortable, globally unique, cluster-safe.
//   - Term and LogIndex are populated by the WAL / Raft layer. In Phase 1
//     Term is always 0 and LogIndex is a monotone counter per queue.
type Message struct {
	// ID is a ULID uniquely identifying this message across all nodes.
	ID string `json:"id"`

	// Namespace and Queue identify which queue the message belongs to.
	Namespace string `json:"namespace"`
	Queue     string `json:"queue"`

	// Body is the raw message payload. Producers own the encoding (JSON, proto, …).
	// Max size is enforced by QueueConfig.MaxMessageSizeKB.
	Body []byte `json:"body"`

	// DeliverAt is the earliest UTC millisecond at which this message may be
	// delivered to consumers. Zero means deliver immediately.
	DeliverAt int64 `json:"deliver_at"`

	// PublishedAt is the UTC millisecond when the producer called Publish.
	PublishedAt int64 `json:"published_at"`

	// Attempt is the 1-based delivery attempt number. Starts at 1 on first
	// delivery; incremented each time the visibility timeout expires and the
	// message is re-queued.
	Attempt int `json:"attempt"`

	// MaxRetries is the maximum number of delivery attempts before the message
	// is moved to the dead-letter queue. Copied from QueueConfig at publish time.
	MaxRetries int `json:"max_retries"`

	// Metadata holds arbitrary key-value pairs set by the producer.
	Metadata map[string]string `json:"metadata,omitempty"`

	// --- WAL / clustering fields ---
	// Term is the Raft term in which this entry was written. Phase 1: always 0.
	Term uint64 `json:"term"`

	// LogIndex is the monotonically increasing index of this entry in the
	// queue's write-ahead log. Used for crash recovery and Raft replication.
	LogIndex uint64 `json:"log_index"`

	// NodeID is the ULID of the node that first wrote this message.
	NodeID string `json:"node_id"`
}

// IsScheduled reports whether the message should be held in the scheduler
// rather than placed immediately into the ready queue.
func (m *Message) IsScheduled(nowMs int64) bool {
	return m.DeliverAt > 0 && m.DeliverAt > nowMs
}

// Clone returns a shallow copy of the message.
func (m *Message) Clone() *Message {
	c := *m
	return &c
}
