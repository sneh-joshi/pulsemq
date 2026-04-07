// Package storage defines the StorageEngine abstraction used by every queue.
//
// Design principle: the queue engine (and every layer above it) must ONLY
// interact with storage through this interface. Never call file I/O directly.
// This makes it trivial to swap LocalStorage for ReplicatedStorage in Phase 2
// without touching any queue logic.
package storage

import (
	"errors"

	"github.com/sneh-joshi/pulsemq/internal/types"
)

// ErrNotFound is returned when a message or index entry does not exist.
var ErrNotFound = errors.New("storage: not found")

// ErrCorrupted is returned when a stored entry fails its checksum.
var ErrCorrupted = errors.New("storage: entry corrupted")

// IndexEntry holds the index record for a single message.
// It maps a message ID to its physical location in the log file and tracks
// its current lifecycle state.
type IndexEntry struct {
	// Offset is the byte offset of this message's entry in log.dat.
	Offset int64

	// Status is the current lifecycle state of the message.
	Status types.Status

	// ReceiptHandle is set when status == StatusInFlight.
	// It is the opaque handle the consumer must present to ACK/NACK.
	ReceiptHandle string

	// VisibilityDeadlineMs is the UTC millisecond after which an in-flight
	// message becomes visible again. Zero when not in-flight.
	VisibilityDeadlineMs int64

	// Attempt is the current 1-based delivery attempt count. Incremented each
	// time the message is re-queued due to a NACK or visibility timeout expiry.
	// Persisted alongside the in-flight state so crash recovery can pick up
	// where it left off without resetting the retry counter.
	Attempt int
}

// StorageEngine is the single abstraction through which all messages are
// persisted and retrieved.
//
// Implementations:
//   - local.Storage  — single-node, disk-backed (Phase 1)
//   - replicated.Storage — Raft-backed cluster (Phase 2, not yet implemented)
//
// All methods must be safe for concurrent use.
type StorageEngine interface {
	// Append writes a message to the append-only log and returns the byte
	// offset at which the entry was written. The caller is responsible for
	// also calling WriteIndex so the entry is findable.
	Append(msg *types.Message) (offset int64, err error)

	// ReadAt reads the message stored at the given log offset.
	// Returns ErrNotFound if the offset is out of range.
	// Returns ErrCorrupted if the stored checksum does not match.
	ReadAt(offset int64) (*types.Message, error)

	// WriteIndex persists (or updates) the index entry for msgID.
	WriteIndex(msgID string, entry IndexEntry) error

	// ReadIndex retrieves the index entry for msgID.
	// Returns ErrNotFound if the message has never been indexed.
	ReadIndex(msgID string) (IndexEntry, error)

	// DeleteIndex removes the index entry for msgID.
	// Called after compaction confirms the log entry has been dropped.
	DeleteIndex(msgID string) error

	// ForEach iterates over every index entry in an unspecified order, calling
	// fn for each one. Iteration stops if fn returns a non-nil error.
	// Used by Queue.loadFromStorage to rebuild in-memory state on startup.
	ForEach(fn func(msgID string, entry IndexEntry) error) error

	// Close flushes all pending writes and releases file handles.
	Close() error
}
