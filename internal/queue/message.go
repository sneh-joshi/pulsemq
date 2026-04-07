// Package queue defines the core queue logic for PulseMQ.
//
// Domain types (Message, Status) live in internal/types to break the import
// cycle between the storage and queue packages. This file re-exports them as
// aliases so callers can continue to use queue.Message / queue.Status without
// any source changes.
package queue

import "github.com/sneh-joshi/pulsemq/internal/types"

// Re-export core domain types from the types package.
// Using Go type aliases (=) so queue.Message IS types.Message — no conversion needed.
type Message = types.Message
type Status = types.Status

// Re-export status constants.
const (
	StatusReady      = types.StatusReady
	StatusInFlight   = types.StatusInFlight
	StatusDeleted    = types.StatusDeleted
	StatusDeadLetter = types.StatusDeadLetter
	StatusScheduled  = types.StatusScheduled
)
