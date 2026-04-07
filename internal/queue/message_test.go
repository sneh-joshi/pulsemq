package queue_test

import (
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/queue"
)

func TestStatus_String(t *testing.T) {
	tests := []struct {
		status queue.Status
		want   string
	}{
		{queue.StatusReady, "ready"},
		{queue.StatusInFlight, "in_flight"},
		{queue.StatusDeleted, "deleted"},
		{queue.StatusDeadLetter, "dead_letter"},
		{queue.StatusScheduled, "scheduled"},
		{queue.Status(99), "unknown"},
	}

	for _, tc := range tests {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q, want %q", tc.status, got, tc.want)
		}
	}
}

func TestMessage_IsScheduled(t *testing.T) {
	now := time.Now().UnixMilli()

	tests := []struct {
		name      string
		deliverAt int64
		nowMs     int64
		want      bool
	}{
		{
			name:      "zero deliverAt is immediate",
			deliverAt: 0,
			nowMs:     now,
			want:      false,
		},
		{
			name:      "past deliverAt is immediate",
			deliverAt: now - 60_000, // 1 minute ago
			nowMs:     now,
			want:      false,
		},
		{
			name:      "future deliverAt is scheduled",
			deliverAt: now + 60_000, // 1 minute from now
			nowMs:     now,
			want:      true,
		},
		{
			name:      "deliverAt == now is immediate",
			deliverAt: now,
			nowMs:     now,
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := &queue.Message{DeliverAt: tc.deliverAt}
			if got := msg.IsScheduled(tc.nowMs); got != tc.want {
				t.Errorf("IsScheduled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMessage_Clone_IsShallowCopy(t *testing.T) {
	original := &queue.Message{
		ID:        "01ABC",
		Namespace: "payments",
		Queue:     "orders",
		Body:      []byte("hello"),
		Attempt:   1,
	}

	clone := original.Clone()

	if clone == original {
		t.Fatal("Clone() returned same pointer, expected new struct")
	}
	if clone.ID != original.ID {
		t.Errorf("Clone().ID = %q, want %q", clone.ID, original.ID)
	}
	if clone.Namespace != original.Namespace {
		t.Errorf("Clone().Namespace = %q, want %q", clone.Namespace, original.Namespace)
	}

	// Mutating the clone must not affect the original struct value.
	clone.Attempt = 99
	if original.Attempt == 99 {
		t.Error("mutating clone.Attempt affected original — Clone is not a copy")
	}
}

func TestMessage_DeliverAtZero_IsAlwaysImmediate(t *testing.T) {
	msg := &queue.Message{DeliverAt: 0}
	// Regardless of what "now" is, a zero DeliverAt is never scheduled.
	for _, nowMs := range []int64{0, 1, time.Now().UnixMilli(), 9999999999999} {
		if msg.IsScheduled(nowMs) {
			t.Errorf("DeliverAt=0 should not be scheduled for nowMs=%d", nowMs)
		}
	}
}
