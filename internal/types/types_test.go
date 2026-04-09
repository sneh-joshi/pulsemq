package types_test

import (
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/types"
)

func TestStatus_String(t *testing.T) {
	cases := []struct {
		s    types.Status
		want string
	}{
		{types.StatusReady, "ready"},
		{types.StatusInFlight, "in_flight"},
		{types.StatusDeleted, "deleted"},
		{types.StatusDeadLetter, "dead_letter"},
		{types.StatusScheduled, "scheduled"},
		{types.Status(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestMessage_IsScheduled(t *testing.T) {
	m := &types.Message{DeliverAt: 1000}
	if !m.IsScheduled(500) {
		t.Error("IsScheduled(500): want true (DeliverAt=1000 > now=500)")
	}
	if m.IsScheduled(1000) {
		t.Error("IsScheduled(1000): want false (DeliverAt equals now)")
	}
	if m.IsScheduled(1001) {
		t.Error("IsScheduled(1001): want false (now past DeliverAt)")
	}
	// Zero DeliverAt means immediate — never scheduled.
	m2 := &types.Message{DeliverAt: 0}
	if m2.IsScheduled(0) {
		t.Error("IsScheduled with DeliverAt=0 should be false")
	}
}

func TestMessage_Clone(t *testing.T) {
	orig := &types.Message{
		ID:        "abc",
		Namespace: "ns",
		Queue:     "q",
		Body:      []byte("payload"),
		Attempt:   2,
	}
	clone := orig.Clone()
	if clone.ID != orig.ID || clone.Namespace != orig.Namespace || clone.Attempt != orig.Attempt {
		t.Error("Clone: fields differ from original")
	}
	// Modifying the clone must not affect the original.
	clone.ID = "xyz"
	if orig.ID != "abc" {
		t.Error("Clone: modifying clone changed original ID")
	}
}
