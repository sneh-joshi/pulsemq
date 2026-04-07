package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/scheduler"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// collected gathers deliveries from the readyFn in a concurrency-safe way.
type collected struct {
	mu      sync.Mutex
	entries []string // "msgID:queueKey"
}

func (c *collected) fn(msgID, queueKey string) {
	c.mu.Lock()
	c.entries = append(c.entries, msgID+":"+queueKey)
	c.mu.Unlock()
}

func (c *collected) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *collected) ids() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.entries))
	copy(out, c.entries)
	return out
}

// waitForCount polls until n deliveries have been collected or timeout elapses.
func waitForCount(t *testing.T, c *collected, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.len() >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ─── Tests ───────────────────────────────────────────────────────────────────

// TestScheduler_ImmediateDelivery verifies that a message with deliverAt in the
// past (or zero-equivalent past) is delivered promptly.
func TestScheduler_ImmediateDelivery(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	pastTime := time.Now().Add(-1 * time.Second).UnixMilli()
	s.Schedule("msg1", "ns/q", pastTime)

	if !waitForCount(t, c, 1, 2*time.Second) {
		t.Fatalf("expected 1 delivery within 2s, got %d", c.len())
	}
	ids := c.ids()
	if ids[0] != "msg1:ns/q" {
		t.Errorf("expected msg1:ns/q, got %s", ids[0])
	}
}

// TestScheduler_FutureDelivery verifies that a message is NOT delivered before
// its deliverAt, and IS delivered after.
func TestScheduler_FutureDelivery(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	deliverAt := time.Now().Add(150 * time.Millisecond).UnixMilli()
	s.Schedule("msg2", "payments/orders", deliverAt)

	// Must NOT be delivered before deliverAt.
	time.Sleep(80 * time.Millisecond)
	if c.len() != 0 {
		t.Fatalf("message delivered too early: expected 0 deliveries before timeout")
	}

	// Must be delivered after.
	if !waitForCount(t, c, 1, 500*time.Millisecond) {
		t.Fatalf("expected delivery within 500ms of schedule, got 0")
	}
}

// TestScheduler_CancelPreventsDelivery verifies that cancelling a scheduled
// message prevents the readyFn from being called.
func TestScheduler_CancelPreventsDelivery(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	deliverAt := time.Now().Add(300 * time.Millisecond).UnixMilli()
	s.Schedule("msg3", "ns/q", deliverAt)
	s.Cancel("msg3")

	// Wait longer than deliverAt — readyFn should NOT fire.
	time.Sleep(500 * time.Millisecond)
	if c.len() != 0 {
		t.Fatalf("expected 0 deliveries after cancel, got %d", c.len())
	}
}

// TestScheduler_OrderedDelivery verifies that multiple messages are delivered
// in deliverAt order (earliest first), regardless of insertion order.
func TestScheduler_OrderedDelivery(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	now := time.Now()
	// Insert in reverse order: msg_c (earliest) added last.
	s.Schedule("msg_b", "ns/q", now.Add(60*time.Millisecond).UnixMilli())
	s.Schedule("msg_a", "ns/q", now.Add(30*time.Millisecond).UnixMilli())
	s.Schedule("msg_c", "ns/q", now.Add(90*time.Millisecond).UnixMilli())

	if !waitForCount(t, c, 3, 2*time.Second) {
		t.Fatalf("expected 3 deliveries, got %d", c.len())
	}

	ids := c.ids()
	expected := []string{"msg_a:ns/q", "msg_b:ns/q", "msg_c:ns/q"}
	for i, want := range expected {
		if ids[i] != want {
			t.Errorf("delivery[%d]: want %s, got %s", i, want, ids[i])
		}
	}
}

// TestScheduler_NewEarlierMessageInterruptsSleep verifies that scheduling a
// message with a sooner deliverAt than the current head interrupts the timer
// and delivers it first.
func TestScheduler_NewEarlierMessageInterruptsSleep(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	now := time.Now()
	// Schedule a far-future message first, then immediately add an earlier one.
	s.Schedule("late", "ns/q", now.Add(10*time.Second).UnixMilli())
	time.Sleep(20 * time.Millisecond) // let the goroutine sleep on "late"
	s.Schedule("early", "ns/q", now.Add(80*time.Millisecond).UnixMilli())

	// "early" must be delivered well before "late"'s original deadline.
	if !waitForCount(t, c, 1, 500*time.Millisecond) {
		t.Fatal("expected early message delivered within 500ms")
	}
	if c.ids()[0] != "early:ns/q" {
		t.Errorf("expected 'early' to be delivered first, got %s", c.ids()[0])
	}
}

// TestScheduler_LenTracksActiveSchedules verifies that Len reflects the number
// of non-cancelled pending messages.
func TestScheduler_LenTracksActiveSchedules(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	future := time.Now().Add(10 * time.Second).UnixMilli()
	s.Schedule("a", "ns/q", future)
	s.Schedule("b", "ns/q", future)
	s.Schedule("c", "ns/q", future)

	if s.Len() != 3 {
		t.Errorf("Len: want 3, got %d", s.Len())
	}

	s.Cancel("b")
	if s.Len() != 2 {
		t.Errorf("Len after cancel: want 2, got %d", s.Len())
	}
}

// TestScheduler_StopNoDeliveries verifies that Stop() prevents future deliveries.
func TestScheduler_StopNoDeliveries(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)

	future := time.Now().Add(500 * time.Millisecond).UnixMilli()
	s.Schedule("msg", "ns/q", future)
	s.Stop() // stop before deliverAt

	time.Sleep(700 * time.Millisecond)
	if c.len() != 0 {
		t.Fatalf("expected 0 deliveries after Stop, got %d", c.len())
	}
}

// TestScheduler_CountByQueue verifies that CountByQueue returns the number of
// pending messages belonging to a specific queue key.
func TestScheduler_CountByQueue(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	future := time.Now().Add(10 * time.Second).UnixMilli()
	s.Schedule("a", "ns/q1", future)
	s.Schedule("b", "ns/q1", future)
	s.Schedule("c", "ns/q2", future)

	if got := s.CountByQueue("ns/q1"); got != 2 {
		t.Errorf("CountByQueue(ns/q1): want 2, got %d", got)
	}
	if got := s.CountByQueue("ns/q2"); got != 1 {
		t.Errorf("CountByQueue(ns/q2): want 1, got %d", got)
	}
	if got := s.CountByQueue("ns/other"); got != 0 {
		t.Errorf("CountByQueue(ns/other): want 0, got %d", got)
	}

	s.Cancel("a")
	if got := s.CountByQueue("ns/q1"); got != 1 {
		t.Errorf("CountByQueue(ns/q1) after cancel: want 1, got %d", got)
	}
}

// TestScheduler_RescheduleReplacesExisting verifies that calling Schedule again
// with the same msgID replaces the previous entry.
func TestScheduler_RescheduleReplacesExisting(t *testing.T) {
	s := scheduler.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &collected{}
	s.Start(ctx, c.fn)
	defer s.Stop()

	// Schedule for 10s out, then immediately re-schedule for 100ms.
	future := time.Now().Add(10 * time.Second).UnixMilli()
	near := time.Now().Add(100 * time.Millisecond).UnixMilli()

	s.Schedule("msg", "ns/q", future)
	s.Schedule("msg", "ns/q", near) // replaces

	if !waitForCount(t, c, 1, time.Second) {
		t.Fatal("re-scheduled message not delivered within 1s")
	}
	if s.Len() != 0 {
		t.Errorf("Len after delivery: want 0, got %d", s.Len())
	}
}
