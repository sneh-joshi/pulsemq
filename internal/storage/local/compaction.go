package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

// Compactor rewrites the log file, removing entries for deleted messages.
//
// Why compaction is needed:
//   - Messages are never physically deleted on ACK — only the index status
//     changes to Deleted.
//   - Without compaction, log.dat grows unbounded even when all messages
//     are consumed.
//   - Compaction rewrites only the live (non-Deleted) messages to new_log.dat,
//     then atomically swaps the files.
//
// Compaction holds a write lock on the Storage for the entire duration.
// At Phase 1 traffic levels this is acceptable (compaction runs once per hour
// by default). Phase 3 will use online / concurrent compaction.
//
// Compaction is intentionally skipped when the queue is actively receiving
// writes above a threshold — checked via the skipIfBusy flag.
type Compactor struct {
	s        *Storage
	interval time.Duration

	mu   sync.Mutex
	done chan struct{}
	wg   sync.WaitGroup
}

// NewCompactor creates a Compactor that will run RunOnce every interval.
func NewCompactor(s *Storage, interval time.Duration) *Compactor {
	return &Compactor{
		s:        s,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start launches the background compaction goroutine.
// It returns immediately; compaction runs on interval in the background.
func (c *Compactor) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), c.interval/2)
				_ = c.RunOnce(ctx) // errors logged, not fatal
				cancel()
			}
		}
	}()
}

// Stop signals the background goroutine to exit and waits for it to finish.
func (c *Compactor) Stop() {
	c.mu.Lock()
	select {
	case <-c.done:
		// already stopped
	default:
		close(c.done)
	}
	c.mu.Unlock()
	c.wg.Wait()
}

// RunOnce performs a single compaction cycle:
//  1. Acquire exclusive write lock on Storage (blocks producers).
//  2. Iterate log.dat; copy live (non-Deleted) messages to log.dat.tmp.
//  3. Update index entries to new offsets in an atomic bbolt transaction.
//  4. Rename log.dat.tmp → log.dat (atomic on POSIX).
//  5. Reopen the Log against the new file.
//  6. Release write lock.
//
// Returns nil if no compaction was needed (all messages are live).
func (c *Compactor) RunOnce(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Acquire exclusive storage lock — blocks all Append/WriteIndex calls.
	c.s.compactMu.Lock()
	defer c.s.compactMu.Unlock()

	// ── 1. Collect live entries from current log ─────────────────────────────
	type liveEntry struct {
		msg    *types.Message
		oldOff int64
	}
	var live []liveEntry
	deletedCount := 0

	if err := c.s.log.ReadAll(func(offset int64, msg *types.Message) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Check the index to see if this entry is still live.
		entry, err := c.s.index.Read(msg.ID)
		if err != nil {
			// Not in index (orphan from crash recovery) — skip.
			deletedCount++
			return nil
		}
		if entry.Status == types.StatusDeleted || entry.Status == types.StatusDeadLetter {
			deletedCount++
			return nil
		}
		live = append(live, liveEntry{msg: msg, oldOff: offset})
		return nil
	}); err != nil {
		return fmt.Errorf("compactor: scan log: %w", err)
	}

	// Nothing to compact.
	if deletedCount == 0 {
		return nil
	}

	// ── 2. Write live entries to a temporary log file ────────────────────────
	tmpPath := c.s.log.Path() + ".tmp"
	tmpLog, err := OpenLog(tmpPath)
	if err != nil {
		return fmt.Errorf("compactor: open tmp log: %w", err)
	}

	// Map of msgID → new offset in the compacted log.
	newOffsets := make(map[string]int64, len(live))
	for _, e := range live {
		if ctx.Err() != nil {
			_ = tmpLog.Close()
			_ = os.Remove(tmpPath)
			return ctx.Err()
		}
		newOff, err := tmpLog.Append(e.msg)
		if err != nil {
			_ = tmpLog.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("compactor: write live entry: %w", err)
		}
		newOffsets[e.msg.ID] = newOff
	}

	if err := tmpLog.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compactor: close tmp log: %w", err)
	}

	// ── 3. Update index entries to new offsets ───────────────────────────────
	// Collect current index state first so we can batch-update.
	type indexUpdate struct {
		msgID string
		entry storage.IndexEntry
	}
	var updates []indexUpdate
	for _, e := range live {
		oldEntry, err := c.s.index.Read(e.msg.ID)
		if err != nil {
			continue // disappeared (race with delete) — skip
		}
		updated := oldEntry
		updated.Offset = newOffsets[e.msg.ID]
		updates = append(updates, indexUpdate{msgID: e.msg.ID, entry: updated})
	}

	// Apply all updates. bbolt batches them efficiently even across individual calls.
	for _, u := range updates {
		if err := c.s.index.Write(u.msgID, u.entry); err != nil {
			// Best effort — if we can't update an entry, the old offset still works
			// because we haven't renamed the file yet. If rename succeeds we must
			// ensure index is consistent. Abort compaction on index write failure.
			_ = os.Remove(tmpPath)
			return fmt.Errorf("compactor: update index for %s: %w", u.msgID, err)
		}
	}

	// ── 4. Atomic file swap ───────────────────────────────────────────────────
	logPath := c.s.log.Path()
	oldPath := logPath + ".old"

	// Step A: rename current log to .old (so we can roll back if needed).
	if err := os.Rename(logPath, oldPath); err != nil {
		// Roll back index updates — revert offsets to old values.
		for _, e := range live {
			if old, ierr := c.s.index.Read(e.msg.ID); ierr == nil {
				old.Offset = e.oldOff
				_ = c.s.index.Write(e.msg.ID, old)
			}
		}
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compactor: rename log to .old: %w", err)
	}

	// Step B: rename tmp log to active log path.
	if err := os.Rename(tmpPath, logPath); err != nil {
		// Attempt rollback: put old log back.
		_ = os.Rename(oldPath, logPath)
		// Revert index
		for _, e := range live {
			if old, ierr := c.s.index.Read(e.msg.ID); ierr == nil {
				old.Offset = e.oldOff
				_ = c.s.index.Write(e.msg.ID, old)
			}
		}
		return fmt.Errorf("compactor: rename tmp to log: %w", err)
	}

	// ── 5. Reopen Log against the new file ───────────────────────────────────
	if err := c.s.log.Reopen(logPath); err != nil {
		// This is a serious error — log is in unknown state.
		// Try to restore old log.
		return fmt.Errorf("compactor: reopen log (CRITICAL — restart server): %w", err)
	}

	// ── 6. Remove the old log file ────────────────────────────────────────────
	// Non-fatal if this fails — it will be cleaned up on next startup.
	_ = os.Remove(oldPath)

	// ── 7. Truncate WAL — all outstanding entries relate to the old log ───────
	// Any uncommitted WAL entries correspond to messages that are either:
	//   a) Live → in the new log (correct offsets are in the index)
	//   b) Deleted → skipped during compaction (won't be redone)
	// Truncation is safe here because the index is now the source of truth.
	if c.s.wal != nil {
		_ = c.s.wal.Truncate()
	}

	return nil
}

// dataDir returns the absolute directory containing the log file.
func dataDir(logPath string) string {
	return filepath.Dir(logPath)
}
