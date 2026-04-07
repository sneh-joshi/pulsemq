package local

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

const (
	logFileName   = "log.dat"
	indexFileName = "index.db"
	walFileName   = "wal.dat"
)

// ─── Local Storage Config ────────────────────────────────────────────────────

// FsyncPolicy controls when writes are flushed to physical disk.
// Values mirror the top-level Config.Storage.Fsync policy names so the queue
// server can pass them straight through without translation.
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"   // fsync after every write (safest, slowest)
	FsyncInterval FsyncPolicy = "interval" // fsync every FsyncIntervalMs milliseconds
	FsyncBatch    FsyncPolicy = "batch"    // fsync after every FsyncBatchSize writes
	FsyncNever    FsyncPolicy = "never"    // never fsync (fastest, risks data loss)
)

// Config holds options that tune local.Storage behaviour.
// All zero-values are safe: DefaultConfig() fills in sensible defaults.
type Config struct {
	Fsync              FsyncPolicy
	FsyncIntervalMs    int           // used when Fsync == FsyncInterval
	FsyncBatchSize     int           // used when Fsync == FsyncBatch
	CompactionInterval time.Duration // how often the background compactor runs
}

// DefaultConfig returns a Config with production-safe defaults.
func DefaultConfig() Config {
	return Config{
		Fsync:              FsyncInterval,
		FsyncIntervalMs:    200, // 200 ms — good balance of safety vs throughput
		FsyncBatchSize:     64,
		CompactionInterval: time.Hour,
	}
}

// ─── Storage ─────────────────────────────────────────────────────────────────

// Storage is the local, single-node implementation of storage.StorageEngine.
// It combines:
//   - An append-only Log (message bodies in log.dat)
//   - A bbolt Index (message ID → offset + status in index.db)
//   - A WAL (write-ahead log for crash safety, wal.dat)
//
// All methods are safe for concurrent use.
//
// Phase 2 note: ReplicatedStorage will wrap this type and add Raft replication
// on top without changing the StorageEngine interface or any caller code.
type Storage struct {
	log   *Log
	index *Index
	wal   *WAL
	dir   string
	cfg   Config

	// pendingWAL maps msgID → WAL sequence number for messages that have been
	// written to the WAL and log but whose index entry has not yet been committed.
	// Type: map[string]uint64 stored in sync.Map for lock-free concurrent access.
	pendingWAL sync.Map

	// writeCount is incremented on every Append; used by FsyncBatch policy.
	writeCount atomic.Int64

	// compactMu serialises compaction against regular reads/writes.
	// RLock is taken by Append/WriteIndex; WLock is taken by Compactor.RunOnce.
	compactMu sync.RWMutex

	// compactor runs the background compaction loop.
	compactor *Compactor

	// fsync background goroutine lifecycle.
	fsyncTicker *time.Ticker
	fsyncDone   chan struct{}
	fsyncWG     sync.WaitGroup
	fsyncOnce   sync.Once // guards stopFsync so it is safe to call multiple times

	closeOnce sync.Once // guards Close so it is safe to call multiple times
}

// Ensure Storage satisfies the interface at compile time.
var _ storage.StorageEngine = (*Storage)(nil)

// ─── Open ─────────────────────────────────────────────────────────────────────

// Open creates (or reopens) a local Storage backed by files in dir.
// An optional Config can be supplied; defaults are used for any zero-field.
//
// dir is expected to be the per-queue data directory, e.g.:
//
//	/data/namespaces/payments/orders/
//
// The variadic signature keeps existing call sites (Open(dir)) unchanged.
func Open(dir string, cfgs ...Config) (*Storage, error) {
	cfg := DefaultConfig()
	if len(cfgs) > 0 {
		// Merge: only override fields that were explicitly set.
		c := cfgs[0]
		if c.Fsync != "" {
			cfg.Fsync = c.Fsync
		}
		if c.FsyncIntervalMs > 0 {
			cfg.FsyncIntervalMs = c.FsyncIntervalMs
		}
		if c.FsyncBatchSize > 0 {
			cfg.FsyncBatchSize = c.FsyncBatchSize
		}
		if c.CompactionInterval > 0 {
			cfg.CompactionInterval = c.CompactionInterval
		}
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("local storage: create dir %s: %w", dir, err)
	}

	lg, err := OpenLog(filepath.Join(dir, logFileName))
	if err != nil {
		return nil, fmt.Errorf("local storage: open log: %w", err)
	}

	idx, err := OpenIndex(filepath.Join(dir, indexFileName))
	if err != nil {
		_ = lg.Close()
		return nil, fmt.Errorf("local storage: open index: %w", err)
	}

	wal, err := OpenWAL(filepath.Join(dir, walFileName))
	if err != nil {
		_ = lg.Close()
		_ = idx.Close()
		return nil, fmt.Errorf("local storage: open wal: %w", err)
	}

	s := &Storage{
		log:   lg,
		index: idx,
		wal:   wal,
		dir:   dir,
		cfg:   cfg,
	}

	// Crash recovery: replay any uncommitted WAL entries.
	if err := s.recover(); err != nil {
		_ = s.closeAll()
		return nil, fmt.Errorf("local storage: crash recovery: %w", err)
	}

	// Start background fsync goroutine (if needed).
	s.startFsync()

	// Start background compactor.
	s.compactor = NewCompactor(s, cfg.CompactionInterval)
	s.compactor.Start()

	return s, nil
}

// ─── Crash recovery ───────────────────────────────────────────────────────────

// recover replays uncommitted WAL entries and re-applies them to the log and
// index. After recovery the WAL is truncated so it does not grow across restarts.
func (s *Storage) recover() error {
	uncommitted, err := s.wal.Replay()
	if err != nil {
		return fmt.Errorf("wal replay: %w", err)
	}

	for _, entry := range uncommitted {
		// If the message is already in the index, the write was committed;
		// the COMMIT record was just lost (late crash). Nothing to redo.
		if _, err := s.index.Read(entry.MsgID); err == nil {
			continue
		}

		// Re-append to the log (the old partial write may already be there but
		// the checksum would be invalid; appending again is safe because the
		// index is the authoritative source of the live offset).
		offset, err := s.log.Append(entry.Msg)
		if err != nil {
			return fmt.Errorf("re-append msg %s: %w", entry.MsgID, err)
		}

		// Rebuild the index entry with READY status.
		ie := storage.IndexEntry{
			Offset: offset,
			Status: types.StatusReady,
		}
		if err := s.index.Write(entry.MsgID, ie); err != nil {
			return fmt.Errorf("re-index msg %s: %w", entry.MsgID, err)
		}
	}

	// Truncate WAL — recovery is complete, clean slate.
	return s.wal.Truncate()
}

// ─── Background fsync ─────────────────────────────────────────────────────────

// startFsync launches the periodic fsync goroutine when the policy requires it.
func (s *Storage) startFsync() {
	if s.cfg.Fsync != FsyncInterval {
		return
	}
	interval := time.Duration(s.cfg.FsyncIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	s.fsyncTicker = time.NewTicker(interval)
	s.fsyncDone = make(chan struct{})
	s.fsyncWG.Add(1)
	go func() {
		defer s.fsyncWG.Done()
		for {
			select {
			case <-s.fsyncDone:
				return
			case <-s.fsyncTicker.C:
				_ = s.log.Sync()
				_ = s.wal.Sync()
			}
		}
	}()
}

// stopFsync shuts down the periodic fsync goroutine.
// Safe to call multiple times.
func (s *Storage) stopFsync() {
	if s.fsyncTicker == nil {
		return
	}
	s.fsyncOnce.Do(func() {
		s.fsyncTicker.Stop()
		close(s.fsyncDone)
	})
	s.fsyncWG.Wait()
}

// maybeSyncAfterWrite performs an fsync according to the configured policy.
// It is called after every Append (log + wal write).
func (s *Storage) maybeSyncAfterWrite() {
	switch s.cfg.Fsync {
	case FsyncAlways:
		_ = s.log.Sync()
		_ = s.wal.Sync()
	case FsyncBatch:
		n := s.writeCount.Add(1)
		if n%int64(s.cfg.FsyncBatchSize) == 0 {
			_ = s.log.Sync()
			_ = s.wal.Sync()
		}
	}
	// FsyncInterval is handled by the background goroutine.
	// FsyncNever does nothing.
}

// ─── StorageEngine implementation ─────────────────────────────────────────────

// Append writes msg to the WAL and then to the log, returning its byte offset.
// The caller must separately call WriteIndex to complete the durable write and
// commit the WAL entry.
//
// Write sequence:
//  1. WAL.Write  → records intent (crash-safe)
//  2. Log.Append → appends message body
//  3. (caller calls WriteIndex → bbolt commit)
//  4. WAL.Commit → marks intent as fulfilled
func (s *Storage) Append(msg *types.Message) (int64, error) {
	s.compactMu.RLock()
	defer s.compactMu.RUnlock()

	// Step 1: Write intent to WAL.
	seq, err := s.wal.Write(msg)
	if err != nil {
		return 0, fmt.Errorf("local storage: wal write: %w", err)
	}

	// Track the pending WAL sequence so WriteIndex can commit it.
	s.pendingWAL.Store(msg.ID, seq)

	// Step 2: Append message body to log.
	offset, err := s.log.Append(msg)
	if err != nil {
		// Remove pending tracking; the next Open() will recover from WAL.
		s.pendingWAL.Delete(msg.ID)
		return 0, fmt.Errorf("local storage: append: %w", err)
	}

	s.maybeSyncAfterWrite()
	return offset, nil
}

// ReadAt reads the message stored at the given byte offset in the log.
func (s *Storage) ReadAt(offset int64) (*types.Message, error) {
	s.compactMu.RLock()
	defer s.compactMu.RUnlock()

	msg, err := s.log.ReadAt(offset)
	if err != nil {
		return nil, fmt.Errorf("local storage: read at %d: %w", offset, err)
	}
	return msg, nil
}

// WriteIndex persists (or updates) the index entry for msgID.
// If msgID has a pending WAL entry (set by Append), its WAL record is committed
// after the bbolt write succeeds — completing the durable write protocol.
func (s *Storage) WriteIndex(msgID string, entry storage.IndexEntry) error {
	s.compactMu.RLock()
	defer s.compactMu.RUnlock()

	if err := s.index.Write(msgID, entry); err != nil {
		return fmt.Errorf("local storage: write index %s: %w", msgID, err)
	}

	// Commit WAL if this is the initial write for a new message.
	if seqVal, ok := s.pendingWAL.LoadAndDelete(msgID); ok {
		seq := seqVal.(uint64)
		if err := s.wal.Commit(seq); err != nil {
			// Non-fatal: the message is safely in log+index (bbolt ACID).
			// On next startup the WAL entry will be replayed but we'll find
			// it's already in the index and skip it. Safe to ignore.
			_ = err
		}
	}

	return nil
}

// ReadIndex retrieves the index entry for msgID.
// Returns storage.ErrNotFound if the message has not been indexed.
func (s *Storage) ReadIndex(msgID string) (storage.IndexEntry, error) {
	entry, err := s.index.Read(msgID)
	if err != nil {
		return storage.IndexEntry{}, fmt.Errorf("local storage: read index %s: %w", msgID, err)
	}
	return entry, nil
}

// DeleteIndex removes the index entry for msgID.
func (s *Storage) DeleteIndex(msgID string) error {
	if err := s.index.Delete(msgID); err != nil {
		return fmt.Errorf("local storage: delete index %s: %w", msgID, err)
	}
	return nil
}

// ForEach iterates over every index entry, calling fn for each one.
// Iteration stops if fn returns a non-nil error.
func (s *Storage) ForEach(fn func(msgID string, entry storage.IndexEntry) error) error {
	if err := s.index.ForEach(fn); err != nil {
		return fmt.Errorf("local storage: foreach index: %w", err)
	}
	return nil
}

// Compactor returns the background Compactor so callers can invoke RunOnce
// directly in tests or trigger on-demand compaction.
func (s *Storage) Compactor() *Compactor {
	return s.compactor
}

// Close flushes and closes the log, index, and WAL.
// Background goroutines (fsync ticker, compactor) are stopped first.
// Safe to call multiple times — only the first call performs the actual close.
func (s *Storage) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		if s.compactor != nil {
			s.compactor.Stop()
		}
		s.stopFsync()
		closeErr = s.closeAll()
	})
	return closeErr
}

// closeAll closes log, index, and wal. Used internally by Open on error paths.
func (s *Storage) closeAll() error {
	logErr := s.log.Close()
	idxErr := s.index.Close()
	walErr := s.wal.Close()
	if logErr != nil {
		return fmt.Errorf("local storage: close log: %w", logErr)
	}
	if idxErr != nil {
		return fmt.Errorf("local storage: close index: %w", idxErr)
	}
	if walErr != nil {
		return fmt.Errorf("local storage: close wal: %w", walErr)
	}
	return nil
}
