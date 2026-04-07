package local

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sneh-joshi/pulsemq/internal/types"
)

// walMagic is the 4-byte header written at the start of every wal.dat file.
// It identifies the file as a PulseMQ WAL and encodes the format version.
var walMagic = [4]byte{0x4C, 0x51, 0x57, 0x01} // "LQW\x01"

// WAL operation byte values.
const (
	walOpWrite  uint8 = 0x01 // a new message is being written
	walOpCommit uint8 = 0x02 // the message write was fully committed (log+index)
)

// walFixedSize is the fixed part of every WAL entry after the 4-byte totalLen:
//
//	op(1) + seq(8) + msgID(26) + payloadLen(4) + checksum(4) = 43
const walFixedSize = 1 + 8 + 26 + 4 + 4

// WALEntry holds a deserialized WRITE entry recovered from wal.dat.
type WALEntry struct {
	Seq   uint64
	MsgID string
	Msg   *types.Message
}

// WAL is a Write-Ahead Log that provides crash safety for message writes.
//
// Write path:
//  1. WAL.Write(msg) → append WRITE entry to wal.dat, fsync (durable intent)
//  2. Log.Append(msg) → append to log.dat
//  3. Index.Write(...)  → bbolt ACID commit
//  4. WAL.Commit(seq) → append COMMIT entry to wal.dat
//
// On crash between steps 1 and 4, the uncommitted WRITE entries are replayed
// during Storage.Open() to ensure no acknowledged message is lost.
//
// WAL truncation: the WAL is truncated (to just the magic header) whenever
// Storage.Open() completes crash recovery. This keeps wal.dat small.
// During a single process lifetime, committed entries accumulate; they are
// not harmful — they are pruned at the next startup.
//
// All methods are safe for concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	seq  atomic.Uint64 // last-used sequence number
}

// OpenWAL opens (or creates) the WAL at path.
// If the file exists it is opened in append mode; the stored sequences are
// scanned to restore the seq counter so new entries continue the sequence.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	w := &WAL{file: f, path: path}

	// If the file is brand new, write the magic header.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: stat %s: %w", path, err)
	}
	if info.Size() == 0 {
		if _, err := f.Write(walMagic[:]); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: write magic: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: sync magic: %w", err)
		}
	} else {
		// Validate magic header.
		var hdr [4]byte
		if _, err := f.ReadAt(hdr[:], 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: read magic: %w", err)
		}
		if hdr != walMagic {
			_ = f.Close()
			return nil, fmt.Errorf("wal: %s has invalid magic header", path)
		}
		// Restore seq counter from existing entries.
		if err := w.restoreSeq(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: restore seq: %w", err)
		}
	}

	return w, nil
}

// Write appends a WRITE entry for msg and returns the assigned sequence number.
// Call Commit(seq) after the corresponding log.dat + index writes succeed.
func (w *WAL) Write(msg *types.Message) (uint64, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("wal: marshal message %s: %w", msg.ID, err)
	}

	seq := w.seq.Add(1)
	entry := w.encodeEntry(walOpWrite, seq, msg.ID, payload)

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.appendEntry(entry); err != nil {
		return 0, fmt.Errorf("wal: write seq %d: %w", seq, err)
	}
	return seq, nil
}

// Commit appends a COMMIT entry for seq, marking the write as fully applied.
func (w *WAL) Commit(seq uint64) error {
	entry := w.encodeEntry(walOpCommit, seq, "", nil)

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.appendEntry(entry); err != nil {
		return fmt.Errorf("wal: commit seq %d: %w", seq, err)
	}
	return nil
}

// Sync flushes the WAL file to physical disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Replay scans wal.dat and returns all WRITE entries that do not have a
// corresponding COMMIT entry. These represent operations that were started
// but not fully applied before the last crash.
//
// Entries with invalid checksums are silently skipped (truncated crash writes).
func (w *WAL) Replay() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	writes := make(map[uint64]WALEntry)    // seq → entry
	committed := make(map[uint64]struct{}) // seq → committed

	if err := w.scanEntries(func(op uint8, seq uint64, msgID string, payload []byte) {
		switch op {
		case walOpWrite:
			var msg types.Message
			if err := json.Unmarshal(payload, &msg); err == nil {
				writes[seq] = WALEntry{Seq: seq, MsgID: msgID, Msg: &msg}
			}
		case walOpCommit:
			committed[seq] = struct{}{}
		}
	}); err != nil {
		return nil, err
	}

	var uncommitted []WALEntry
	for seq, entry := range writes {
		if _, ok := committed[seq]; !ok {
			uncommitted = append(uncommitted, entry)
		}
	}
	return uncommitted, nil
}

// Truncate resets the WAL to an empty state (magic header only).
// Call this after crash recovery has been successfully applied so the
// WAL does not grow across restarts.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(int64(len(walMagic))); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	return w.file.Sync()
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync on close: %w", err)
	}
	return w.file.Close()
}

// Path returns the filesystem path of the WAL file.
func (w *WAL) Path() string { return w.path }

// ---- internal helpers -------------------------------------------------------

// appendEntry writes buf to the end of the WAL file (no locking — callers hold mu).
func (w *WAL) appendEntry(buf []byte) (int64, error) {
	offset, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := w.file.Write(buf); err != nil {
		return 0, err
	}
	return offset, nil
}

// encodeEntry serialises a WAL entry to bytes.
// Layout:
//
//	[totalLen:4][op:1][seq:8][msgID:26][payloadLen:4][payload:N][checksum:4]
//
// totalLen = 1+8+26+4+N+4 = 43+N (includes checksum so scanEntries reads the
// exact right number of bytes at offset+4).
func (w *WAL) encodeEntry(op uint8, seq uint64, msgID string, payload []byte) []byte {
	msgIDPadded := padULID(msgID)
	// totalLen is the byte count for everything AFTER the 4-byte length prefix,
	// including the 4-byte checksum at the end.
	totalLen := uint32(walFixedSize + len(payload)) // walFixedSize=43 already includes checksum(4)

	buf := make([]byte, 0, 4+int(totalLen))
	bw := &byteWriter{buf: buf}

	bw.writeUint32(totalLen)
	bw.writeByte(op)
	bw.writeUint64(seq)
	bw.write(msgIDPadded[:])
	bw.writeUint32(uint32(len(payload)))
	bw.write(payload)

	// Checksum covers every byte from op through payload (excludes the 4-byte
	// length prefix and the checksum field itself).
	checksum := crc32.ChecksumIEEE(bw.buf[4:])
	bw.writeUint32(checksum)

	return bw.buf
}

// scanEntries iterates over every entry in the WAL file, calling fn for each
// valid entry. Invalid (corrupt or truncated) entries are silently skipped.
func (w *WAL) scanEntries(fn func(op uint8, seq uint64, msgID string, payload []byte)) error {
	offset := int64(len(walMagic)) // skip magic header

	for {
		var lenBuf [4]byte
		_, err := w.file.ReadAt(lenBuf[:], offset)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			break
		}

		totalLen := binary.BigEndian.Uint32(lenBuf[:])
		if totalLen < uint32(walFixedSize) { // minimum valid entry (no payload)
			break
		}

		entryBuf := make([]byte, totalLen)
		if _, err := w.file.ReadAt(entryBuf, offset+4); err != nil {
			break
		}

		// Verify checksum (covers all bytes except the trailing 4-byte CRC).
		storedCRC := binary.BigEndian.Uint32(entryBuf[len(entryBuf)-4:])
		computed := crc32.ChecksumIEEE(entryBuf[:len(entryBuf)-4])
		if storedCRC != computed {
			// Corrupt entry (crash mid-write) — stop scanning.
			break
		}

		// Parse fixed fields.
		op := entryBuf[0]
		seq := binary.BigEndian.Uint64(entryBuf[1:])
		msgIDRaw := entryBuf[9:35]
		payloadLen := binary.BigEndian.Uint32(entryBuf[35:])
		var payload []byte
		if payloadLen > 0 && int(39+payloadLen) <= len(entryBuf)-4 {
			payload = entryBuf[39 : 39+payloadLen]
		}

		fn(op, seq, string(msgIDRaw), payload)
		offset += 4 + int64(totalLen)
	}
	return nil
}

// restoreSeq scans existing WAL entries and sets w.seq to the highest seq seen.
func (w *WAL) restoreSeq() error {
	var maxSeq uint64
	if err := w.scanEntries(func(_ uint8, seq uint64, _ string, _ []byte) {
		if seq > maxSeq {
			maxSeq = seq
		}
	}); err != nil {
		return err
	}
	w.seq.Store(maxSeq)
	return nil
}
