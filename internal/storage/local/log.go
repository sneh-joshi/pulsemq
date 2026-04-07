// Package local provides a single-node, disk-backed implementation of
// storage.StorageEngine using an append-only log file for message bodies
// and a bbolt database for the message index.
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

	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

// logVersion identifies the binary format written to log.dat.
// Increment this if the on-disk format ever changes — old files will be
// rejected rather than silently misread.
const logVersion uint8 = 1

// Log is an append-only file that stores raw message entries.
// Each entry is a length-prefixed binary record:
//
//	[totalLen : 4 bytes, uint32, big-endian]
//	[version  : 1 byte]
//	[term     : 8 bytes, uint64]   ← Raft term; Phase 1 always 0
//	[logIndex : 8 bytes, uint64]   ← monotone WAL index per log
//	[nodeID   : 26 bytes]          ← ULID of the origin node
//	[msgID    : 26 bytes]          ← ULID of the message
//	[publishedAt : 8 bytes, int64] ← UTC ms
//	[deliverAt   : 8 bytes, int64] ← UTC ms; 0 = immediate
//	[attempt     : 4 bytes, int32]
//	[maxRetries  : 4 bytes, int32]
//	[nsLen    : 2 bytes, uint16]
//	[queueLen : 2 bytes, uint16]
//	[bodyLen  : 4 bytes, uint32]
//	[metaLen  : 4 bytes, uint32]
//	--- variable length ---
//	[namespace : nsLen bytes]
//	[queue     : queueLen bytes]
//	[body      : bodyLen bytes]
//	[meta      : metaLen bytes]    ← JSON-encoded map[string]string
//	--- integrity ---
//	[checksum : 4 bytes, uint32, CRC32 of everything above]
//
// totalLen covers all bytes after the 4-byte length prefix itself.
//
// The format is cluster-ready: term, logIndex, and nodeID are present from
// day one so Phase 2 (Raft) can use them without a format migration.
type Log struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	logIndex atomic.Uint64 // monotone counter, incremented on every Append
}

// fixedHeaderSize is the number of bytes in the fixed part of each entry
// (after the 4-byte totalLen prefix, before variable-length fields).
const fixedHeaderSize = 1 + 8 + 8 + 26 + 26 + 8 + 8 + 4 + 4 + 2 + 2 + 4 + 4 // = 105

// OpenLog opens (or creates) the log file at path.
// It scans existing entries to restore the logIndex counter so that new
// entries continue the sequence after a restart.
func OpenLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, fmt.Errorf("log: open %s: %w", path, err)
	}

	l := &Log{file: f, path: path}

	// Restore logIndex by counting existing valid entries.
	if err := l.replayIndex(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("log: replay %s: %w", path, err)
	}

	return l, nil
}

// Append serialises msg to the log and returns the byte offset of the entry.
// The offset is stored in the index so ReadAt can seek directly to the entry.
func (l *Log) Append(msg *types.Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	idx := l.logIndex.Add(1) // 1-based

	entry, err := encodeEntry(msg, idx)
	if err != nil {
		return 0, fmt.Errorf("log: encode: %w", err)
	}

	// Seek to end of file to get the offset we will write at.
	offset, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("log: seek end: %w", err)
	}

	// Write length prefix (4 bytes) then entry.
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(entry)))

	if _, err := l.file.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("log: write len prefix: %w", err)
	}
	if _, err := l.file.Write(entry); err != nil {
		return 0, fmt.Errorf("log: write entry: %w", err)
	}

	return offset, nil
}

// ReadAt reads and decodes the entry at the given byte offset.
func (l *Log) ReadAt(offset int64) (*types.Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.readAt(offset)
}

// readAt is the non-locking inner read used by both ReadAt and replayIndex.
func (l *Log) readAt(offset int64) (*types.Message, error) {
	// Read 4-byte length prefix.
	var lenBuf [4]byte
	if _, err := l.file.ReadAt(lenBuf[:], offset); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("log: read len prefix at %d: %w", offset, err)
	}
	entryLen := binary.BigEndian.Uint32(lenBuf[:])
	if entryLen == 0 {
		return nil, storage.ErrNotFound
	}

	// Read the full entry.
	buf := make([]byte, entryLen)
	if _, err := l.file.ReadAt(buf, offset+4); err != nil {
		return nil, fmt.Errorf("log: read entry at %d: %w", offset, err)
	}

	msg, err := decodeEntry(buf)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// Path returns the filesystem path of this log file.
func (l *Log) Path() string { return l.path }

// ReadAll calls fn for every valid entry in the log, in order.
// Entries with corrupt checksums are silently skipped (they are from a
// crash mid-write and will be cleaned up by the next compaction).
// Iteration stops early if fn returns a non-nil error.
func (l *Log) ReadAll(fn func(offset int64, msg *types.Message) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var offset int64
	for {
		msg, err := l.readAt(offset)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				break // clean EOF
			}
			if errors.Is(err, storage.ErrCorrupted) {
				// Corrupt trailing entry (crash mid-write) — stop iteration.
				break
			}
			return fmt.Errorf("log: ReadAll at offset %d: %w", offset, err)
		}

		// Compute entry size to advance offset.
		var lenBuf [4]byte
		if _, err := l.file.ReadAt(lenBuf[:], offset); err != nil {
			break
		}
		entryLen := binary.BigEndian.Uint32(lenBuf[:])
		entryOffset := offset
		offset += 4 + int64(entryLen)

		if err := fn(entryOffset, msg); err != nil {
			return err
		}
	}
	return nil
}

// Reopen closes the current file and reopens the file at path.
// Used by compaction after atomically renaming the compacted log into place.
func (l *Log) Reopen(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("log: sync before reopen: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("log: close before reopen: %w", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return fmt.Errorf("log: reopen %s: %w", path, err)
	}

	l.file = f
	l.path = path

	// Rebuild logIndex by re-scanning the new file.
	l.logIndex.Store(0)
	if err := l.replayIndex(); err != nil {
		return fmt.Errorf("log: rebuild index after reopen: %w", err)
	}
	return nil
}

// Sync flushes the OS file buffer to physical disk.
// Called by the Storage layer according to the configured fsync policy.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

// Close flushes and closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("log: sync: %w", err)
	}
	return l.file.Close()
}

// replayIndex scans the log from the beginning to count valid entries and
// rebuild the logIndex counter. Invalid / truncated trailing entries are
// ignored (they would be from a crash mid-write).
func (l *Log) replayIndex() error {
	var offset int64
	var count uint64

	for {
		var lenBuf [4]byte
		_, err := l.file.ReadAt(lenBuf[:], offset)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}

		entryLen := binary.BigEndian.Uint32(lenBuf[:])
		if entryLen == 0 {
			break
		}

		// Skip past the entry — we just need the count.
		offset += 4 + int64(entryLen)
		count++
	}

	l.logIndex.Store(count)
	return nil
}

// ---- binary encoding helpers -----------------------------------------------

// encodeEntry serialises a message into the on-disk format described above.
func encodeEntry(msg *types.Message, logIndex uint64) ([]byte, error) {
	// Encode metadata as JSON.
	var metaBytes []byte
	if len(msg.Metadata) > 0 {
		var err error
		metaBytes, err = json.Marshal(msg.Metadata)
		if err != nil {
			return nil, fmt.Errorf("encode metadata: %w", err)
		}
	}

	ns := []byte(msg.Namespace)
	q := []byte(msg.Queue)

	// Pad / truncate ULID strings to exactly 26 bytes.
	nodeID := padULID(msg.NodeID)
	msgID := padULID(msg.ID)

	totalSize := fixedHeaderSize + len(ns) + len(q) + len(msg.Body) + len(metaBytes) + 4 // +4 for checksum
	buf := make([]byte, 0, totalSize)
	w := &byteWriter{buf: buf}

	w.writeByte(logVersion)
	w.writeUint64(msg.Term)
	w.writeUint64(logIndex)
	w.write(nodeID[:])
	w.write(msgID[:])
	w.writeInt64(msg.PublishedAt)
	w.writeInt64(msg.DeliverAt)
	w.writeInt32(int32(msg.Attempt))
	w.writeInt32(int32(msg.MaxRetries))
	w.writeUint16(uint16(len(ns)))
	w.writeUint16(uint16(len(q)))
	w.writeUint32(uint32(len(msg.Body)))
	w.writeUint32(uint32(len(metaBytes)))
	w.write(ns)
	w.write(q)
	w.write(msg.Body)
	w.write(metaBytes)

	checksum := crc32.ChecksumIEEE(w.buf)
	w.writeUint32(checksum)

	return w.buf, nil
}

// decodeEntry deserialises an entry buffer (without the 4-byte length prefix).
func decodeEntry(buf []byte) (*types.Message, error) {
	if len(buf) < fixedHeaderSize+4 {
		return nil, fmt.Errorf("log: entry too short (%d bytes): %w", len(buf), storage.ErrCorrupted)
	}

	// Verify checksum — covers all bytes except the trailing 4-byte CRC itself.
	storedCRC := binary.BigEndian.Uint32(buf[len(buf)-4:])
	computedCRC := crc32.ChecksumIEEE(buf[:len(buf)-4])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("log: checksum mismatch (stored=%x computed=%x): %w",
			storedCRC, computedCRC, storage.ErrCorrupted)
	}

	r := &byteReader{buf: buf}

	version := r.readByte()
	if version != logVersion {
		return nil, fmt.Errorf("log: unsupported version %d", version)
	}

	msg := &types.Message{}
	msg.Term = r.readUint64()
	msg.LogIndex = r.readUint64()
	msg.NodeID = trimNull(r.read(26))
	msg.ID = trimNull(r.read(26))
	msg.PublishedAt = r.readInt64()
	msg.DeliverAt = r.readInt64()
	msg.Attempt = int(r.readInt32())
	msg.MaxRetries = int(r.readInt32())

	nsLen := int(r.readUint16())
	qLen := int(r.readUint16())
	bodyLen := int(r.readUint32())
	metaLen := int(r.readUint32())

	msg.Namespace = string(r.read(nsLen))
	msg.Queue = string(r.read(qLen))

	body := r.read(bodyLen)
	msg.Body = make([]byte, bodyLen)
	copy(msg.Body, body)

	if metaLen > 0 {
		metaBytes := r.read(metaLen)
		if err := json.Unmarshal(metaBytes, &msg.Metadata); err != nil {
			return nil, fmt.Errorf("log: decode metadata: %w", err)
		}
	}

	return msg, nil
}

// trimNull converts a fixed-width 26-byte ULID field back to a Go string,
// stripping trailing null bytes that result from right-padding shorter IDs.
func trimNull(b []byte) string {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return string(b)
}

// padULID returns a 26-byte slice from a ULID string, right-padded with zeros
// if shorter and truncated if longer. All valid ULIDs are exactly 26 chars.
func padULID(s string) [26]byte {
	var out [26]byte
	copy(out[:], []byte(s))
	return out
}

// ---- minimal byte-level writer / reader ------------------------------------

type byteWriter struct{ buf []byte }

func (w *byteWriter) writeByte(v byte)     { w.buf = append(w.buf, v) }
func (w *byteWriter) write(v []byte)       { w.buf = append(w.buf, v...) }
func (w *byteWriter) writeUint16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *byteWriter) writeUint32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *byteWriter) writeUint64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }
func (w *byteWriter) writeInt32(v int32)   { w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v)) }
func (w *byteWriter) writeInt64(v int64)   { w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v)) }

type byteReader struct {
	buf    []byte
	offset int
}

func (r *byteReader) readByte() byte {
	v := r.buf[r.offset]
	r.offset++
	return v
}
func (r *byteReader) read(n int) []byte {
	v := r.buf[r.offset : r.offset+n]
	r.offset += n
	return v
}
func (r *byteReader) readUint16() uint16 {
	v := binary.BigEndian.Uint16(r.buf[r.offset:])
	r.offset += 2
	return v
}
func (r *byteReader) readUint32() uint32 {
	v := binary.BigEndian.Uint32(r.buf[r.offset:])
	r.offset += 4
	return v
}
func (r *byteReader) readUint64() uint64 {
	v := binary.BigEndian.Uint64(r.buf[r.offset:])
	r.offset += 8
	return v
}
func (r *byteReader) readInt32() int32 {
	return int32(r.readUint32())
}
func (r *byteReader) readInt64() int64 {
	return int64(r.readUint64())
}
