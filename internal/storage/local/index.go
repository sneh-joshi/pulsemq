package local

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"go.etcd.io/bbolt"

	"github.com/sneh-joshi/pulsemq/internal/storage"
	"github.com/sneh-joshi/pulsemq/internal/types"
)

var (
	bucketIndex = []byte("index") // bucket name inside bbolt
)

// Index is a bbolt-backed persistent index that maps message IDs to their
// storage.IndexEntry (offset in log.dat + lifecycle state).
//
// bbolt is chosen for Phase 1 because it is:
//   - Pure Go (no CGO, no external process)
//   - ACID — the index is always consistent even after a crash
//   - Single file (index.db inside the queue directory)
//   - Well-maintained (used by etcd in production)
//
// Phase 2 (ReplicatedStorage) will provide its own index implementation;
// this type is internal to the local package.
type Index struct {
	db *bbolt.DB
}

// OpenIndex opens (or creates) the bbolt index at path.
func OpenIndex(path string) (*Index, error) {
	opts := &bbolt.Options{Timeout: 0} // non-blocking open
	db, err := bbolt.Open(path, 0o640, opts)
	if err != nil {
		return nil, fmt.Errorf("index: open %s: %w", path, err)
	}

	// Ensure the index bucket exists.
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketIndex)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("index: init bucket: %w", err)
	}

	return &Index{db: db}, nil
}

// Write upserts the index entry for msgID.
func (idx *Index) Write(msgID string, entry storage.IndexEntry) error {
	val, err := marshalEntry(entry)
	if err != nil {
		return fmt.Errorf("index: marshal entry for %s: %w", msgID, err)
	}

	return idx.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketIndex).Put([]byte(msgID), val)
	})
}

// Read retrieves the index entry for msgID.
// Returns storage.ErrNotFound if the message has never been indexed.
func (idx *Index) Read(msgID string) (storage.IndexEntry, error) {
	var entry storage.IndexEntry

	err := idx.db.View(func(tx *bbolt.Tx) error {
		val := tx.Bucket(bucketIndex).Get([]byte(msgID))
		if val == nil {
			return storage.ErrNotFound
		}
		var err error
		entry, err = unmarshalEntry(val)
		return err
	})

	return entry, err
}

// Delete removes the index entry for msgID.
func (idx *Index) Delete(msgID string) error {
	return idx.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketIndex).Delete([]byte(msgID))
	})
}

// ForEach iterates over every index entry, calling fn for each one.
// Iteration stops early if fn returns a non-nil error.
// Useful for crash-recovery and compaction.
func (idx *Index) ForEach(fn func(msgID string, entry storage.IndexEntry) error) error {
	return idx.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketIndex).ForEach(func(k, v []byte) error {
			entry, err := unmarshalEntry(v)
			if err != nil {
				return err
			}
			return fn(string(k), entry)
		})
	})
}

// Close closes the underlying bbolt database.
func (idx *Index) Close() error {
	return idx.db.Close()
}

// ---- serialisation helpers -------------------------------------------------
// IndexEntry is serialised as a compact binary structure to keep bbolt small:
//
//	[offset    : 8 bytes, int64   ]
//	[status    : 1 byte            ]
//	[visDeadMs : 8 bytes, int64   ]
//	[receiptLen: 2 bytes, uint16  ]
//	[receipt   : receiptLen bytes ]
//	[attempt   : 2 bytes, uint16  ]  ← added in v2; absent in old entries
//
// Backward compatibility: entries written before the attempt field was added
// will be shorter by 2 bytes. unmarshalEntry defaults Attempt to 1 in that case.

func marshalEntry(e storage.IndexEntry) ([]byte, error) {
	receipt := []byte(e.ReceiptHandle)
	// v2 layout: fixed(19) + receipt + attempt(2)
	buf := make([]byte, 8+1+8+2+len(receipt)+2)
	binary.BigEndian.PutUint64(buf[0:], uint64(e.Offset))
	buf[8] = uint8(e.Status)
	binary.BigEndian.PutUint64(buf[9:], uint64(e.VisibilityDeadlineMs))
	binary.BigEndian.PutUint16(buf[17:], uint16(len(receipt)))
	copy(buf[19:], receipt)
	attempt := e.Attempt
	if attempt < 1 {
		attempt = 1
	}
	binary.BigEndian.PutUint16(buf[19+len(receipt):], uint16(attempt))
	return buf, nil
}

func unmarshalEntry(buf []byte) (storage.IndexEntry, error) {
	if len(buf) < 19 {
		// Fallback: try JSON (forward-compat with any future format change)
		var e storage.IndexEntry
		if jerr := json.Unmarshal(buf, &e); jerr == nil {
			return e, nil
		}
		return storage.IndexEntry{}, fmt.Errorf("index: entry too short (%d bytes)", len(buf))
	}
	receiptLen := binary.BigEndian.Uint16(buf[17:])
	if int(receiptLen) > len(buf)-19 {
		return storage.IndexEntry{}, fmt.Errorf("index: receipt length %d exceeds buffer", receiptLen)
	}
	e := storage.IndexEntry{
		Offset:               int64(binary.BigEndian.Uint64(buf[0:])),
		Status:               types.Status(buf[8]),
		VisibilityDeadlineMs: int64(binary.BigEndian.Uint64(buf[9:])),
		ReceiptHandle:        string(buf[19 : 19+receiptLen]),
		Attempt:              1, // default for v1 entries
	}
	// v2: attempt field follows receipt
	trailingStart := 19 + int(receiptLen)
	if len(buf) >= trailingStart+2 {
		e.Attempt = int(binary.BigEndian.Uint16(buf[trailingStart:]))
		if e.Attempt < 1 {
			e.Attempt = 1
		}
	}
	return e, nil
}
