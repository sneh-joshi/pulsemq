// Package node manages the identity of this PulseMQ server instance.
// Every node has a persistent ULID that is generated on first start and stored
// in the data directory. This identity is embedded in every WAL entry so that,
// when clustering is added in Phase 2, the origin of each log entry is always
// traceable without coordination.
package node

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const nodeIDFile = "node_id"

// ID is a ULID string that uniquely identifies a PulseMQ process.
// It is stable across restarts within the same data directory.
type ID string

func (id ID) String() string { return string(id) }

// IsZero reports whether the ID is the zero value.
func (id ID) IsZero() bool { return id == "" }

// Node holds the persistent identity of this server instance.
type Node struct {
	id      ID
	dataDir string
}

// New returns a Node whose ID is loaded from dataDir/node_id.
// If the file does not exist a new ULID is generated and written.
// If nodeIDOverride is "auto" or empty the file-based ID is used.
func New(dataDir string, nodeIDOverride string) (*Node, error) {
	if dataDir == "" {
		return nil, errors.New("node: dataDir must not be empty")
	}

	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("node: create data dir: %w", err)
	}

	// Explicit override takes precedence (useful in tests / container envs).
	if nodeIDOverride != "" && nodeIDOverride != "auto" {
		if err := validateULID(nodeIDOverride); err != nil {
			return nil, fmt.Errorf("node: invalid id override %q: %w", nodeIDOverride, err)
		}
		return &Node{id: ID(nodeIDOverride), dataDir: dataDir}, nil
	}

	id, err := loadOrGenerate(dataDir)
	if err != nil {
		return nil, err
	}
	return &Node{id: id, dataDir: dataDir}, nil
}

// ID returns the node's stable ULID string.
func (n *Node) ID() ID { return n.id }

// DataDir returns the root data directory for this node.
func (n *Node) DataDir() string { return n.dataDir }

// loadOrGenerate reads the node ID from disk, creating a new one if absent.
func loadOrGenerate(dataDir string) (ID, error) {
	path := filepath.Join(dataDir, nodeIDFile)

	data, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if err := validateULID(id); err != nil {
			return "", fmt.Errorf("node: persisted id %q is invalid: %w", id, err)
		}
		return ID(id), nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("node: read id file: %w", err)
	}

	// Generate a new ULID using a cryptographically secure entropy source.
	id, err := generateULID()
	if err != nil {
		return "", fmt.Errorf("node: generate id: %w", err)
	}

	if err := os.WriteFile(path, []byte(id.String()+"\n"), 0o640); err != nil {
		return "", fmt.Errorf("node: persist id: %w", err)
	}

	return id, nil
}

// monoEntropy is a package-level monotone entropy source shared across all
// generateULID calls. Using a single shared source ensures that ULIDs remain
// lexicographically ordered even when generated within the same millisecond.
var (
	monoMu      sync.Mutex
	monoEntropy io.Reader = ulid.Monotonic(rand.Reader, 0)
)

// generateULID creates a new time-ordered ULID using the shared monotone
// entropy source. The mutex ensures monotonicity across concurrent calls.
func generateULID() (ID, error) {
	monoMu.Lock()
	defer monoMu.Unlock()
	ms := ulid.Timestamp(time.Now())
	id, err := ulid.New(ms, monoEntropy)
	if err != nil {
		return "", err
	}
	return ID(id.String()), nil
}

// validateULID returns an error if s is not a well-formed ULID string.
func validateULID(s string) error {
	_, err := ulid.ParseStrict(s)
	return err
}

// NewID generates a fresh ULID. Exposed for use by other packages that need
// unique IDs (messages, receipt handles, subscriptions).
func NewID() (string, error) {
	id, err := generateULID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// MustNewID is like NewID but panics on error. Use only in tests or init code.
func MustNewID() string {
	id, err := NewID()
	if err != nil {
		panic(fmt.Sprintf("node.MustNewID: %v", err))
	}
	return id
}
