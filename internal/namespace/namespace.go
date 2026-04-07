// Package namespace manages the PulseMQ namespace registry.
//
// A namespace is a logical grouping of queues (e.g. "payments", "notifications").
// Namespaces are created implicitly on first use via Ensure(), or explicitly
// via Create(). They are persisted to a JSON file in the server's data directory
// so they survive restarts.
//
// Design rules:
//   - Namespace names must be 1-64 lowercase alphanumeric characters or hyphens.
//   - Deleting a namespace only succeeds when the caller has already removed all
//     queues within it (the Registry does not own queues — queues are managed by
//     the queue.Manager).
//   - All methods are safe for concurrent use.
package namespace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

// nameRe validates namespace names: 1–64 chars, lowercase letters/digits/hyphens,
// must start with a letter or digit.
var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{0,63}$`)

// ErrNotFound is returned when a namespace that doesn't exist is requested.
var ErrNotFound = errors.New("namespace: not found")

// ErrAlreadyExists is returned when Create is called for an existing namespace.
var ErrAlreadyExists = errors.New("namespace: already exists")

// ErrInvalidName is returned when a namespace name fails validation.
var ErrInvalidName = errors.New("namespace: invalid name")

// Namespace is the metadata stored for each registered namespace.
type Namespace struct {
	Name      string `json:"name"`
	CreatedAt int64  `json:"created_at"` // UTC milliseconds
}

// Registry is the in-memory + on-disk store for all namespace records.
type Registry struct {
	mu         sync.RWMutex
	namespaces map[string]*Namespace
	filePath   string
}

// New creates a Registry and loads any previously persisted namespaces from
// dataDir/namespaces.json. If the file doesn't exist the registry starts empty.
func New(dataDir string) (*Registry, error) {
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("namespace: create data dir: %w", err)
	}

	r := &Registry{
		namespaces: make(map[string]*Namespace),
		filePath:   filepath.Join(dataDir, "namespaces.json"),
	}

	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// Create explicitly registers a new namespace.
// Returns ErrAlreadyExists if the name is already registered.
// Returns ErrInvalidName if the name is not valid.
func (r *Registry) Create(name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.namespaces[name]; ok {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}

	r.namespaces[name] = &Namespace{
		Name:      name,
		CreatedAt: time.Now().UnixMilli(),
	}
	return r.save()
}

// Ensure registers a namespace if it does not already exist, or is a no-op
// if it does. Used for implicit creation on first queue use.
// Returns ErrInvalidName if the name fails validation.
func (r *Registry) Ensure(name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.namespaces[name]; ok {
		return nil
	}

	r.namespaces[name] = &Namespace{
		Name:      name,
		CreatedAt: time.Now().UnixMilli(),
	}
	return r.save()
}

// Delete removes a namespace from the registry.
// The caller is responsible for ensuring no queues remain in the namespace.
// Returns ErrNotFound if the namespace doesn't exist.
func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.namespaces[name]; !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	delete(r.namespaces, name)
	return r.save()
}

// Exists reports whether the given namespace is registered.
func (r *Registry) Exists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.namespaces[name]
	return ok
}

// Get returns the Namespace record, or ErrNotFound.
func (r *Registry) Get(name string) (*Namespace, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ns, ok := r.namespaces[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	// Return a copy to avoid mutation of internal state.
	cp := *ns
	return &cp, nil
}

// List returns all registered namespaces sorted by name.
func (r *Registry) List() []*Namespace {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*Namespace, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		cp := *ns
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ValidateName reports whether name is a valid namespace name without mutating
// the registry.
func ValidateName(name string) bool { return nameRe.MatchString(name) }

// ─── Persistence ──────────────────────────────────────────────────────────────

// fileModel is the on-disk JSON structure.
type fileModel struct {
	Namespaces []*Namespace `json:"namespaces"`
}

// load reads namespaces.json. If the file does not exist it is a no-op.
// Must be called before mu is held (called only from New).
func (r *Registry) load() error {
	data, err := os.ReadFile(r.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // nothing to load
		}
		return fmt.Errorf("namespace: read %s: %w", r.filePath, err)
	}

	var m fileModel
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("namespace: parse %s: %w", r.filePath, err)
	}

	for _, ns := range m.Namespaces {
		r.namespaces[ns.Name] = ns
	}
	return nil
}

// save writes the current registry to disk atomically (write to temp file,
// rename). Must be called with mu held.
func (r *Registry) save() error {
	nsList := make([]*Namespace, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		nsList = append(nsList, ns)
	}
	sort.Slice(nsList, func(i, j int) bool { return nsList[i].Name < nsList[j].Name })

	data, err := json.MarshalIndent(fileModel{Namespaces: nsList}, "", "  ")
	if err != nil {
		return fmt.Errorf("namespace: marshal: %w", err)
	}

	tmp := r.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return fmt.Errorf("namespace: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, r.filePath); err != nil {
		return fmt.Errorf("namespace: rename to %s: %w", r.filePath, err)
	}
	return nil
}
