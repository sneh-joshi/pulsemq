package node_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/node"
)

func TestNew_GeneratesIDOnFirstStart(t *testing.T) {
	dir := t.TempDir()

	n, err := node.New(dir, "auto")
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if n.ID().IsZero() {
		t.Fatal("expected non-zero ID")
	}
	if len(n.ID().String()) != 26 {
		t.Errorf("ULID should be 26 chars, got %d: %s", len(n.ID().String()), n.ID())
	}
}

func TestNew_PersistsIDActrossRestarts(t *testing.T) {
	dir := t.TempDir()

	n1, err := node.New(dir, "auto")
	if err != nil {
		t.Fatalf("first New() error: %v", err)
	}

	n2, err := node.New(dir, "auto")
	if err != nil {
		t.Fatalf("second New() error: %v", err)
	}

	if n1.ID() != n2.ID() {
		t.Errorf("ID changed across restarts: %s != %s", n1.ID(), n2.ID())
	}
}

func TestNew_IDStoredInDataDir(t *testing.T) {
	dir := t.TempDir()

	n, err := node.New(dir, "auto")
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "node_id"))
	if err != nil {
		t.Fatalf("node_id file not found: %v", err)
	}

	persisted := strings.TrimSpace(string(data))
	if persisted != n.ID().String() {
		t.Errorf("persisted ID %q != returned ID %q", persisted, n.ID())
	}
}

func TestNew_ExplicitOverride(t *testing.T) {
	dir := t.TempDir()
	override := node.MustNewID()

	n, err := node.New(dir, override)
	if err != nil {
		t.Fatalf("New() with override error: %v", err)
	}

	if n.ID().String() != override {
		t.Errorf("expected override ID %s, got %s", override, n.ID())
	}
}

func TestNew_InvalidOverride_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	_, err := node.New(dir, "not-a-valid-ulid")
	if err == nil {
		t.Fatal("expected error for invalid ULID override")
	}
}

func TestNew_EmptyDataDir_ReturnsError(t *testing.T) {
	_, err := node.New("", "auto")
	if err == nil {
		t.Fatal("expected error for empty dataDir")
	}
}

func TestNew_CreatesDataDirIfAbsent(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "subdir", "data")

	_, err := node.New(dir, "auto")
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("expected data dir to be created")
	}
}

func TestNew_CorruptIDFile_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "node_id")
	if err := os.WriteFile(idFile, []byte("garbage-not-a-ulid\n"), 0o640); err != nil {
		t.Fatal(err)
	}

	_, err := node.New(dir, "auto")
	if err == nil {
		t.Fatal("expected error for corrupt node_id file")
	}
}

func TestMustNewID_UniqueAcrossCalls(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := node.MustNewID()
		if ids[id] {
			t.Fatalf("duplicate ULID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestMustNewID_IsMonotonicallyIncreasing(t *testing.T) {
	a := node.MustNewID()
	b := node.MustNewID()
	// ULIDs are lexicographically sortable by time.
	if a >= b {
		t.Errorf("expected %s < %s (ULIDs must be monotonically increasing)", a, b)
	}
}

func TestNode_DataDir(t *testing.T) {
dir := t.TempDir()
n, err := node.New(dir, "auto")
if err != nil {
t.Fatalf("New: %v", err)
}
if n.DataDir() != dir {
t.Errorf("DataDir(): want %q, got %q", dir, n.DataDir())
}
}

func TestNewID_ReturnsValidULID(t *testing.T) {
id, err := node.NewID()
if err != nil {
t.Fatalf("NewID: %v", err)
}
if len(id) != 26 {
t.Errorf("ULID should be 26 chars, got %d: %q", len(id), id)
}
}

func TestMustNewID_NoPanic(t *testing.T) {
// Should not panic under normal conditions.
id := node.MustNewID()
if len(id) != 26 {
t.Errorf("ULID should be 26 chars, got %d", len(id))
}
}

// TestNew_UnreadableIDFile_ReturnsError verifies that when the node_id file
// exists but cannot be read (permission denied), New() returns an error.
// This covers the non-ErrNotExist branch in node.go (lines 85-86).
// The test is skipped when running as root (where chmod 000 has no effect).
func TestNew_UnreadableIDFile_ReturnsError(t *testing.T) {
if os.Getuid() == 0 {
t.Skip("running as root — permission restrictions have no effect")
}
dir := t.TempDir()
idFile := filepath.Join(dir, "node_id")
// Write a valid-looking node_id file, then make it unreadable.
if err := os.WriteFile(idFile, []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV\n"), 0o640); err != nil {
t.Fatalf("WriteFile: %v", err)
}
if err := os.Chmod(idFile, 0o000); err != nil {
t.Fatalf("Chmod: %v", err)
}
t.Cleanup(func() { _ = os.Chmod(idFile, 0o640) })

_, err := node.New(dir, "auto")
if err == nil {
t.Fatal("expected error for unreadable node_id file, got nil")
}
}

// TestNew_MkdirAllFails_ReturnsError verifies that New returns an error when
// os.MkdirAll fails (e.g., a file already exists at the target path).
// This covers lines 47-49 in node.go.
func TestNew_MkdirAllFails_ReturnsError(t *testing.T) {
parent := t.TempDir()
// Create a FILE at the path where the node would create a directory.
blocked := filepath.Join(parent, "blocked")
if err := os.WriteFile(blocked, []byte("block"), 0o640); err != nil {
t.Fatalf("WriteFile: %v", err)
}
// Now try to use "blocked/data" as the dataDir — MkdirAll must fail because
// "blocked" is a file, not a directory.
_, err := node.New(filepath.Join(blocked, "data"), "auto")
if err == nil {
t.Fatal("expected error when os.MkdirAll fails, got nil")
}
}

// TestNew_WriteFileFails_ReturnsError verifies that New returns an error when
// writing the node_id file fails (e.g., the directory is not writable).
// This covers lines 95-97 in node.go.
func TestNew_WriteFileFails_ReturnsError(t *testing.T) {
if os.Getuid() == 0 {
t.Skip("running as root — permission restrictions have no effect")
}
dir := t.TempDir()
// Make the directory read-only: MkdirAll will succeed (dir exists) but
// WriteFile will fail (directory not writable).
if err := os.Chmod(dir, 0o500); err != nil {
t.Fatalf("Chmod: %v", err)
}
t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

_, err := node.New(dir, "auto")
if err == nil {
t.Fatal("expected error when node_id WriteFile fails, got nil")
}
}
