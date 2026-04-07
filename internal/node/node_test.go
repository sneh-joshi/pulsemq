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
