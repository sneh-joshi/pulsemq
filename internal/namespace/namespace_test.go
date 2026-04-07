package namespace_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sneh-joshi/pulsemq/internal/namespace"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "namespace-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ─── Basic CRUD ───────────────────────────────────────────────────────────────

func TestCreate_and_List(t *testing.T) {
	r, err := namespace.New(tempDir(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	names := []string{"payments", "notifications", "analytics"}
	for _, n := range names {
		if err := r.Create(n); err != nil {
			t.Fatalf("Create(%q): %v", n, err)
		}
	}

	list := r.List()
	if len(list) != len(names) {
		t.Fatalf("List len = %d, want %d", len(list), len(names))
	}
	// List must be sorted.
	if list[0].Name != "analytics" || list[1].Name != "notifications" || list[2].Name != "payments" {
		t.Fatalf("List order wrong: got %v", list)
	}
}

func TestCreate_Duplicate(t *testing.T) {
	r, _ := namespace.New(tempDir(t))

	if err := r.Create("orders"); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := r.Create("orders")
	if !errors.Is(err, namespace.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}

func TestCreate_InvalidName(t *testing.T) {
	r, _ := namespace.New(tempDir(t))

	bad := []string{"", "UPPER", "has space", "-leading-hyphen", "a/b"}
	for _, n := range bad {
		if err := r.Create(n); !errors.Is(err, namespace.ErrInvalidName) {
			t.Errorf("Create(%q): want ErrInvalidName, got %v", n, err)
		}
	}
}

func TestDelete(t *testing.T) {
	r, _ := namespace.New(tempDir(t))

	_ = r.Create("temp")
	if err := r.Delete("temp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if r.Exists("temp") {
		t.Fatal("namespace should not exist after delete")
	}
}

func TestDelete_NotFound(t *testing.T) {
	r, _ := namespace.New(tempDir(t))
	err := r.Delete("ghost")
	if !errors.Is(err, namespace.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestExists(t *testing.T) {
	r, _ := namespace.New(tempDir(t))
	_ = r.Create("x")
	if !r.Exists("x") {
		t.Fatal("expected Exists to be true")
	}
	if r.Exists("y") {
		t.Fatal("expected Exists to be false")
	}
}

func TestGet(t *testing.T) {
	r, _ := namespace.New(tempDir(t))
	_ = r.Create("acme")

	ns, err := r.Get("acme")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ns.Name != "acme" {
		t.Fatalf("Name = %q, want acme", ns.Name)
	}
	if ns.CreatedAt == 0 {
		t.Fatal("CreatedAt should not be zero")
	}

	_, err = r.Get("ghost")
	if !errors.Is(err, namespace.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// ─── Ensure (idempotent create) ───────────────────────────────────────────────

func TestEnsure_Idempotent(t *testing.T) {
	r, _ := namespace.New(tempDir(t))

	for i := 0; i < 5; i++ {
		if err := r.Ensure("auto"); err != nil {
			t.Fatalf("Ensure[%d]: %v", i, err)
		}
	}
	if len(r.List()) != 1 {
		t.Fatalf("want 1 namespace, got %d", len(r.List()))
	}
}

func TestEnsure_InvalidName(t *testing.T) {
	r, _ := namespace.New(tempDir(t))
	if err := r.Ensure("Bad Name"); !errors.Is(err, namespace.ErrInvalidName) {
		t.Fatalf("want ErrInvalidName, got %v", err)
	}
}

// ─── Persistence (round-trip through disk) ───────────────────────────────────

func TestPersistence(t *testing.T) {
	dir := tempDir(t)

	r1, _ := namespace.New(dir)
	_ = r1.Create("alpha")
	_ = r1.Create("beta")

	// Simulate process restart: create a new Registry from the same dir.
	r2, err := namespace.New(dir)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}

	list := r2.List()
	if len(list) != 2 {
		t.Fatalf("after reload: got %d namespaces, want 2", len(list))
	}
	if list[0].Name != "alpha" || list[1].Name != "beta" {
		t.Fatalf("after reload: wrong names: %v", list)
	}
}

func TestPersistence_DeleteSurvivedReload(t *testing.T) {
	dir := tempDir(t)

	r1, _ := namespace.New(dir)
	_ = r1.Create("keep")
	_ = r1.Create("drop")
	_ = r1.Delete("drop")

	r2, _ := namespace.New(dir)
	list := r2.List()
	if len(list) != 1 || list[0].Name != "keep" {
		t.Fatalf("after reload: expected [keep], got %v", list)
	}
}

func TestPersistence_FileLocation(t *testing.T) {
	dir := tempDir(t)
	r, _ := namespace.New(dir)
	_ = r.Create("check")

	// The file must exist at <dataDir>/namespaces.json.
	path := filepath.Join(dir, "namespaces.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf("namespaces.json not created at %s", path)
	}
}

// ─── Validation helper ────────────────────────────────────────────────────────

func TestValidateName(t *testing.T) {
	valid := []string{"a", "abc", "my-queue", "q1", "123abc"}
	for _, n := range valid {
		if !namespace.ValidateName(n) {
			t.Errorf("ValidateName(%q) = false, want true", n)
		}
	}
	invalid := []string{"", "A", "has space", "-foo", "a/b", "toolong" + string(make([]byte, 60))}
	for _, n := range invalid {
		if namespace.ValidateName(n) {
			t.Errorf("ValidateName(%q) = true, want false", n)
		}
	}
}
