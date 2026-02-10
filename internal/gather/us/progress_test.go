package us

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProgressTrackerMarkEmpty(t *testing.T) {
	dir := t.TempDir()

	pt, err := newProgressTracker(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := pt.MarkEmpty([]string{"AAAA", "BBBB", "CCCC"}); err != nil {
		t.Fatal(err)
	}
	pt.Close()

	// Reload and verify.
	pt2, err := newProgressTracker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pt2.Close()

	for _, sym := range []string{"AAAA", "BBBB", "CCCC"} {
		if !pt2.IsTriedEmpty(sym) {
			t.Errorf("expected %q to be tried-empty after reload", sym)
		}
	}
	if pt2.IsTriedEmpty("DDDD") {
		t.Error("DDDD should not be tried-empty")
	}
}

func TestProgressTrackerCompleted(t *testing.T) {
	dir := t.TempDir()

	pt, err := newProgressTracker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Close()

	if pt.IsCompleted("2025-02-10") {
		t.Error("should not be completed before marking")
	}

	if err := pt.MarkCompleted("2025-02-10"); err != nil {
		t.Fatal(err)
	}

	if !pt.IsCompleted("2025-02-10") {
		t.Error("should be completed after marking")
	}

	if pt.IsCompleted("2025-02-11") {
		t.Error("different date should not be completed")
	}
}

func TestProgressTrackerResume(t *testing.T) {
	dir := t.TempDir()

	// Simulate partial run: write some entries directly.
	path := filepath.Join(dir, ".tried-empty")
	if err := os.WriteFile(path, []byte("XXXX\nYYYY\nZZZZ\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	pt, err := newProgressTracker(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Close()

	if !pt.IsTriedEmpty("XXXX") {
		t.Error("XXXX should be loaded from partial run")
	}
	if !pt.IsTriedEmpty("YYYY") {
		t.Error("YYYY should be loaded from partial run")
	}

	// Add more.
	if err := pt.MarkEmpty([]string{"WWWW"}); err != nil {
		t.Fatal(err)
	}
	if !pt.IsTriedEmpty("WWWW") {
		t.Error("WWWW should be tried-empty after marking")
	}
}

func TestProgressTrackerReset(t *testing.T) {
	dir := t.TempDir()

	pt, err := newProgressTracker(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := pt.MarkEmpty([]string{"AAAA"}); err != nil {
		t.Fatal(err)
	}
	if !pt.IsTriedEmpty("AAAA") {
		t.Fatal("AAAA should be tried-empty")
	}

	if err := pt.Reset(); err != nil {
		t.Fatal(err)
	}

	if pt.IsTriedEmpty("AAAA") {
		t.Error("AAAA should not be tried-empty after reset")
	}

	// .tried-empty file should be gone (or empty).
	path := filepath.Join(dir, ".tried-empty")
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(data) > 0 {
		t.Error(".tried-empty file should be empty after reset")
	}

	pt.Close()
}
