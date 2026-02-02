package offset

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreCommitAndGet(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		DataDir:           dir,
		FlushIntervalMs:   100,
		ShutdownTimeoutMs: 1000,
	}

	store, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	//nolint:errcheck // Test cleanup
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.Start(ctx)

	// Commit an offset
	if err := store.Commit("group1", 0, 100); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Get the offset
	offset := store.Get("group1", 0)
	if offset != 100 {
		t.Errorf("Get returned %d, want 100", offset)
	}

	// Get for non-existent group
	offset = store.Get("group2", 0)
	if offset != 0 {
		t.Errorf("Get for non-existent group returned %d, want 0", offset)
	}
}

func TestStoreMonotonicOffsets(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		DataDir:           dir,
		FlushIntervalMs:   100,
		ShutdownTimeoutMs: 1000,
	}

	store, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	//nolint:errcheck // Test cleanup
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.Start(ctx)

	// Commit increasing offsets
	if err := store.Commit("group1", 0, 10); err != nil {
		t.Fatalf("Commit 10 failed: %v", err)
	}
	if err := store.Commit("group1", 0, 20); err != nil {
		t.Fatalf("Commit 20 failed: %v", err)
	}

	// Try to commit lower offset (should fail)
	if err := store.Commit("group1", 0, 15); err == nil {
		t.Error("Commit of lower offset should have failed")
	}

	// Verify final offset
	offset := store.Get("group1", 0)
	if offset != 20 {
		t.Errorf("Get returned %d, want 20", offset)
	}
}

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		DataDir:           dir,
		FlushIntervalMs:   50,
		ShutdownTimeoutMs: 1000,
	}

	// Create store and commit offsets
	store1, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store1.Start(ctx)

	if commitErr := store1.Commit("group1", 0, 100); commitErr != nil {
		t.Fatalf("Commit failed: %v", commitErr)
	}
	if commitErr := store1.Commit("group1", 1, 200); commitErr != nil {
		t.Fatalf("Commit failed: %v", commitErr)
	}
	if commitErr := store1.Commit("group2", 0, 300); commitErr != nil {
		t.Fatalf("Commit failed: %v", commitErr)
	}

	// Wait for flush
	time.Sleep(150 * time.Millisecond)

	// Close store
	cancel()
	if closeErr := store1.Close(); closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	// Create new store and verify offsets were loaded
	store2, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	defer func() { _ = store2.Close() }() //nolint:errcheck // Test cleanup

	if offset := store2.Get("group1", 0); offset != 100 {
		t.Errorf("After restart, group1 partition 0 offset = %d, want 100", offset)
	}
	if offset := store2.Get("group1", 1); offset != 200 {
		t.Errorf("After restart, group1 partition 1 offset = %d, want 200", offset)
	}
	if offset := store2.Get("group2", 0); offset != 300 {
		t.Errorf("After restart, group2 partition 0 offset = %d, want 300", offset)
	}
}

func TestStoreGetAll(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		DataDir:           dir,
		FlushIntervalMs:   100,
		ShutdownTimeoutMs: 1000,
	}

	store, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	//nolint:errcheck // Test cleanup
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.Start(ctx)

	// Commit multiple partitions
	if err := store.Commit("group1", 0, 100); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := store.Commit("group1", 1, 200); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := store.Commit("group1", 2, 300); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Get all offsets
	offsets := store.GetAll("group1")
	if len(offsets) != 3 {
		t.Errorf("GetAll returned %d offsets, want 3", len(offsets))
	}
	if offsets[0] != 100 || offsets[1] != 200 || offsets[2] != 300 {
		t.Errorf("GetAll returned %v, want map[0:100 1:200 2:300]", offsets)
	}

	// Get all for non-existent group
	offsets = store.GetAll("group2")
	if len(offsets) != 0 {
		t.Errorf("GetAll for non-existent group returned %d offsets, want 0", len(offsets))
	}
}

func TestParseOffsetFilename(t *testing.T) {
	tests := []struct {
		filename  string
		wantGroup string
		wantPart  int
		wantErr   bool
	}{
		{"group1-0.offset", "group1", 0, false},
		{"my-group-5.offset", "my-group", 5, false},
		{"group1-0.txt", "", 0, true},
		{"invalid", "", 0, true},
	}

	for _, tt := range tests {
		group, part, err := parseOffsetFilename(tt.filename)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseOffsetFilename(%q) expected error, got nil", tt.filename)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseOffsetFilename(%q) unexpected error: %v", tt.filename, err)
			continue
		}
		if group != tt.wantGroup || part != tt.wantPart {
			t.Errorf("parseOffsetFilename(%q) = (%q, %d), want (%q, %d)",
				tt.filename, group, part, tt.wantGroup, tt.wantPart)
		}
	}
}

func TestStoreOffsetFilename(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		DataDir:           dir,
		FlushIntervalMs:   100,
		ShutdownTimeoutMs: 1000,
	}

	store, err := NewStore(cfg, logger)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	//nolint:errcheck // Test cleanup
	defer func() { _ = store.Close() }()

	filename := store.offsetFilename("test-group", 5)
	expected := filepath.Join(dir, "offsets", "test-group-5.offset")
	if filename != expected {
		t.Errorf("offsetFilename returned %q, want %q", filename, expected)
	}
}
