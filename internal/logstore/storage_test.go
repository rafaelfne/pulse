package logstore

import (
	"context"
	"testing"
	"time"

	"pulse/internal/event"
	"pulse/internal/log"
)

func TestStorageBasic(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New("info")

	cfg := Config{
		DataDir:           tmpDir,
		NumPartitions:     2,
		FlushIntervalMs:   100,
		ShutdownTimeoutMs: 1000,
	}

	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	defer storage.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage.Start(ctx)

	// Append to partition 0
	e0 := event.Entry{Key: "key0", Data: []byte("data0"), Timestamp: 1000}
	offset0, err := storage.Append(0, &e0)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if offset0 != 0 {
		t.Errorf("Append() offset = %d, want 0", offset0)
	}

	// Append to partition 1
	e1 := event.Entry{Key: "key1", Data: []byte("data1"), Timestamp: 2000}
	offset1, err := storage.Append(1, &e1)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if offset1 != 0 {
		t.Errorf("Append() offset = %d, want 0", offset1)
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Read from partition 0
	entries0, err := storage.Read(0, 0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(entries0) != 1 {
		t.Errorf("Read() returned %d entries, want 1", len(entries0))
	}

	// Read from partition 1
	entries1, err := storage.Read(1, 0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(entries1) != 1 {
		t.Errorf("Read() returned %d entries, want 1", len(entries1))
	}
}

func TestStorageNextOffset(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New("info")

	cfg := Config{
		DataDir:       tmpDir,
		NumPartitions: 1,
	}

	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage.Start(ctx)
	defer storage.Close()

	// Initial offset
	offset, err := storage.NextOffset(0)
	if err != nil {
		t.Fatalf("NextOffset() error = %v", err)
	}
	if offset != 0 {
		t.Errorf("Initial NextOffset() = %d, want 0", offset)
	}

	// Append entry
	e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: 1000}
	if _, err := storage.Append(0, &e); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Check next offset
	offset, err = storage.NextOffset(0)
	if err != nil {
		t.Fatalf("NextOffset() error = %v", err)
	}
	if offset != 1 {
		t.Errorf("After append, NextOffset() = %d, want 1", offset)
	}
}

func TestStorageInvalidPartition(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New("info")

	cfg := Config{
		DataDir:       tmpDir,
		NumPartitions: 2,
	}

	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage.Start(ctx)
	defer storage.Close()

	e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: 1000}

	// Invalid partition
	_, err = storage.Append(5, &e)
	if err == nil {
		t.Error("Append() to invalid partition should return error")
	}

	_, err = storage.Read(5, 0, 10)
	if err == nil {
		t.Error("Read() from invalid partition should return error")
	}

	_, err = storage.NextOffset(5)
	if err == nil {
		t.Error("NextOffset() for invalid partition should return error")
	}
}

func TestStorageMultipleAppends(t *testing.T) {
	tmpDir := t.TempDir()
	logger := log.New("info")

	cfg := Config{
		DataDir:       tmpDir,
		NumPartitions: 1,
	}

	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage.Start(ctx)
	defer storage.Close()

	// Append multiple entries
	for i := 0; i < 10; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		offset, err := storage.Append(0, &e)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if offset != int64(i) {
			t.Errorf("Append() offset = %d, want %d", offset, i)
		}
	}

	// Flush before reading
	storage.FlushAll()

	// Read all
	entries, err := storage.Read(0, 0, 20)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("Read() returned %d entries, want 10", len(entries))
	}
}
