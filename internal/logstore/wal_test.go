package logstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rafaelfne/pulse/internal/event"
)

func TestWALBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer wal.Close()

	// Test initial offset
	if wal.NextOffset() != 0 {
		t.Errorf("Initial NextOffset() = %d, want 0", wal.NextOffset())
	}

	// Append entries
	entries := []event.Entry{
		{Key: "key1", Data: []byte("data1"), Timestamp: 1000},
		{Key: "key2", Data: []byte("data2"), Timestamp: 2000},
		{Key: "key3", Data: []byte("data3"), Timestamp: 3000},
	}

	for i, e := range entries {
		offset, err := wal.Append(&e)
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if offset != int64(i) {
			t.Errorf("Append() offset = %d, want %d", offset, i)
		}
	}

	// Flush
	if err := wal.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Check next offset
	if wal.NextOffset() != 3 {
		t.Errorf("NextOffset() = %d, want 3", wal.NextOffset())
	}
}

func TestWALPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Write entries
	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	entries := []event.Entry{
		{Key: "key1", Data: []byte("data1"), Timestamp: 1000},
		{Key: "key2", Data: []byte("data2"), Timestamp: 2000},
	}

	for _, e := range entries {
		if _, err := wal.Append(&e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen and check offset
	wal2, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error = %v", err)
	}
	defer wal2.Close()

	if wal2.NextOffset() != 2 {
		t.Errorf("After reopen, NextOffset() = %d, want 2", wal2.NextOffset())
	}

	// Append more
	e := event.Entry{Key: "key3", Data: []byte("data3"), Timestamp: 3000}
	offset, err := wal2.Append(&e)
	if err != nil {
		t.Fatalf("Append() after reopen error = %v", err)
	}
	if offset != 2 {
		t.Errorf("Append() after reopen offset = %d, want 2", offset)
	}
}

func TestWALMultiplePartitions(t *testing.T) {
	tmpDir := t.TempDir()

	wal0, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL(0) error = %v", err)
	}
	defer wal0.Close()

	wal1, err := OpenWAL(tmpDir, 1)
	if err != nil {
		t.Fatalf("OpenWAL(1) error = %v", err)
	}
	defer wal1.Close()

	// Append to different partitions
	e0 := event.Entry{Key: "p0", Data: []byte("data0"), Timestamp: 1000}
	e1 := event.Entry{Key: "p1", Data: []byte("data1"), Timestamp: 2000}

	offset0, _ := wal0.Append(&e0)
	offset1, _ := wal1.Append(&e1)

	if offset0 != 0 {
		t.Errorf("wal0 offset = %d, want 0", offset0)
	}
	if offset1 != 0 {
		t.Errorf("wal1 offset = %d, want 0", offset1)
	}

	// Check separate directories exist
	dir0 := filepath.Join(tmpDir, "partition-0")
	dir1 := filepath.Join(tmpDir, "partition-1")

	if _, err := os.Stat(dir0); os.IsNotExist(err) {
		t.Error("partition-0 directory does not exist")
	}
	if _, err := os.Stat(dir1); os.IsNotExist(err) {
		t.Error("partition-1 directory does not exist")
	}
}

func TestWALSync(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer wal.Close()

	e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: 1000}
	if _, err := wal.Append(&e); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
}
