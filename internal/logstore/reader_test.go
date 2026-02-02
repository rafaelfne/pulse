package logstore

import (
	"os"
	"path/filepath"
	"testing"

	"pulse/internal/event"
)

func TestReaderBasic(t *testing.T) {
	tmpDir := t.TempDir()

	// Write some entries
	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	entries := []event.Entry{
		{Key: "key1", Data: []byte("data1"), Timestamp: 1000},
		{Key: "key2", Data: []byte("data2"), Timestamp: 2000},
		{Key: "key3", Data: []byte("data3"), Timestamp: 3000},
	}

	for _, e := range entries {
		if _, err := wal.Append(&e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	wal.Close()

	// Read entries
	reader, err := NewReader(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	read, err := reader.Read(0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(read) != 3 {
		t.Errorf("Read() returned %d entries, want 3", len(read))
	}

	// Verify content
	for i, e := range read {
		if e.Offset != int64(i) {
			t.Errorf("Entry %d offset = %d, want %d", i, e.Offset, i)
		}
	}
}

func TestReaderWithOffset(t *testing.T) {
	tmpDir := t.TempDir()

	// Write entries
	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for i := 0; i < 10; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := wal.Append(&e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	wal.Close()

	// Read from offset 5
	reader, err := NewReader(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	read, err := reader.Read(5, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(read) != 5 {
		t.Errorf("Read(5, 10) returned %d entries, want 5", len(read))
	}

	if read[0].Offset != 5 {
		t.Errorf("First entry offset = %d, want 5", read[0].Offset)
	}
}

func TestReaderWithLimit(t *testing.T) {
	tmpDir := t.TempDir()

	// Write entries
	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for i := 0; i < 10; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := wal.Append(&e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	wal.Close()

	// Read with limit
	reader, err := NewReader(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	read, err := reader.Read(0, 3)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(read) != 3 {
		t.Errorf("Read(0, 3) returned %d entries, want 3", len(read))
	}
}

func TestRotateWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Write entries to WAL
	wal, err := OpenWAL(tmpDir, 0)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for i := 0; i < 5; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := wal.Append(&e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	wal.Close()

	// Rotate WAL to segment
	if err := RotateWAL(tmpDir, 0, 0); err != nil {
		t.Fatalf("RotateWAL() error = %v", err)
	}

	// Check segment file exists
	partitionDir := filepath.Join(tmpDir, "partition-0")
	segPath := filepath.Join(partitionDir, "00000000000000000000.seg")
	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		t.Error("Segment file does not exist after rotation")
	}

	// Check WAL is gone
	walPath := filepath.Join(partitionDir, "wal.log")
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Error("WAL file still exists after rotation")
	}

	// Read from segment
	reader, err := NewReader(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	if err := reader.Refresh(); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	read, err := reader.Read(0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(read) != 5 {
		t.Errorf("Read() returned %d entries, want 5", len(read))
	}
}

func TestReaderEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	reader, err := NewReader(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	read, err := reader.Read(0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if len(read) != 0 {
		t.Errorf("Read() returned %d entries, want 0", len(read))
	}
}
