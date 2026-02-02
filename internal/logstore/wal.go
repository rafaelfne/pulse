package logstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/rafaelfne/pulse/internal/event"
)

// WAL represents a write-ahead log for a single partition.
// Single-writer only; not safe for concurrent writes.
type WAL struct {
	partitionID int
	dir         string
	file        *os.File
	writer      *bufio.Writer
	nextOffset  int64
	mu          sync.Mutex
}

// OpenWAL opens or creates a WAL for the given partition.
func OpenWAL(dir string, partitionID int) (*WAL, error) {
	partitionDir := filepath.Join(dir, fmt.Sprintf("partition-%d", partitionID))
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, fmt.Errorf("create partition dir: %w", err)
	}

	walPath := filepath.Join(partitionDir, "wal.log")

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	// Calculate next offset by reading existing file
	nextOffset, err := calculateNextOffset(walPath)
	if err != nil {
		_ = file.Close() // Best effort close on error
		return nil, fmt.Errorf("calculate next offset: %w", err)
	}

	wal := &WAL{
		partitionID: partitionID,
		dir:         partitionDir,
		file:        file,
		writer:      bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		nextOffset:  nextOffset,
	}

	return wal, nil
}

// Append writes an entry to the WAL and returns its offset.
func (w *WAL) Append(e *event.Entry) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Assign offset
	e.Offset = w.nextOffset

	// Encode entry
	encoded, err := e.Encode()
	if err != nil {
		return 0, fmt.Errorf("encode entry: %w", err)
	}

	// Write length prefix (4 bytes) + entry
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(encoded)))

	if _, err := w.writer.Write(lenBuf); err != nil {
		return 0, fmt.Errorf("write length: %w", err)
	}
	if _, err := w.writer.Write(encoded); err != nil {
		return 0, fmt.Errorf("write entry: %w", err)
	}

	offset := w.nextOffset
	w.nextOffset++

	return offset, nil
}

// Flush flushes buffered writes to disk.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush buffer: %w", err)
	}
	return nil
}

// Sync flushes and syncs to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync file: %w", err)
	}
	return nil
}

// Close closes the WAL after flushing.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush on close: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

// NextOffset returns the next offset that will be assigned.
func (w *WAL) NextOffset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextOffset
}

// calculateNextOffset reads the WAL file to determine the next offset.
func calculateNextOffset(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // New file starts at 0
		}
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer func() {
		_ = file.Close() // Best effort close
	}()

	reader := bufio.NewReader(file)
	var lastOffset int64 = -1

	for {
		// Read length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf)
		if length == 0 || length > 10*1024*1024 { // Sanity check: max 10MB
			return 0, fmt.Errorf("invalid entry length: %d", length)
		}

		// Read entry
		entryBuf := make([]byte, length)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			return 0, fmt.Errorf("read entry: %w", err)
		}

		// Decode to get offset
		entry, err := event.Decode(entryBuf)
		if err != nil {
			return 0, fmt.Errorf("decode entry: %w", err)
		}

		lastOffset = entry.Offset
	}

	return lastOffset + 1, nil
}
