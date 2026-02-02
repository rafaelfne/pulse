package logstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"pulse/internal/event"
)

// Segment represents a read-only log segment file.
type Segment struct {
	BaseOffset int64
	path       string
}

// Reader reads entries from WAL and segments.
type Reader struct {
	partitionDir string
	segments     []Segment
}

// NewReader creates a new Reader for a partition.
func NewReader(dir string, partitionID int) (*Reader, error) {
	partitionDir := filepath.Join(dir, fmt.Sprintf("partition-%d", partitionID))

	// Load segments
	segments, err := loadSegments(partitionDir)
	if err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	return &Reader{
		partitionDir: partitionDir,
		segments:     segments,
	}, nil
}

// Read reads entries starting from the given offset, up to limit entries.
func (r *Reader) Read(startOffset int64, limit int) ([]*event.Entry, error) {
	if limit <= 0 {
		return nil, nil
	}

	var entries []*event.Entry

	// Read from segments first
	for _, seg := range r.segments {
		if len(entries) >= limit {
			break
		}
		segEntries, err := r.readFromSegment(seg, startOffset, limit-len(entries))
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", seg.path, err)
		}
		entries = append(entries, segEntries...)
	}

	// Then read from WAL
	if len(entries) < limit {
		walPath := filepath.Join(r.partitionDir, "wal.log")
		walEntries, err := r.readFromFile(walPath, startOffset, limit-len(entries))
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read wal: %w", err)
		}
		entries = append(entries, walEntries...)
	}

	return entries, nil
}

// readFromSegment reads entries from a segment file.
func (r *Reader) readFromSegment(seg Segment, startOffset int64, limit int) ([]*event.Entry, error) {
	return r.readFromFile(seg.path, startOffset, limit)
}

// readFromFile reads entries from a file (segment or WAL).
func (r *Reader) readFromFile(path string, startOffset int64, limit int) ([]*event.Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close file: %w", closeErr)
		}
	}()

	reader := bufio.NewReader(file)
	var entries []*event.Entry

	for len(entries) < limit {
		// Read length prefix
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return entries, fmt.Errorf("read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf)
		if length == 0 || length > 10*1024*1024 {
			return entries, fmt.Errorf("invalid entry length: %d", length)
		}

		// Read entry
		entryBuf := make([]byte, length)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			return entries, fmt.Errorf("read entry: %w", err)
		}

		// Decode
		entry, err := event.Decode(entryBuf)
		if err != nil {
			return entries, fmt.Errorf("decode entry: %w", err)
		}

		// Add if offset matches
		if entry.Offset >= startOffset {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// Refresh reloads segment list (call after rotation).
func (r *Reader) Refresh() error {
	segments, err := loadSegments(r.partitionDir)
	if err != nil {
		return fmt.Errorf("refresh segments: %w", err)
	}
	r.segments = segments
	return nil
}

// loadSegments loads all segment files from the partition directory.
func loadSegments(dir string) ([]Segment, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}

	var segments []Segment
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
			continue
		}

		// Parse base offset from filename: 00000000000000000042.seg
		baseName := strings.TrimSuffix(entry.Name(), ".seg")
		baseOffset, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			continue // Skip invalid segment names
		}

		segments = append(segments, Segment{
			BaseOffset: baseOffset,
			path:       filepath.Join(dir, entry.Name()),
		})
	}

	// Sort by base offset
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].BaseOffset < segments[j].BaseOffset
	})

	return segments, nil
}

// RotateWAL rotates the WAL to a segment file.
func RotateWAL(dir string, partitionID int, baseOffset int64) error {
	partitionDir := filepath.Join(dir, fmt.Sprintf("partition-%d", partitionID))
	walPath := filepath.Join(partitionDir, "wal.log")
	segPath := filepath.Join(partitionDir, fmt.Sprintf("%020d.seg", baseOffset))

	// Rename WAL to segment
	if err := os.Rename(walPath, segPath); err != nil {
		if os.IsNotExist(err) {
			return nil // WAL doesn't exist, nothing to rotate
		}
		return fmt.Errorf("rename wal to segment: %w", err)
	}

	return nil
}
