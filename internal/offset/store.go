package offset

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store persists consumer group offsets to disk.
// Each (groupId, partition) pair has its own file.
type Store struct {
	logger            *slog.Logger
	flushTicker       *time.Ticker
	offsets           map[string]map[int]int64 // groupId -> partition -> offset
	stopCh            chan struct{}
	doneCh            chan struct{}
	dataDir           string
	mu                sync.RWMutex
	flushIntervalMs   int
	shutdownTimeoutMs int
}

// Config holds offset store configuration.
type Config struct {
	DataDir           string
	FlushIntervalMs   int
	ShutdownTimeoutMs int
}

// NewStore creates a new offset store.
func NewStore(cfg Config, logger *slog.Logger) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir is required")
	}
	if cfg.FlushIntervalMs <= 0 {
		cfg.FlushIntervalMs = 1000
	}
	if cfg.ShutdownTimeoutMs <= 0 {
		cfg.ShutdownTimeoutMs = 5000
	}

	offsetDir := filepath.Join(cfg.DataDir, "offsets")
	if err := os.MkdirAll(offsetDir, 0755); err != nil {
		return nil, fmt.Errorf("create offset dir: %w", err)
	}

	s := &Store{
		dataDir:           offsetDir,
		logger:            logger,
		flushIntervalMs:   cfg.FlushIntervalMs,
		shutdownTimeoutMs: cfg.ShutdownTimeoutMs,
		offsets:           make(map[string]map[int]int64),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}

	// Load existing offsets
	if err := s.loadOffsets(); err != nil {
		return nil, fmt.Errorf("load offsets: %w", err)
	}

	return s, nil
}

// Start starts the background flush routine.
func (s *Store) Start(ctx context.Context) {
	s.flushTicker = time.NewTicker(time.Duration(s.flushIntervalMs) * time.Millisecond)

	go func() {
		defer close(s.doneCh)
		for {
			select {
			case <-s.flushTicker.C:
				if err := s.flush(); err != nil {
					s.logger.Error("flush offsets failed", "error", err)
				}
			case <-s.stopCh:
				s.flushTicker.Stop()
				// Final flush on shutdown
				if err := s.flush(); err != nil {
					s.logger.Error("final flush offsets failed", "error", err)
				}
				return
			case <-ctx.Done():
				s.flushTicker.Stop()
				// Final flush when context is cancelled
				if err := s.flush(); err != nil {
					s.logger.Error("final flush offsets failed", "error", err)
				}
				return
			}
		}
	}()
}

// Close stops the store and flushes remaining offsets.
func (s *Store) Close() error {
	close(s.stopCh)

	// Wait for flush to complete with timeout
	timer := time.NewTimer(time.Duration(s.shutdownTimeoutMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-s.doneCh:
		s.logger.Info("offset store closed successfully")
		return nil
	case <-timer.C:
		return fmt.Errorf("offset store shutdown timeout after %dms", s.shutdownTimeoutMs)
	}
}

// Commit commits an offset for a consumer group and partition.
func (s *Store) Commit(groupID string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.offsets[groupID] == nil {
		s.offsets[groupID] = make(map[int]int64)
	}

	currentOffset := s.offsets[groupID][partition]
	if offset < currentOffset {
		return fmt.Errorf("cannot commit offset %d: current offset is %d (offsets must be monotonically increasing)", offset, currentOffset)
	}

	s.offsets[groupID][partition] = offset
	return nil
}

// Get retrieves the committed offset for a consumer group and partition.
// Returns 0 if no offset has been committed.
func (s *Store) Get(groupID string, partition int) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if partitions, ok := s.offsets[groupID]; ok {
		return partitions[partition]
	}
	return 0
}

// GetAll retrieves all offsets for a consumer group.
func (s *Store) GetAll(groupID string) map[int]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[int]int64)
	if partitions, ok := s.offsets[groupID]; ok {
		for p, o := range partitions {
			result[p] = o
		}
	}
	return result
}

// flush writes all offsets to disk.
func (s *Store) flush() error {
	s.mu.RLock()
	snapshot := make(map[string]map[int]int64)
	for group, partitions := range s.offsets {
		snapshot[group] = make(map[int]int64)
		for p, o := range partitions {
			snapshot[group][p] = o
		}
	}
	s.mu.RUnlock()

	// Write each group+partition to its own file
	for groupID, partitions := range snapshot {
		for partition, offset := range partitions {
			if err := s.writeOffset(groupID, partition, offset); err != nil {
				return fmt.Errorf("write offset for group=%s partition=%d: %w", groupID, partition, err)
			}
		}
	}

	return nil
}

// writeOffset writes a single offset to disk.
func (s *Store) writeOffset(groupID string, partition int, offset int64) error {
	filename := s.offsetFilename(groupID, partition)
	tmpFilename := filename + ".tmp"

	// Write to temp file first
	f, err := os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	if _, err := f.Write(buf); err != nil {
		_ = f.Close() //nolint:errcheck // Best-effort cleanup
		return fmt.Errorf("write offset: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close() //nolint:errcheck // Best-effort cleanup
		return fmt.Errorf("sync offset file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("rename offset file: %w", err)
	}

	return nil
}

// loadOffsets loads all offsets from disk.
func (s *Store) loadOffsets() error {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read offset dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) == ".tmp" {
			// Clean up temp files from previous runs
			_ = os.Remove(filepath.Join(s.dataDir, name)) //nolint:errcheck // Best-effort cleanup
			continue
		}

		groupID, partition, err := parseOffsetFilename(name)
		if err != nil {
			s.logger.Warn("skip invalid offset file", "filename", name, "error", err)
			continue
		}

		offset, err := s.readOffset(groupID, partition)
		if err != nil {
			s.logger.Error("failed to read offset file", "filename", name, "error", err)
			continue
		}

		if s.offsets[groupID] == nil {
			s.offsets[groupID] = make(map[int]int64)
		}
		s.offsets[groupID][partition] = offset

		s.logger.Debug("loaded offset", "group", groupID, "partition", partition, "offset", offset)
	}

	return nil
}

// readOffset reads a single offset from disk.
func (s *Store) readOffset(groupID string, partition int) (int64, error) {
	filename := s.offsetFilename(groupID, partition)

	f, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("open offset file: %w", err)
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // Read-only file, close error not critical

	buf := make([]byte, 8)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, fmt.Errorf("read offset: %w", err)
	}

	offset := int64(binary.BigEndian.Uint64(buf))
	return offset, nil
}

// offsetFilename returns the filename for a group+partition offset.
func (s *Store) offsetFilename(groupID string, partition int) string {
	return filepath.Join(s.dataDir, fmt.Sprintf("%s-%d.offset", groupID, partition))
}

// parseOffsetFilename parses a group ID and partition from an offset filename.
func parseOffsetFilename(filename string) (string, int, error) {
	if filepath.Ext(filename) != ".offset" {
		return "", 0, fmt.Errorf("invalid extension")
	}

	base := filename[:len(filename)-7] // Remove ".offset"

	// Find the last hyphen to split groupID and partition
	lastHyphen := -1
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '-' {
			lastHyphen = i
			break
		}
	}

	if lastHyphen == -1 {
		return "", 0, fmt.Errorf("invalid filename format: no hyphen found")
	}

	groupID := base[:lastHyphen]
	partitionStr := base[lastHyphen+1:]

	var partition int
	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		return "", 0, fmt.Errorf("parse partition failed: %w", err)
	}

	return groupID, partition, nil
}
