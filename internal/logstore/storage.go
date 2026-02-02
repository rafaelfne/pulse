package logstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"pulse/internal/event"
)

// Config holds storage configuration.
type Config struct {
	DataDir           string
	NumPartitions     int
	FlushIntervalMs   int
	SegmentMaxBytes   int64
	SegmentMaxAgeMs   int64
	ShutdownTimeoutMs int
}

// Storage manages WALs and segments for all partitions.
type Storage struct {
	cfg     Config
	logger  *slog.Logger
	wals    []*WAL
	readers []*Reader
	mu      sync.RWMutex
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewStorage creates a new Storage instance.
func NewStorage(cfg Config, logger *slog.Logger) (*Storage, error) {
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 1
	}
	if cfg.FlushIntervalMs <= 0 {
		cfg.FlushIntervalMs = 1000
	}
	if cfg.SegmentMaxBytes <= 0 {
		cfg.SegmentMaxBytes = 100 * 1024 * 1024 // 100MB
	}
	if cfg.ShutdownTimeoutMs <= 0 {
		cfg.ShutdownTimeoutMs = 5000
	}

	wals := make([]*WAL, cfg.NumPartitions)
	readers := make([]*Reader, cfg.NumPartitions)

	// Open WAL for each partition
	for i := 0; i < cfg.NumPartitions; i++ {
		wal, err := OpenWAL(cfg.DataDir, i)
		if err != nil {
			// Close already opened WALs
			for j := 0; j < i; j++ {
				_ = wals[j].Close() // Best effort close
			}
			return nil, fmt.Errorf("open wal partition %d: %w", i, err)
		}
		wals[i] = wal

		// Create reader
		reader, err := NewReader(cfg.DataDir, i)
		if err != nil {
			// Close WALs
			for j := 0; j <= i; j++ {
				_ = wals[j].Close() // Best effort close
			}
			return nil, fmt.Errorf("create reader partition %d: %w", i, err)
		}
		readers[i] = reader
	}

	s := &Storage{
		cfg:     cfg,
		logger:  logger,
		wals:    wals,
		readers: readers,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	return s, nil
}

// Start starts background tasks (flushing, rotation).
func (s *Storage) Start(ctx context.Context) {
	go s.flushLoop(ctx)
}

// Append appends an entry to the specified partition.
func (s *Storage) Append(partitionID int, e *event.Entry) (int64, error) {
	if partitionID < 0 || partitionID >= len(s.wals) {
		return 0, fmt.Errorf("invalid partition: %d", partitionID)
	}

	s.mu.RLock()
	wal := s.wals[partitionID]
	s.mu.RUnlock()

	offset, err := wal.Append(e)
	if err != nil {
		return 0, fmt.Errorf("append to partition %d: %w", partitionID, err)
	}

	return offset, nil
}

// Read reads entries from a partition starting at offset.
func (s *Storage) Read(partitionID int, startOffset int64, limit int) ([]*event.Entry, error) {
	if partitionID < 0 || partitionID >= len(s.readers) {
		return nil, fmt.Errorf("invalid partition: %d", partitionID)
	}

	s.mu.RLock()
	reader := s.readers[partitionID]
	s.mu.RUnlock()

	entries, err := reader.Read(startOffset, limit)
	if err != nil {
		return nil, fmt.Errorf("read partition %d: %w", partitionID, err)
	}

	return entries, nil
}

// NextOffset returns the next offset for a partition.
func (s *Storage) NextOffset(partitionID int) (int64, error) {
	if partitionID < 0 || partitionID >= len(s.wals) {
		return 0, fmt.Errorf("invalid partition: %d", partitionID)
	}

	s.mu.RLock()
	wal := s.wals[partitionID]
	s.mu.RUnlock()

	return wal.NextOffset(), nil
}

// Close closes all WALs and stops background tasks.
func (s *Storage) Close() error {
	close(s.stopCh)
	<-s.doneCh

	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for i, wal := range s.wals {
		if err := wal.Close(); err != nil {
			s.logger.Error("error closing wal", "partition", i, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// flushLoop periodically flushes all WALs.
func (s *Storage) flushLoop(ctx context.Context) {
	defer close(s.doneCh)

	ticker := time.NewTicker(time.Duration(s.cfg.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flushAll()
		case <-s.stopCh:
			s.logger.Info("flush loop stopping")
			s.flushAll() // Final flush
			return
		case <-ctx.Done():
			s.logger.Info("flush loop context cancelled")
			s.flushAll() // Final flush
			return
		}
	}
}

// FlushAll flushes all WALs (exported for testing).
func (s *Storage) FlushAll() {
	s.flushAll()
}

// flushAll flushes all WALs.
func (s *Storage) flushAll() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i, wal := range s.wals {
		if err := wal.Flush(); err != nil {
			s.logger.Error("flush error", "partition", i, "error", err)
		}
	}
}
