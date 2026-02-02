package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"pulse/internal/event"
	"pulse/internal/logstore"
)

// Config holds consumer configuration.
type Config struct {
	MaxFetchSize int
}

// StreamResult represents a batch of entries read from a partition.
type StreamResult struct {
	Entries []*event.Entry `json:"entries"`
}

// Consumer reads from log storage.
type Consumer struct {
	cfg     Config
	logger  *slog.Logger
	storage *logstore.Storage
}

// NewConsumer creates a new Consumer instance.
func NewConsumer(cfg Config, logger *slog.Logger, storage *logstore.Storage) *Consumer {
	if cfg.MaxFetchSize <= 0 {
		cfg.MaxFetchSize = 1000
	}

	return &Consumer{
		cfg:     cfg,
		logger:  logger,
		storage: storage,
	}
}

// Stream reads entries from a partition starting at the given offset.
func (c *Consumer) Stream(ctx context.Context, partitionID int, startOffset int64, limit int) (StreamResult, error) {
	if limit <= 0 {
		limit = 100 // Default limit
	}
	if limit > c.cfg.MaxFetchSize {
		limit = c.cfg.MaxFetchSize
	}

	entries, err := c.storage.Read(partitionID, startOffset, limit)
	if err != nil {
		return StreamResult{}, fmt.Errorf("read from partition %d: %w", partitionID, err)
	}

	return StreamResult{Entries: entries}, nil
}

// GetNextOffset returns the next offset for a partition.
func (c *Consumer) GetNextOffset(ctx context.Context, partitionID int) (int64, error) {
	offset, err := c.storage.NextOffset(partitionID)
	if err != nil {
		return 0, fmt.Errorf("get next offset for partition %d: %w", partitionID, err)
	}
	return offset, nil
}
