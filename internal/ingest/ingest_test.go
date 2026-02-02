package ingest

import (
	"context"
	"testing"
	"time"

	"pulse/internal/event"
	"pulse/internal/log"
	"pulse/internal/logstore"
)

func setupStorageForIngest(t *testing.T, numPartitions int) (*logstore.Storage, context.Context, context.CancelFunc) {
	tmpDir := t.TempDir()
	logger := log.New("error") // Use error level to reduce test noise

	storageCfg := logstore.Config{
		DataDir:       tmpDir,
		NumPartitions: numPartitions,
	}
	storage, err := logstore.NewStorage(storageCfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	storage.Start(ctx)

	return storage, ctx, cancel
}

func TestIngestBasic(t *testing.T) {
	storage, ctx, cancel := setupStorageForIngest(t, 2)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	ingestCfg := Config{
		NumPartitions: 2,
		WorkerCount:   2,
	}
	ing := NewIngest(ingestCfg, logger, storage)

	ing.Start(ctx)
	defer ing.Close()

	// Ingest batch
	events := []event.Event{
		{Key: "key1", Data: []byte("data1")},
		{Key: "key2", Data: []byte("data2")},
	}

	result, err := ing.IngestBatch(ctx, events)
	if err != nil {
		t.Fatalf("IngestBatch() error = %v", err)
	}

	if len(result.Results) != 2 {
		t.Errorf("IngestBatch() returned %d results, want 2", len(result.Results))
	}

	// Verify results
	for i, res := range result.Results {
		if res.Partition < 0 || res.Partition >= 2 {
			t.Errorf("Result %d partition out of bounds: %d", i, res.Partition)
		}
		if res.Offset < 0 {
			t.Errorf("Result %d has negative offset: %d", i, res.Offset)
		}
	}
}

func TestIngestEmptyBatch(t *testing.T) {
	storage, ctx, cancel := setupStorageForIngest(t, 1)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	ingestCfg := Config{
		NumPartitions: 1,
	}
	ing := NewIngest(ingestCfg, logger, storage)

	ing.Start(ctx)
	defer ing.Close()

	result, err := ing.IngestBatch(ctx, []event.Event{})
	if err != nil {
		t.Fatalf("IngestBatch() error = %v", err)
	}

	if len(result.Results) != 0 {
		t.Errorf("IngestBatch() with empty batch should return 0 results, got %d", len(result.Results))
	}
}

func TestIngestBatchSizeLimit(t *testing.T) {
	storage, ctx, cancel := setupStorageForIngest(t, 1)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	ingestCfg := Config{
		NumPartitions: 1,
		MaxBatchSize:  5,
	}
	ing := NewIngest(ingestCfg, logger, storage)

	ing.Start(ctx)
	defer ing.Close()

	// Create batch larger than max
	events := make([]event.Event, 10)
	for i := range events {
		events[i] = event.Event{Key: "key", Data: []byte("data")}
	}

	_, err := ing.IngestBatch(ctx, events)
	if err == nil {
		t.Error("IngestBatch() should return error for batch exceeding max size")
	}
}

func TestIngestContextCancellation(t *testing.T) {
	storage, _, storageCancel := setupStorageForIngest(t, 1)
	defer storageCancel()
	defer storage.Close()

	logger := log.New("error")

	ingestCfg := Config{
		NumPartitions: 1,
		MaxQueueSize:  1, // Small queue
	}
	ing := NewIngest(ingestCfg, logger, storage)

	ctx, cancel := context.WithCancel(context.Background())
	ing.Start(ctx)
	defer ing.Close()

	// Cancel context immediately
	cancel()

	events := []event.Event{
		{Key: "key", Data: []byte("data")},
	}

	_, err := ing.IngestBatch(ctx, events)
	if err == nil {
		t.Error("IngestBatch() should return error when context is cancelled")
	}
}

func TestIngestShutdown(t *testing.T) {
	storage, ctx, cancel := setupStorageForIngest(t, 1)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	ingestCfg := Config{
		NumPartitions:     1,
		ShutdownTimeoutMs: 1000,
	}
	ing := NewIngest(ingestCfg, logger, storage)

	ing.Start(ctx)

	// Close immediately
	if err := ing.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Try to ingest after close
	events := []event.Event{
		{Key: "key", Data: []byte("data")},
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	_, err := ing.IngestBatch(ctx2, events)
	if err == nil {
		t.Error("IngestBatch() should fail after Close()")
	}
}
