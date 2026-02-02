package consumer

import (
	"context"
	"testing"

	"github.com/rafaelfne/pulse/internal/event"
	"github.com/rafaelfne/pulse/internal/log"
	"github.com/rafaelfne/pulse/internal/logstore"
)

func setupStorage(t *testing.T) (*logstore.Storage, context.Context, context.CancelFunc) {
	tmpDir := t.TempDir()
	logger := log.New("error") // Use error level to reduce test noise

	storageCfg := logstore.Config{
		DataDir:       tmpDir,
		NumPartitions: 1,
	}
	storage, err := logstore.NewStorage(storageCfg, logger)
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	storage.Start(ctx)

	return storage, ctx, cancel
}

func TestConsumerStream(t *testing.T) {
	storage, ctx, cancel := setupStorage(t)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	// Write some entries
	for i := 0; i < 5; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := storage.Append(0, &e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	storage.FlushAll()

	consumerCfg := Config{
		MaxFetchSize: 1000,
	}
	cons := NewConsumer(consumerCfg, logger, storage)

	// Stream from offset 0
	result, err := cons.Stream(ctx, 0, 0, 10)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	if len(result.Entries) != 5 {
		t.Errorf("Stream() returned %d entries, want 5", len(result.Entries))
	}

	// Verify order
	for i, entry := range result.Entries {
		if entry.Offset != int64(i) {
			t.Errorf("Entry %d offset = %d, want %d", i, entry.Offset, i)
		}
	}
}

func TestConsumerStreamWithLimit(t *testing.T) {
	storage, ctx, cancel := setupStorage(t)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	// Write entries
	for i := 0; i < 10; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := storage.Append(0, &e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	storage.FlushAll()

	consumerCfg := Config{
		MaxFetchSize: 1000,
	}
	cons := NewConsumer(consumerCfg, logger, storage)

	// Stream with limit
	result, err := cons.Stream(ctx, 0, 0, 3)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	if len(result.Entries) != 3 {
		t.Errorf("Stream() with limit 3 returned %d entries", len(result.Entries))
	}
}

func TestConsumerStreamFromOffset(t *testing.T) {
	storage, ctx, cancel := setupStorage(t)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	// Write entries
	for i := 0; i < 10; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := storage.Append(0, &e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	storage.FlushAll()

	consumerCfg := Config{}
	cons := NewConsumer(consumerCfg, logger, storage)

	// Stream from offset 5
	result, err := cons.Stream(ctx, 0, 5, 10)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	if len(result.Entries) != 5 {
		t.Errorf("Stream() from offset 5 returned %d entries, want 5", len(result.Entries))
	}

	if result.Entries[0].Offset != 5 {
		t.Errorf("First entry offset = %d, want 5", result.Entries[0].Offset)
	}
}

func TestConsumerGetNextOffset(t *testing.T) {
	storage, ctx, cancel := setupStorage(t)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	consumerCfg := Config{}
	cons := NewConsumer(consumerCfg, logger, storage)

	// Initial offset
	offset, err := cons.GetNextOffset(ctx, 0)
	if err != nil {
		t.Fatalf("GetNextOffset() error = %v", err)
	}
	if offset != 0 {
		t.Errorf("Initial GetNextOffset() = %d, want 0", offset)
	}

	// Append entry
	e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: 1000}
	if _, err := storage.Append(0, &e); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Check next offset
	offset, err = cons.GetNextOffset(ctx, 0)
	if err != nil {
		t.Fatalf("GetNextOffset() error = %v", err)
	}
	if offset != 1 {
		t.Errorf("After append, GetNextOffset() = %d, want 1", offset)
	}
}

func TestConsumerMaxFetchSize(t *testing.T) {
	storage, ctx, cancel := setupStorage(t)
	defer cancel()
	defer storage.Close()

	logger := log.New("error")

	// Write many entries
	for i := 0; i < 100; i++ {
		e := event.Entry{Key: "key", Data: []byte("data"), Timestamp: int64(i)}
		if _, err := storage.Append(0, &e); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	storage.FlushAll()

	consumerCfg := Config{
		MaxFetchSize: 10,
	}
	cons := NewConsumer(consumerCfg, logger, storage)

	// Try to fetch more than max
	result, err := cons.Stream(ctx, 0, 0, 1000)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	if len(result.Entries) > 10 {
		t.Errorf("Stream() returned %d entries, should be capped at 10", len(result.Entries))
	}
}
