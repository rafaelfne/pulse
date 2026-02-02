package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"pulse/internal/event"
	"pulse/internal/logstore"
	"pulse/internal/partition"
)

// Config holds ingest configuration.
type Config struct {
	NumPartitions     int
	MaxBatchSize      int
	MaxQueueSize      int
	WorkerCount       int
	ShutdownTimeoutMs int
}

// Result represents the result of ingesting a single event.
type Result struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

// BatchResult represents the result of ingesting a batch of events.
type BatchResult struct {
	Results []Result `json:"results"`
}

// Ingest coordinates event ingestion with partitioning and storage.
type Ingest struct {
	cfg     Config
	logger  *slog.Logger
	router  *partition.Router
	storage *logstore.Storage
	queue   chan work
	wg      sync.WaitGroup
	stopCh  chan struct{}
}

type work struct {
	events  []event.Event
	resultC chan<- batchResult
}

type batchResult struct {
	results []Result
	err     error
}

// NewIngest creates a new Ingest instance.
func NewIngest(cfg Config, logger *slog.Logger, storage *logstore.Storage) *Ingest {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1000
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4
	}
	if cfg.ShutdownTimeoutMs <= 0 {
		cfg.ShutdownTimeoutMs = 5000
	}

	router := partition.NewRouter(cfg.NumPartitions)

	return &Ingest{
		cfg:     cfg,
		logger:  logger,
		router:  router,
		storage: storage,
		queue:   make(chan work, cfg.MaxQueueSize),
		stopCh:  make(chan struct{}),
	}
}

// Start starts background workers.
func (ing *Ingest) Start(ctx context.Context) {
	for i := 0; i < ing.cfg.WorkerCount; i++ {
		ing.wg.Add(1)
		go ing.worker(ctx, i)
	}
	ing.logger.Info("ingest workers started", "count", ing.cfg.WorkerCount)
}

// IngestBatch ingests a batch of events and returns their partition/offset.
func (ing *Ingest) IngestBatch(ctx context.Context, events []event.Event) (BatchResult, error) {
	if len(events) == 0 {
		return BatchResult{}, nil
	}

	if len(events) > ing.cfg.MaxBatchSize {
		return BatchResult{}, fmt.Errorf("batch size %d exceeds max %d", len(events), ing.cfg.MaxBatchSize)
	}

	resultC := make(chan batchResult, 1)
	w := work{
		events:  events,
		resultC: resultC,
	}

	select {
	case ing.queue <- w:
		// Sent successfully
	case <-ctx.Done():
		return BatchResult{}, ctx.Err()
	case <-ing.stopCh:
		return BatchResult{}, fmt.Errorf("ingest shutting down")
	}

	// Wait for result
	select {
	case res := <-resultC:
		if res.err != nil {
			return BatchResult{}, res.err
		}
		return BatchResult{Results: res.results}, nil
	case <-ctx.Done():
		return BatchResult{}, ctx.Err()
	}
}

// Close stops all workers gracefully.
func (ing *Ingest) Close() error {
	ing.logger.Info("stopping ingest workers")
	close(ing.stopCh)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		ing.wg.Wait()
		close(done)
	}()

	timeout := time.Duration(ing.cfg.ShutdownTimeoutMs) * time.Millisecond
	select {
	case <-done:
		ing.logger.Info("ingest workers stopped")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("ingest shutdown timeout after %v", timeout)
	}
}

// worker processes incoming batches.
func (ing *Ingest) worker(ctx context.Context, id int) {
	defer ing.wg.Done()

	for {
		select {
		case w := <-ing.queue:
			ing.processBatch(w)
		case <-ing.stopCh:
			ing.logger.Info("worker stopping", "id", id)
			return
		case <-ctx.Done():
			ing.logger.Info("worker context cancelled", "id", id)
			return
		}
	}
}

// processBatch processes a batch of events.
func (ing *Ingest) processBatch(w work) {
	results := make([]Result, len(w.events))

	for i, evt := range w.events {
		// Route to partition
		partitionID := ing.router.Route(evt.Key)

		// Create entry
		entry := event.NewEntry(0, evt) // Offset will be assigned by storage

		// Append to storage
		offset, err := ing.storage.Append(partitionID, entry)
		if err != nil {
			w.resultC <- batchResult{err: fmt.Errorf("append to partition %d: %w", partitionID, err)}
			return
		}

		results[i] = Result{
			Partition: partitionID,
			Offset:    offset,
		}
	}

	w.resultC <- batchResult{results: results}
}
