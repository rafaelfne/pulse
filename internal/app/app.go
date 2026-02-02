package app

import (
	"context"
	"fmt"
	"log/slog"

	"pulse/internal/config"
	"pulse/internal/consumer"
	"pulse/internal/group"
	"pulse/internal/ingest"
	"pulse/internal/logstore"
	"pulse/internal/offset"
	"pulse/internal/server"
)

// App represents the main application with its lifecycle.
type App struct {
	logger       *slog.Logger
	storage      *logstore.Storage
	offsetStore  *offset.Store
	groupManager *group.Manager
	ingest       *ingest.Ingest
	consumer     *consumer.Consumer
	server       *server.Server
	cfg          config.Config
}

// New creates a new App instance with the provided configuration and logger.
func New(cfg config.Config, logger *slog.Logger) *App {
	return &App{
		cfg:    cfg,
		logger: logger,
	}
}

// Start initializes and starts the application.
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("starting application",
		"env", a.cfg.Env,
		"log_level", a.cfg.LogLevel,
		"num_partitions", a.cfg.NumPartitions,
		"data_dir", a.cfg.DataDir,
	)

	// Initialize storage
	storageCfg := logstore.Config{
		DataDir:           a.cfg.DataDir,
		NumPartitions:     a.cfg.NumPartitions,
		FlushIntervalMs:   a.cfg.FlushIntervalMs,
		SegmentMaxBytes:   a.cfg.SegmentMaxBytes,
		SegmentMaxAgeMs:   a.cfg.SegmentMaxAgeMs,
		ShutdownTimeoutMs: a.cfg.ShutdownTimeoutMs,
	}

	storage, err := logstore.NewStorage(storageCfg, a.logger)
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}
	a.storage = storage

	// Start storage background tasks
	a.storage.Start(ctx)
	a.logger.Info("storage initialized")

	// Initialize offset store
	offsetCfg := offset.Config{
		DataDir:           a.cfg.DataDir,
		FlushIntervalMs:   a.cfg.OffsetFlushIntervalMs,
		ShutdownTimeoutMs: a.cfg.ShutdownTimeoutMs,
	}

	offsetStore, err := offset.NewStore(offsetCfg, a.logger)
	if err != nil {
		return fmt.Errorf("create offset store: %w", err)
	}
	a.offsetStore = offsetStore
	a.offsetStore.Start(ctx)
	a.logger.Info("offset store initialized")

	// Initialize consumer group manager
	groupCfg := group.Config{
		NumPartitions:          a.cfg.NumPartitions,
		ConsumerTimeoutMs:      a.cfg.ConsumerTimeoutMs,
		MaxInflightPerConsumer: a.cfg.MaxInflightPerConsumer,
		ShutdownTimeoutMs:      a.cfg.ShutdownTimeoutMs,
	}

	a.groupManager = group.NewManager(groupCfg, a.logger)
	a.groupManager.Start(ctx)
	a.logger.Info("consumer group manager initialized")

	// Initialize ingest
	ingestCfg := ingest.Config{
		NumPartitions:     a.cfg.NumPartitions,
		MaxBatchSize:      a.cfg.MaxBatchSize,
		MaxQueueSize:      a.cfg.MaxQueueSize,
		WorkerCount:       a.cfg.WorkerCount,
		ShutdownTimeoutMs: a.cfg.ShutdownTimeoutMs,
	}

	a.ingest = ingest.NewIngest(ingestCfg, a.logger, a.storage)
	a.ingest.Start(ctx)
	a.logger.Info("ingest initialized")

	// Initialize consumer
	consumerCfg := consumer.Config{
		MaxFetchSize: a.cfg.MaxFetchSize,
	}

	a.consumer = consumer.NewConsumer(consumerCfg, a.logger, a.storage)
	a.logger.Info("consumer initialized")

	// Initialize server
	serverCfg := server.Config{
		Host:              a.cfg.ServerHost,
		Port:              a.cfg.ServerPort,
		ReadTimeoutMs:     a.cfg.ReadTimeoutMs,
		WriteTimeoutMs:    a.cfg.WriteTimeoutMs,
		ShutdownTimeoutMs: a.cfg.ShutdownTimeoutMs,
		MaxBodyBytes:      a.cfg.MaxBodyBytes,
		EnableDocs:        a.cfg.EnableDocs,
	}

	a.server = server.NewServer(serverCfg, a.logger, a.ingest, a.consumer, a.groupManager, a.offsetStore)
	a.logger.Info("server initialized", "port", a.cfg.ServerPort, "docs_enabled", a.cfg.EnableDocs)

	// Start server (blocks until context cancelled)
	return a.server.Start(ctx)
}

// Stop performs graceful shutdown of the application.
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("stopping application")

	var firstErr error

	// Stop server
	if a.server != nil {
		if err := a.server.Stop(ctx); err != nil {
			a.logger.Error("error stopping server", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Stop ingest
	if a.ingest != nil {
		if err := a.ingest.Close(); err != nil {
			a.logger.Error("error stopping ingest", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Stop consumer group manager
	if a.groupManager != nil {
		if err := a.groupManager.Close(); err != nil {
			a.logger.Error("error stopping group manager", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Stop offset store
	if a.offsetStore != nil {
		if err := a.offsetStore.Close(); err != nil {
			a.logger.Error("error stopping offset store", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Stop storage
	if a.storage != nil {
		if err := a.storage.Close(); err != nil {
			a.logger.Error("error stopping storage", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	a.logger.Info("application stopped successfully")
	return firstErr
}
