package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rafaelfne/pulse/internal/app"
	"github.com/rafaelfne/pulse/internal/config"
	"github.com/rafaelfne/pulse/internal/log"
	"github.com/rafaelfne/pulse/internal/runtime"
	"github.com/rafaelfne/pulse/internal/version"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger := log.New(cfg.LogLevel)
	logger.Info("pulse starting",
		"version", version.Version,
		"commit", version.Commit,
		"build_time", version.BuildTime,
	)

	// Create root context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start application
	application := app.New(cfg, logger)

	// Start app in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := application.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	go runtime.WaitForShutdownSignal(ctx, cancel, cfg.ShutdownTimeoutMs, logger)

	// Wait for error or context cancellation
	select {
	case err := <-errChan:
		logger.Error("application error", "error", err)
		cancel()
	case <-ctx.Done():
		logger.Info("shutdown initiated")
	}

	// Stop application gracefully
	shutdownCtx := context.Background()
	if err := application.Stop(shutdownCtx); err != nil {
		logger.Error("error during shutdown", "error", err)
		os.Exit(1)
	}

	logger.Info("pulse stopped")
}
