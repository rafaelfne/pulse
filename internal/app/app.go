package app

import (
	"context"
	"log/slog"

	"github.com/rafaelfne/pulse/internal/config"
)

// App represents the main application with its lifecycle.
type App struct {
	logger *slog.Logger
	cfg    config.Config
}

// New creates a new App instance with the provided configuration and logger.
func New(cfg config.Config, logger *slog.Logger) *App {
	return &App{
		cfg:    cfg,
		logger: logger,
	}
}

// Start initializes and starts the application.
// This is where future components (servers, workers, etc.) would be started.
func (a *App) Start(ctx context.Context) error {
	a.logger.Info("starting application",
		"env", a.cfg.Env,
		"log_level", a.cfg.LogLevel,
	)

	// Future: start HTTP server, gRPC server, background workers, etc.

	// Block until context is cancelled
	<-ctx.Done()
	a.logger.Info("application context cancelled")

	return nil
}

// Stop performs graceful shutdown of the application.
// This is where future components would be stopped cleanly.
func (a *App) Stop(ctx context.Context) error {
	a.logger.Info("stopping application")

	// Future: stop servers, flush buffers, close connections, etc.

	a.logger.Info("application stopped successfully")
	return nil
}
