package runtime

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// WaitForShutdownSignal blocks until SIGINT or SIGTERM is received,
// then cancels the provided context and waits for the timeout.
func WaitForShutdownSignal(ctx context.Context, cancel context.CancelFunc, timeoutMs int, logger *slog.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received shutdown signal", "signal", sig.String())

	// Cancel the context to signal shutdown
	cancel()

	// Wait for graceful shutdown timeout
	timeout := time.Duration(timeoutMs) * time.Millisecond
	logger.Info("waiting for graceful shutdown", "timeout_ms", timeoutMs)

	select {
	case <-time.After(timeout):
		logger.Warn("shutdown timeout exceeded")
	case <-ctx.Done():
		logger.Info("shutdown completed")
	}
}
