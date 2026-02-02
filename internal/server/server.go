package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"pulse/internal/consumer"
	"pulse/internal/event"
	"pulse/internal/ingest"
)

//go:embed openapi.yaml
var openapiSpec []byte

// Config holds server configuration.
type Config struct {
	Host              string
	Port              int
	ReadTimeoutMs     int
	WriteTimeoutMs    int
	ShutdownTimeoutMs int
	MaxBodyBytes      int64
	EnableDocs        bool
}

// Server is the HTTP server for ingest and streaming.
type Server struct {
	cfg      Config
	logger   *slog.Logger
	ingest   *ingest.Ingest
	consumer *consumer.Consumer
	server   *http.Server
	metrics  *Metrics
}

// Metrics tracks server metrics.
type Metrics struct {
	IngestRequests  atomic.Int64
	IngestEvents    atomic.Int64
	IngestErrors    atomic.Int64
	StreamRequests  atomic.Int64
	StreamEvents    atomic.Int64
	StreamErrors    atomic.Int64
	IngestLatencyNs atomic.Int64 // Sum of latencies for avg calculation
}

// NewServer creates a new HTTP server.
func NewServer(cfg Config, logger *slog.Logger, ing *ingest.Ingest, cons *consumer.Consumer) *Server {
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}
	if cfg.ReadTimeoutMs <= 0 {
		cfg.ReadTimeoutMs = 5000
	}
	if cfg.WriteTimeoutMs <= 0 {
		cfg.WriteTimeoutMs = 10000
	}
	if cfg.ShutdownTimeoutMs <= 0 {
		cfg.ShutdownTimeoutMs = 10000
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 10 * 1024 * 1024 // 10MB
	}

	s := &Server{
		cfg:      cfg,
		logger:   logger,
		ingest:   ing,
		consumer: cons,
		metrics:  &Metrics{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleIngest)
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)

	// Register docs endpoints if enabled
	if cfg.EnableDocs {
		mux.HandleFunc("/openapi.yaml", s.handleOpenAPISpec)
		mux.HandleFunc("/docs", s.handleScalarUI)
	}

	s.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.ReadTimeoutMs) * time.Millisecond,
		WriteTimeout: time.Duration(cfg.WriteTimeoutMs) * time.Millisecond,
	}

	return s
}

// Start starts the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info("starting http server", "addr", s.server.Addr)

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Wait for error or context cancellation
	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case <-ctx.Done():
		return nil
	}
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping http server")

	shutdownCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ShutdownTimeoutMs)*time.Millisecond)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	s.logger.Info("http server stopped")
	return nil
}

// handleIngest handles POST /events
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.IngestRequests.Add(1)

	// Limit body size
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	defer func() {
		_ = r.Body.Close() // Best effort close
	}()

	// Parse request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.metrics.IngestErrors.Add(1)
		s.logger.Error("read body error", "error", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var batch event.Batch
	if unmarshalErr := json.Unmarshal(body, &batch); unmarshalErr != nil {
		s.metrics.IngestErrors.Add(1)
		s.logger.Error("unmarshal error", "error", unmarshalErr)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Ingest batch
	result, err := s.ingest.IngestBatch(r.Context(), batch.Events)
	if err != nil {
		s.metrics.IngestErrors.Add(1)
		s.logger.Error("ingest error", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.metrics.IngestEvents.Add(int64(len(batch.Events)))
	s.metrics.IngestLatencyNs.Add(time.Since(start).Nanoseconds())

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Error("encode ingest response error", "error", err)
	}
}

// handleStream handles GET /stream?partition=X&offset=Y&limit=Z
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.StreamRequests.Add(1)

	// Parse query params
	query := r.URL.Query()
	partitionStr := query.Get("partition")
	offsetStr := query.Get("offset")
	limitStr := query.Get("limit")

	if partitionStr == "" {
		s.metrics.StreamErrors.Add(1)
		http.Error(w, "Missing partition parameter", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		s.metrics.StreamErrors.Add(1)
		http.Error(w, "Invalid partition", http.StatusBadRequest)
		return
	}

	offset := int64(0)
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			s.metrics.StreamErrors.Add(1)
			http.Error(w, "Invalid offset", http.StatusBadRequest)
			return
		}
	}

	limit := 100 // Default
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			s.metrics.StreamErrors.Add(1)
			http.Error(w, "Invalid limit", http.StatusBadRequest)
			return
		}
	}

	// Stream from consumer
	result, err := s.consumer.Stream(r.Context(), partition, offset, limit)
	if err != nil {
		s.metrics.StreamErrors.Add(1)
		s.logger.Error("stream error", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.metrics.StreamEvents.Add(int64(len(result.Entries)))

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Error("encode stream response error", "error", err)
	}
}

// handleMetrics handles GET /metrics
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ingestRequests := s.metrics.IngestRequests.Load()
	ingestEvents := s.metrics.IngestEvents.Load()
	ingestLatencySum := s.metrics.IngestLatencyNs.Load()

	var avgLatencyMs float64
	if ingestRequests > 0 {
		avgLatencyMs = float64(ingestLatencySum) / float64(ingestRequests) / 1e6
	}

	metrics := map[string]any{
		"ingest_requests":       ingestRequests,
		"ingest_events":         ingestEvents,
		"ingest_errors":         s.metrics.IngestErrors.Load(),
		"ingest_avg_latency_ms": avgLatencyMs,
		"stream_requests":       s.metrics.StreamRequests.Load(),
		"stream_events":         s.metrics.StreamEvents.Load(),
		"stream_errors":         s.metrics.StreamErrors.Load(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		s.logger.Error("encode metrics response error", "error", err)
	}
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		s.logger.Error("write health response error", "error", err)
	}
}

// handleOpenAPISpec handles GET /openapi.yaml
func (s *Server) handleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(openapiSpec); err != nil {
		s.logger.Error("write openapi spec error", "error", err)
	}
}

// handleScalarUI handles GET /docs
func (s *Server) handleScalarUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	html := `<!DOCTYPE html>
<html>
<head>
    <title>Pulse API Documentation</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
    <script
        id="api-reference"
        data-url="/openapi.yaml"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(html)); err != nil {
		s.logger.Error("write scalar ui error", "error", err)
	}
}
