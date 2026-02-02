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
	"pulse/internal/group"
	"pulse/internal/ingest"
	"pulse/internal/offset"
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
	logger       *slog.Logger
	ingest       *ingest.Ingest
	consumer     *consumer.Consumer
	groupManager *group.Manager
	offsetStore  *offset.Store
	server       *http.Server
	metrics      *Metrics
	cfg          Config
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

	// Phase 2 metrics
	GroupStreamRequests atomic.Int64
	GroupStreamEvents   atomic.Int64
	GroupStreamErrors   atomic.Int64
	AckRequests         atomic.Int64
	AckErrors           atomic.Int64
	HeartbeatRequests   atomic.Int64
	HeartbeatErrors     atomic.Int64
	AckLatencyNs        atomic.Int64 // Sum of ACK latencies
}

// NewServer creates a new HTTP server.
func NewServer(cfg Config, logger *slog.Logger, ing *ingest.Ingest, cons *consumer.Consumer, grpMgr *group.Manager, offStore *offset.Store) *Server {
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
		cfg:          cfg,
		logger:       logger,
		ingest:       ing,
		consumer:     cons,
		groupManager: grpMgr,
		offsetStore:  offStore,
		metrics:      &Metrics{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleIngest)
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/health", s.handleHealth)

	// Phase 2 consumer group endpoints
	mux.HandleFunc("/groups/", s.handleGroupRoutes)

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
		_ = r.Body.Close() //nolint:errcheck // Best-effort close
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

		// Phase 2 metrics
		"group_stream_requests": s.metrics.GroupStreamRequests.Load(),
		"group_stream_events":   s.metrics.GroupStreamEvents.Load(),
		"group_stream_errors":   s.metrics.GroupStreamErrors.Load(),
		"ack_requests":          s.metrics.AckRequests.Load(),
		"ack_errors":            s.metrics.AckErrors.Load(),
		"heartbeat_requests":    s.metrics.HeartbeatRequests.Load(),
		"heartbeat_errors":      s.metrics.HeartbeatErrors.Load(),
	}

	// Add ACK latency if available
	ackRequests := s.metrics.AckRequests.Load()
	if ackRequests > 0 {
		ackLatencySum := s.metrics.AckLatencyNs.Load()
		metrics["ack_avg_latency_ms"] = float64(ackLatencySum) / float64(ackRequests) / 1e6
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

// handleGroupRoutes routes consumer group requests.
func (s *Server) handleGroupRoutes(w http.ResponseWriter, r *http.Request) {
	const groupsPrefix = "/groups/"
	const groupsPrefixLen = len(groupsPrefix)

	// Parse path: /groups/{groupId}/{action}
	path := r.URL.Path
	if len(path) < groupsPrefixLen {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Extract group ID and action
	remaining := path[groupsPrefixLen:]
	var groupID, action string

	// Find the next slash
	slashIdx := -1
	for i, c := range remaining {
		if c == '/' {
			slashIdx = i
			break
		}
	}

	if slashIdx == -1 {
		// No action specified
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	groupID = remaining[:slashIdx]
	action = remaining[slashIdx+1:]

	if groupID == "" {
		http.Error(w, "Missing group ID", http.StatusBadRequest)
		return
	}

	switch action {
	case "stream":
		s.handleGroupStream(w, r, groupID)
	case "ack":
		s.handleGroupAck(w, r, groupID)
	case "heartbeat":
		s.handleGroupHeartbeat(w, r, groupID)
	default:
		http.Error(w, "Unknown action", http.StatusNotFound)
	}
}

// handleGroupStream handles GET /groups/{groupId}/stream?consumerId=x&partition=p&limit=n
func (s *Server) handleGroupStream(w http.ResponseWriter, r *http.Request, groupID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.GroupStreamRequests.Add(1)

	query := r.URL.Query()
	consumerID := query.Get("consumerId")
	partitionStr := query.Get("partition")
	limitStr := query.Get("limit")

	if consumerID == "" {
		s.metrics.GroupStreamErrors.Add(1)
		http.Error(w, "Missing consumerId parameter", http.StatusBadRequest)
		return
	}

	if partitionStr == "" {
		s.metrics.GroupStreamErrors.Add(1)
		http.Error(w, "Missing partition parameter", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		s.metrics.GroupStreamErrors.Add(1)
		http.Error(w, "Invalid partition", http.StatusBadRequest)
		return
	}

	limit := 100 // Default
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			s.metrics.GroupStreamErrors.Add(1)
			http.Error(w, "Invalid limit", http.StatusBadRequest)
			return
		}
	}

	// Register consumer and validate partition assignment
	_, regErr := s.groupManager.RegisterConsumer(groupID, consumerID)
	if regErr != nil {
		s.metrics.GroupStreamErrors.Add(1)
		s.logger.Error("register consumer error", "error", regErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	valErr := s.groupManager.ValidateConsumerPartition(groupID, consumerID, partition)
	if valErr != nil {
		s.metrics.GroupStreamErrors.Add(1)
		http.Error(w, fmt.Sprintf("Partition not assigned to consumer: %v", valErr), http.StatusForbidden)
		return
	}

	// Check inflight limit
	inflight := s.groupManager.GetInflight(groupID, consumerID, partition)
	if inflight >= s.groupManager.MaxInflight() {
		s.metrics.GroupStreamErrors.Add(1)
		http.Error(w, "Max inflight messages reached", http.StatusTooManyRequests)
		return
	}

	// Get committed offset
	offset := s.offsetStore.Get(groupID, partition)

	// Stream from consumer
	result, err := s.consumer.Stream(r.Context(), partition, offset, limit)
	if err != nil {
		s.metrics.GroupStreamErrors.Add(1)
		s.logger.Error("stream error", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Track inflight for returned events - validate count first
	numEvents := len(result.Entries)
	if inflight+numEvents > s.groupManager.MaxInflight() {
		// Would exceed limit, return fewer events
		allowed := s.groupManager.MaxInflight() - inflight
		if allowed > 0 {
			result.Entries = result.Entries[:allowed]
			numEvents = allowed
		} else {
			result.Entries = nil
			numEvents = 0
		}
	}

	// Track the allowed events
	for i := 0; i < numEvents; i++ {
		if trackErr := s.groupManager.TrackInflight(groupID, consumerID, partition); trackErr != nil {
			s.logger.Error("track inflight failed", "error", trackErr)
			// This shouldn't happen since we checked the count, but log if it does
			break
		}
	}

	s.metrics.GroupStreamEvents.Add(int64(len(result.Entries)))

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Error("encode stream response error", "error", err)
	}
}

// AckRequest represents an ACK request.
type AckRequest struct {
	ConsumerID string `json:"consumerId"`
	Partition  int    `json:"partition"`
	Offset     int64  `json:"offset"`
}

// handleGroupAck handles POST /groups/{groupId}/ack
func (s *Server) handleGroupAck(w http.ResponseWriter, r *http.Request, groupID string) {
	start := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.AckRequests.Add(1)

	// Parse request body
	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.metrics.AckErrors.Add(1)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.ConsumerID == "" {
		s.metrics.AckErrors.Add(1)
		http.Error(w, "Missing consumerId", http.StatusBadRequest)
		return
	}

	// Validate consumer owns partition
	if err := s.groupManager.ValidateConsumerPartition(groupID, req.ConsumerID, req.Partition); err != nil {
		s.metrics.AckErrors.Add(1)
		http.Error(w, fmt.Sprintf("Partition not assigned to consumer: %v", err), http.StatusForbidden)
		return
	}

	// Commit offset
	if err := s.offsetStore.Commit(groupID, req.Partition, req.Offset); err != nil {
		s.metrics.AckErrors.Add(1)
		s.logger.Error("commit offset error", "error", err, "group", groupID, "partition", req.Partition, "offset", req.Offset)
		http.Error(w, "Failed to commit offset", http.StatusInternalServerError)
		return
	}

	// Release inflight
	if err := s.groupManager.ReleaseInflight(groupID, req.ConsumerID, req.Partition); err != nil {
		s.logger.Warn("release inflight error", "error", err)
	}

	s.metrics.AckLatencyNs.Add(time.Since(start).Nanoseconds())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := map[string]string{"status": "ok"}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("encode ack response error", "error", err)
	}
}

// HeartbeatRequest represents a heartbeat request.
type HeartbeatRequest struct {
	ConsumerID string `json:"consumerId"`
}

// handleGroupHeartbeat handles POST /groups/{groupId}/heartbeat
func (s *Server) handleGroupHeartbeat(w http.ResponseWriter, r *http.Request, groupID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.metrics.HeartbeatRequests.Add(1)

	// Parse request body
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.metrics.HeartbeatErrors.Add(1)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.ConsumerID == "" {
		s.metrics.HeartbeatErrors.Add(1)
		http.Error(w, "Missing consumerId", http.StatusBadRequest)
		return
	}

	// Send heartbeat
	if err := s.groupManager.Heartbeat(groupID, req.ConsumerID); err != nil {
		// If consumer not found, try to register it
		_, regErr := s.groupManager.RegisterConsumer(groupID, req.ConsumerID)
		if regErr != nil {
			s.metrics.HeartbeatErrors.Add(1)
			s.logger.Error("heartbeat and registration error", "heartbeat_error", err, "registration_error", regErr)
			http.Error(w, "Failed to send heartbeat", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := map[string]string{"status": "ok"}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("encode heartbeat response error", "error", err)
	}
}
