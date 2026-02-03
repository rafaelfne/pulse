package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"pulse/internal/consumer"
	"pulse/internal/group"
	"pulse/internal/ingest"
	"pulse/internal/logstore"
	"pulse/internal/offset"
)

// testServerDeps creates common test dependencies for server tests.
func testServerDeps(t *testing.T) (*slog.Logger, *logstore.Storage, *ingest.Ingest, *consumer.Consumer, *offset.Store, *group.Manager) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Create storage
	storageCfg := logstore.Config{
		DataDir:           t.TempDir(),
		NumPartitions:     2,
		FlushIntervalMs:   1000,
		SegmentMaxBytes:   10 * 1024 * 1024,
		SegmentMaxAgeMs:   3600000,
		ShutdownTimeoutMs: 5000,
	}
	storage, err := logstore.NewStorage(storageCfg, logger)
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	ctx := context.Background()
	storage.Start(ctx)

	// Create ingest
	ingestCfg := ingest.Config{
		NumPartitions:     2,
		MaxBatchSize:      1000,
		MaxQueueSize:      10000,
		WorkerCount:       2,
		ShutdownTimeoutMs: 5000,
	}
	ing := ingest.NewIngest(ingestCfg, logger, storage)
	ing.Start(ctx)

	// Create consumer
	consumerCfg := consumer.Config{
		MaxFetchSize: 1000,
	}
	cons := consumer.NewConsumer(consumerCfg, logger, storage)

	// Create offset store
	offsetCfg := offset.Config{
		DataDir:           t.TempDir(),
		FlushIntervalMs:   1000,
		ShutdownTimeoutMs: 5000,
	}
	offStore, err := offset.NewStore(offsetCfg, logger)
	if err != nil {
		t.Fatalf("create offset store: %v", err)
	}
	offStore.Start(ctx)

	// Create group manager
	groupCfg := group.Config{
		NumPartitions:          2,
		ConsumerTimeoutMs:      30000,
		MaxInflightPerConsumer: 100,
		ShutdownTimeoutMs:      5000,
	}
	grpMgr := group.NewManager(groupCfg, logger)
	grpMgr.Start(ctx)

	return logger, storage, ing, cons, offStore, grpMgr
}

func TestDocsEndpoints(t *testing.T) {
	logger, storage, ing, cons, offStore, grpMgr := testServerDeps(t)
	defer func() {
		_ = storage.Close()  //nolint:errcheck // Test cleanup
		_ = ing.Close()      //nolint:errcheck // Test cleanup
		_ = offStore.Close() //nolint:errcheck // Test cleanup
		_ = grpMgr.Close()   //nolint:errcheck // Test cleanup
	}()

	tests := []struct {
		name           string
		enableDocs     bool
		path           string
		method         string
		wantStatus     int
		wantContains   string
		wantNotFound   bool
		checkMediaType bool
		wantMediaType  string
	}{
		{
			name:           "docs enabled - openapi spec",
			enableDocs:     true,
			path:           "/openapi.yaml",
			method:         http.MethodGet,
			wantStatus:     http.StatusOK,
			wantContains:   "openapi: 3.0.3",
			wantMediaType:  "application/yaml",
			checkMediaType: true,
		},
		{
			name:           "docs enabled - scalar ui",
			enableDocs:     true,
			path:           "/docs",
			method:         http.MethodGet,
			wantStatus:     http.StatusOK,
			wantContains:   "Pulse API Documentation",
			wantMediaType:  "text/html",
			checkMediaType: true,
		},
		{
			name:         "docs enabled - scalar ui has script tag",
			enableDocs:   true,
			path:         "/docs",
			method:       http.MethodGet,
			wantStatus:   http.StatusOK,
			wantContains: "@scalar/api-reference",
		},
		{
			name:       "docs disabled - openapi spec returns 404",
			enableDocs: false,
			path:       "/openapi.yaml",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "docs disabled - scalar ui returns 404",
			enableDocs: false,
			path:       "/docs",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "openapi spec - method not allowed",
			enableDocs: true,
			path:       "/openapi.yaml",
			method:     http.MethodPost,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "scalar ui - method not allowed",
			enableDocs: true,
			path:       "/docs",
			method:     http.MethodPost,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Host:              "",
				Port:              8080,
				ReadTimeoutMs:     5000,
				WriteTimeoutMs:    10000,
				ShutdownTimeoutMs: 5000,
				MaxBodyBytes:      10 * 1024 * 1024,
				EnableDocs:        tt.enableDocs,
			}

			srv := NewServer(cfg, logger, ing, cons, grpMgr, offStore)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			srv.server.Handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}

			if tt.wantContains != "" {
				body := w.Body.String()
				if !strings.Contains(body, tt.wantContains) {
					t.Errorf("body does not contain %q, got: %s", tt.wantContains, body)
				}
			}

			if tt.checkMediaType {
				contentType := w.Header().Get("Content-Type")
				if !strings.HasPrefix(contentType, tt.wantMediaType) {
					t.Errorf("Content-Type = %q, want prefix %q", contentType, tt.wantMediaType)
				}
			}
		})
	}
}

func TestNonDocsEndpointsStillWork(t *testing.T) {
	logger, storage, ing, cons, offStore, grpMgr := testServerDeps(t)
	defer func() {
		_ = storage.Close()  //nolint:errcheck // Test cleanup
		_ = ing.Close()      //nolint:errcheck // Test cleanup
		_ = offStore.Close() //nolint:errcheck // Test cleanup
		_ = grpMgr.Close()   //nolint:errcheck // Test cleanup
	}()

	// Test with docs disabled
	cfg := Config{
		Host:              "",
		Port:              8080,
		ReadTimeoutMs:     5000,
		WriteTimeoutMs:    10000,
		ShutdownTimeoutMs: 5000,
		MaxBodyBytes:      10 * 1024 * 1024,
		EnableDocs:        false,
	}

	srv := NewServer(cfg, logger, ing, cons, grpMgr, offStore)

	// Test health endpoint
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("health status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); body != "OK" {
		t.Errorf("health body = %q, want %q", body, "OK")
	}

	// Test metrics endpoint
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w = httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("metrics status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "ingest_requests") {
		t.Errorf("metrics body does not contain expected fields")
	}
}
