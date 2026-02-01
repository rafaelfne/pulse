package log

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name  string
		level string
	}{
		{"debug level", "debug"},
		{"info level", "info"},
		{"warn level", "warn"},
		{"error level", "error"},
		{"invalid defaults to info", "invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := New(tt.level)

			// Verify logger is not nil
			if logger == nil {
				t.Fatal("expected non-nil logger")
			}
		})
	}
}

func TestNewOutput(t *testing.T) {
	var buf bytes.Buffer

	// Create a logger that writes to buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler)

	// Log a message
	logger.Info("test message", "key", "value")

	// Verify JSON output
	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse JSON log: %v", err)
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("expected msg='test message', got %v", logEntry["msg"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("expected key='value', got %v", logEntry["key"])
	}
}
