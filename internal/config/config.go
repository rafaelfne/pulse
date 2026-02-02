package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds application configuration loaded from environment variables.
type Config struct {
	LogLevel          string
	Env               string
	ShutdownTimeoutMs int

	// Storage
	DataDir         string
	NumPartitions   int
	FlushIntervalMs int
	SegmentMaxBytes int64
	SegmentMaxAgeMs int64

	// Ingest
	MaxBatchSize int
	MaxQueueSize int
	WorkerCount  int

	// Consumer
	MaxFetchSize int

	// Server
	ServerHost     string
	ServerPort     int
	ReadTimeoutMs  int
	WriteTimeoutMs int
	MaxBodyBytes   int64
}

// Load reads configuration from environment variables with defaults.
func Load() (Config, error) {
	cfg := Config{
		LogLevel:          getEnv("PULSE_LOG_LEVEL", "info"),
		Env:               getEnv("PULSE_ENV", "local"),
		ShutdownTimeoutMs: getEnvInt("PULSE_SHUTDOWN_TIMEOUT_MS", 5000),

		// Storage
		DataDir:         getEnv("PULSE_DATA_DIR", "./data"),
		NumPartitions:   getEnvInt("PULSE_NUM_PARTITIONS", 4),
		FlushIntervalMs: getEnvInt("PULSE_FLUSH_INTERVAL_MS", 1000),
		SegmentMaxBytes: int64(getEnvInt("PULSE_SEGMENT_MAX_BYTES", 100*1024*1024)),
		SegmentMaxAgeMs: int64(getEnvInt("PULSE_SEGMENT_MAX_AGE_MS", 3600000)),

		// Ingest
		MaxBatchSize: getEnvInt("PULSE_MAX_BATCH_SIZE", 1000),
		MaxQueueSize: getEnvInt("PULSE_MAX_QUEUE_SIZE", 10000),
		WorkerCount:  getEnvInt("PULSE_WORKER_COUNT", 4),

		// Consumer
		MaxFetchSize: getEnvInt("PULSE_MAX_FETCH_SIZE", 1000),

		// Server
		ServerHost:     getEnv("PULSE_SERVER_HOST", ""),
		ServerPort:     getEnvInt("PULSE_SERVER_PORT", 8080),
		ReadTimeoutMs:  getEnvInt("PULSE_READ_TIMEOUT_MS", 5000),
		WriteTimeoutMs: getEnvInt("PULSE_WRITE_TIMEOUT_MS", 10000),
		MaxBodyBytes:   int64(getEnvInt("PULSE_MAX_BODY_BYTES", 10*1024*1024)),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// Validate ensures configuration values are valid.
func (c Config) Validate() error {
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("invalid log level: %s (must be one of: debug, info, warn, error)", c.LogLevel)
	}

	if c.ShutdownTimeoutMs <= 0 {
		return fmt.Errorf("shutdown timeout must be positive, got: %d", c.ShutdownTimeoutMs)
	}

	if c.NumPartitions <= 0 {
		return fmt.Errorf("num partitions must be positive, got: %d", c.NumPartitions)
	}

	if c.ServerPort <= 0 || c.ServerPort > 65535 {
		return fmt.Errorf("server port must be between 1 and 65535, got: %d", c.ServerPort)
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
