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

	// Consumer Groups (Phase 2)
	ConsumerTimeoutMs      int
	MaxInflightPerConsumer int
	OffsetFlushIntervalMs  int

	// Server
	ServerHost     string
	ServerPort     int
	ReadTimeoutMs  int
	WriteTimeoutMs int
	MaxBodyBytes   int64

	// Documentation
	EnableDocs bool

	// Cluster (Phase 3)
	ClusterEnabled        bool
	NodeID                string
	NodeAddress           string
	ClusterPeers          []string
	HeartbeatIntervalMs   int
	ElectionTimeoutMs     int
	HealthCheckIntervalMs int
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

		// Consumer Groups (Phase 2)
		ConsumerTimeoutMs:      getEnvInt("PULSE_CONSUMER_TIMEOUT_MS", 30000),
		MaxInflightPerConsumer: getEnvInt("PULSE_MAX_INFLIGHT_PER_CONSUMER", 100),
		OffsetFlushIntervalMs:  getEnvInt("PULSE_OFFSET_FLUSH_INTERVAL_MS", 1000),

		// Server
		ServerHost:     getEnv("PULSE_SERVER_HOST", ""),
		ServerPort:     getEnvInt("PULSE_SERVER_PORT", 8080),
		ReadTimeoutMs:  getEnvInt("PULSE_READ_TIMEOUT_MS", 5000),
		WriteTimeoutMs: getEnvInt("PULSE_WRITE_TIMEOUT_MS", 10000),
		MaxBodyBytes:   int64(getEnvInt("PULSE_MAX_BODY_BYTES", 10*1024*1024)),

		// Documentation
		EnableDocs: getEnvBool("PULSE_ENABLE_DOCS", isDevEnv(getEnv("PULSE_ENV", "local"))),

		// Cluster (Phase 3)
		ClusterEnabled:        getEnvBool("PULSE_CLUSTER_ENABLED", false),
		NodeID:                getEnv("PULSE_NODE_ID", ""),
		NodeAddress:           getEnv("PULSE_NODE_ADDRESS", ""),
		ClusterPeers:          getEnvStringSlice("PULSE_CLUSTER_PEERS", []string{}),
		HeartbeatIntervalMs:   getEnvInt("PULSE_HEARTBEAT_INTERVAL_MS", 1000),
		ElectionTimeoutMs:     getEnvInt("PULSE_ELECTION_TIMEOUT_MS", 5000),
		HealthCheckIntervalMs: getEnvInt("PULSE_HEALTH_CHECK_INTERVAL_MS", 1000),
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

	// Cluster validation (Phase 3)
	if c.ClusterEnabled {
		if c.NodeID == "" {
			return fmt.Errorf("node ID is required when cluster mode is enabled")
		}
		if c.NodeAddress == "" {
			return fmt.Errorf("node address is required when cluster mode is enabled")
		}
		if c.HeartbeatIntervalMs <= 0 {
			return fmt.Errorf("heartbeat interval must be positive, got: %d", c.HeartbeatIntervalMs)
		}
		if c.ElectionTimeoutMs <= 0 {
			return fmt.Errorf("election timeout must be positive, got: %d", c.ElectionTimeoutMs)
		}
		if c.HealthCheckIntervalMs <= 0 {
			return fmt.Errorf("health check interval must be positive, got: %d", c.HealthCheckIntervalMs)
		}
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

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func isDevEnv(env string) bool {
	return env == "local" || env == "dev" || env == "development"
}

func getEnvStringSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		// Support comma-separated values
		var result []string
		for _, v := range splitAndTrim(value, ",") {
			if v != "" {
				result = append(result, v)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

func splitAndTrim(s string, sep string) []string {
	parts := []string{}
	for _, part := range splitString(s, sep) {
		trimmed := trimString(part)
		parts = append(parts, trimmed)
	}
	return parts
}

func splitString(s string, sep string) []string {
	if s == "" {
		return []string{}
	}
	result := []string{}
	current := ""
	for i := 0; i < len(s); i++ {
		if i+len(sep) <= len(s) && s[i:i+len(sep)] == sep {
			result = append(result, current)
			current = ""
			i += len(sep) - 1
		} else {
			current += string(s[i])
		}
	}
	result = append(result, current)
	return result
}

func trimString(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
