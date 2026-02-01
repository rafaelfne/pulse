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
}

// Load reads configuration from environment variables with defaults.
func Load() (Config, error) {
	cfg := Config{
		LogLevel:          getEnv("PULSE_LOG_LEVEL", "info"),
		Env:               getEnv("PULSE_ENV", "local"),
		ShutdownTimeoutMs: getEnvInt("PULSE_SHUTDOWN_TIMEOUT_MS", 5000),
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
