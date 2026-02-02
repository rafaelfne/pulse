package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	//nolint:govet // test struct field alignment is not critical
	tests := []struct {
		name      string
		env       map[string]string
		wantErr   bool
		checkFunc func(*testing.T, Config)
	}{
		{
			name: "defaults",
			env:  map[string]string{},
			checkFunc: func(t *testing.T, cfg Config) {
				if cfg.LogLevel != "info" {
					t.Errorf("expected LogLevel=info, got %s", cfg.LogLevel)
				}
				if cfg.Env != "local" {
					t.Errorf("expected Env=local, got %s", cfg.Env)
				}
				if cfg.ShutdownTimeoutMs != 5000 {
					t.Errorf("expected ShutdownTimeoutMs=5000, got %d", cfg.ShutdownTimeoutMs)
				}
			},
		},
		{
			name: "custom values",
			env: map[string]string{
				"PULSE_LOG_LEVEL":           "debug",
				"PULSE_ENV":                 "production",
				"PULSE_SHUTDOWN_TIMEOUT_MS": "10000",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if cfg.LogLevel != "debug" {
					t.Errorf("expected LogLevel=debug, got %s", cfg.LogLevel)
				}
				if cfg.Env != "production" {
					t.Errorf("expected Env=production, got %s", cfg.Env)
				}
				if cfg.ShutdownTimeoutMs != 10000 {
					t.Errorf("expected ShutdownTimeoutMs=10000, got %d", cfg.ShutdownTimeoutMs)
				}
			},
		},
		{
			name: "invalid log level",
			env: map[string]string{
				"PULSE_LOG_LEVEL": "invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid shutdown timeout",
			env: map[string]string{
				"PULSE_SHUTDOWN_TIMEOUT_MS": "-100",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear and set environment
			os.Clearenv()
			for k, v := range tt.env {
				if err := os.Setenv(k, v); err != nil {
					t.Fatalf("failed to set env: %v", err)
				}
			}

			cfg, err := Load()
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkFunc != nil {
				tt.checkFunc(t, cfg)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     4,
				ServerPort:        8080,
			},
			wantErr: false,
		},
		{
			name: "invalid log level",
			cfg: Config{
				LogLevel:          "invalid",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     4,
				ServerPort:        8080,
			},
			wantErr: true,
		},
		{
			name: "negative timeout",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: -100,
				NumPartitions:     4,
				ServerPort:        8080,
			},
			wantErr: true,
		},
		{
			name: "zero partitions",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     0,
				ServerPort:        8080,
			},
			wantErr: true,
		},
		{
			name: "invalid port",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     4,
				ServerPort:        99999,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
