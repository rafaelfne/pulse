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
			name: "docs enabled by default in local env",
			env: map[string]string{
				"PULSE_ENV": "local",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if !cfg.EnableDocs {
					t.Errorf("expected EnableDocs=true in local env, got false")
				}
			},
		},
		{
			name: "docs enabled by default in dev env",
			env: map[string]string{
				"PULSE_ENV": "dev",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if !cfg.EnableDocs {
					t.Errorf("expected EnableDocs=true in dev env, got false")
				}
			},
		},
		{
			name: "docs disabled by default in production env",
			env: map[string]string{
				"PULSE_ENV": "production",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if cfg.EnableDocs {
					t.Errorf("expected EnableDocs=false in production env, got true")
				}
			},
		},
		{
			name: "docs can be explicitly enabled",
			env: map[string]string{
				"PULSE_ENV":         "production",
				"PULSE_ENABLE_DOCS": "true",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if !cfg.EnableDocs {
					t.Errorf("expected EnableDocs=true when explicitly set, got false")
				}
			},
		},
		{
			name: "docs can be explicitly disabled",
			env: map[string]string{
				"PULSE_ENV":         "local",
				"PULSE_ENABLE_DOCS": "false",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if cfg.EnableDocs {
					t.Errorf("expected EnableDocs=false when explicitly set, got true")
				}
			},
		},
		{
			name: "cluster disabled by default",
			env:  map[string]string{},
			checkFunc: func(t *testing.T, cfg Config) {
				if cfg.ClusterEnabled {
					t.Errorf("expected ClusterEnabled=false by default, got true")
				}
			},
		},
		{
			name: "cluster enabled with valid config",
			env: map[string]string{
				"PULSE_CLUSTER_ENABLED": "true",
				"PULSE_NODE_ID":         "node1",
				"PULSE_NODE_ADDRESS":    "192.168.1.1:8080",
				// PULSE_CLUSTER_PEERS is expected to be a comma-separated list of peer nodes
				// in the form "nodeID:host:port". The network layer will consume this format.
				"PULSE_CLUSTER_PEERS":         "node2:192.168.1.2:8080,node3:192.168.1.3:8080",
				"PULSE_HEARTBEAT_INTERVAL_MS": "1000",
				"PULSE_ELECTION_TIMEOUT_MS":   "5000",
			},
			checkFunc: func(t *testing.T, cfg Config) {
				if !cfg.ClusterEnabled {
					t.Errorf("expected ClusterEnabled=true, got false")
				}
				if cfg.NodeID != "node1" {
					t.Errorf("expected NodeID=node1, got %s", cfg.NodeID)
				}
				if cfg.NodeAddress != "192.168.1.1:8080" {
					t.Errorf("expected NodeAddress=192.168.1.1:8080, got %s", cfg.NodeAddress)
				}
				if len(cfg.ClusterPeers) != 2 {
					t.Errorf("expected 2 cluster peers, got %d", len(cfg.ClusterPeers))
				}
				if cfg.HeartbeatIntervalMs != 1000 {
					t.Errorf("expected HeartbeatIntervalMs=1000, got %d", cfg.HeartbeatIntervalMs)
				}
			},
		},
		{
			name: "cluster enabled without node ID",
			env: map[string]string{
				"PULSE_CLUSTER_ENABLED": "true",
				"PULSE_NODE_ADDRESS":    "192.168.1.1:8080",
			},
			wantErr: true,
		},
		{
			name: "cluster enabled without node address",
			env: map[string]string{
				"PULSE_CLUSTER_ENABLED": "true",
				"PULSE_NODE_ID":         "node1",
			},
			wantErr: true,
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
		{
			name: "cluster enabled with valid config",
			cfg: Config{
				LogLevel:              "info",
				Env:                   "local",
				ShutdownTimeoutMs:     5000,
				NumPartitions:         4,
				ServerPort:            8080,
				ClusterEnabled:        true,
				NodeID:                "node1",
				NodeAddress:           "192.168.1.1:8080",
				HeartbeatIntervalMs:   1000,
				ElectionTimeoutMs:     5000,
				HealthCheckIntervalMs: 1000,
			},
			wantErr: false,
		},
		{
			name: "cluster enabled without node ID",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     4,
				ServerPort:        8080,
				ClusterEnabled:    true,
				NodeAddress:       "192.168.1.1:8080",
			},
			wantErr: true,
		},
		{
			name: "cluster enabled without node address",
			cfg: Config{
				LogLevel:          "info",
				Env:               "local",
				ShutdownTimeoutMs: 5000,
				NumPartitions:     4,
				ServerPort:        8080,
				ClusterEnabled:    true,
				NodeID:            "node1",
			},
			wantErr: true,
		},
		{
			name: "cluster enabled with invalid heartbeat interval",
			cfg: Config{
				LogLevel:            "info",
				Env:                 "local",
				ShutdownTimeoutMs:   5000,
				NumPartitions:       4,
				ServerPort:          8080,
				ClusterEnabled:      true,
				NodeID:              "node1",
				NodeAddress:         "192.168.1.1:8080",
				HeartbeatIntervalMs: -100,
			},
			wantErr: true,
		},
		{
			name: "cluster enabled with invalid election timeout",
			cfg: Config{
				LogLevel:            "info",
				Env:                 "local",
				ShutdownTimeoutMs:   5000,
				NumPartitions:       4,
				ServerPort:          8080,
				ClusterEnabled:      true,
				NodeID:              "node1",
				NodeAddress:         "192.168.1.1:8080",
				HeartbeatIntervalMs: 1000,
				ElectionTimeoutMs:   0,
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
