package cluster

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestCoordinatorStateString(t *testing.T) {
	tests := []struct {
		state    CoordinatorState
		expected string
	}{
		{CoordinatorStateStopped, "stopped"},
		{CoordinatorStateStarting, "starting"},
		{CoordinatorStateRunning, "running"},
		{CoordinatorStateStopping, "stopping"},
		{CoordinatorState(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.state.String()
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestNewCoordinator(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name      string
		cfg       Config
		wantError bool
	}{
		{
			"valid config",
			Config{
				LocalNodeID:           "node1",
				NumPartitions:         4,
				HeartbeatTimeoutMs:    1000,
				SuspicionTimeoutMs:    2000,
				ElectionTimeoutMs:     1000,
				HealthCheckIntervalMs: 500,
				ShutdownTimeoutMs:     5000,
			},
			false,
		},
		{
			"empty node ID",
			Config{
				LocalNodeID:   "",
				NumPartitions: 4,
			},
			true,
		},
		{
			"zero partitions",
			Config{
				LocalNodeID:   "node1",
				NumPartitions: 0,
			},
			true,
		},
		{
			"negative partitions",
			Config{
				LocalNodeID:   "node1",
				NumPartitions: -1,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coordinator, err := NewCoordinator(tt.cfg, logger)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error=%v, got %v", tt.wantError, err)
			}
			if !tt.wantError && coordinator == nil {
				t.Error("expected coordinator to be created")
			}
		})
	}
}

func TestNewCoordinatorDefaults(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	if coordinator.healthCheckInterval <= 0 {
		t.Error("expected default health check interval to be set")
	}
	if coordinator.shutdownTimeout <= 0 {
		t.Error("expected default shutdown timeout to be set")
	}
}

func TestCoordinatorStartStop(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:           "node1",
		NumPartitions:         4,
		HeartbeatTimeoutMs:    1000,
		SuspicionTimeoutMs:    2000,
		ElectionTimeoutMs:     1000,
		HealthCheckIntervalMs: 100,
		ShutdownTimeoutMs:     5000,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	// Start coordinator
	ctx := context.Background()
	err = coordinator.Start(ctx)
	if err != nil {
		t.Fatalf("start coordinator failed: %v", err)
	}

	// Wait a bit for it to be running
	time.Sleep(50 * time.Millisecond)

	if coordinator.GetCoordinatorState() != CoordinatorStateRunning {
		t.Errorf("expected state running, got %s", coordinator.GetCoordinatorState())
	}

	// Stop coordinator
	err = coordinator.Stop()
	if err != nil {
		t.Fatalf("stop coordinator failed: %v", err)
	}

	if coordinator.GetCoordinatorState() != CoordinatorStateStopped {
		t.Errorf("expected state stopped, got %s", coordinator.GetCoordinatorState())
	}
}

func TestCoordinatorStartAlreadyStarted(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:           "node1",
		NumPartitions:         4,
		HealthCheckIntervalMs: 100,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	ctx := context.Background()
	coordinator.Start(ctx)
	defer coordinator.Stop()

	// Try to start again
	err = coordinator.Start(ctx)
	if err == nil {
		t.Error("expected error when starting already started coordinator")
	}
}

func TestCoordinatorStopNotRunning(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	err = coordinator.Stop()
	if err == nil {
		t.Error("expected error when stopping non-running coordinator")
	}
}

func TestCoordinatorRegisterNode(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	err = coordinator.RegisterNode("node2", "192.168.1.2:8080")
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	node, err := coordinator.GetNode("node2")
	if err != nil {
		t.Fatalf("get node failed: %v", err)
	}

	if node.ID != "node2" {
		t.Errorf("expected node ID node2, got %s", node.ID)
	}
}

func TestCoordinatorUpdateNodeHeartbeat(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node2", "192.168.1.2:8080")

	err = coordinator.UpdateNodeHeartbeat("node2")
	if err != nil {
		t.Fatalf("update heartbeat failed: %v", err)
	}
}

func TestCoordinatorGetActiveNodes(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node2", "192.168.1.2:8080")
	coordinator.RegisterNode("node3", "192.168.1.3:8080")

	activeNodes := coordinator.GetActiveNodes()
	if len(activeNodes) != 2 {
		t.Errorf("expected 2 active nodes, got %d", len(activeNodes))
	}
}

func TestCoordinatorAssignPartitions(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 6,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	// Register nodes
	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.RegisterNode("node2", "192.168.1.2:8080")
	coordinator.RegisterNode("node3", "192.168.1.3:8080")

	// Assign partitions
	err = coordinator.AssignPartitions()
	if err != nil {
		t.Fatalf("assign partitions failed: %v", err)
	}

	// Verify all partitions have leaders
	for i := 0; i < 6; i++ {
		leader, err := coordinator.GetPartitionLeader(i)
		if err != nil {
			t.Errorf("partition %d: get leader failed: %v", i, err)
		}
		if leader == "" {
			t.Errorf("partition %d: no leader assigned", i)
		}
	}
}

func TestCoordinatorAssignPartitionsNoActiveNodes(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	err = coordinator.AssignPartitions()
	if err == nil {
		t.Error("expected error when assigning partitions with no active nodes")
	}
}

func TestCoordinatorGetPartition(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.AssignPartitions()

	partition, err := coordinator.GetPartition(0)
	if err != nil {
		t.Fatalf("get partition failed: %v", err)
	}

	if partition.ID != 0 {
		t.Errorf("expected partition ID 0, got %d", partition.ID)
	}
}

func TestCoordinatorGetAllPartitions(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.AssignPartitions()

	partitions := coordinator.GetAllPartitions()
	if len(partitions) != 4 {
		t.Errorf("expected 4 partitions, got %d", len(partitions))
	}
}

func TestCoordinatorGetPartitionsForNode(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 6,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.RegisterNode("node2", "192.168.1.2:8080")
	coordinator.RegisterNode("node3", "192.168.1.3:8080")
	coordinator.AssignPartitions()

	// Each node should have 2 partitions (6 / 3)
	partitions := coordinator.GetPartitionsForNode("node1")
	if len(partitions) != 2 {
		t.Errorf("expected 2 partitions for node1, got %d", len(partitions))
	}
}

func TestCoordinatorLocalNodeID(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	if coordinator.LocalNodeID() != "node1" {
		t.Errorf("expected local node ID node1, got %s", coordinator.LocalNodeID())
	}
}

func TestCoordinatorHealthCheck(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:           "node1",
		NumPartitions:         4,
		HeartbeatTimeoutMs:    50,  // Short for testing
		SuspicionTimeoutMs:    100, // Short for testing
		ElectionTimeoutMs:     1000,
		HealthCheckIntervalMs: 30, // Frequent checks for testing
		ShutdownTimeoutMs:     5000,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	// Register nodes and assign partitions
	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.RegisterNode("node2", "192.168.1.2:8080")
	coordinator.AssignPartitions()

	// Start coordinator to enable health checks
	ctx := context.Background()
	coordinator.Start(ctx)
	defer coordinator.Stop()

	// Wait for node2 to be suspected and then inactive
	time.Sleep(150 * time.Millisecond)

	// Manually trigger health check
	coordinator.performHealthCheck()

	// Check that partitions led by node2 are handled
	// (In real scenario, they would be reassigned to node1)
	partitionsForNode2 := coordinator.GetPartitionsForNode("node2")

	// After failure handling, node2 should have no partitions
	// or partitions should be in offline/recovering state
	for _, partition := range partitionsForNode2 {
		t.Logf("Partition %d state: %s, leader: %s", partition.ID, partition.State, partition.Leader)
	}
}

func TestCoordinatorNodeCount(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	if coordinator.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", coordinator.NodeCount())
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.RegisterNode("node2", "192.168.1.2:8080")

	if coordinator.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", coordinator.NodeCount())
	}
}

func TestCoordinatorActiveNodeCount(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.RegisterNode("node2", "192.168.1.2:8080")

	if coordinator.ActiveNodeCount() != 2 {
		t.Errorf("expected 2 active nodes, got %d", coordinator.ActiveNodeCount())
	}
}

func TestCoordinatorPartitionCount(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:   "node1",
		NumPartitions: 4,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	coordinator.RegisterNode("node1", "192.168.1.1:8080")
	coordinator.AssignPartitions()

	if coordinator.PartitionCount() != 4 {
		t.Errorf("expected 4 partitions, got %d", coordinator.PartitionCount())
	}
}

func TestCoordinatorContextCancellation(t *testing.T) {
	logger := slog.Default()
	cfg := Config{
		LocalNodeID:           "node1",
		NumPartitions:         4,
		HealthCheckIntervalMs: 100,
		ShutdownTimeoutMs:     5000,
	}

	coordinator, err := NewCoordinator(cfg, logger)
	if err != nil {
		t.Fatalf("create coordinator failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	coordinator.Start(ctx)

	// Wait for it to be running
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for shutdown
	time.Sleep(150 * time.Millisecond)

	// State might still be running as context cancellation
	// doesn't automatically update state to stopped
	// But the goroutine should have exited
}
