package cluster

import (
	"log/slog"
	"testing"
	"time"
)

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateActive, "active"},
		{NodeStateSuspected, "suspected"},
		{NodeStateInactive, "inactive"},
		{NodeState(999), "unknown"},
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

func TestNewRegistry(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}

	registry := NewRegistry(cfg, logger)

	if registry.localNodeID != "node1" {
		t.Errorf("expected local node ID node1, got %s", registry.localNodeID)
	}
	if registry.heartbeatTimeoutMs != 1000 {
		t.Errorf("expected heartbeat timeout 1000, got %d", registry.heartbeatTimeoutMs)
	}
	if registry.suspicionTimeoutMs != 2000 {
		t.Errorf("expected suspicion timeout 2000, got %d", registry.suspicionTimeoutMs)
	}
}

func TestNewRegistryDefaults(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID: "node1",
	}

	registry := NewRegistry(cfg, logger)

	if registry.heartbeatTimeoutMs <= 0 {
		t.Error("expected default heartbeat timeout to be set")
	}
	if registry.suspicionTimeoutMs <= 0 {
		t.Error("expected default suspicion timeout to be set")
	}
}

func TestRegisterNode(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	tests := []struct {
		name      string
		nodeID    string
		address   string
		wantError bool
	}{
		{"valid node", "node2", "192.168.1.2:8080", false},
		{"empty node ID", "", "192.168.1.2:8080", true},
		{"empty address", "node3", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.RegisterNode(tt.nodeID, tt.address)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error=%v, got %v", tt.wantError, err)
			}
		})
	}
}

func TestRegisterNodeUpdate(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	// Register node
	err := registry.RegisterNode("node2", "192.168.1.2:8080")
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	// Update node with new address
	err = registry.RegisterNode("node2", "192.168.1.3:8080")
	if err != nil {
		t.Fatalf("update node failed: %v", err)
	}

	node, err := registry.GetNode("node2")
	if err != nil {
		t.Fatalf("get node failed: %v", err)
	}

	if node.Address != "192.168.1.3:8080" {
		t.Errorf("expected address 192.168.1.3:8080, got %s", node.Address)
	}
}

func TestUpdateHeartbeat(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	// Register node
	err := registry.RegisterNode("node2", "192.168.1.2:8080")
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	// Get initial heartbeat
	node1, _ := registry.GetNode("node2")
	initialHeartbeat := node1.LastHeartbeat

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Update heartbeat
	err = registry.UpdateHeartbeat("node2")
	if err != nil {
		t.Fatalf("update heartbeat failed: %v", err)
	}

	// Check heartbeat was updated
	node2, _ := registry.GetNode("node2")
	if !node2.LastHeartbeat.After(initialHeartbeat) {
		t.Error("heartbeat was not updated")
	}
}

func TestUpdateHeartbeatNonExistent(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	err := registry.UpdateHeartbeat("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent node")
	}
}

func TestGetNode(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	err := registry.RegisterNode("node2", "192.168.1.2:8080")
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	node, err := registry.GetNode("node2")
	if err != nil {
		t.Fatalf("get node failed: %v", err)
	}

	if node.ID != "node2" {
		t.Errorf("expected node ID node2, got %s", node.ID)
	}
	if node.Address != "192.168.1.2:8080" {
		t.Errorf("expected address 192.168.1.2:8080, got %s", node.Address)
	}
	if node.State != NodeStateActive {
		t.Errorf("expected state active, got %s", node.State)
	}
}

func TestGetNodeNonExistent(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	_, err := registry.GetNode("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent node")
	}
}

func TestGetAllNodes(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	registry.RegisterNode("node2", "192.168.1.2:8080")
	registry.RegisterNode("node3", "192.168.1.3:8080")

	nodes := registry.GetAllNodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestGetActiveNodes(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	registry.RegisterNode("node2", "192.168.1.2:8080")
	registry.RegisterNode("node3", "192.168.1.3:8080")

	// Mark one node as suspected
	registry.mu.Lock()
	registry.nodes["node3"].State = NodeStateSuspected
	registry.mu.Unlock()

	activeNodes := registry.GetActiveNodes()
	if len(activeNodes) != 1 {
		t.Errorf("expected 1 active node, got %d", len(activeNodes))
	}
	if activeNodes[0].ID != "node2" {
		t.Errorf("expected active node to be node2, got %s", activeNodes[0].ID)
	}
}

func TestGetActiveNodesSorted(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	// Register nodes in non-sorted order
	registry.RegisterNode("node5", "192.168.1.5:8080")
	registry.RegisterNode("node2", "192.168.1.2:8080")
	registry.RegisterNode("node3", "192.168.1.3:8080")

	activeNodes := registry.GetActiveNodes()
	if len(activeNodes) != 3 {
		t.Errorf("expected 3 active nodes, got %d", len(activeNodes))
	}

	// Verify sorted order
	expected := []string{"node2", "node3", "node5"}
	for i, node := range activeNodes {
		if node.ID != expected[i] {
			t.Errorf("expected node %s at position %d, got %s", expected[i], i, node.ID)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	registry.RegisterNode("node2", "192.168.1.2:8080")

	err := registry.RemoveNode("node2")
	if err != nil {
		t.Fatalf("remove node failed: %v", err)
	}

	_, err = registry.GetNode("node2")
	if err == nil {
		t.Error("expected error after removing node")
	}
}

func TestRemoveNodeNonExistent(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	err := registry.RemoveNode("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent node")
	}
}

func TestCheckNodeHealth(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 50,  // 50ms for fast testing
		SuspicionTimeoutMs: 100, // 100ms for fast testing
	}
	registry := NewRegistry(cfg, logger)

	// Register a node
	registry.RegisterNode("node2", "192.168.1.2:8080")

	// Immediately check - should be active
	inactive := registry.CheckNodeHealth()
	if len(inactive) != 0 {
		t.Error("expected no inactive nodes immediately")
	}

	node, _ := registry.GetNode("node2")
	if node.State != NodeStateActive {
		t.Errorf("expected active state, got %s", node.State)
	}

	// Wait for heartbeat timeout
	time.Sleep(60 * time.Millisecond)
	inactive = registry.CheckNodeHealth()
	if len(inactive) != 0 {
		t.Error("expected no inactive nodes yet, should be suspected")
	}

	node, _ = registry.GetNode("node2")
	if node.State != NodeStateSuspected {
		t.Errorf("expected suspected state, got %s", node.State)
	}

	// Wait for suspicion timeout
	time.Sleep(60 * time.Millisecond)
	inactive = registry.CheckNodeHealth()
	if len(inactive) != 1 {
		t.Errorf("expected 1 inactive node, got %d", len(inactive))
	}
	if inactive[0] != "node2" {
		t.Errorf("expected node2 to be inactive, got %s", inactive[0])
	}

	node, _ = registry.GetNode("node2")
	if node.State != NodeStateInactive {
		t.Errorf("expected inactive state, got %s", node.State)
	}
}

func TestCheckNodeHealthSkipsLocalNode(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 50,
		SuspicionTimeoutMs: 100,
	}
	registry := NewRegistry(cfg, logger)

	// Register local node
	registry.RegisterNode("node1", "192.168.1.1:8080")

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)
	inactive := registry.CheckNodeHealth()

	// Local node should not be marked as suspected or inactive
	node, _ := registry.GetNode("node1")
	if node.State != NodeStateActive {
		t.Errorf("local node should remain active, got %s", node.State)
	}
	if len(inactive) != 0 {
		t.Error("local node should not be in inactive list")
	}
}

func TestUpdateHeartbeatRecovery(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 50,
		SuspicionTimeoutMs: 100,
	}
	registry := NewRegistry(cfg, logger)

	registry.RegisterNode("node2", "192.168.1.2:8080")

	// Wait for node to be suspected
	time.Sleep(60 * time.Millisecond)
	registry.CheckNodeHealth()

	node, _ := registry.GetNode("node2")
	if node.State != NodeStateSuspected {
		t.Errorf("expected suspected state, got %s", node.State)
	}

	// Send heartbeat to recover
	registry.UpdateHeartbeat("node2")

	node, _ = registry.GetNode("node2")
	if node.State != NodeStateActive {
		t.Errorf("expected node to recover to active, got %s", node.State)
	}
}

func TestIsLocalNode(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	if !registry.IsLocalNode("node1") {
		t.Error("expected node1 to be local node")
	}
	if registry.IsLocalNode("node2") {
		t.Error("expected node2 to not be local node")
	}
}

func TestLocalNodeID(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	if registry.LocalNodeID() != "node1" {
		t.Errorf("expected local node ID node1, got %s", registry.LocalNodeID())
	}
}

func TestNodeCount(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	if registry.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", registry.NodeCount())
	}

	registry.RegisterNode("node2", "192.168.1.2:8080")
	registry.RegisterNode("node3", "192.168.1.3:8080")

	if registry.NodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", registry.NodeCount())
	}
}

func TestActiveNodeCount(t *testing.T) {
	logger := slog.Default()
	cfg := RegistryConfig{
		LocalNodeID:        "node1",
		HeartbeatTimeoutMs: 1000,
		SuspicionTimeoutMs: 2000,
	}
	registry := NewRegistry(cfg, logger)

	registry.RegisterNode("node2", "192.168.1.2:8080")
	registry.RegisterNode("node3", "192.168.1.3:8080")

	if registry.ActiveNodeCount() != 2 {
		t.Errorf("expected 2 active nodes, got %d", registry.ActiveNodeCount())
	}

	// Mark one as suspected
	registry.mu.Lock()
	registry.nodes["node3"].State = NodeStateSuspected
	registry.mu.Unlock()

	if registry.ActiveNodeCount() != 1 {
		t.Errorf("expected 1 active node, got %d", registry.ActiveNodeCount())
	}
}
