package cluster

import (
	"log/slog"
	"testing"
)

func TestPartitionStateString(t *testing.T) {
	tests := []struct {
		state    PartitionState
		expected string
	}{
		{PartitionStateActive, "active"},
		{PartitionStateRecovering, "recovering"},
		{PartitionStateOffline, "offline"},
		{PartitionState(999), "unknown"},
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

func TestNewPartitionRegistry(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	if registry.partitions == nil {
		t.Error("expected partitions map to be initialized")
	}
	if registry.PartitionCount() != 0 {
		t.Errorf("expected 0 partitions, got %d", registry.PartitionCount())
	}
}

func TestRegisterPartition(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	tests := []struct {
		name        string
		partitionID int
		leader      string
		replicas    []string
		wantError   bool
	}{
		{"valid partition", 0, "node1", []string{"node1", "node2"}, false},
		{"negative partition ID", -1, "node1", []string{"node1"}, true},
		{"empty leader", 1, "", []string{"node1"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.RegisterPartition(tt.partitionID, tt.leader, tt.replicas)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error=%v, got %v", tt.wantError, err)
			}
		})
	}
}

func TestRegisterPartitionUpdate(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	// Register partition
	err := registry.RegisterPartition(0, "node1", []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("register partition failed: %v", err)
	}

	// Update partition with new leader
	err = registry.RegisterPartition(0, "node2", []string{"node2", "node3"})
	if err != nil {
		t.Fatalf("update partition failed: %v", err)
	}

	partition, err := registry.GetPartition(0)
	if err != nil {
		t.Fatalf("get partition failed: %v", err)
	}

	if partition.Leader != "node2" {
		t.Errorf("expected leader node2, got %s", partition.Leader)
	}
	if len(partition.Replicas) != 2 {
		t.Errorf("expected 2 replicas, got %d", len(partition.Replicas))
	}
}

func TestUpdatePartitionLeader(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1", "node2"})

	err := registry.UpdatePartitionLeader(0, "node2")
	if err != nil {
		t.Fatalf("update partition leader failed: %v", err)
	}

	partition, _ := registry.GetPartition(0)
	if partition.Leader != "node2" {
		t.Errorf("expected leader node2, got %s", partition.Leader)
	}
	if partition.State != PartitionStateActive {
		t.Errorf("expected state active, got %s", partition.State)
	}
}

func TestUpdatePartitionLeaderNonExistent(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	err := registry.UpdatePartitionLeader(0, "node1")
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestUpdatePartitionLeaderEmptyLeader(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})

	err := registry.UpdatePartitionLeader(0, "")
	if err == nil {
		t.Error("expected error for empty leader")
	}
}

func TestUpdatePartitionState(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})

	err := registry.UpdatePartitionState(0, PartitionStateRecovering)
	if err != nil {
		t.Fatalf("update partition state failed: %v", err)
	}

	partition, _ := registry.GetPartition(0)
	if partition.State != PartitionStateRecovering {
		t.Errorf("expected state recovering, got %s", partition.State)
	}
}

func TestUpdatePartitionStateNonExistent(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	err := registry.UpdatePartitionState(0, PartitionStateOffline)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestGetPartition(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1", "node2"})

	partition, err := registry.GetPartition(0)
	if err != nil {
		t.Fatalf("get partition failed: %v", err)
	}

	if partition.ID != 0 {
		t.Errorf("expected partition ID 0, got %d", partition.ID)
	}
	if partition.Leader != "node1" {
		t.Errorf("expected leader node1, got %s", partition.Leader)
	}
	if len(partition.Replicas) != 2 {
		t.Errorf("expected 2 replicas, got %d", len(partition.Replicas))
	}
	if partition.State != PartitionStateActive {
		t.Errorf("expected state active, got %s", partition.State)
	}
}

func TestGetPartitionNonExistent(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	_, err := registry.GetPartition(0)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestGetPartitionLeader(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})

	leader, err := registry.GetPartitionLeader(0)
	if err != nil {
		t.Fatalf("get partition leader failed: %v", err)
	}

	if leader != "node1" {
		t.Errorf("expected leader node1, got %s", leader)
	}
}

func TestGetPartitionLeaderNonExistent(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	_, err := registry.GetPartitionLeader(0)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestGetAllPartitions(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})
	registry.RegisterPartition(1, "node2", []string{"node2"})
	registry.RegisterPartition(2, "node1", []string{"node1"})

	partitions := registry.GetAllPartitions()
	if len(partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(partitions))
	}

	// Verify sorted order
	for i, partition := range partitions {
		if partition.ID != i {
			t.Errorf("expected partition ID %d at position %d, got %d", i, i, partition.ID)
		}
	}
}

func TestGetPartitionsForNode(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})
	registry.RegisterPartition(1, "node2", []string{"node2"})
	registry.RegisterPartition(2, "node1", []string{"node1"})
	registry.RegisterPartition(3, "node3", []string{"node3"})

	partitions := registry.GetPartitionsForNode("node1")
	if len(partitions) != 2 {
		t.Errorf("expected 2 partitions for node1, got %d", len(partitions))
	}

	// Verify correct partitions
	expected := []int{0, 2}
	for i, partition := range partitions {
		if partition.ID != expected[i] {
			t.Errorf("expected partition ID %d, got %d", expected[i], partition.ID)
		}
	}
}

func TestGetPartitionsWithReplica(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1", "node2"})
	registry.RegisterPartition(1, "node2", []string{"node2", "node3"})
	registry.RegisterPartition(2, "node3", []string{"node3", "node1"})

	// node1 is in partitions 0 and 2
	partitions := registry.GetPartitionsWithReplica("node1")
	if len(partitions) != 2 {
		t.Errorf("expected 2 partitions with node1 replica, got %d", len(partitions))
	}

	expected := []int{0, 2}
	for i, partition := range partitions {
		if partition.ID != expected[i] {
			t.Errorf("expected partition ID %d, got %d", expected[i], partition.ID)
		}
	}
}

func TestRemovePartition(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})

	err := registry.RemovePartition(0)
	if err != nil {
		t.Fatalf("remove partition failed: %v", err)
	}

	_, err = registry.GetPartition(0)
	if err == nil {
		t.Error("expected error after removing partition")
	}
}

func TestRemovePartitionNonExistent(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	err := registry.RemovePartition(0)
	if err == nil {
		t.Error("expected error for non-existent partition")
	}
}

func TestPartitionCount(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	if registry.PartitionCount() != 0 {
		t.Errorf("expected 0 partitions, got %d", registry.PartitionCount())
	}

	registry.RegisterPartition(0, "node1", []string{"node1"})
	registry.RegisterPartition(1, "node2", []string{"node2"})

	if registry.PartitionCount() != 2 {
		t.Errorf("expected 2 partitions, got %d", registry.PartitionCount())
	}
}

func TestAssignPartitionsRoundRobin(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	nodes := []string{"node1", "node2", "node3"}
	err := registry.AssignPartitionsRoundRobin(6, nodes)
	if err != nil {
		t.Fatalf("assign partitions failed: %v", err)
	}

	if registry.PartitionCount() != 6 {
		t.Errorf("expected 6 partitions, got %d", registry.PartitionCount())
	}

	// Verify round-robin assignment
	expectedLeaders := []string{"node1", "node2", "node3", "node1", "node2", "node3"}
	for i := 0; i < 6; i++ {
		partition, _ := registry.GetPartition(i)
		if partition.Leader != expectedLeaders[i] {
			t.Errorf("partition %d: expected leader %s, got %s", i, expectedLeaders[i], partition.Leader)
		}
		if partition.State != PartitionStateActive {
			t.Errorf("partition %d: expected state active, got %s", i, partition.State)
		}
	}
}

func TestAssignPartitionsRoundRobinDeterministic(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	// Nodes in random order
	nodes := []string{"node3", "node1", "node2"}
	err := registry.AssignPartitionsRoundRobin(3, nodes)
	if err != nil {
		t.Fatalf("assign partitions failed: %v", err)
	}

	// Should be sorted deterministically: node1, node2, node3
	expectedLeaders := []string{"node1", "node2", "node3"}
	for i := 0; i < 3; i++ {
		partition, _ := registry.GetPartition(i)
		if partition.Leader != expectedLeaders[i] {
			t.Errorf("partition %d: expected leader %s, got %s", i, expectedLeaders[i], partition.Leader)
		}
	}
}

func TestAssignPartitionsRoundRobinErrors(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name          string
		numPartitions int
		nodes         []string
		wantError     bool
	}{
		{"valid", 3, []string{"node1", "node2"}, false},
		{"zero partitions", 0, []string{"node1"}, true},
		{"negative partitions", -1, []string{"node1"}, true},
		{"empty nodes", 3, []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewPartitionRegistry(logger)
			err := registry.AssignPartitionsRoundRobin(tt.numPartitions, tt.nodes)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error=%v, got %v", tt.wantError, err)
			}
		})
	}
}

func TestMarkPartitionsOfflineForNode(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1"})
	registry.RegisterPartition(1, "node2", []string{"node2"})
	registry.RegisterPartition(2, "node1", []string{"node1"})
	registry.RegisterPartition(3, "node3", []string{"node3"})

	offlinePartitions := registry.MarkPartitionsOfflineForNode("node1")

	if len(offlinePartitions) != 2 {
		t.Errorf("expected 2 offline partitions, got %d", len(offlinePartitions))
	}

	// Verify partitions 0 and 2 are offline
	for _, pid := range []int{0, 2} {
		partition, _ := registry.GetPartition(pid)
		if partition.State != PartitionStateOffline {
			t.Errorf("partition %d: expected state offline, got %s", pid, partition.State)
		}
	}

	// Verify partitions 1 and 3 are still active
	for _, pid := range []int{1, 3} {
		partition, _ := registry.GetPartition(pid)
		if partition.State != PartitionStateActive {
			t.Errorf("partition %d: expected state active, got %s", pid, partition.State)
		}
	}
}

func TestPartitionCopyIsolation(t *testing.T) {
	logger := slog.Default()
	registry := NewPartitionRegistry(logger)

	registry.RegisterPartition(0, "node1", []string{"node1", "node2"})

	// Get partition and modify the returned copy
	partition, _ := registry.GetPartition(0)
	partition.Leader = "modified"
	partition.Replicas[0] = "modified"

	// Verify original is not modified
	originalPartition, _ := registry.GetPartition(0)
	if originalPartition.Leader == "modified" {
		t.Error("leader should not be modified")
	}
	if originalPartition.Replicas[0] == "modified" {
		t.Error("replicas should not be modified")
	}
}
