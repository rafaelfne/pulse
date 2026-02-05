package cluster

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

// PartitionState represents the state of a partition.
type PartitionState int

const (
	// PartitionStateActive indicates the partition is active with a healthy leader.
	PartitionStateActive PartitionState = iota
	// PartitionStateRecovering indicates the partition is recovering (leader election in progress).
	PartitionStateRecovering
	// PartitionStateOffline indicates the partition is offline (no leader available).
	PartitionStateOffline
)

// String returns the string representation of PartitionState.
func (s PartitionState) String() string {
	switch s {
	case PartitionStateActive:
		return "active"
	case PartitionStateRecovering:
		return "recovering"
	case PartitionStateOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// PartitionMeta holds metadata about a partition in the cluster.
type PartitionMeta struct {
	ID       int
	Leader   string   // Node ID of the leader
	Replicas []string // Node IDs of replicas (including leader)
	State    PartitionState
}

// PartitionRegistry manages partition metadata and ownership.
type PartitionRegistry struct {
	logger     *slog.Logger
	partitions map[int]*PartitionMeta // partitionID -> PartitionMeta
	mu         sync.RWMutex
}

// NewPartitionRegistry creates a new partition registry.
func NewPartitionRegistry(logger *slog.Logger) *PartitionRegistry {
	return &PartitionRegistry{
		logger:     logger,
		partitions: make(map[int]*PartitionMeta),
	}
}

// RegisterPartition registers or updates partition metadata.
func (r *PartitionRegistry) RegisterPartition(partitionID int, leader string, replicas []string) error {
	if partitionID < 0 {
		return fmt.Errorf("partition ID must be non-negative, got: %d", partitionID)
	}
	if leader == "" {
		return fmt.Errorf("leader cannot be empty for partition %d", partitionID)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Make a copy of replicas to prevent external mutation
	replicasCopy := make([]string, len(replicas))
	copy(replicasCopy, replicas)

	partition, exists := r.partitions[partitionID]
	if exists {
		// Update existing partition
		partition.Leader = leader
		partition.Replicas = replicasCopy
		partition.State = PartitionStateActive
		r.logger.Debug("updated partition", "partition", partitionID, "leader", leader, "replicas", len(replicas))
	} else {
		// Create new partition
		partition = &PartitionMeta{
			ID:       partitionID,
			Leader:   leader,
			Replicas: replicasCopy,
			State:    PartitionStateActive,
		}
		r.partitions[partitionID] = partition
		r.logger.Info("registered partition", "partition", partitionID, "leader", leader, "replicas", len(replicas))
	}

	return nil
}

// UpdatePartitionLeader updates the leader for a partition.
func (r *PartitionRegistry) UpdatePartitionLeader(partitionID int, leader string) error {
	if leader == "" {
		return fmt.Errorf("leader cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	partition, exists := r.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	oldLeader := partition.Leader
	partition.Leader = leader
	partition.State = PartitionStateActive

	r.logger.Info("updated partition leader",
		"partition", partitionID,
		"old_leader", oldLeader,
		"new_leader", leader)

	return nil
}

// UpdatePartitionState updates the state of a partition.
func (r *PartitionRegistry) UpdatePartitionState(partitionID int, state PartitionState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	partition, exists := r.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	oldState := partition.State
	partition.State = state

	r.logger.Info("updated partition state",
		"partition", partitionID,
		"old_state", oldState.String(),
		"new_state", state.String())

	return nil
}

// GetPartition returns partition metadata by ID.
func (r *PartitionRegistry) GetPartition(partitionID int) (*PartitionMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partition, exists := r.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// Return a deep copy to prevent external mutation
	return r.copyPartition(partition), nil
}

// GetPartitionLeader returns the leader node ID for a partition.
func (r *PartitionRegistry) GetPartitionLeader(partitionID int) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partition, exists := r.partitions[partitionID]
	if !exists {
		return "", fmt.Errorf("partition %d not found", partitionID)
	}

	return partition.Leader, nil
}

// GetAllPartitions returns all partition metadata.
func (r *PartitionRegistry) GetAllPartitions() []*PartitionMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partitions := make([]*PartitionMeta, 0, len(r.partitions))
	for _, partition := range r.partitions {
		partitions = append(partitions, r.copyPartition(partition))
	}

	// Sort by partition ID for deterministic ordering
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	return partitions
}

// GetPartitionsForNode returns all partitions where the node is the leader.
func (r *PartitionRegistry) GetPartitionsForNode(nodeID string) []*PartitionMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partitions := make([]*PartitionMeta, 0)
	for _, partition := range r.partitions {
		if partition.Leader == nodeID {
			partitions = append(partitions, r.copyPartition(partition))
		}
	}

	// Sort by partition ID for deterministic ordering
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	return partitions
}

// GetPartitionsWithReplica returns all partitions where the node is a replica.
func (r *PartitionRegistry) GetPartitionsWithReplica(nodeID string) []*PartitionMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partitions := make([]*PartitionMeta, 0)
	for _, partition := range r.partitions {
		for _, replica := range partition.Replicas {
			if replica == nodeID {
				partitions = append(partitions, r.copyPartition(partition))
				break
			}
		}
	}

	// Sort by partition ID for deterministic ordering
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	return partitions
}

// RemovePartition removes a partition from the registry.
func (r *PartitionRegistry) RemovePartition(partitionID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.partitions[partitionID]; !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	delete(r.partitions, partitionID)
	r.logger.Info("removed partition", "partition", partitionID)
	return nil
}

// PartitionCount returns the total number of partitions.
func (r *PartitionRegistry) PartitionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.partitions)
}

// AssignPartitionsRoundRobin assigns partitions to nodes using round-robin strategy.
// This is a simple assignment strategy for the foundation layer.
func (r *PartitionRegistry) AssignPartitionsRoundRobin(numPartitions int, nodes []string) error {
	if numPartitions <= 0 {
		return fmt.Errorf("numPartitions must be positive, got: %d", numPartitions)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("nodes list cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Sort nodes for deterministic assignment
	sortedNodes := make([]string, len(nodes))
	copy(sortedNodes, nodes)
	sort.Strings(sortedNodes)

	r.logger.Info("assigning partitions",
		"num_partitions", numPartitions,
		"num_nodes", len(sortedNodes))

	// Assign each partition to a leader using round-robin
	for i := 0; i < numPartitions; i++ {
		leader := sortedNodes[i%len(sortedNodes)]

		// For now, leader is the only replica (replication comes later)
		replicas := []string{leader}

		partition := &PartitionMeta{
			ID:       i,
			Leader:   leader,
			Replicas: replicas,
			State:    PartitionStateActive,
		}
		r.partitions[i] = partition

		r.logger.Debug("assigned partition",
			"partition", i,
			"leader", leader)
	}

	return nil
}

// MarkPartitionsOfflineForNode marks all partitions led by a node as offline.
func (r *PartitionRegistry) MarkPartitionsOfflineForNode(nodeID string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	offlinePartitions := make([]int, 0)
	for _, partition := range r.partitions {
		if partition.Leader == nodeID {
			partition.State = PartitionStateOffline
			offlinePartitions = append(offlinePartitions, partition.ID)
			r.logger.Warn("marked partition offline",
				"partition", partition.ID,
				"leader", nodeID)
		}
	}

	return offlinePartitions
}

// copyPartition creates a deep copy of partition metadata.
func (r *PartitionRegistry) copyPartition(p *PartitionMeta) *PartitionMeta {
	replicas := make([]string, len(p.Replicas))
	copy(replicas, p.Replicas)

	return &PartitionMeta{
		ID:       p.ID,
		Leader:   p.Leader,
		Replicas: replicas,
		State:    p.State,
	}
}
