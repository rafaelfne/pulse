package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// CoordinatorState represents the state of the cluster coordinator.
type CoordinatorState int

const (
	// CoordinatorStateStopped indicates coordinator is stopped.
	CoordinatorStateStopped CoordinatorState = iota
	// CoordinatorStateStarting indicates coordinator is starting up.
	CoordinatorStateStarting
	// CoordinatorStateRunning indicates coordinator is running.
	CoordinatorStateRunning
	// CoordinatorStateStopping indicates coordinator is shutting down.
	CoordinatorStateStopping
)

// String returns the string representation of CoordinatorState.
func (s CoordinatorState) String() string {
	switch s {
	case CoordinatorStateStopped:
		return "stopped"
	case CoordinatorStateStarting:
		return "starting"
	case CoordinatorStateRunning:
		return "running"
	case CoordinatorStateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}

// Coordinator manages the cluster state, coordinating node registry,
// partition registry, and leader elections.
// Note: Coordinator cannot be restarted after Stop() is called due to
// closed channels. Create a new Coordinator instance for restart.
type Coordinator struct {
	logger              *slog.Logger
	nodeRegistry        *Registry
	partitionRegistry   *PartitionRegistry
	electionManager     *ElectionManager
	state               CoordinatorState
	healthCheckTicker   *time.Ticker
	stopCh              chan struct{}
	doneCh              chan struct{}
	numPartitions       int
	healthCheckInterval time.Duration
	shutdownTimeout     time.Duration
	mu                  sync.RWMutex
}

// Config holds configuration for the coordinator.
type Config struct {
	LocalNodeID           string
	NumPartitions         int
	HeartbeatTimeoutMs    int
	SuspicionTimeoutMs    int
	ElectionTimeoutMs     int
	HealthCheckIntervalMs int
	ShutdownTimeoutMs     int
}

// NewCoordinator creates a new cluster coordinator.
func NewCoordinator(cfg Config, logger *slog.Logger) (*Coordinator, error) {
	if cfg.LocalNodeID == "" {
		return nil, fmt.Errorf("local node ID is required")
	}
	if cfg.NumPartitions <= 0 {
		return nil, fmt.Errorf("num partitions must be positive, got: %d", cfg.NumPartitions)
	}
	if cfg.HealthCheckIntervalMs <= 0 {
		cfg.HealthCheckIntervalMs = 1000
	}
	if cfg.ShutdownTimeoutMs <= 0 {
		cfg.ShutdownTimeoutMs = 5000
	}

	nodeRegistryCfg := RegistryConfig{
		LocalNodeID:        cfg.LocalNodeID,
		HeartbeatTimeoutMs: cfg.HeartbeatTimeoutMs,
		SuspicionTimeoutMs: cfg.SuspicionTimeoutMs,
	}
	nodeRegistry := NewRegistry(nodeRegistryCfg, logger)

	partitionRegistry := NewPartitionRegistry(logger)

	electionCfg := ElectionConfig{
		ElectionTimeoutMs: cfg.ElectionTimeoutMs,
	}
	electionManager := NewElectionManager(electionCfg, logger)

	return &Coordinator{
		logger:              logger,
		nodeRegistry:        nodeRegistry,
		partitionRegistry:   partitionRegistry,
		electionManager:     electionManager,
		state:               CoordinatorStateStopped,
		numPartitions:       cfg.NumPartitions,
		healthCheckInterval: time.Duration(cfg.HealthCheckIntervalMs) * time.Millisecond,
		shutdownTimeout:     time.Duration(cfg.ShutdownTimeoutMs) * time.Millisecond,
		stopCh:              make(chan struct{}),
		doneCh:              make(chan struct{}),
	}, nil
}

// Start starts the coordinator background tasks.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.state != CoordinatorStateStopped {
		c.mu.Unlock()
		return fmt.Errorf("coordinator already started")
	}
	c.state = CoordinatorStateStarting
	c.mu.Unlock()

	c.logger.Info("starting cluster coordinator",
		"node_id", c.nodeRegistry.LocalNodeID(),
		"num_partitions", c.numPartitions)

	// Start health check loop
	c.healthCheckTicker = time.NewTicker(c.healthCheckInterval)

	go func() {
		defer close(c.doneCh)

		c.mu.Lock()
		c.state = CoordinatorStateRunning
		c.mu.Unlock()

		for {
			select {
			case <-c.healthCheckTicker.C:
				c.performHealthCheck()
			case <-c.stopCh:
				c.healthCheckTicker.Stop()
				return
			case <-ctx.Done():
				c.healthCheckTicker.Stop()
				return
			}
		}
	}()

	c.logger.Info("cluster coordinator started")
	return nil
}

// Stop stops the coordinator gracefully.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	if c.state != CoordinatorStateRunning {
		c.mu.Unlock()
		return fmt.Errorf("coordinator not running")
	}
	c.state = CoordinatorStateStopping
	c.mu.Unlock()

	c.logger.Info("stopping cluster coordinator")

	close(c.stopCh)

	// Wait for background tasks to complete with timeout
	select {
	case <-c.doneCh:
		c.logger.Info("cluster coordinator stopped successfully")
	case <-time.After(c.shutdownTimeout):
		c.logger.Warn("cluster coordinator shutdown timeout exceeded")
	}

	c.mu.Lock()
	c.state = CoordinatorStateStopped
	c.mu.Unlock()

	return nil
}

// RegisterNode registers a node in the cluster.
func (c *Coordinator) RegisterNode(nodeID, address string) error {
	return c.nodeRegistry.RegisterNode(nodeID, address)
}

// UpdateNodeHeartbeat updates the heartbeat timestamp for a node.
func (c *Coordinator) UpdateNodeHeartbeat(nodeID string) error {
	return c.nodeRegistry.UpdateHeartbeat(nodeID)
}

// GetNode returns node information by ID.
func (c *Coordinator) GetNode(nodeID string) (*Node, error) {
	return c.nodeRegistry.GetNode(nodeID)
}

// GetActiveNodes returns all active nodes.
func (c *Coordinator) GetActiveNodes() []*Node {
	return c.nodeRegistry.GetActiveNodes()
}

// AssignPartitions assigns partitions to active nodes using round-robin.
func (c *Coordinator) AssignPartitions() error {
	activeNodes := c.nodeRegistry.GetActiveNodes()
	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes available for partition assignment")
	}

	// Extract node IDs
	nodeIDs := make([]string, len(activeNodes))
	for i, node := range activeNodes {
		nodeIDs[i] = node.ID
	}

	// Assign partitions using round-robin strategy
	if err := c.partitionRegistry.AssignPartitionsRoundRobin(c.numPartitions, nodeIDs); err != nil {
		return fmt.Errorf("assign partitions: %w", err)
	}

	c.logger.Info("partitions assigned",
		"num_partitions", c.numPartitions,
		"num_nodes", len(nodeIDs))

	return nil
}

// GetPartition returns partition metadata by ID.
func (c *Coordinator) GetPartition(partitionID int) (*PartitionMeta, error) {
	return c.partitionRegistry.GetPartition(partitionID)
}

// GetPartitionLeader returns the leader node ID for a partition.
func (c *Coordinator) GetPartitionLeader(partitionID int) (string, error) {
	return c.partitionRegistry.GetPartitionLeader(partitionID)
}

// GetAllPartitions returns all partition metadata.
func (c *Coordinator) GetAllPartitions() []*PartitionMeta {
	return c.partitionRegistry.GetAllPartitions()
}

// GetPartitionsForNode returns partitions where the node is the leader.
func (c *Coordinator) GetPartitionsForNode(nodeID string) []*PartitionMeta {
	return c.partitionRegistry.GetPartitionsForNode(nodeID)
}

// GetCoordinatorState returns the current coordinator state.
func (c *Coordinator) GetCoordinatorState() CoordinatorState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// LocalNodeID returns the local node ID.
func (c *Coordinator) LocalNodeID() string {
	return c.nodeRegistry.LocalNodeID()
}

// performHealthCheck checks node health and handles failures.
func (c *Coordinator) performHealthCheck() {
	// Check for failed nodes
	inactiveNodes := c.nodeRegistry.CheckNodeHealth()

	if len(inactiveNodes) == 0 {
		return
	}

	c.logger.Warn("detected inactive nodes", "count", len(inactiveNodes))

	// Handle each failed node
	for _, nodeID := range inactiveNodes {
		c.handleNodeFailure(nodeID)
	}
}

// handleNodeFailure handles the failure of a node.
func (c *Coordinator) handleNodeFailure(nodeID string) {
	c.logger.Warn("handling node failure", "node_id", nodeID)

	// Mark partitions led by failed node as offline
	offlinePartitions := c.partitionRegistry.MarkPartitionsOfflineForNode(nodeID)

	if len(offlinePartitions) == 0 {
		return
	}

	c.logger.Warn("partitions offline due to node failure",
		"node_id", nodeID,
		"partitions", offlinePartitions)

	// Get active nodes for re-election
	activeNodes := c.nodeRegistry.GetActiveNodes()
	if len(activeNodes) == 0 {
		c.logger.Error("no active nodes available for re-election")
		return
	}

	nodeIDs := make([]string, len(activeNodes))
	for i, node := range activeNodes {
		nodeIDs[i] = node.ID
	}

	// Promote new leaders for offline partitions
	for _, partitionID := range offlinePartitions {
		c.logger.Info("promoting new leader",
			"partition", partitionID,
			"failed_node", nodeID)

		newLeader, err := c.electionManager.PromoteNewLeader(partitionID, nodeID, nodeIDs)
		if err != nil {
			c.logger.Error("failed to promote new leader",
				"partition", partitionID,
				"error", err)
			continue
		}

		// Update partition with new leader
		if err := c.partitionRegistry.UpdatePartitionLeader(partitionID, newLeader); err != nil {
			c.logger.Error("failed to update partition leader",
				"partition", partitionID,
				"error", err)
			continue
		}

		c.logger.Info("new leader promoted",
			"partition", partitionID,
			"new_leader", newLeader)
	}
}

// NodeCount returns the total number of registered nodes.
func (c *Coordinator) NodeCount() int {
	return c.nodeRegistry.NodeCount()
}

// ActiveNodeCount returns the number of active nodes.
func (c *Coordinator) ActiveNodeCount() int {
	return c.nodeRegistry.ActiveNodeCount()
}

// PartitionCount returns the total number of partitions.
func (c *Coordinator) PartitionCount() int {
	return c.partitionRegistry.PartitionCount()
}
