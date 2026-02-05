package cluster

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// NodeState represents the state of a node in the cluster.
type NodeState int

const (
	// NodeStateActive indicates the node is active and responding.
	NodeStateActive NodeState = iota
	// NodeStateSuspected indicates the node is suspected of being down.
	NodeStateSuspected
	// NodeStateInactive indicates the node is confirmed down.
	NodeStateInactive
)

// String returns the string representation of NodeState.
func (s NodeState) String() string {
	switch s {
	case NodeStateActive:
		return "active"
	case NodeStateSuspected:
		return "suspected"
	case NodeStateInactive:
		return "inactive"
	default:
		return "unknown"
	}
}

// Node represents a single node in the cluster.
type Node struct {
	ID            string
	Address       string
	State         NodeState
	LastHeartbeat time.Time
}

// Registry manages cluster membership and tracks node states.
type Registry struct {
	logger             *slog.Logger
	nodes              map[string]*Node // nodeID -> Node
	mu                 sync.RWMutex
	heartbeatTimeoutMs int
	suspicionTimeoutMs int
	localNodeID        string
}

// RegistryConfig holds configuration for the node registry.
type RegistryConfig struct {
	LocalNodeID        string
	HeartbeatTimeoutMs int
	SuspicionTimeoutMs int
}

// NewRegistry creates a new node registry.
func NewRegistry(cfg RegistryConfig, logger *slog.Logger) *Registry {
	if cfg.HeartbeatTimeoutMs <= 0 {
		cfg.HeartbeatTimeoutMs = 5000
	}
	if cfg.SuspicionTimeoutMs <= 0 {
		cfg.SuspicionTimeoutMs = 10000
	}

	return &Registry{
		logger:             logger,
		nodes:              make(map[string]*Node),
		localNodeID:        cfg.LocalNodeID,
		heartbeatTimeoutMs: cfg.HeartbeatTimeoutMs,
		suspicionTimeoutMs: cfg.SuspicionTimeoutMs,
	}
}

// RegisterNode registers or updates a node in the registry.
func (r *Registry) RegisterNode(nodeID, address string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}
	if address == "" {
		return fmt.Errorf("address cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]
	if exists {
		// Update existing node
		node.Address = address
		node.State = NodeStateActive
		node.LastHeartbeat = time.Now()
		r.logger.Debug("updated node", "node_id", nodeID, "address", address)
	} else {
		// Create new node
		node = &Node{
			ID:            nodeID,
			Address:       address,
			State:         NodeStateActive,
			LastHeartbeat: time.Now(),
		}
		r.nodes[nodeID] = node
		r.logger.Info("registered node", "node_id", nodeID, "address", address)
	}

	return nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a node.
func (r *Registry) UpdateHeartbeat(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.LastHeartbeat = time.Now()
	if node.State != NodeStateActive {
		node.State = NodeStateActive
		r.logger.Info("node recovered", "node_id", nodeID)
	}

	return nil
}

// GetNode returns a node by ID.
func (r *Registry) GetNode(nodeID string) (*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, exists := r.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Return a copy to prevent external mutation
	nodeCopy := *node
	return &nodeCopy, nil
}

// GetAllNodes returns all registered nodes.
func (r *Registry) GetAllNodes() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}

	return nodes
}

// GetActiveNodes returns all nodes in active state.
func (r *Registry) GetActiveNodes() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*Node, 0)
	for _, node := range r.nodes {
		if node.State == NodeStateActive {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}

	// Sort by node ID for deterministic ordering
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return nodes
}

// RemoveNode removes a node from the registry.
func (r *Registry) RemoveNode(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(r.nodes, nodeID)
	r.logger.Info("removed node", "node_id", nodeID)
	return nil
}

// CheckNodeHealth checks node health and updates states based on heartbeat timeouts.
// Returns nodes that transitioned to inactive state.
func (r *Registry) CheckNodeHealth() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	heartbeatTimeout := time.Duration(r.heartbeatTimeoutMs) * time.Millisecond
	suspicionTimeout := time.Duration(r.suspicionTimeoutMs) * time.Millisecond

	inactiveNodes := make([]string, 0)

	for nodeID, node := range r.nodes {
		// Skip the local node
		if nodeID == r.localNodeID {
			continue
		}

		timeSinceHeartbeat := now.Sub(node.LastHeartbeat)

		switch node.State {
		case NodeStateActive:
			// Check if node should be suspected
			if timeSinceHeartbeat > heartbeatTimeout {
				node.State = NodeStateSuspected
				r.logger.Warn("node suspected",
					"node_id", nodeID,
					"time_since_heartbeat", timeSinceHeartbeat)
			}
		case NodeStateSuspected:
			// Check if suspected node should be marked inactive
			if timeSinceHeartbeat > suspicionTimeout {
				node.State = NodeStateInactive
				inactiveNodes = append(inactiveNodes, nodeID)
				r.logger.Error("node marked inactive",
					"node_id", nodeID,
					"time_since_heartbeat", timeSinceHeartbeat)
			}
		}
	}

	return inactiveNodes
}

// IsLocalNode checks if the given node ID is the local node.
func (r *Registry) IsLocalNode(nodeID string) bool {
	return nodeID == r.localNodeID
}

// LocalNodeID returns the local node ID.
func (r *Registry) LocalNodeID() string {
	return r.localNodeID
}

// NodeCount returns the total number of registered nodes.
func (r *Registry) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// ActiveNodeCount returns the number of active nodes.
func (r *Registry) ActiveNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, node := range r.nodes {
		if node.State == NodeStateActive {
			count++
		}
	}
	return count
}
