package group

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Manager manages consumer groups, assignments, and heartbeats.
type Manager struct {
	logger                 *slog.Logger
	heartbeatTicker        *time.Ticker
	groups                 map[string]*Group // groupId -> Group
	stopCh                 chan struct{}
	mu                     sync.RWMutex
	numPartitions          int
	consumerTimeoutMs      int
	maxInflightPerConsumer int
}

// Group represents a consumer group with its members and assignments.
type Group struct {
	consumers   map[string]*Consumer   // consumerId -> Consumer
	assignments map[int]string         // partition -> consumerId
	inflight    map[string]map[int]int // consumerId -> partition -> inflight count
	ID          string
}

// Consumer represents a single consumer in a group.
type Consumer struct {
	ID            string
	GroupID       string
	LastHeartbeat time.Time
	Partitions    []int
}

// Config holds consumer group manager configuration.
type Config struct {
	NumPartitions          int
	ConsumerTimeoutMs      int
	MaxInflightPerConsumer int
	ShutdownTimeoutMs      int
}

// NewManager creates a new consumer group manager.
func NewManager(cfg Config, logger *slog.Logger) *Manager {
	if cfg.ConsumerTimeoutMs <= 0 {
		cfg.ConsumerTimeoutMs = 30000
	}
	if cfg.MaxInflightPerConsumer <= 0 {
		cfg.MaxInflightPerConsumer = 100
	}

	return &Manager{
		logger:                 logger,
		numPartitions:          cfg.NumPartitions,
		consumerTimeoutMs:      cfg.ConsumerTimeoutMs,
		maxInflightPerConsumer: cfg.MaxInflightPerConsumer,
		groups:                 make(map[string]*Group),
		stopCh:                 make(chan struct{}),
	}
}

// Start starts the background heartbeat checker.
func (m *Manager) Start(ctx context.Context) {
	// Check for dead consumers every 1/3 of the timeout period
	checkInterval := time.Duration(m.consumerTimeoutMs/3) * time.Millisecond
	m.heartbeatTicker = time.NewTicker(checkInterval)

	go func() {
		for {
			select {
			case <-m.heartbeatTicker.C:
				m.checkDeadConsumers()
			case <-m.stopCh:
				m.heartbeatTicker.Stop()
				return
			case <-ctx.Done():
				m.heartbeatTicker.Stop()
				return
			}
		}
	}()
}

// Close stops the manager.
func (m *Manager) Close() error {
	close(m.stopCh)
	m.logger.Info("consumer group manager closed")
	return nil
}

// RegisterConsumer registers a consumer in a group and assigns partitions.
func (m *Manager) RegisterConsumer(groupID, consumerID string) ([]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	group := m.ensureGroup(groupID)

	// Check if consumer already exists
	if consumer, exists := group.consumers[consumerID]; exists {
		// Update heartbeat and return existing assignments
		consumer.LastHeartbeat = time.Now()
		return consumer.Partitions, nil
	}

	// Create new consumer
	consumer := &Consumer{
		ID:            consumerID,
		GroupID:       groupID,
		LastHeartbeat: time.Now(),
	}
	group.consumers[consumerID] = consumer

	// Rebalance partitions
	m.rebalanceGroup(group)

	m.logger.Info("consumer registered",
		"group", groupID,
		"consumer", consumerID,
		"partitions", consumer.Partitions)

	return consumer.Partitions, nil
}

// Heartbeat updates the heartbeat timestamp for a consumer.
func (m *Manager) Heartbeat(groupID, consumerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	consumer, exists := group.consumers[consumerID]
	if !exists {
		return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
	}

	consumer.LastHeartbeat = time.Now()
	return nil
}

// GetAssignment returns the partition assignment for a consumer.
func (m *Manager) GetAssignment(groupID, consumerID string) ([]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	consumer, exists := group.consumers[consumerID]
	if !exists {
		return nil, fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
	}

	return consumer.Partitions, nil
}

// ValidateConsumerPartition validates that a consumer owns a partition.
func (m *Manager) ValidateConsumerPartition(groupID, consumerID string, partition int) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	assignedConsumer, exists := group.assignments[partition]
	if !exists {
		return fmt.Errorf("partition %d not assigned in group %s", partition, groupID)
	}

	if assignedConsumer != consumerID {
		return fmt.Errorf("partition %d assigned to consumer %s, not %s", partition, assignedConsumer, consumerID)
	}

	return nil
}

// TrackInflight increments the inflight count for a consumer partition.
func (m *Manager) TrackInflight(groupID, consumerID string, partition int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	if group.inflight[consumerID] == nil {
		group.inflight[consumerID] = make(map[int]int)
	}

	currentInflight := group.inflight[consumerID][partition]
	if currentInflight >= m.maxInflightPerConsumer {
		return fmt.Errorf("max inflight (%d) reached for consumer %s partition %d", m.maxInflightPerConsumer, consumerID, partition)
	}

	group.inflight[consumerID][partition]++
	return nil
}

// ReleaseInflight decrements the inflight count for a consumer partition.
func (m *Manager) ReleaseInflight(groupID, consumerID string, partition int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, exists := m.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	if group.inflight[consumerID] == nil {
		return nil
	}

	if group.inflight[consumerID][partition] > 0 {
		group.inflight[consumerID][partition]--
	}

	return nil
}

// GetInflight returns the inflight count for a consumer partition.
func (m *Manager) GetInflight(groupID, consumerID string, partition int) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	if !exists {
		return 0
	}

	if group.inflight[consumerID] == nil {
		return 0
	}

	return group.inflight[consumerID][partition]
}

// GetGroupConsumers returns all consumers in a group.
func (m *Manager) GetGroupConsumers(groupID string) []Consumer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	if !exists {
		return nil
	}

	result := make([]Consumer, 0, len(group.consumers))
	for _, consumer := range group.consumers {
		result = append(result, *consumer)
	}
	return result
}

// GetGroupAssignments returns partition assignments for a group.
func (m *Manager) GetGroupAssignments(groupID string) map[int]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	if !exists {
		return nil
	}

	result := make(map[int]string)
	for partition, consumerID := range group.assignments {
		result[partition] = consumerID
	}
	return result
}

// MaxInflight returns the configured max inflight per consumer.
func (m *Manager) MaxInflight() int {
	return m.maxInflightPerConsumer
}

// ensureGroup creates a group if it doesn't exist. Caller must hold lock.
func (m *Manager) ensureGroup(groupID string) *Group {
	group, exists := m.groups[groupID]
	if !exists {
		group = &Group{
			ID:          groupID,
			consumers:   make(map[string]*Consumer),
			assignments: make(map[int]string),
			inflight:    make(map[string]map[int]int),
		}
		m.groups[groupID] = group
	}
	return group
}

// rebalanceGroup performs round-robin partition assignment. Caller must hold lock.
func (m *Manager) rebalanceGroup(group *Group) {
	// Clear existing assignments
	group.assignments = make(map[int]string)
	for _, consumer := range group.consumers {
		consumer.Partitions = nil
	}

	if len(group.consumers) == 0 {
		return
	}

	// Build consumer list for round-robin
	consumerList := make([]string, 0, len(group.consumers))
	for consumerID := range group.consumers {
		consumerList = append(consumerList, consumerID)
	}

	// Sort consumer list for deterministic and stable assignments
	sort.Strings(consumerList)

	// Assign partitions round-robin
	for partition := 0; partition < m.numPartitions; partition++ {
		consumerID := consumerList[partition%len(consumerList)]
		group.assignments[partition] = consumerID
		group.consumers[consumerID].Partitions = append(group.consumers[consumerID].Partitions, partition)
	}

	m.logger.Info("rebalanced group",
		"group", group.ID,
		"consumers", len(group.consumers),
		"partitions", m.numPartitions)
}

// checkDeadConsumers checks for consumers that haven't sent heartbeats and removes them.
func (m *Manager) checkDeadConsumers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	timeout := time.Duration(m.consumerTimeoutMs) * time.Millisecond

	for groupID, group := range m.groups {
		deadConsumers := make([]string, 0)

		for consumerID, consumer := range group.consumers {
			if now.Sub(consumer.LastHeartbeat) > timeout {
				deadConsumers = append(deadConsumers, consumerID)
			}
		}

		// Remove dead consumers and rebalance
		for _, consumerID := range deadConsumers {
			m.logger.Warn("removing dead consumer",
				"group", groupID,
				"consumer", consumerID,
				"last_heartbeat", group.consumers[consumerID].LastHeartbeat)

			delete(group.consumers, consumerID)
			delete(group.inflight, consumerID)
		}

		if len(deadConsumers) > 0 {
			m.rebalanceGroup(group)
		}
	}
}
