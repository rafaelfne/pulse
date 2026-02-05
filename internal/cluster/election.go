package cluster

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// ElectionState represents the state of a leader election.
type ElectionState int

const (
	// ElectionStateIdle indicates no election in progress.
	ElectionStateIdle ElectionState = iota
	// ElectionStateInProgress indicates election is in progress.
	ElectionStateInProgress
	// ElectionStateComplete indicates election completed successfully.
	ElectionStateComplete
)

// String returns the string representation of ElectionState.
func (s ElectionState) String() string {
	switch s {
	case ElectionStateIdle:
		return "idle"
	case ElectionStateInProgress:
		return "in_progress"
	case ElectionStateComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// Election manages leader election for a single partition.
type Election struct {
	partitionID     int
	candidates      []string
	currentLeader   string
	state           ElectionState
	electionStart   time.Time
	electionTimeout time.Duration
	mu              sync.RWMutex
}

// ElectionManager manages leader elections across partitions.
type ElectionManager struct {
	logger          *slog.Logger
	elections       map[int]*Election // partitionID -> Election
	electionTimeout time.Duration
	mu              sync.RWMutex
}

// ElectionConfig holds configuration for the election manager.
type ElectionConfig struct {
	ElectionTimeoutMs int
}

// NewElectionManager creates a new election manager.
func NewElectionManager(cfg ElectionConfig, logger *slog.Logger) *ElectionManager {
	if cfg.ElectionTimeoutMs <= 0 {
		cfg.ElectionTimeoutMs = 5000
	}

	return &ElectionManager{
		logger:          logger,
		elections:       make(map[int]*Election),
		electionTimeout: time.Duration(cfg.ElectionTimeoutMs) * time.Millisecond,
	}
}

// StartElection initiates a leader election for a partition.
// Uses priority-based election: lowest node ID wins (deterministic).
func (m *ElectionManager) StartElection(partitionID int, candidates []string) (string, error) {
	if partitionID < 0 {
		return "", fmt.Errorf("partition ID must be non-negative, got: %d", partitionID)
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no candidates available for partition %d", partitionID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	election, exists := m.elections[partitionID]
	if !exists {
		election = &Election{
			partitionID:     partitionID,
			electionTimeout: m.electionTimeout,
		}
		m.elections[partitionID] = election
	}

	election.mu.Lock()
	defer election.mu.Unlock()

	// If election is already in progress, check if timed out
	if election.state == ElectionStateInProgress {
		if time.Since(election.electionStart) < election.electionTimeout {
			return "", fmt.Errorf("election already in progress for partition %d", partitionID)
		}
		m.logger.Warn("previous election timed out, starting new election",
			"partition", partitionID)
	}

	// Start new election
	election.state = ElectionStateInProgress
	election.electionStart = time.Now()
	election.candidates = make([]string, len(candidates))
	copy(election.candidates, candidates)

	m.logger.Info("started election",
		"partition", partitionID,
		"candidates", len(candidates))

	// Priority-based election: sort candidates and pick first (lowest node ID)
	sortedCandidates := make([]string, len(candidates))
	copy(sortedCandidates, candidates)
	sort.Strings(sortedCandidates)

	newLeader := sortedCandidates[0]
	election.currentLeader = newLeader
	election.state = ElectionStateComplete

	m.logger.Info("election completed",
		"partition", partitionID,
		"leader", newLeader,
		"duration_ms", time.Since(election.electionStart).Milliseconds())

	return newLeader, nil
}

// GetLeader returns the current leader for a partition.
func (m *ElectionManager) GetLeader(partitionID int) (string, error) {
	m.mu.RLock()
	election, exists := m.elections[partitionID]
	m.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("no election found for partition %d", partitionID)
	}

	election.mu.RLock()
	defer election.mu.RUnlock()

	if election.state != ElectionStateComplete {
		return "", fmt.Errorf("election not complete for partition %d", partitionID)
	}

	return election.currentLeader, nil
}

// GetElectionState returns the election state for a partition.
func (m *ElectionManager) GetElectionState(partitionID int) (ElectionState, error) {
	m.mu.RLock()
	election, exists := m.elections[partitionID]
	m.mu.RUnlock()

	if !exists {
		return ElectionStateIdle, fmt.Errorf("no election found for partition %d", partitionID)
	}

	election.mu.RLock()
	defer election.mu.RUnlock()

	return election.state, nil
}

// PromoteNewLeader forces a leader change for a partition.
// Used when current leader fails. Excludes the failed node from candidates.
func (m *ElectionManager) PromoteNewLeader(partitionID int, failedNode string, availableNodes []string) (string, error) {
	if partitionID < 0 {
		return "", fmt.Errorf("partition ID must be non-negative, got: %d", partitionID)
	}

	// Filter out failed node from candidates
	candidates := make([]string, 0, len(availableNodes))
	for _, node := range availableNodes {
		if node != failedNode {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no available candidates for partition %d after excluding failed node", partitionID)
	}

	m.logger.Info("promoting new leader",
		"partition", partitionID,
		"failed_node", failedNode,
		"candidates", len(candidates))

	return m.StartElection(partitionID, candidates)
}

// ResetElection resets the election state for a partition.
func (m *ElectionManager) ResetElection(partitionID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	election, exists := m.elections[partitionID]
	if !exists {
		return fmt.Errorf("no election found for partition %d", partitionID)
	}

	election.mu.Lock()
	defer election.mu.Unlock()

	election.state = ElectionStateIdle
	election.candidates = nil
	election.currentLeader = ""

	m.logger.Info("reset election", "partition", partitionID)
	return nil
}

// CheckElectionTimeout checks if any elections have timed out.
// Returns partition IDs of timed out elections.
func (m *ElectionManager) CheckElectionTimeout() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	timedOut := make([]int, 0)
	now := time.Now()

	for partitionID, election := range m.elections {
		election.mu.RLock()
		if election.state == ElectionStateInProgress {
			if now.Sub(election.electionStart) > election.electionTimeout {
				timedOut = append(timedOut, partitionID)
			}
		}
		election.mu.RUnlock()
	}

	return timedOut
}

// ElectionCount returns the total number of elections tracked.
func (m *ElectionManager) ElectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.elections)
}

// RemoveElection removes election tracking for a partition.
func (m *ElectionManager) RemoveElection(partitionID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.elections[partitionID]; !exists {
		return fmt.Errorf("no election found for partition %d", partitionID)
	}

	delete(m.elections, partitionID)
	m.logger.Debug("removed election", "partition", partitionID)
	return nil
}

// IsLeaderElected checks if a leader has been elected for a partition.
func (m *ElectionManager) IsLeaderElected(partitionID int) bool {
	m.mu.RLock()
	election, exists := m.elections[partitionID]
	m.mu.RUnlock()

	if !exists {
		return false
	}

	election.mu.RLock()
	defer election.mu.RUnlock()

	return election.state == ElectionStateComplete && election.currentLeader != ""
}
