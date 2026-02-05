package cluster

import (
	"log/slog"
	"testing"
	"time"
)

func TestElectionStateString(t *testing.T) {
	tests := []struct {
		state    ElectionState
		expected string
	}{
		{ElectionStateIdle, "idle"},
		{ElectionStateInProgress, "in_progress"},
		{ElectionStateComplete, "complete"},
		{ElectionState(999), "unknown"},
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

func TestNewElectionManager(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}

	manager := NewElectionManager(cfg, logger)

	if manager.electionTimeout != 1000*time.Millisecond {
		t.Errorf("expected timeout 1000ms, got %v", manager.electionTimeout)
	}
	if manager.elections == nil {
		t.Error("expected elections map to be initialized")
	}
}

func TestNewElectionManagerDefaults(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{}

	manager := NewElectionManager(cfg, logger)

	if manager.electionTimeout <= 0 {
		t.Error("expected default election timeout to be set")
	}
}

func TestStartElection(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	tests := []struct {
		name        string
		partitionID int
		candidates  []string
		wantError   bool
	}{
		{"valid election", 0, []string{"node1", "node2", "node3"}, false},
		{"negative partition ID", -1, []string{"node1"}, true},
		{"no candidates", 1, []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leader, err := manager.StartElection(tt.partitionID, tt.candidates)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error=%v, got %v", tt.wantError, err)
			}
			if !tt.wantError && leader == "" {
				t.Error("expected leader to be elected")
			}
		})
	}
}

func TestStartElectionDeterministic(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Test with different orderings - should always elect node1 (lowest ID)
	testCases := [][]string{
		{"node1", "node2", "node3"},
		{"node3", "node2", "node1"},
		{"node2", "node1", "node3"},
	}

	for i, candidates := range testCases {
		leader, err := manager.StartElection(i, candidates)
		if err != nil {
			t.Fatalf("election failed: %v", err)
		}
		if leader != "node1" {
			t.Errorf("expected node1 to be elected, got %s", leader)
		}
	}
}

func TestStartElectionSingleCandidate(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	leader, err := manager.StartElection(0, []string{"node1"})
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}
	if leader != "node1" {
		t.Errorf("expected node1 to be elected, got %s", leader)
	}
}

func TestStartElectionInProgress(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Start first election
	_, err := manager.StartElection(0, []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("first election failed: %v", err)
	}

	// Manually set state to in progress (simulate long election)
	manager.mu.Lock()
	election := manager.elections[0]
	election.mu.Lock()
	election.state = ElectionStateInProgress
	election.electionStart = time.Now()
	election.mu.Unlock()
	manager.mu.Unlock()

	// Try to start another election - should fail
	_, err = manager.StartElection(0, []string{"node3", "node4"})
	if err == nil {
		t.Error("expected error for concurrent election")
	}
}

func TestStartElectionAfterTimeout(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 50, // Short timeout for testing
	}
	manager := NewElectionManager(cfg, logger)

	// Start first election
	_, err := manager.StartElection(0, []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("first election failed: %v", err)
	}

	// Manually set state to in progress with old timestamp
	manager.mu.Lock()
	election := manager.elections[0]
	election.mu.Lock()
	election.state = ElectionStateInProgress
	election.electionStart = time.Now().Add(-100 * time.Millisecond)
	election.mu.Unlock()
	manager.mu.Unlock()

	// Should be able to start new election after timeout
	leader, err := manager.StartElection(0, []string{"node3", "node4"})
	if err != nil {
		t.Fatalf("second election failed: %v", err)
	}
	if leader != "node3" {
		t.Errorf("expected node3 to be elected, got %s", leader)
	}
}

func TestGetLeader(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Start election
	expectedLeader, err := manager.StartElection(0, []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	// Get leader
	leader, err := manager.GetLeader(0)
	if err != nil {
		t.Fatalf("get leader failed: %v", err)
	}
	if leader != expectedLeader {
		t.Errorf("expected leader %s, got %s", expectedLeader, leader)
	}
}

func TestGetLeaderNoElection(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	_, err := manager.GetLeader(0)
	if err == nil {
		t.Error("expected error for non-existent election")
	}
}

func TestGetElectionState(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Start election
	_, err := manager.StartElection(0, []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	// Get state
	state, err := manager.GetElectionState(0)
	if err != nil {
		t.Fatalf("get election state failed: %v", err)
	}
	if state != ElectionStateComplete {
		t.Errorf("expected state complete, got %s", state)
	}
}

func TestGetElectionStateNoElection(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	_, err := manager.GetElectionState(0)
	if err == nil {
		t.Error("expected error for non-existent election")
	}
}

func TestPromoteNewLeader(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Start initial election
	initialLeader, err := manager.StartElection(0, []string{"node1", "node2", "node3"})
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}
	if initialLeader != "node1" {
		t.Errorf("expected node1 as initial leader, got %s", initialLeader)
	}

	// Promote new leader after node1 fails
	newLeader, err := manager.PromoteNewLeader(0, "node1", []string{"node1", "node2", "node3"})
	if err != nil {
		t.Fatalf("promote new leader failed: %v", err)
	}

	// Should elect node2 (next lowest ID after excluding node1)
	if newLeader != "node2" {
		t.Errorf("expected node2 as new leader, got %s", newLeader)
	}
}

func TestPromoteNewLeaderNoCandidates(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	_, err := manager.PromoteNewLeader(0, "node1", []string{"node1"})
	if err == nil {
		t.Error("expected error when no candidates available")
	}
}

func TestPromoteNewLeaderNegativePartition(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	_, err := manager.PromoteNewLeader(-1, "node1", []string{"node2", "node3"})
	if err == nil {
		t.Error("expected error for negative partition ID")
	}
}

func TestResetElection(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// Start election
	_, err := manager.StartElection(0, []string{"node1", "node2"})
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	// Reset election
	err = manager.ResetElection(0)
	if err != nil {
		t.Fatalf("reset election failed: %v", err)
	}

	// Verify state is idle
	state, _ := manager.GetElectionState(0)
	if state != ElectionStateIdle {
		t.Errorf("expected state idle after reset, got %s", state)
	}

	// Leader should not be available
	_, err = manager.GetLeader(0)
	if err == nil {
		t.Error("expected error getting leader after reset")
	}
}

func TestResetElectionNonExistent(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	err := manager.ResetElection(0)
	if err == nil {
		t.Error("expected error for non-existent election")
	}
}

func TestCheckElectionTimeout(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 50, // Short timeout for testing
	}
	manager := NewElectionManager(cfg, logger)

	// Start election and complete it
	manager.StartElection(0, []string{"node1", "node2"})

	// Manually set another election to in progress with old timestamp
	manager.mu.Lock()
	manager.elections[1] = &Election{
		partitionID:     1,
		state:           ElectionStateInProgress,
		electionStart:   time.Now().Add(-100 * time.Millisecond),
		electionTimeout: 50 * time.Millisecond,
	}
	manager.mu.Unlock()

	timedOut := manager.CheckElectionTimeout()

	// Only partition 1 should be timed out
	if len(timedOut) != 1 {
		t.Errorf("expected 1 timed out election, got %d", len(timedOut))
	}
	if len(timedOut) > 0 && timedOut[0] != 1 {
		t.Errorf("expected partition 1 to be timed out, got %d", timedOut[0])
	}
}

func TestElectionCount(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	if manager.ElectionCount() != 0 {
		t.Errorf("expected 0 elections, got %d", manager.ElectionCount())
	}

	manager.StartElection(0, []string{"node1", "node2"})
	manager.StartElection(1, []string{"node1", "node2"})

	if manager.ElectionCount() != 2 {
		t.Errorf("expected 2 elections, got %d", manager.ElectionCount())
	}
}

func TestRemoveElection(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	manager.StartElection(0, []string{"node1", "node2"})

	err := manager.RemoveElection(0)
	if err != nil {
		t.Fatalf("remove election failed: %v", err)
	}

	if manager.ElectionCount() != 0 {
		t.Errorf("expected 0 elections after removal, got %d", manager.ElectionCount())
	}
}

func TestRemoveElectionNonExistent(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	err := manager.RemoveElection(0)
	if err == nil {
		t.Error("expected error for non-existent election")
	}
}

func TestIsLeaderElected(t *testing.T) {
	logger := slog.Default()
	cfg := ElectionConfig{
		ElectionTimeoutMs: 1000,
	}
	manager := NewElectionManager(cfg, logger)

	// No election yet
	if manager.IsLeaderElected(0) {
		t.Error("expected false before election")
	}

	// Start election
	manager.StartElection(0, []string{"node1", "node2"})

	// Should have leader now
	if !manager.IsLeaderElected(0) {
		t.Error("expected true after election")
	}

	// Reset election
	manager.ResetElection(0)

	// Should not have leader after reset
	if manager.IsLeaderElected(0) {
		t.Error("expected false after reset")
	}
}
