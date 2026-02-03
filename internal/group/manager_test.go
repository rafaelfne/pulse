package group

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestRegisterConsumer(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register first consumer
	partitions, err := mgr.RegisterConsumer("group1", "consumer1")
	if err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Should get all 4 partitions
	if len(partitions) != 4 {
		t.Errorf("consumer1 got %d partitions, want 4", len(partitions))
	}

	// Register second consumer
	_, err = mgr.RegisterConsumer("group1", "consumer2")
	if err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Partitions should be distributed
	partitions1, _ := mgr.GetAssignment("group1", "consumer1") //nolint:errcheck // Test setup
	partitions2, _ := mgr.GetAssignment("group1", "consumer2") //nolint:errcheck // Test setup

	total := len(partitions1) + len(partitions2)
	if total != 4 {
		t.Errorf("total partitions = %d, want 4", total)
	}
}

func TestHeartbeat(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register consumer
	_, err := mgr.RegisterConsumer("group1", "consumer1")
	if err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Send heartbeat
	if err := mgr.Heartbeat("group1", "consumer1"); err != nil {
		t.Errorf("Heartbeat failed: %v", err)
	}

	// Heartbeat for non-existent consumer
	if err := mgr.Heartbeat("group1", "consumer2"); err == nil {
		t.Error("Heartbeat for non-existent consumer should fail")
	}
}

func TestDeadConsumerDetection(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      200, // Short timeout for testing
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register consumer
	_, err := mgr.RegisterConsumer("group1", "consumer1")
	if err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Wait for consumer to be marked dead
	time.Sleep(300 * time.Millisecond)

	// Consumer should be removed
	consumers := mgr.GetGroupConsumers("group1")
	if len(consumers) != 0 {
		t.Errorf("dead consumer not removed, got %d consumers", len(consumers))
	}
}

func TestInflightTracking(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 2, // Small limit for testing
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register consumer
	_, err := mgr.RegisterConsumer("group1", "consumer1")
	if err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Track inflight
	if err := mgr.TrackInflight("group1", "consumer1", 0); err != nil {
		t.Errorf("TrackInflight failed: %v", err)
	}
	if err := mgr.TrackInflight("group1", "consumer1", 0); err != nil {
		t.Errorf("TrackInflight failed: %v", err)
	}

	// Should hit limit
	if err := mgr.TrackInflight("group1", "consumer1", 0); err == nil {
		t.Error("TrackInflight should fail when limit reached")
	}

	// Check inflight count
	inflight := mgr.GetInflight("group1", "consumer1", 0)
	if inflight != 2 {
		t.Errorf("inflight count = %d, want 2", inflight)
	}

	// Release one
	if err := mgr.ReleaseInflight("group1", "consumer1", 0); err != nil {
		t.Errorf("ReleaseInflight failed: %v", err)
	}

	// Check count again
	inflight = mgr.GetInflight("group1", "consumer1", 0)
	if inflight != 1 {
		t.Errorf("inflight count = %d, want 1", inflight)
	}

	// Should be able to track again
	if err := mgr.TrackInflight("group1", "consumer1", 0); err != nil {
		t.Errorf("TrackInflight after release failed: %v", err)
	}
}

func TestValidateConsumerPartition(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register two consumers
	partitions1, _ := mgr.RegisterConsumer("group1", "consumer1") //nolint:errcheck // Test setup
	_, _ = mgr.RegisterConsumer("group1", "consumer2")            //nolint:errcheck // Test setup

	// Validate correct assignment
	if len(partitions1) > 0 {
		partition := partitions1[0]
		if err := mgr.ValidateConsumerPartition("group1", "consumer1", partition); err != nil {
			t.Errorf("ValidateConsumerPartition failed: %v", err)
		}
	}

	// Validate wrong assignment
	if err := mgr.ValidateConsumerPartition("group1", "consumer1", 999); err == nil {
		t.Error("ValidateConsumerPartition should fail for unassigned partition")
	}
}

func TestGetGroupConsumers(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register consumers
	_, _ = mgr.RegisterConsumer("group1", "consumer1") //nolint:errcheck // Test setup
	_, _ = mgr.RegisterConsumer("group1", "consumer2") //nolint:errcheck // Test setup

	// Get consumers
	consumers := mgr.GetGroupConsumers("group1")
	if len(consumers) != 2 {
		t.Errorf("GetGroupConsumers returned %d consumers, want 2", len(consumers))
	}
}

func TestGetGroupAssignments(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          4,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register consumers
	_, _ = mgr.RegisterConsumer("group1", "consumer1") //nolint:errcheck // Test setup
	_, _ = mgr.RegisterConsumer("group1", "consumer2") //nolint:errcheck // Test setup

	// Get assignments
	assignments := mgr.GetGroupAssignments("group1")
	if len(assignments) != 4 {
		t.Errorf("GetGroupAssignments returned %d assignments, want 4", len(assignments))
	}
}

func TestRoundRobinAssignment(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := Config{
		NumPartitions:          6,
		ConsumerTimeoutMs:      5000,
		MaxInflightPerConsumer: 100,
	}

	mgr := NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	defer func() { _ = mgr.Close() }() //nolint:errcheck // Test cleanup

	// Register 3 consumers
	_, _ = mgr.RegisterConsumer("group1", "consumer1") //nolint:errcheck // Test setup
	_, _ = mgr.RegisterConsumer("group1", "consumer2") //nolint:errcheck // Test setup
	_, _ = mgr.RegisterConsumer("group1", "consumer3") //nolint:errcheck // Test setup

	// Each should get 2 partitions
	p1, _ := mgr.GetAssignment("group1", "consumer1") //nolint:errcheck // Test setup
	p2, _ := mgr.GetAssignment("group1", "consumer2") //nolint:errcheck // Test setup
	p3, _ := mgr.GetAssignment("group1", "consumer3") //nolint:errcheck // Test setup

	if len(p1) != 2 || len(p2) != 2 || len(p3) != 2 {
		t.Errorf("partition distribution not balanced: consumer1=%d, consumer2=%d, consumer3=%d",
			len(p1), len(p2), len(p3))
	}
}
