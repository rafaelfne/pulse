# Phase 2 Implementation Summary

## Overview

Phase 2 adds consumer groups with persistent offset tracking, ACK-based commit semantics, backpressure controls, and heartbeat-based failure detection to Pulse.

## Components Implemented

### 1. Offset Store (`internal/offset`)

**Purpose:** Persistent offset tracking per (groupId, partition)

**Key Features:**
- Atomic file writes with sync for durability
- Background flush at configurable intervals (default: 1000ms)
- Monotonically increasing offsets enforced
- Survives restarts with automatic recovery
- One file per (groupId, partition) pair

**API:**
- `Commit(groupID, partition, offset)` - Commit offset with monotonicity check
- `Get(groupID, partition)` - Retrieve committed offset (returns 0 if not committed)
- `GetAll(groupID)` - Get all offsets for a group

### 2. Consumer Group Manager (`internal/group`)

**Purpose:** Manage consumer groups, partition assignments, and heartbeats

**Key Features:**
- Static round-robin partition assignment
- Consumer registration and heartbeat tracking
- Dead consumer detection (configurable timeout, default: 30s)
- Inflight message tracking per consumer
- Automatic partition reassignment on consumer failure
- Committed offsets preserved on reassignment

**API:**
- `RegisterConsumer(groupID, consumerID)` - Register consumer, get partition assignment
- `Heartbeat(groupID, consumerID)` - Update heartbeat timestamp
- `ValidateConsumerPartition(groupID, consumerID, partition)` - Validate partition ownership
- `TrackInflight/ReleaseInflight` - Manage inflight message counts
- `GetInflight(groupID, consumerID, partition)` - Get current inflight count

### 3. HTTP API Endpoints (`internal/server`)

**New Endpoints:**

1. **GET /groups/{groupId}/stream**
   - Query params: `consumerId`, `partition`, `limit`
   - Automatically registers consumer on first call
   - Validates partition assignment
   - Checks inflight limit before streaming
   - Starts streaming from committed offset
   - Tracks inflight messages

2. **POST /groups/{groupId}/ack**
   - Body: `{consumerId, partition, offset}`
   - Validates partition ownership
   - Commits offset (with monotonicity check)
   - Releases one inflight message

3. **POST /groups/{groupId}/heartbeat**
   - Body: `{consumerId}`
   - Updates heartbeat timestamp
   - Auto-registers consumer if not found

**New Metrics:**
- `group_stream_requests`, `group_stream_events`, `group_stream_errors`
- `ack_requests`, `ack_errors`, `ack_avg_latency_ms`
- `heartbeat_requests`, `heartbeat_errors`

### 4. Configuration (`internal/config`)

**New Settings:**
- `PULSE_CONSUMER_TIMEOUT_MS` - Heartbeat timeout (default: 30000)
- `PULSE_MAX_INFLIGHT_PER_CONSUMER` - Max inflight messages (default: 100)
- `PULSE_OFFSET_FLUSH_INTERVAL_MS` - Offset flush interval (default: 1000)

### 5. Application Wiring (`internal/app`)

- Initialize offset store and group manager on startup
- Wire into server
- Proper shutdown ordering: server → ingest → group manager → offset store → storage

## Guarantees

### At-Least-Once Delivery
- Events delivered at least once
- Explicit ACK required to commit
- Uncommitted messages redelivered on restart

### Partition Assignment
- Round-robin assignment across consumers
- Each partition assigned to ≤ 1 consumer per group
- Multiple groups can consume independently

### Offset Persistence
- Asynchronous flush to disk
- Monotonically increasing per (group, partition)
- Survives restarts

### Failure Handling
- Heartbeat-based failure detection
- Dead consumer → partition reassignment
- Committed offsets preserved (no rewind)

### Backpressure
- Max inflight enforced per consumer
- Stream blocked at limit
- ACKs release inflight slots

## Testing

### Unit Tests
- `internal/offset` - 6 tests covering persistence, monotonicity, restart recovery
- `internal/group` - 8 tests covering registration, heartbeats, dead consumer detection, inflight tracking
- `internal/server` - Existing tests updated with Phase 2 dependencies

### Integration Test
- End-to-end test validates: ingest → group stream → ACK → heartbeat → metrics

### Security
- CodeQL scan: 0 alerts
- No security vulnerabilities found

## Design Decisions

### Static vs Dynamic Assignment
- **Choice:** Static round-robin
- **Rationale:** Simpler, deterministic, no coordination overhead
- **Trade-off:** No automatic rebalancing within same consumer set

### Offset Storage Format
- **Choice:** One file per (groupId, partition)
- **Rationale:** Simple, atomic writes, fast reads
- **Trade-off:** More files at scale (acceptable for single-node)

### Asynchronous Offset Flush
- **Choice:** Background flush with configurable interval
- **Rationale:** Better throughput, amortizes fsync cost
- **Trade-off:** Small window for offset loss on crash (acceptable for at-least-once)

### Inflight Tracking
- **Choice:** Track at group manager, validate before streaming
- **Rationale:** Centralized enforcement, prevents overload
- **Trade-off:** Additional coordination overhead (acceptable with RWMutex)

## Performance Considerations

- **Offset Store:** Buffered writes, periodic flush (1s default)
- **Group Manager:** RWMutex for concurrent reads
- **Inflight Validation:** Pre-check count to avoid partial tracking
- **Minimal Allocations:** Reuse maps, avoid per-event allocations

## Operational Notes

### Monitoring
Monitor these metrics:
- `group_stream_errors` - Should be 0
- `ack_errors` - Should be 0
- `heartbeat_errors` - Should be 0
- `ack_avg_latency_ms` - Should be <5ms

### Consumer Best Practices
- Send heartbeats every 10s (1/3 of timeout)
- ACK messages promptly after processing
- Handle 429 (Too Many Requests) by backing off
- Track your partition assignment
- Handle reassignment gracefully (403 Forbidden)

### Capacity Planning
- Max inflight × num consumers = max memory overhead
- Offset files: `num_groups × num_partitions` files
- Heartbeat checking: 1/3 timeout interval overhead

## Future Improvements (Not in Phase 2)

- Dynamic partition count
- Exactly-once semantics
- Consumer priority/weights
- Lag-based assignment
- Compaction of offset files
- Distributed coordination (multi-node)

## Verification Checklist

- ✅ All tests pass (`go test ./...`)
- ✅ Build succeeds (`go build ./...`)
- ✅ Integration test passes
- ✅ CodeQL security scan clean
- ✅ Documentation updated (README, API docs)
- ✅ Code review feedback addressed
- ✅ Consistent error handling
- ✅ Structured logging throughout
- ✅ Proper shutdown lifecycle
- ✅ No global mutable state

## Files Changed

**New Files:**
- `internal/offset/store.go` (318 lines)
- `internal/offset/store_test.go` (233 lines)
- `internal/group/manager.go` (304 lines)
- `internal/group/manager_test.go` (278 lines)

**Modified Files:**
- `internal/config/config.go` (+6 lines)
- `internal/server/server.go` (+273 lines)
- `internal/server/server_test.go` (+53 lines, refactored)
- `internal/app/app.go` (+50 lines)
- `docs/api.md` (+180 lines)
- `README.md` (+40 lines)

**Total:** ~1,700 lines of production code + tests + documentation
