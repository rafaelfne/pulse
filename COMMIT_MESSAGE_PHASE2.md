# Implement Phase 2: Consumer Groups, Offset Tracking, and Backpressure

## Summary

This commit implements Phase 2 of the Pulse project, adding consumer groups with persistent offset tracking, ACK-based commit semantics, backpressure controls, and heartbeat-based failure detection.

## What Changed

### New Packages

1. **internal/offset** - Persistent offset store
   - Per (groupId, partition) offset tracking to disk
   - Atomic file writes with sync for durability
   - Background flush at configurable intervals
   - Monotonically increasing offsets enforced
   - Survives restarts with automatic recovery

2. **internal/group** - Consumer group manager
   - Static round-robin partition assignment
   - Consumer registration and heartbeat tracking
   - Dead consumer detection (configurable timeout)
   - Inflight message tracking per consumer
   - Automatic partition reassignment on consumer failure
   - Committed offsets preserved (no rewind)

### Updated Components

1. **internal/config** - Added Phase 2 configuration
   - PULSE_CONSUMER_TIMEOUT_MS (default: 30000)
   - PULSE_MAX_INFLIGHT_PER_CONSUMER (default: 100)
   - PULSE_OFFSET_FLUSH_INTERVAL_MS (default: 1000)

2. **internal/server** - Added Phase 2 API endpoints
   - GET /groups/{groupId}/stream - Stream with consumer group
   - POST /groups/{groupId}/ack - Commit offset
   - POST /groups/{groupId}/heartbeat - Send heartbeat
   - Added Phase 2 metrics (group_stream_*, ack_*, heartbeat_*)
   - Enhanced inflight validation to prevent partial tracking

3. **internal/app** - Wired Phase 2 components
   - Initialize offset store and group manager
   - Proper lifecycle management with ordered shutdown

### Documentation

- Updated docs/api.md with Phase 2 endpoints and semantics
- Updated README.md with Phase 2 features and usage
- Added PHASE2_SUMMARY.md with implementation details
- Documented guarantees and operational best practices

## Guarantees

**At-Least-Once Delivery:**
- Consumers must explicitly ACK to commit progress
- Uncommitted messages redelivered on restart

**Partition Assignment:**
- Round-robin assignment across consumers
- Each partition assigned to at most one consumer per group
- Automatic reassignment on consumer failure

**Offset Persistence:**
- Offsets persisted to disk asynchronously
- Monotonically increasing per (group, partition)
- Survives process restarts

**Backpressure:**
- Configurable max inflight per consumer
- Stream blocked when limit reached
- ACKs release inflight slots

**Failure Handling:**
- Heartbeat timeout detection
- Partition reassignment preserves committed offsets
- No rewind on consumer failure

## Testing

- 6 unit tests for offset store (persistence, monotonicity, restart)
- 8 unit tests for group manager (assignment, heartbeats, failure detection)
- Integration test validates end-to-end Phase 2 flow
- All existing tests pass
- CodeQL security scan: 0 alerts

## Design Trade-offs

- Static partition assignment (simpler, deterministic)
- Asynchronous offset flush (better throughput, small loss window)
- Single-node coordination (acceptable for current scope)
- At-least-once semantics only (exactly-once not required)

## Performance

- Offset store uses buffered writes with periodic flush
- Group manager uses RWMutex for concurrent reads
- Inflight count validated before tracking (no partial state)
- Minimal allocations in hot paths

## How to Test

```bash
# Run tests
make test

# Build
make build

# Start with Phase 2 config
PULSE_CONSUMER_TIMEOUT_MS=60000 \
PULSE_MAX_INFLIGHT_PER_CONSUMER=200 \
./pulse

# Test consumer group streaming
curl "http://localhost:8080/groups/my-group/stream?consumerId=consumer-1&partition=0&limit=10"

# Commit offset
curl -X POST http://localhost:8080/groups/my-group/ack \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "consumer-1", "partition": 0, "offset": 42}'

# Send heartbeat
curl -X POST http://localhost:8080/groups/my-group/heartbeat \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "consumer-1"}'

# Check metrics
curl http://localhost:8080/metrics
```

## Code Review Feedback Addressed

- ✅ Extracted test helper to reduce duplication
- ✅ Replaced magic number with named constant
- ✅ Enhanced inflight tracking to validate count before tracking
- ✅ Fixed error logging to report correct error in heartbeat handler

## Security Summary

- CodeQL scan found 0 security alerts
- No sensitive data logged
- Offsets validated for monotonicity
- All inputs validated before processing
- Proper error handling throughout

## Follow-ups

None required. Phase 2 is complete and production-ready.

Future enhancements (not in scope):
- Dynamic partition count
- Exactly-once semantics
- Consumer priority/weights
- Lag-based assignment
- Distributed coordination
