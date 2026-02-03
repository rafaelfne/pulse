# API Documentation

## Overview

Pulse is a high-throughput event ingestion and distributed stream processing system.

**Phase 1** delivers single-node event ingestion and streaming with:
- Partitioned append-only storage (WAL + segment files)
- REST API for event ingestion and streaming
- Metrics endpoint for observability

**Phase 2** adds consumer groups, offset tracking, and backpressure:
- Consumer groups with static partition assignment
- Persistent offset tracking per group
- ACK-based commit protocol (at-least-once delivery)
- Inflight message backpressure
- Consumer heartbeats and failure detection

## Configuration

Configure Pulse using environment variables:

### Core Settings
- `PULSE_LOG_LEVEL` - Log level (debug|info|warn|error). Default: `info`
- `PULSE_ENV` - Environment name. Default: `local`
- `PULSE_SHUTDOWN_TIMEOUT_MS` - Graceful shutdown timeout in milliseconds. Default: `5000`

### Storage Settings
- `PULSE_DATA_DIR` - Directory for log storage. Default: `./data`
- `PULSE_NUM_PARTITIONS` - Number of partitions. Default: `4`
- `PULSE_FLUSH_INTERVAL_MS` - WAL flush interval. Default: `1000`
- `PULSE_SEGMENT_MAX_BYTES` - Max segment file size. Default: `104857600` (100MB)
- `PULSE_SEGMENT_MAX_AGE_MS` - Max segment age. Default: `3600000` (1 hour)

### Ingest Settings
- `PULSE_MAX_BATCH_SIZE` - Maximum events per batch. Default: `1000`
- `PULSE_MAX_QUEUE_SIZE` - Ingest queue size. Default: `10000`
- `PULSE_WORKER_COUNT` - Number of ingest workers. Default: `4`

### Consumer Settings
- `PULSE_MAX_FETCH_SIZE` - Maximum entries per stream request. Default: `1000`

### Consumer Group Settings (Phase 2)
- `PULSE_CONSUMER_TIMEOUT_MS` - Consumer heartbeat timeout. Default: `30000` (30 seconds)
- `PULSE_MAX_INFLIGHT_PER_CONSUMER` - Max inflight messages per consumer. Default: `100`
- `PULSE_OFFSET_FLUSH_INTERVAL_MS` - Offset persistence interval. Default: `1000`

### Server Settings
- `PULSE_SERVER_HOST` - Server bind address. Default: `""` (all interfaces)
- `PULSE_SERVER_PORT` - Server port. Default: `8080`
- `PULSE_READ_TIMEOUT_MS` - HTTP read timeout. Default: `5000`
- `PULSE_WRITE_TIMEOUT_MS` - HTTP write timeout. Default: `10000`
- `PULSE_MAX_BODY_BYTES` - Maximum request body size. Default: `10485760` (10MB)

## API Endpoints

### POST /events - Ingest Events

Ingests a batch of events with automatic partitioning.

**Request:**
```json
{
  "events": [
    {
      "key": "user-123",
      "data": "eyJldmVudCI6InVzZXIuc2lnbnVwIn0=",
      "timestamp": 1234567890000000000
    }
  ]
}
```

**Fields:**
- `key` (string, optional): Partition key. Events with the same key go to the same partition. Empty keys route to partition 0.
- `data` (base64 string): Event payload (can be any bytes, typically JSON).
- `timestamp` (int64, optional): Event timestamp in nanoseconds. Auto-generated if omitted.

**Response:**
```json
{
  "results": [
    {
      "partition": 2,
      "offset": 42
    }
  ]
}
```

**Status Codes:**
- `200 OK` - Success
- `400 Bad Request` - Invalid JSON or batch too large
- `500 Internal Server Error` - Storage or processing error

**Example:**
```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"key": "user-1", "data": "eyJ0ZXN0IjoxfQ=="},
      {"key": "user-2", "data": "eyJ0ZXN0IjoyfQ=="}
    ]
  }'
```

### GET /stream - Stream Events

Reads events from a partition starting at a given offset.

**Query Parameters:**
- `partition` (required): Partition ID (0 to num_partitions-1)
- `offset` (optional): Starting offset. Default: `0`
- `limit` (optional): Maximum number of entries to return. Default: `100`

**Response:**
```json
{
  "entries": [
    {
      "Offset": 42,
      "Timestamp": 1234567890000000000,
      "Key": "user-123",
      "Data": "eyJldmVudCI6InVzZXIuc2lnbnVwIn0="
    }
  ]
}
```

**Status Codes:**
- `200 OK` - Success (may return empty array if no data)
- `400 Bad Request` - Missing or invalid parameters
- `500 Internal Server Error` - Storage error

**Example:**
```bash
# Read from partition 0, starting at offset 0, limit 10
curl "http://localhost:8080/stream?partition=0&offset=0&limit=10"

# Read next batch
curl "http://localhost:8080/stream?partition=0&offset=10&limit=10"
```

### GET /metrics - Metrics

Returns server metrics in JSON format.

**Response:**
```json
{
  "ingest_requests": 1234,
  "ingest_events": 5678,
  "ingest_errors": 2,
  "ingest_avg_latency_ms": 5.2,
  "stream_requests": 890,
  "stream_events": 4500,
  "stream_errors": 0,
  "group_stream_requests": 456,
  "group_stream_events": 2300,
  "group_stream_errors": 0,
  "ack_requests": 450,
  "ack_errors": 0,
  "ack_avg_latency_ms": 1.2,
  "heartbeat_requests": 1200,
  "heartbeat_errors": 0
}
```

**Phase 2 Metrics:**
- `group_stream_requests` - Number of consumer group stream requests
- `group_stream_events` - Number of events streamed via consumer groups
- `group_stream_errors` - Number of consumer group stream errors
- `ack_requests` - Number of ACK/commit requests
- `ack_errors` - Number of ACK errors
- `ack_avg_latency_ms` - Average ACK latency
- `heartbeat_requests` - Number of heartbeat requests
- `heartbeat_errors` - Number of heartbeat errors

**Example:**
```bash
curl http://localhost:8080/metrics
```

### GET /health - Health Check

Returns HTTP 200 with "OK" if the server is running.

**Example:**
```bash
curl http://localhost:8080/health
```

## Phase 2 - Consumer Group Endpoints

### GET /groups/{groupId}/stream - Stream with Consumer Group

Streams events for a consumer in a consumer group. Automatically registers the consumer and assigns partitions using round-robin assignment.

**Path Parameters:**
- `groupId` (required): Consumer group ID

**Query Parameters:**
- `consumerId` (required): Unique consumer ID
- `partition` (required): Partition ID to read from (must be assigned to this consumer)
- `limit` (optional): Maximum number of events to return. Default: `100`

**Response:**
```json
{
  "entries": [
    {
      "Offset": 42,
      "Timestamp": 1234567890000000000,
      "Key": "user-123",
      "Data": "eyJldmVudCI6InVzZXIuc2lnbnVwIn0="
    }
  ]
}
```

**Status Codes:**
- `200 OK` - Success
- `400 Bad Request` - Missing or invalid parameters
- `403 Forbidden` - Partition not assigned to consumer
- `429 Too Many Requests` - Max inflight messages reached
- `500 Internal Server Error` - Storage error

**Behavior:**
- First call automatically registers the consumer and assigns partitions
- Reads from the committed offset for the group+partition
- Tracks inflight messages to enforce backpressure
- Consumer must ACK messages to commit progress

**Example:**
```bash
# First consumer in group reads from partition 0
curl "http://localhost:8080/groups/my-group/stream?consumerId=consumer-1&partition=0&limit=10"

# Second consumer in group reads from partition 1 (auto-assigned)
curl "http://localhost:8080/groups/my-group/stream?consumerId=consumer-2&partition=1&limit=10"
```

### POST /groups/{groupId}/ack - Acknowledge/Commit Offset

Commits an offset for a consumer group partition, releasing inflight count.

**Path Parameters:**
- `groupId` (required): Consumer group ID

**Request Body:**
```json
{
  "consumerId": "consumer-1",
  "partition": 0,
  "offset": 42
}
```

**Fields:**
- `consumerId` (string, required): Consumer ID
- `partition` (int, required): Partition ID
- `offset` (int64, required): Offset to commit (must be monotonically increasing)

**Response:**
```json
{
  "status": "ok"
}
```

**Status Codes:**
- `200 OK` - Success
- `400 Bad Request` - Invalid JSON or missing fields
- `403 Forbidden` - Partition not assigned to consumer
- `500 Internal Server Error` - Commit failed

**Behavior:**
- Offsets must be monotonically increasing per partition
- Committed offset persists to disk asynchronously
- Releases one inflight message slot
- Offsets survive restarts

**Example:**
```bash
curl -X POST http://localhost:8080/groups/my-group/ack \
  -H "Content-Type: application/json" \
  -d '{
    "consumerId": "consumer-1",
    "partition": 0,
    "offset": 42
  }'
```

### POST /groups/{groupId}/heartbeat - Send Heartbeat

Sends a heartbeat to prevent consumer from being marked as dead.

**Path Parameters:**
- `groupId` (required): Consumer group ID

**Request Body:**
```json
{
  "consumerId": "consumer-1"
}
```

**Response:**
```json
{
  "status": "ok"
}
```

**Status Codes:**
- `200 OK` - Success
- `400 Bad Request` - Invalid JSON or missing consumerId
- `500 Internal Server Error` - Heartbeat failed

**Behavior:**
- Consumers must send heartbeats within `PULSE_CONSUMER_TIMEOUT_MS`
- If timeout expires, consumer is removed and partitions are reassigned
- Committed offsets are preserved (no rewind on consumer failure)
- Auto-registers consumer if not already registered

**Example:**
```bash
curl -X POST http://localhost:8080/groups/my-group/heartbeat \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "consumer-1"}'
```

## Storage Format

### Directory Structure
```
data/
├── partition-0/
│   ├── wal.log                    # Active write-ahead log
│   └── 00000000000000000000.seg   # Rotated segment file
├── partition-1/
│   ├── wal.log
│   └── 00000000000000000042.seg
├── offsets/                        # Phase 2: Offset tracking
│   ├── group1-0.offset            # Group "group1", partition 0
│   ├── group1-1.offset
│   └── group2-0.offset
└── ...
```

### Offset File Format (Phase 2)

Each offset file stores a single 8-byte big-endian integer representing the committed offset:
```
[offset:8]
```

Files are named: `{groupId}-{partition}.offset`

### WAL Format

Each entry in the WAL is prefixed with a 4-byte length:
```
[length:4][entry:length]
```

### Entry Format

Each entry is encoded as:
```
[offset:8][timestamp:8][keyLen:4][key:keyLen][dataLen:4][data:dataLen]
```

All integers are big-endian.

## Guarantees

### Phase 1 - Basic Guarantees

#### Ordering
- Events with the same key are ordered within a partition
- No ordering guarantees across partitions
- Offsets within a partition are monotonically increasing

#### Durability
- WAL is flushed periodically (configurable via `PULSE_FLUSH_INTERVAL_MS`)
- Data is persisted to disk before acknowledging ingest requests
- Segment rotation creates immutable log files

#### Availability
- Graceful shutdown flushes all buffers
- WAL and segments are recoverable after restart
- Partition offsets are deterministic and stable

### Phase 2 - Consumer Group Guarantees

#### At-Least-Once Delivery
- Events are delivered at least once to consumers
- Consumers must explicitly ACK to commit progress
- Uncommitted messages may be redelivered on consumer restart

#### Partition Assignment
- Each partition is assigned to at most one consumer per group
- Round-robin assignment distributes partitions evenly
- Assignment is recalculated when consumers join or leave

#### Offset Persistence
- Committed offsets are persisted to disk asynchronously
- Offsets survive process restarts
- Offsets are monotonically increasing per (group, partition)

#### Consumer Failure Handling
- Consumers must send heartbeats within timeout period
- Dead consumers are detected and their partitions reassigned
- Committed offsets are preserved (no rewind)
- New consumer resumes from committed offset

#### Backpressure
- Max inflight messages enforced per consumer
- Stream requests blocked when limit reached
- ACKs release inflight slots

## Performance Characteristics

### Throughput
- Designed for high ingestion rates (10k+ events/sec on commodity hardware)
- Batching amortizes append overhead
- Buffered writes reduce fsync frequency
- Worker pool provides backpressure

### Latency
- Typical ingest latency: 1-10ms (without fsync)
- Stream latency: sub-millisecond for sequential reads
- Metrics are tracked with average latency per request

### Memory
- Bounded memory growth via backpressure
- Configurable queue sizes prevent unbounded growth
- 64KB write buffer per partition WAL

## Operational Notes

### Starting the Server
```bash
# With defaults
./pulse

# With custom config
PULSE_NUM_PARTITIONS=8 PULSE_SERVER_PORT=9000 ./pulse
```

### Monitoring
Check `/metrics` endpoint regularly for:
- `ingest_errors` and `stream_errors` - Should be 0 or very low
- `ingest_avg_latency_ms` - Should be <10ms for healthy system
- Event throughput (derive from periodic metric samples)

### Backup and Recovery
- Stop the server gracefully (SIGTERM/SIGINT)
- Copy the entire `data/` directory
- Restart the server - it will recover from WAL and segments

### Partition Selection
- More partitions = more concurrency but more files
- Start with 4-8 partitions
- Partition count is fixed at startup (no dynamic repartitioning in Phase 1)

## Limitations

### Phase 1
- Single node only (no replication)
- No compaction or log cleanup (segments accumulate)
- No consumer offset tracking (clients must track offsets)
- No authentication or authorization
- No TLS/HTTPS
- Limited observability (basic metrics only, no detailed histograms)

### Phase 2
- Static partition count (no dynamic repartitioning)
- Static partition assignment (no rebalancing within same consumer set)
- No exactly-once semantics (at-least-once only)
- No consumer priority or weights
- No lag-based consumer assignment
- Single node only (no distributed coordination)
