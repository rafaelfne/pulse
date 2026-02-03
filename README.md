# Pulse

**High-performance event ingestion and distributed stream processing in Go.**

Pulse is designed for scalable, low-latency event ingestion with consumer groups and offset tracking. Built with explicit wiring, minimal allocations, and a backpressure-first mindset.

## Current State

### Phase 1 - Single-Node Ingest + Streaming (✅ Complete)

Phase 1 delivers:
- **High-throughput event ingestion** via REST API (`POST /events`)
- **Partitioned append-only storage** with WAL and segment files
- **Deterministic streaming** via REST API (`GET /stream`)
- **Metrics endpoint** for observability (`GET /metrics`)
- **Restart-safe persistence** with automatic recovery

### Phase 2 - Consumer Groups + Offset Tracking (✅ Complete)

Phase 2 adds:
- **Consumer groups** with static round-robin partition assignment
- **Persistent offset tracking** per (groupId, partition)
- **ACK-based commit protocol** for at-least-once delivery
- **Backpressure** via configurable inflight message limits
- **Heartbeat-based failure detection** with automatic partition reassignment
- **Consumer group streaming API** (`GET /groups/{groupId}/stream`)
- **Offset commit API** (`POST /groups/{groupId}/ack`)
- **Heartbeat API** (`POST /groups/{groupId}/heartbeat`)

## Quick Start

```bash
# Build
make build

# Run with defaults (4 partitions, port 8080)
make run

# Or with custom config
PULSE_NUM_PARTITIONS=8 PULSE_SERVER_PORT=9000 make run
```

### Ingest Events

```bash
# Ingest a batch of events
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"key": "user-1", "data": "eyJ0ZXN0IjoxfQ=="},
      {"key": "user-2", "data": "eyJ0ZXN0IjoyfQ=="}
    ]
  }'
```

### Stream Events

```bash
# Phase 1: Direct partition streaming
curl "http://localhost:8080/stream?partition=0&offset=0&limit=100"

# Phase 2: Consumer group streaming
curl "http://localhost:8080/groups/my-group/stream?consumerId=consumer-1&partition=0&limit=100"
```

### Commit Offsets (Phase 2)

```bash
curl -X POST http://localhost:8080/groups/my-group/ack \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "consumer-1", "partition": 0, "offset": 42}'
```

### Send Heartbeat (Phase 2)

```bash
curl -X POST http://localhost:8080/groups/my-group/heartbeat \
  -H "Content-Type: application/json" \
  -d '{"consumerId": "consumer-1"}'
```

### Check Metrics

```bash
curl http://localhost:8080/metrics
```

### API Documentation

Interactive API documentation is available via Scalar UI when running in local/dev mode:

```bash
# Start Pulse (docs enabled by default in local/dev)
make run

# Open in browser
open http://localhost:8080/docs
```

The OpenAPI spec is also available at:
```bash
curl http://localhost:8080/openapi.yaml
```

To disable docs in production:
```bash
PULSE_ENABLE_DOCS=false make run
```

### Benchmark

```bash
# Run benchmark (requires jq and bc)
./scripts/benchmark.sh

# Custom benchmark
NUM_BATCHES=1000 BATCH_SIZE=100 ./scripts/benchmark.sh
```

## Architecture

See [docs/api.md](docs/api.md) for complete API documentation and [docs/architecture.md](docs/architecture.md) for design principles.

### Storage Model

- **Partitions**: Fixed number of independent log partitions (default: 4)
- **WAL**: Per-partition write-ahead log with buffered writes
- **Segments**: Rotated immutable segment files (100MB default)
- **Offsets**: Monotonically increasing per partition

### Performance

- Designed for **10k+ events/sec** on commodity hardware
- **1-10ms ingest latency** (without fsync per event)
- **Sub-millisecond streaming** for sequential reads
- **Bounded memory** via backpressure and configurable queues

## Current State

Phase 1 and Phase 2 are complete and include:

**Phase 1:**
- ✅ Event model and API contracts
- ✅ Hash-based partitioning
- ✅ Per-partition WAL with segment rotation
- ✅ Offset-based log reader
- ✅ HTTP ingest endpoint (`POST /events`)
- ✅ HTTP streaming endpoint (`GET /stream`)
- ✅ Metrics endpoint (`GET /metrics`)
- ✅ Graceful shutdown with buffer flushing
- ✅ Restart recovery from persisted logs
- ✅ Comprehensive test coverage
- ✅ Production-grade error handling
- ✅ Benchmark harness

**Phase 2:**
- ✅ Consumer group manager with static partition assignment
- ✅ Persistent offset store per (groupId, partition)
- ✅ Consumer group streaming endpoint (`GET /groups/{groupId}/stream`)
- ✅ ACK/commit endpoint (`POST /groups/{groupId}/ack`)
- ✅ Heartbeat endpoint (`POST /groups/{groupId}/heartbeat`)
- ✅ Inflight message tracking and backpressure
- ✅ Dead consumer detection with partition reassignment
- ✅ At-least-once delivery semantics
- ✅ Consumer group metrics
- ✅ Comprehensive test coverage

## Getting Started

### Prerequisites

- Go 1.24 or later
- golangci-lint (optional, for linting)

### Running

```bash
# Run the application
make run

# Build the application
make build

# Run tests
make test

# Run end-to-end integration test (fast mode, ~10k events)
make e2e

# Run end-to-end integration test (stress mode, ~200k events)
make e2e-stress

# Format code
make fmt

# Run linters
make lint
```

### End-to-End Integration Tests

Pulse includes a high-volume end-to-end integration test that exercises the full system under realistic load conditions. The test produces and consumes events, validates correctness, and generates a colored terminal summary.

**Quick Start:**

```bash
# Run fast CI mode (10,000 events, ~1 second)
make e2e

# Run stress mode (200,000 events, colored output)
make e2e-stress
```

**Configuration:**

The test can be customized via environment variables:

- `PULSE_E2E_EVENTS` - Number of events to generate. Default: `10000` (CI), `200000` (stress)
- `PULSE_E2E_PAYLOAD_BYTES` - Event payload size in bytes. Default: `128`
- `PULSE_E2E_BATCH_SIZE` - Batch size for ingestion. Default: `500`
- `PULSE_E2E_MODE` - Test mode: `ci` (fast) or `stress` (high volume). Default: `ci`
- `NO_COLOR=1` - Disable colored output

**Example:**

```bash
# Custom test with 50k events and 256-byte payloads
PULSE_E2E_EVENTS=50000 PULSE_E2E_PAYLOAD_BYTES=256 make e2e

# Stress test without colors (for CI environments)
NO_COLOR=1 make e2e-stress
```

**What it validates:**

- ✅ Event count matches (produced == consumed)
- ✅ Per-partition offset continuity (no gaps)
- ✅ Offset ordering within partitions
- ✅ Data integrity (payload size verification)
- ✅ In-process startup and shutdown
- ✅ High-throughput ingestion and consumption rates

## Configuration

Configure Pulse using environment variables. See [docs/api.md](docs/api.md) for complete configuration reference.

### Key Settings

**Storage:**
- `PULSE_DATA_DIR` - Data directory. Default: `./data`
- `PULSE_NUM_PARTITIONS` - Number of partitions. Default: `4`
- `PULSE_FLUSH_INTERVAL_MS` - WAL flush interval. Default: `1000` (1 second)

**Server:**
- `PULSE_SERVER_PORT` - HTTP port. Default: `8080`
- `PULSE_MAX_BATCH_SIZE` - Max events per batch. Default: `1000`
- `PULSE_ENABLE_DOCS` - Enable API docs. Default: `true` in local/dev, `false` otherwise

**Consumer Groups (Phase 2):**
- `PULSE_CONSUMER_TIMEOUT_MS` - Consumer heartbeat timeout. Default: `30000` (30 seconds)
- `PULSE_MAX_INFLIGHT_PER_CONSUMER` - Max inflight messages per consumer. Default: `100`
- `PULSE_OFFSET_FLUSH_INTERVAL_MS` - Offset persistence interval. Default: `1000`

**Logging:**
- `PULSE_LOG_LEVEL` - Log level (debug|info|warn|error). Default: `info`

Example:
```bash
PULSE_NUM_PARTITIONS=8 \
PULSE_LOG_LEVEL=debug \
PULSE_SERVER_PORT=9000 \
PULSE_CONSUMER_TIMEOUT_MS=60000 \
make run
```

### Graceful Shutdown

The application handles `SIGINT` and `SIGTERM` signals gracefully, flushing all buffers before exit:

```bash
# Start the app
make run

# In another terminal, send shutdown signal
kill -TERM $(pgrep pulse)
```

## Development

### Project Structure

```
.
├── cmd/pulse/              # Main application entry point
├── internal/               # Private application code
│   ├── app/               # Application lifecycle and wiring
│   ├── config/            # Configuration management
│   ├── consumer/          # Event streaming consumer
│   ├── event/             # Event model and encoding
│   ├── group/             # Consumer group manager (Phase 2)
│   ├── ingest/            # Event ingestion pipeline
│   ├── log/               # Structured logging
│   ├── logstore/          # WAL, segments, and storage
│   ├── offset/            # Offset tracking store (Phase 2)
│   ├── partition/         # Partition routing
│   ├── runtime/           # Runtime utilities (shutdown handling)
│   ├── server/            # HTTP server and handlers
│   └── version/           # Version information
├── docs/                  # Documentation
│   ├── api.md            # Phase 1 API documentation
│   └── architecture.md   # Design principles
├── scripts/               # Build and utility scripts
│   ├── benchmark.sh      # Performance benchmark
│   └── lint.sh           # Linter setup
└── Makefile               # Build automation
```

### Linting

Install golangci-lint:

```bash
# Using the provided script
./scripts/lint.sh

# Or install manually
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.62.2
```

### Testing

```bash
make test
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for design principles and architectural decisions.

## License

TBD

## Contributing

TBD
