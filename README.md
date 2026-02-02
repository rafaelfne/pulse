# Pulse

**High-performance event ingestion and distributed stream processing in Go.**

Pulse is designed for scalable, low-latency event ingestion with a future focus on distributed stream processing. Built with explicit wiring, minimal allocations, and a backpressure-first mindset.

## Phase 1 - Single-Node Ingest + Streaming (✅ Complete)

Phase 1 delivers:
- **High-throughput event ingestion** via REST API (`POST /events`)
- **Partitioned append-only storage** with WAL and segment files
- **Deterministic streaming** via REST API (`GET /stream`)
- **Metrics endpoint** for observability (`GET /metrics`)
- **Restart-safe persistence** with automatic recovery

### Quick Start

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
# Read from partition 0, starting at offset 0
curl "http://localhost:8080/stream?partition=0&offset=0&limit=100"
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

Phase 1 is complete and includes:
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

# Format code
make fmt

# Run linters
make lint
```

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

**Logging:**
- `PULSE_LOG_LEVEL` - Log level (debug|info|warn|error). Default: `info`

Example:
```bash
PULSE_NUM_PARTITIONS=8 \
PULSE_LOG_LEVEL=debug \
PULSE_SERVER_PORT=9000 \
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
│   ├── ingest/            # Event ingestion pipeline
│   ├── log/               # Structured logging
│   ├── logstore/          # WAL, segments, and storage
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
