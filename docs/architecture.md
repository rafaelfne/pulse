# Pulse Architecture

## Vision

Pulse is a high-performance event ingestion and distributed stream processing system built in Go. The architecture prioritizes:

1. **Explicit Wiring** - No reflection, no global state, clear dependency injection
2. **Low Allocations** - Careful memory management to minimize GC pressure
3. **Backpressure-First Mindset** - Built-in flow control from the ground up
4. **Operational Simplicity** - Clear observability, graceful degradation, and predictable behavior

## Current State

**Single-node skeleton only.**

The current implementation provides:
- Basic application lifecycle with graceful shutdown
- Environment-based configuration
- Structured JSON logging using Go's `log/slog`
- Signal handling for `SIGINT` and `SIGTERM`
- Build and development tooling

### What's NOT Implemented Yet

- Event ingestion endpoints (HTTP, gRPC)
- Write-Ahead Log (WAL) or any persistence layer
- Consumer groups, clustering, or distributed processing
- Stream processing logic
- Backpressure mechanisms
- Metrics and observability beyond logging

## Design Principles

### 1. Explicit Wiring

All dependencies are passed explicitly through constructors. No service locators, no global state, no init() functions with side effects.

```go
// Good: explicit dependencies
app := app.New(cfg, logger)

// Bad: implicit dependencies (avoided)
// app := app.New() // where does config/logger come from?
```

### 2. Graceful Lifecycle

Every component has clear `Start()` and `Stop()` methods. Shutdown is coordinated through context cancellation with configurable timeouts.

### 3. Configuration

All configuration comes from environment variables. Validation happens at startup to fail fast. No runtime configuration changes (yet).

### 4. Error Handling

Errors are wrapped with context using `fmt.Errorf` with `%w` for stack traces. Errors are logged with structured context.

### 5. Testing Philosophy

- Unit tests for business logic
- Integration tests for cross-component behavior
- No mocks for interfaces we own (use real implementations or fakes)

### 6. API Documentation

Pulse uses a **static OpenAPI specification** for API documentation. Key decisions:

- **Static YAML file** (`docs/openapi.yaml`) maintained alongside code
- **Embedded in binary** using `go:embed` for zero external dependencies
- **No reflection-based generation** - explicit schemas, no runtime overhead
- **Scalar UI integration** via CDN for interactive documentation
- **Environment-gated** - enabled by default in local/dev, disabled in production

This approach prioritizes:
- **Clarity**: API contract is explicit and human-readable
- **Simplicity**: No code generation or complex tooling
- **Performance**: Zero runtime cost when disabled
- **Reliability**: Documentation cannot drift from spec (spec is the documentation)

The OpenAPI spec is served at `/openapi.yaml` and the interactive Scalar UI at `/docs` when `PULSE_ENABLE_DOCS=true`.

## Future Architecture

### Phase 1: Ingestion (Next)

- HTTP and gRPC endpoints for event submission
- Request validation and batching
- Memory-backed queue before WAL
- Basic rate limiting

### Phase 2: Persistence

- Write-Ahead Log (WAL) implementation
- Segment rotation and compaction
- Checkpointing for consumer offsets
- Disk I/O optimization (mmap, direct I/O)

### Phase 3: Distribution

- Cluster coordination (leader election)
- Partition assignment and rebalancing
- Consumer group protocol
- Replication for durability

### Phase 4: Stream Processing

- Windowing and aggregations
- Stateful processing with state stores
- Exactly-once semantics
- Complex event processing (CEP) primitives

## Technology Choices

### Why Go?

- Excellent concurrency primitives (goroutines, channels)
- Simple memory model and GC tuning
- Strong standard library (`context`, `slog`, `net/http`)
- Easy deployment (static binaries)
- Good performance characteristics for I/O-bound workloads

### Why `log/slog`?

- Standard library, no external dependencies for logging
- Structured logging with levels
- JSON output for machine parsing
- Performance-conscious design

### Why Environment Variables for Config?

- 12-factor app principles
- Works across deployment environments (local, k8s, VMs)
- No config file parsing complexity
- Clear configuration surface

## Non-Goals

- **Not a Kafka replacement** - Different design choices, different trade-offs
- **Not schema enforcement** - Payload-agnostic for now
- **Not an analytics database** - Focus on streaming, not batch queries

## References

- [The Twelve-Factor App](https://12factor.net/)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Designing Data-Intensive Applications](https://dataintensive.net/) (Martin Kleppmann)
