# Pulse

**High-performance event ingestion and distributed stream processing in Go.**

Pulse is designed for scalable, low-latency event ingestion with a future focus on distributed stream processing. Built with explicit wiring, minimal allocations, and a backpressure-first mindset.

## Current State

This is the foundational bootstrap. The repository includes:
- Go module structure and tooling
- Graceful shutdown lifecycle
- Environment-based configuration
- Structured JSON logging with `slog`
- Build, test, and lint automation

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

### Configuration

Configure Pulse using environment variables:

- `PULSE_LOG_LEVEL` - Log level (debug, info, warn, error). Default: `info`
- `PULSE_ENV` - Environment name (local, dev, prod). Default: `local`
- `PULSE_SHUTDOWN_TIMEOUT_MS` - Graceful shutdown timeout in milliseconds. Default: `5000`

### Graceful Shutdown

The application handles `SIGINT` and `SIGTERM` signals gracefully:

```bash
# Start the app
make run

# In another terminal, send shutdown signal
pkill -SIGTERM pulse
```

## Development

### Project Structure

```
.
├── cmd/pulse/           # Main application entry point
├── internal/            # Private application code
│   ├── app/            # Application lifecycle
│   ├── config/         # Configuration management
│   ├── log/            # Structured logging
│   ├── runtime/        # Runtime utilities (shutdown handling)
│   └── version/        # Version information
├── docs/               # Documentation
├── scripts/            # Build and utility scripts
└── Makefile            # Build automation
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
