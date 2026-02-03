.PHONY: fmt test build run lint clean e2e e2e-stress help

# Default target
.DEFAULT_GOAL := help

## fmt: Format all Go source files
fmt:
	@echo "Formatting Go code..."
	@go fmt ./...

## test: Run all tests
test:
	@echo "Running tests..."
	@go test ./...

## build: Build all Go packages
build:
	@echo "Building..."
	@go build ./...

## run: Run the pulse application
run:
	@echo "Running pulse..."
	@go run ./cmd/pulse

## e2e: Run end-to-end integration test (fast mode)
e2e:
	@echo "Running e2e integration test (fast mode)..."
	@PULSE_E2E_MODE=ci go run ./cmd/pulsetest

## e2e-stress: Run end-to-end integration test (stress mode with colored output)
e2e-stress:
	@echo "Running e2e integration test (stress mode)..."
	@PULSE_E2E_MODE=stress go run ./cmd/pulsetest

## lint: Run golangci-lint
lint:
	@echo "Running linters..."
	@export PATH="$$PATH:$$(go env GOPATH)/bin" && \
	if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Install it or run ./scripts/lint.sh"; \
		exit 1; \
	fi

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@go clean ./...
	@rm -f pulse

## help: Display this help message
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
