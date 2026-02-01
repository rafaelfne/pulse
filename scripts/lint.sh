#!/usr/bin/env bash

set -euo pipefail

# Script to install and run golangci-lint

GOLANGCI_LINT_VERSION="v1.62.2"

echo "Checking for golangci-lint..."

if ! command -v golangci-lint &> /dev/null; then
    echo "golangci-lint not found. Installing version ${GOLANGCI_LINT_VERSION}..."
    
    # Install golangci-lint
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)/bin" "${GOLANGCI_LINT_VERSION}"
    
    if ! command -v golangci-lint &> /dev/null; then
        echo "Failed to install golangci-lint. Please install manually:"
        echo "  https://golangci-lint.run/usage/install/"
        exit 1
    fi
    
    echo "golangci-lint installed successfully."
else
    echo "golangci-lint found: $(golangci-lint version)"
fi

echo "Running golangci-lint..."
golangci-lint run "$@"
