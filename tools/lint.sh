#!/usr/bin/env bash
set -euo pipefail

# Run all linters for Go and Python

echo "=== Go vet ==="
go vet ./...

echo "=== Go lint ==="
if command -v golangci-lint &> /dev/null; then
  golangci-lint run ./...
else
  echo "golangci-lint not installed, skipping"
fi

echo "=== Python lint ==="
if command -v ruff &> /dev/null; then
  cd python && ruff check src/ tests/
else
  echo "ruff not installed, skipping"
fi

echo "All linting complete."
