#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${ROOT_DIR}/benchmarks/results"
mkdir -p "${RESULTS_DIR}"

echo "==> Running avry-proxy Grey Failure Benchmark Harness..."
go run "${ROOT_DIR}/cmd/bench" \
  -duration=8s \
  -chaos-at=1s \
  -rps=2000 \
  -concurrency=64 \
  -out="${RESULTS_DIR}/latest_benchmark.md" \
  -json-out="${RESULTS_DIR}/latest_benchmark.json"

echo "==> Results saved to ${RESULTS_DIR}/latest_benchmark.md"
