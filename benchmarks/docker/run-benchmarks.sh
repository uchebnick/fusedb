#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create results directory
mkdir -p results

echo "=== FuseDB Container Benchmark Suite ==="
echo "This runs pure Go benchmarks inside containers (no HTTP overhead)"
echo ""

# Clean up any existing containers
echo "Cleaning up existing containers..."
docker-compose -f docker-compose.benchmark.yml down -v 2>/dev/null || true

# Build the benchmark image
echo ""
echo "Building benchmark image..."
docker-compose -f docker-compose.benchmark.yml build

# Run benchmarks sequentially
BENCHMARKS=(
  "oneleaf-read:OneLeaf Read-Only"
  "oneleaf-write:OneLeaf Write-Only"
  "oneleaf-mixed:OneLeaf Mixed 50/50"
  "pebble-read:Pebble Read-Only"
  "pebble-write:Pebble Write-Only"
  "pebble-mixed:Pebble Mixed 50/50"
)

for bench in "${BENCHMARKS[@]}"; do
  IFS=':' read -r service name <<< "$bench"

  echo ""
  echo "=========================================="
  echo "Running: $name"
  echo "=========================================="

  # Run the benchmark
  docker-compose -f docker-compose.benchmark.yml run --rm "$service"

  # Rename result file
  if [ -f "results/results.json" ]; then
    mv results/results.json "results/${service}.json"
    echo "Results saved to: results/${service}.json"
  fi

  echo ""
done

echo ""
echo "=== All benchmarks complete ==="
echo ""
echo "Generating comparison report..."

# Generate comparison report
cat > results/COMPARISON.md << 'EOF'
# FuseDB vs Pebble Benchmark Comparison

**Date:** $(date)
**Environment:** Docker containers with 2 CPUs, 512MB RAM limit

## Configuration
- Records: 100,000
- Operations: 500,000
- Value Size: 1KB
- Cache Size: 64MB
- Workers: 1 (single-threaded)

## Results

EOF

# Parse and display results
for bench in "${BENCHMARKS[@]}"; do
  IFS=':' read -r service name <<< "$bench"

  if [ -f "results/${service}.json" ]; then
    echo "### $name" >> results/COMPARISON.md
    echo '```json' >> results/COMPARISON.md
    cat "results/${service}.json" >> results/COMPARISON.md
    echo '```' >> results/COMPARISON.md
    echo "" >> results/COMPARISON.md
  fi
done

echo "Comparison report saved to: results/COMPARISON.md"
echo ""
echo "To view results:"
echo "  cat results/COMPARISON.md"
echo ""
echo "Individual results:"
ls -lh results/*.json

# Cleanup
docker-compose -f docker-compose.benchmark.yml down -v
