#!/bin/bash
# Run radar data availability analysis using Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default: analyze last 7 days
DAYS=${1:-7}

echo "Running radar data availability analysis (last $DAYS days) using Docker..."
echo ""

# Run analysis in Docker container
docker run --rm \
  -v "$PROJECT_ROOT/scripts:/app/scripts:ro" \
  -v "$PROJECT_ROOT/src:/app/src:ro" \
  -v "$PROJECT_ROOT/reports:/app/reports" \
  lfranko/imeteo-radar:latest \
  python3 /app/scripts/analyze_radar_history.py --days "$DAYS" --output "/app/reports/radar_availability_$(date -u +%Y%m%d_%H%M%S).txt"

echo ""
echo "Report generated in $PROJECT_ROOT/reports/"
echo ""
echo "To view the latest report:"
echo "  cat $PROJECT_ROOT/reports/radar_availability_*.txt | tail -50"
