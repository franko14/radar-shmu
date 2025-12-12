#!/bin/bash
# Generate radar data availability report for the last week

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/reports"

# Create reports directory
mkdir -p "$OUTPUT_DIR"

# Generate timestamp for report filename
TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
REPORT_FILE="$OUTPUT_DIR/radar_availability_${TIMESTAMP}.txt"

echo "Generating radar data availability report..."
echo "Output: $REPORT_FILE"
echo ""

# Run analysis (default: 7 days)
python3 "$SCRIPT_DIR/analyze_radar_history.py" --days 7 --output "$REPORT_FILE"

# Show last few lines of report
echo ""
echo "Report preview (last 20 lines):"
echo "================================"
tail -n 20 "$REPORT_FILE"
echo ""
echo "Full report saved to: $REPORT_FILE"
