#!/bin/bash
#
# Memory Profile Suite for iMeteo Radar
#
# Runs comprehensive memory profiling for all workloads and generates
# a full report suitable for determining Kubernetes pod memory limits.
#
# Usage:
#   ./scripts/run_memory_profile_suite.sh [OPTIONS]
#
# Options:
#   --quick       Run only single-fetch profiles (skip backload and composite)
#   --output DIR  Output directory for results (default: /tmp/memory-profiles)
#   --help        Show this help message
#

set -e

# Default configuration
OUTPUT_DIR="/tmp/memory-profiles"
QUICK_MODE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROFILER="$SCRIPT_DIR/profile_memory_rss.py"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Memory Profile Suite for iMeteo Radar"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick       Run only single-fetch profiles (skip backload and composite)"
            echo "  --output DIR  Output directory for results (default: /tmp/memory-profiles)"
            echo "  --help        Show this help message"
            echo ""
            echo "Workloads tested:"
            echo "  - fetch --source dwd (single)"
            echo "  - fetch --source shmu (single)"
            echo "  - fetch --source chmi (single)"
            echo "  - fetch --source arso (single)"
            echo "  - fetch --source omsz (single)"
            echo "  - fetch --source dwd --backload --hours 1"
            echo "  - composite (latest)"
            echo "  - composite --backload --hours 1"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}iMeteo Radar - Memory Profile Suite${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo ""
echo "Output directory: $OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"
echo "Quick mode: $QUICK_MODE"
echo ""

# Check if psutil is available
if ! python3 -c "import psutil" 2>/dev/null; then
    echo -e "${RED}Error: psutil is not installed${NC}"
    echo "Install with: pip install psutil"
    echo "Or: pip install -e '.[profiling]'"
    exit 1
fi

# Results summary
declare -A RESULTS

run_profile() {
    local name="$1"
    local cmd="$2"
    local json_file="$OUTPUT_DIR/${name}_${TIMESTAMP}.json"

    echo ""
    echo -e "${YELLOW}----------------------------------------------------------------------${NC}"
    echo -e "${YELLOW}Running: $name${NC}"
    echo -e "${YELLOW}----------------------------------------------------------------------${NC}"
    echo "Command: python $PROFILER $cmd --json $json_file"
    echo ""

    if python3 "$PROFILER" $cmd --json "$json_file"; then
        echo -e "${GREEN}[PASS]${NC} $name completed successfully"
        RESULTS["$name"]="PASS"

        # Extract peak RSS from JSON
        if [ -f "$json_file" ]; then
            peak_rss=$(python3 -c "import json; d=json.load(open('$json_file')); print(d.get('summary', {}).get('peak_rss_mb', 'N/A'))" 2>/dev/null || echo "N/A")
            echo "Peak RSS: ${peak_rss} MB"
        fi
    else
        echo -e "${RED}[FAIL]${NC} $name failed"
        RESULTS["$name"]="FAIL"
    fi
}

# Run single-fetch profiles for all sources
SOURCES=("dwd" "shmu" "chmi" "arso" "omsz")

for source in "${SOURCES[@]}"; do
    run_profile "fetch_${source}" "fetch --source $source"
done

# Run additional profiles if not in quick mode
if [ "$QUICK_MODE" = false ]; then
    # DWD backload 1 hour
    run_profile "fetch_dwd_backload_1h" "fetch --source dwd --backload --hours 1"

    # Composite latest
    run_profile "composite_latest" "composite"

    # Composite backload 1 hour (if composite latest succeeded)
    if [ "${RESULTS[composite_latest]}" = "PASS" ]; then
        run_profile "composite_backload_1h" "composite --backload --hours 1"
    else
        echo -e "${YELLOW}Skipping composite backload (composite latest failed)${NC}"
    fi
fi

# Generate summary report
SUMMARY_FILE="$OUTPUT_DIR/summary_${TIMESTAMP}.txt"

echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}Memory Profile Summary${NC}"
echo -e "${BLUE}======================================================================${NC}"

{
    echo "iMeteo Radar - Memory Profile Summary"
    echo "======================================"
    echo ""
    echo "Generated: $(date)"
    echo "Timestamp: $TIMESTAMP"
    echo ""
    echo "Results:"
    echo "--------"
} > "$SUMMARY_FILE"

echo ""
echo "Results:"
echo "--------"

for name in "${!RESULTS[@]}"; do
    status="${RESULTS[$name]}"
    json_file="$OUTPUT_DIR/${name}_${TIMESTAMP}.json"

    if [ "$status" = "PASS" ] && [ -f "$json_file" ]; then
        peak_rss=$(python3 -c "import json; d=json.load(open('$json_file')); print(d.get('summary', {}).get('peak_rss_mb', 'N/A'))" 2>/dev/null || echo "N/A")
        k8s_request=$(python3 -c "import json; d=json.load(open('$json_file')); print(d.get('recommendations', {}).get('k8s_memory_request', 'N/A'))" 2>/dev/null || echo "N/A")
        k8s_limit=$(python3 -c "import json; d=json.load(open('$json_file')); print(d.get('recommendations', {}).get('k8s_memory_limit', 'N/A'))" 2>/dev/null || echo "N/A")

        printf "  %-30s %s  Peak: %6s MB  Request: %8s  Limit: %8s\n" "$name" "$status" "$peak_rss" "$k8s_request" "$k8s_limit"
        printf "  %-30s %s  Peak: %6s MB  Request: %8s  Limit: %8s\n" "$name" "$status" "$peak_rss" "$k8s_request" "$k8s_limit" >> "$SUMMARY_FILE"
    else
        printf "  %-30s %s\n" "$name" "$status"
        printf "  %-30s %s\n" "$name" "$status" >> "$SUMMARY_FILE"
    fi
done

echo ""
echo "Summary saved to: $SUMMARY_FILE"
echo "JSON results in: $OUTPUT_DIR/"

# Generate combined JSON report
COMBINED_JSON="$OUTPUT_DIR/combined_${TIMESTAMP}.json"

python3 << EOF
import json
import os
from datetime import datetime

output_dir = "$OUTPUT_DIR"
timestamp = "$TIMESTAMP"
combined = {
    "metadata": {
        "generated": datetime.now().isoformat() + "Z",
        "timestamp": timestamp,
    },
    "workloads": {}
}

# Find all JSON files with this timestamp
for name in os.listdir(output_dir):
    if name.endswith(f"_{timestamp}.json") and name != f"combined_{timestamp}.json":
        workload_name = name.replace(f"_{timestamp}.json", "")
        filepath = os.path.join(output_dir, name)
        try:
            with open(filepath) as f:
                data = json.load(f)
            combined["workloads"][workload_name] = {
                "command": data.get("command", {}),
                "summary": data.get("summary", {}),
                "recommendations": data.get("recommendations", {})
            }
        except Exception as e:
            print(f"Warning: Could not load {name}: {e}")

# Save combined JSON
combined_path = os.path.join(output_dir, f"combined_{timestamp}.json")
with open(combined_path, "w") as f:
    json.dump(combined, f, indent=2)

print(f"Combined JSON saved to: {combined_path}")
EOF

echo ""
echo -e "${GREEN}Profile suite completed!${NC}"
echo ""
