#!/bin/bash
#
# Run composite generation every 2 minutes for 20 minutes
# - Excludes ARSO
# - Backloads 1 hour of data
# - Outputs to ./outputs/composite (individual sources to ./outputs/{country})
#

set -e

INTERVAL_SECONDS=120  # 2 minutes

echo "========================================"
echo "Composite Loop Started (Running Indefinitely)"
echo "========================================"
echo "Interval: ${INTERVAL_SECONDS}s (2 min)"
echo "Sources: dwd,shmu,chmi,omsz,imgw (excluding ARSO)"
echo "Backload: 1 hour"
echo "Output: ./outputs/composite"
echo "Press Ctrl+C to stop"
echo "========================================"
echo ""

ITERATION=0
while true; do
    ITERATION=$((ITERATION + 1))
    echo ""
    echo "========================================"
    echo "Iteration $ITERATION - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"

    # Run composite with backload
    imeteo-radar composite \
        --sources dwd,shmu,chmi,omsz,imgw \
        --output ./outputs/composite \
        --backload \
        --hours 1 \
        --update-extent \
        2>&1

    echo ""
    echo "Waiting ${INTERVAL_SECONDS} seconds until next run..."
    sleep $INTERVAL_SECONDS
done

echo ""
echo "========================================"
echo "Composite Loop Completed - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
