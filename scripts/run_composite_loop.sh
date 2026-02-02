#!/bin/bash
#
# Run composite generation every 2 minutes for 20 minutes
# - Excludes ARSO
# - Backloads 1 hour of data
# - Outputs to ./outputs/composite (individual sources to ./outputs/{country})
#

set -e

INTERVAL_SECONDS=120  # 2 minutes
DURATION_SECONDS=1200 # 20 minutes
ITERATIONS=$((DURATION_SECONDS / INTERVAL_SECONDS))

echo "========================================"
echo "Composite Loop Started"
echo "========================================"
echo "Interval: ${INTERVAL_SECONDS}s (2 min)"
echo "Duration: ${DURATION_SECONDS}s (20 min)"
echo "Iterations: ${ITERATIONS}"
echo "Sources: dwd,shmu,chmi,omsz,imgw (excluding ARSO)"
echo "Backload: 1 hour"
echo "Output: ./outputs/composite"
echo "========================================"
echo ""

for i in $(seq 1 $ITERATIONS); do
    echo ""
    echo "========================================"
    echo "Iteration $i of $ITERATIONS - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"

    # Run composite with backload
    imeteo-radar composite \
        --sources dwd,shmu,chmi,omsz,imgw \
        --output ./outputs/composite \
        --backload \
        --hours 1 \
        --update-extent \
        2>&1

    # Check if this is the last iteration
    if [ $i -lt $ITERATIONS ]; then
        echo ""
        echo "Waiting ${INTERVAL_SECONDS} seconds until next run..."
        sleep $INTERVAL_SECONDS
    fi
done

echo ""
echo "========================================"
echo "Composite Loop Completed - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
