#!/usr/bin/env bash
# Simulates K8s CronJobs locally for 1 hour to verify memory optimization changes.
# Runs the 3 production CronJob commands every 5 minutes and logs peak RSS.
#
# Usage: ./scripts/test-memory-optimization.sh
# Requires: docker, .env with DO Spaces credentials

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="imeteo-radar:mem-test"
LOG_DIR="$PROJECT_DIR/outputs/mem-test-logs"
INTERVAL_SEC=300  # 5 minutes
ITERATIONS=12     # 12 × 5 min = 1 hour

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN:${NC} $1"; }

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
    log "Loaded .env"
else
    warn "No .env found — S3 uploads will be disabled"
fi

# Build image
log "Building Docker image: $IMAGE_NAME"
docker build -t "$IMAGE_NAME" "$PROJECT_DIR"

# Prepare log directory
mkdir -p "$LOG_DIR"
log "Logs: $LOG_DIR"

# Common docker run args
DOCKER_ENV=(
    -e "TZ=Europe/Berlin"
    -e "DIGITALOCEAN_SPACES_KEY=${DIGITALOCEAN_SPACES_KEY:-}"
    -e "DIGITALOCEAN_SPACES_SECRET=${DIGITALOCEAN_SPACES_SECRET:-}"
    -e "DIGITALOCEAN_SPACES_ENDPOINT=${DIGITALOCEAN_SPACES_ENDPOINT:-}"
    -e "DIGITALOCEAN_SPACES_REGION=${DIGITALOCEAN_SPACES_REGION:-}"
    -e "DIGITALOCEAN_SPACES_BUCKET=${DIGITALOCEAN_SPACES_BUCKET:-}"
    -e "DIGITALOCEAN_SPACES_URL=${DIGITALOCEAN_SPACES_URL:-}"
    -e "TRACEMALLOC=1"
)

# Shared volume for data/cache across runs (simulates K8s PVC)
VOLUME_ARGS=(
    -v "$PROJECT_DIR/outputs/mem-test-data:/tmp/iradar"
    -v "$PROJECT_DIR/outputs/mem-test-cache:/tmp/iradar-data"
)

mkdir -p "$PROJECT_DIR/outputs/mem-test-data" "$PROJECT_DIR/outputs/mem-test-cache"

# Run a single CronJob command and capture peak RSS
run_job() {
    local job_name="$1"
    shift
    local log_file="$LOG_DIR/${job_name}_$(date '+%H%M%S').log"

    log "${GREEN}Starting:${NC} $job_name"
    local start_time=$SECONDS

    # Run with memory tracking via /usr/bin/time if available
    docker run --rm \
        --name "memtest-${job_name}-$$" \
        --memory=3g \
        "${DOCKER_ENV[@]}" \
        "${VOLUME_ARGS[@]}" \
        "$IMAGE_NAME" \
        "$@" \
        > "$log_file" 2>&1 || true

    local elapsed=$(( SECONDS - start_time ))
    local exit_msg="done in ${elapsed}s"

    # Extract tracemalloc peak from log if present
    local peak=$(grep -i "tracemalloc\|peak memory\|Peak:" "$log_file" 2>/dev/null | tail -1 || echo "")
    if [ -n "$peak" ]; then
        exit_msg="$exit_msg | $peak"
    fi

    log "${GREEN}Finished:${NC} $job_name ($exit_msg) -> $log_file"
}

# Summary header
echo ""
echo "============================================="
echo "  Memory Optimization Verification Test"
echo "  Image:      $IMAGE_NAME"
echo "  Iterations: $ITERATIONS (every ${INTERVAL_SEC}s = 1 hour)"
echo "  Jobs:       dwd-fetch, chmi-fetch, composite"
echo "============================================="
echo ""

# Main loop
for i in $(seq 1 $ITERATIONS); do
    log "=== Iteration $i/$ITERATIONS ==="

    # Run CronJob 1: DWD fetch (backload 1 hour)
    run_job "dwd-fetch" \
        imeteo-radar fetch --source dwd --backload --hours 1 \
        --output /tmp/iradar/germany

    # Run CronJob 2: CHMI fetch (backload 1 hour)
    run_job "chmi-fetch" \
        imeteo-radar fetch --source chmi --backload --hours 1 \
        --output /tmp/iradar/czechia

    # Run CronJob 3: Composite
    run_job "composite" \
        imeteo-radar composite --no-individual \
        --formats png,avif --resolutions full,2000 \
        --avif-speed 8 --avif-quality 50 \
        --output /tmp/iradar/composite

    if [ "$i" -lt "$ITERATIONS" ]; then
        log "Sleeping ${INTERVAL_SEC}s until next iteration..."
        sleep "$INTERVAL_SEC"
    fi
done

echo ""
log "=== Test complete ==="
log "Logs directory: $LOG_DIR"

# Print summary of peak memory from all logs
echo ""
echo "=== Memory peaks (from tracemalloc) ==="
grep -rh "tracemalloc\|peak memory\|Peak:" "$LOG_DIR"/ 2>/dev/null | sort || echo "(no tracemalloc output found)"

echo ""
echo "=== Error summary ==="
error_count=$(grep -rlc "ERROR\|Traceback\|OOM" "$LOG_DIR"/ 2>/dev/null | wc -l || echo 0)
echo "Files with errors: $error_count"
if [ "$error_count" -gt 0 ]; then
    grep -rh "ERROR\|Traceback" "$LOG_DIR"/ 2>/dev/null | head -20
fi
