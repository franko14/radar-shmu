#!/bin/bash
#
# Docker-based composite loop - simulates production pod-like behavior
#
# This script runs composite generation in Docker containers with ephemeral storage,
# simulating how the application behaves in production Kubernetes pods where:
# - Each run starts with fresh local storage
# - Metadata (extent, mask, grid) must be loaded from S3
# - Data is downloaded from source APIs and cached to S3
#
# Uses .env file for environment variables.
# SAFETY: Validates that only stage bucket is configured to prevent accidental prod writes.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_DIR}/.env"
IMAGE_NAME="imeteo-radar:docker-loop"
INTERVAL_SECONDS=120  # 2 minutes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "Docker Composite Loop (Production Simulation)"
echo "========================================"

# Check .env file exists
if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}ERROR: .env file not found at ${ENV_FILE}${NC}"
    echo "Please create .env file with DigitalOcean Spaces credentials."
    echo "See .env.example for required variables."
    exit 1
fi

# Load .env file for validation
set -a
source "$ENV_FILE"
set +a

# SAFETY CHECK: Ensure we're using stage bucket only
if [[ -z "$DIGITALOCEAN_SPACES_BUCKET" ]]; then
    echo -e "${RED}ERROR: DIGITALOCEAN_SPACES_BUCKET not set in .env${NC}"
    exit 1
fi

if [[ "$DIGITALOCEAN_SPACES_BUCKET" != *"stage"* ]]; then
    echo -e "${RED}========================================"
    echo "SAFETY ERROR: Non-stage bucket detected!"
    echo "========================================"
    echo "Configured bucket: $DIGITALOCEAN_SPACES_BUCKET"
    echo ""
    echo "This script is intended for testing with stage bucket only."
    echo "Production bucket writes should only happen through CI/CD pipelines."
    echo ""
    echo "Please update .env to use a stage bucket (e.g., 'imeteo-stage')."
    echo -e "========================================${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Stage bucket confirmed: ${DIGITALOCEAN_SPACES_BUCKET}${NC}"

# Build Docker image
echo ""
echo "========================================"
echo "Building Docker Image: ${IMAGE_NAME}"
echo "========================================"
cd "$PROJECT_DIR"

docker build -t "$IMAGE_NAME" . || {
    echo -e "${RED}ERROR: Docker build failed${NC}"
    exit 1
}

echo -e "${GREEN}✓ Docker image built successfully${NC}"

# Docker's --env-file handles .env parsing correctly (quotes, spaces, etc.)

echo ""
echo "========================================"
echo "Starting Docker Composite Loop"
echo "========================================"
echo "Image: ${IMAGE_NAME}"
echo "Interval: ${INTERVAL_SECONDS}s (2 min)"
echo "Sources: dwd,shmu,chmi,omsz,imgw (excluding ARSO)"
echo "Bucket: ${DIGITALOCEAN_SPACES_BUCKET}"
echo -e "${YELLOW}Storage: Ephemeral (fresh container each run)${NC}"
echo "Press Ctrl+C to stop"
echo "========================================"

# Create outputs directory if it doesn't exist
mkdir -p "${PROJECT_DIR}/outputs/composite"

ITERATION=0
while true; do
    ITERATION=$((ITERATION + 1))

    echo ""
    echo "========================================"
    echo "Iteration $ITERATION - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"
    echo -e "${YELLOW}Starting fresh container (ephemeral storage)...${NC}"

    # Run composite in a fresh container
    # - No volume mounts for /tmp/iradar or /tmp/iradar-data (ephemeral)
    # - Only mount outputs directory to see results
    # - Pass all env vars from .env using Docker's --env-file
    # - Memory limit: 1.5GB to match production constraints
    CONTAINER_NAME="iradar-composite-$$"

    docker run --rm \
        --name "$CONTAINER_NAME" \
        --memory=1536m \
        --memory-swap=1536m \
        --env-file "$ENV_FILE" \
        -v "${PROJECT_DIR}/outputs/composite:/app/outputs/composite" \
        "$IMAGE_NAME" \
        imeteo-radar composite \
            --formats png \
            --resolutions full \
            --avif-quality 50 \
            --no-individual \
            --output /app/outputs/composite \
        2>&1 &

    DOCKER_PID=$!

    # Monitor memory usage while container is running
    echo -e "${YELLOW}Monitoring memory usage (limit: 1.5GB)...${NC}"
    PEAK_MEMORY=0
    while kill -0 $DOCKER_PID 2>/dev/null; do
        # Get container memory usage
        MEM_USAGE=$(docker stats --no-stream --format "{{.MemUsage}}" "$CONTAINER_NAME" 2>/dev/null | cut -d'/' -f1 | tr -d ' ')
        if [[ -n "$MEM_USAGE" ]]; then
            # Convert to MB for display
            MEM_MB=$(echo "$MEM_USAGE" | sed 's/MiB//' | sed 's/GiB/*1024/' | bc 2>/dev/null || echo "0")
            if [[ -n "$MEM_MB" && "$MEM_MB" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                MEM_INT=${MEM_MB%.*}
                if [[ $MEM_INT -gt $PEAK_MEMORY ]]; then
                    PEAK_MEMORY=$MEM_INT
                fi
            fi
        fi
        sleep 2
    done

    wait $DOCKER_PID
    EXIT_CODE=$?

    echo -e "${GREEN}Peak memory usage: ${PEAK_MEMORY}MB / 1536MB${NC}"

    if [[ $EXIT_CODE -ne 0 ]]; then
        echo -e "${RED}WARNING: Composite run failed (exit code: $EXIT_CODE), continuing to next iteration...${NC}"
    fi

    echo ""
    echo -e "${GREEN}✓ Container completed and removed (storage cleared)${NC}"
    echo "Waiting ${INTERVAL_SECONDS} seconds until next run..."
    sleep $INTERVAL_SECONDS
done
