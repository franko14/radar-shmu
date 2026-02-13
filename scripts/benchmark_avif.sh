#!/bin/bash
#
# Benchmark AVIF encoding under production resource constraints
#
# Runs a single composite iteration inside a Docker container with CPU and memory
# limits matching production pods (500m CPU, 1024Mi RAM). Uses `docker run --cpus`
# and `--memory` flags which are always enforced (unlike docker-compose deploy limits).
#
# Environment variables:
#   AVIF_SPEED    - libaom/svt speed preset (default: 8)
#   AVIF_CODEC    - codec selection: auto, aom, svt, rav1e (default: auto)
#   MEMORY_LIMIT  - container memory limit (default: 1536m)
#   CPU_LIMIT     - container CPU limit (default: 0.5)
#
# Usage:
#   ./scripts/benchmark_avif.sh
#   AVIF_SPEED=4 ./scripts/benchmark_avif.sh
#   AVIF_SPEED=6 AVIF_CODEC=svt ./scripts/benchmark_avif.sh
#
#   # Compare speeds:
#   for s in 4 6 8 10; do echo "=== speed=$s ==="; AVIF_SPEED=$s ./scripts/benchmark_avif.sh; done
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${PROJECT_DIR}/.env"
IMAGE_NAME="imeteo-radar:avif-bench"

AVIF_SPEED="${AVIF_SPEED:-8}"
AVIF_CODEC="${AVIF_CODEC:-auto}"
MEMORY_LIMIT="${MEMORY_LIMIT:-2048m}"
CPU_LIMIT="${CPU_LIMIT:-0.5}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo "========================================"
echo "AVIF Encoding Benchmark"
echo "========================================"
echo -e "Speed:    ${CYAN}${AVIF_SPEED}${NC}"
echo -e "Codec:    ${CYAN}${AVIF_CODEC}${NC}"
echo -e "CPU:      ${CYAN}${CPU_LIMIT} cores${NC} (production: 500m)"
echo -e "Memory:   ${CYAN}${MEMORY_LIMIT}${NC} (production: 1024Mi)"
echo -e "Platform: ${CYAN}linux/amd64${NC}"
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

# Safety check: stage bucket only
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
    echo "Please update .env to use a stage bucket (e.g., 'imeteo-stage')."
    echo -e "========================================${NC}"
    exit 1
fi

echo -e "${GREEN}Stage bucket confirmed: ${DIGITALOCEAN_SPACES_BUCKET}${NC}"

# Build Docker image for linux/amd64 (matches production, uses Rosetta on Apple Silicon)
echo ""
echo "========================================"
echo "Building Docker Image: ${IMAGE_NAME} (linux/amd64)"
echo "========================================"
cd "$PROJECT_DIR"

docker build --platform linux/amd64 -t "$IMAGE_NAME" . || {
    echo -e "${RED}ERROR: Docker build failed${NC}"
    exit 1
}

echo -e "${GREEN}Docker image built successfully${NC}"

# Create output directory
OUTPUT_DIR="${PROJECT_DIR}/outputs/benchmark"
mkdir -p "$OUTPUT_DIR"

# Run benchmark
echo ""
echo "========================================"
echo "Running Benchmark - $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

CONTAINER_NAME="iradar-avif-bench-$$"
START_TIME=$(date +%s)

docker run --rm \
    --name "$CONTAINER_NAME" \
    --platform linux/amd64 \
    --cpus="$CPU_LIMIT" \
    --memory="$MEMORY_LIMIT" \
    --env-file "$ENV_FILE" \
    -e "TZ=Europe/Berlin" \
    -v "${OUTPUT_DIR}:/app/outputs/composite" \
    "$IMAGE_NAME" \
    imeteo-radar composite \
        --formats png,avif \
        --avif-quality 50 \
        --avif-speed "$AVIF_SPEED" \
        --avif-codec "$AVIF_CODEC" \
        --resolutions full,2000 \
        --no-individual \
        --disable-upload \
        --output /app/outputs/composite

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "========================================"
echo "Benchmark Results"
echo "========================================"
echo -e "Speed:        ${CYAN}${AVIF_SPEED}${NC}"
echo -e "Codec:        ${CYAN}${AVIF_CODEC}${NC}"
echo -e "Total time:   ${CYAN}${ELAPSED}s${NC}"
echo ""

# Show output files
echo "Output files:"
if command -v find &>/dev/null; then
    find "$OUTPUT_DIR" -name "*.avif" -exec ls -lh {} \; 2>/dev/null || echo "  (no AVIF files found)"
    find "$OUTPUT_DIR" -name "*.png" -exec ls -lh {} \; 2>/dev/null || echo "  (no PNG files found)"
fi

echo ""
echo -e "${GREEN}Benchmark complete.${NC}"
echo "Look for 'AVIF encode' timing lines in the output above for per-image breakdown."
