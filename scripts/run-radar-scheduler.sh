#!/bin/bash
#
# Radar Image Scheduler - Local Testing
#
# Runs every 5 minutes to generate:
# - 3 individual radar images (DWD, SHMU, CHMI)
# - 1 composite radar image
#
# Usage:
#   ./scripts/run-radar-scheduler.sh
#
# Stop with: Ctrl+C or kill the process
#

set -e

# Configuration
INTERVAL=300  # 5 minutes in seconds
OUTPUTS_DIR="$(pwd)/test-output"
DOCKER_IMAGE="imeteo-radar:test"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create output directories
mkdir -p "$OUTPUTS_DIR/germany"
mkdir -p "$OUTPUTS_DIR/slovakia"
mkdir -p "$OUTPUTS_DIR/czechia"
mkdir -p "$OUTPUTS_DIR/composite"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Radar Image Scheduler - Local Testing${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Interval: ${YELLOW}Every 5 minutes${NC}"
echo -e "Outputs:  ${YELLOW}$OUTPUTS_DIR${NC}"
echo -e "Docker:   ${YELLOW}$DOCKER_IMAGE${NC}"
echo -e ""
echo -e "Press ${YELLOW}Ctrl+C${NC} to stop"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to run radar generation
run_radar_generation() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    echo -e "${GREEN}[${timestamp}] Starting radar generation...${NC}"

    # Check if Docker image exists
    if ! docker image inspect "$DOCKER_IMAGE" &> /dev/null; then
        echo -e "${YELLOW}⚠️  Docker image not found, building...${NC}"
        docker build -t "$DOCKER_IMAGE" .
    fi

    # Generate individual radar images in parallel
    echo -e "${BLUE}📡 Generating individual radar images...${NC}"

    docker run --rm -v "$OUTPUTS_DIR/germany:/tmp/germany" "$DOCKER_IMAGE" \
        imeteo-radar fetch --source dwd &
    PID_DWD=$!

    docker run --rm -v "$OUTPUTS_DIR/slovakia:/tmp/slovakia" "$DOCKER_IMAGE" \
        imeteo-radar fetch --source shmu &
    PID_SHMU=$!

    docker run --rm -v "$OUTPUTS_DIR/czechia:/tmp/czechia" "$DOCKER_IMAGE" \
        imeteo-radar fetch --source chmi &
    PID_CHMI=$!

    # Wait for individual fetches to complete
    wait $PID_DWD
    echo -e "${GREEN}✅ DWD complete${NC}"

    wait $PID_SHMU
    echo -e "${GREEN}✅ SHMU complete${NC}"

    wait $PID_CHMI
    echo -e "${GREEN}✅ CHMI complete${NC}"

    # Generate composite
    echo -e "${BLUE}🎨 Generating composite...${NC}"
    docker run --rm -v "$OUTPUTS_DIR/composite:/tmp/composite" "$DOCKER_IMAGE" \
        imeteo-radar composite --sources dwd,shmu,chmi --output /tmp/composite

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Composite complete${NC}"
    else
        echo -e "${YELLOW}⚠️  Composite generation failed (may be due to no common timestamp)${NC}"
    fi

    # Show file counts
    echo -e "${BLUE}📊 Statistics:${NC}"
    echo -e "  Germany:   $(ls -1 $OUTPUTS_DIR/germany/*.png 2>/dev/null | wc -l | tr -d ' ') images"
    echo -e "  Slovakia:  $(ls -1 $OUTPUTS_DIR/slovakia/*.png 2>/dev/null | wc -l | tr -d ' ') images"
    echo -e "  Czechia:   $(ls -1 $OUTPUTS_DIR/czechia/*.png 2>/dev/null | wc -l | tr -d ' ') images"
    echo -e "  Composite: $(ls -1 $OUTPUTS_DIR/composite/*.png 2>/dev/null | wc -l | tr -d ' ') images"

    local end_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[${end_timestamp}] Radar generation complete!${NC}"
    echo ""
}

# Trap Ctrl+C to exit cleanly
trap 'echo -e "\n${YELLOW}Stopping scheduler...${NC}"; exit 0' INT TERM

# Main loop
while true; do
    run_radar_generation

    echo -e "${BLUE}⏰ Waiting 5 minutes until next run...${NC}"
    sleep $INTERVAL
done
