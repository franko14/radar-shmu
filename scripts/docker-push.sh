#!/bin/bash
# Docker Image Push Script for iMeteo Radar
# Builds and pushes Docker image to DockerHub

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DOCKERHUB_REPO="lfranko/imeteo-radar"
DOCKERFILE="Dockerfile"

echo -e "${GREEN}=== Docker Image Build & Push Script ===${NC}"
echo ""

# Extract version from pyproject.toml
if [ -f "pyproject.toml" ]; then
    VERSION=$(grep -E '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
    echo -e "${GREEN}Detected version:${NC} ${VERSION}"
else
    echo -e "${RED}Error: pyproject.toml not found${NC}"
    exit 1
fi

# Confirm action
echo ""
echo -e "${YELLOW}This will build and push:${NC}"
echo "  - ${DOCKERHUB_REPO}:latest"
echo "  - ${DOCKERHUB_REPO}:${VERSION}"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Aborted${NC}"
    exit 0
fi

# Build the image
echo ""
echo -e "${GREEN}Building Docker image...${NC}"
docker build -t ${DOCKERHUB_REPO}:latest -t ${DOCKERHUB_REPO}:${VERSION} -f ${DOCKERFILE} .

if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed${NC}"
    exit 1
fi

echo -e "${GREEN}Build successful!${NC}"
echo ""

# Push to DockerHub
echo -e "${GREEN}Pushing to DockerHub...${NC}"
echo "Pushing ${DOCKERHUB_REPO}:latest..."
docker push ${DOCKERHUB_REPO}:latest

echo "Pushing ${DOCKERHUB_REPO}:${VERSION}..."
docker push ${DOCKERHUB_REPO}:${VERSION}

if [ $? -ne 0 ]; then
    echo -e "${RED}Push failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}=== Success! ===${NC}"
echo "Images published:"
echo "  - ${DOCKERHUB_REPO}:latest"
echo "  - ${DOCKERHUB_REPO}:${VERSION}"
echo ""
echo "View on DockerHub: https://hub.docker.com/r/${DOCKERHUB_REPO}"
echo ""
echo -e "${YELLOW}Team members can now pull the image with:${NC}"
echo "  docker pull ${DOCKERHUB_REPO}:latest"
echo ""
