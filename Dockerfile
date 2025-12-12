# Multi-stage build for iMeteo Radar
# Stage 1: Build stage
FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    libhdf5-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY pyproject.toml ./
COPY src/ ./src/

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip setuptools wheel
RUN pip install .

# Stage 2: Runtime stage
FROM python:3.11-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libhdf5-serial-dev \
    libgomp1 \
    libglib2.0-0 \
    libgl1 \
    libgthread-2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r radar && useradd -r -g radar -m -d /home/radar radar

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Copy application code
COPY --from=builder /app/src ./src
COPY --from=builder /app/pyproject.toml ./pyproject.toml

# Copy scripts directory if it exists
COPY scripts/ ./scripts/

# Create output directories with proper permissions
RUN mkdir -p /tmp/germany /tmp/slovakia /tmp/czechia /tmp/composite /app/outputs /home/radar/.config/matplotlib \
    && chown -R radar:radar /tmp/germany /tmp/slovakia /tmp/czechia /tmp/composite /app/outputs /home/radar

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV PYTHONUNBUFFERED=1
ENV MPLCONFIGDIR="/home/radar/.config/matplotlib"

# Switch to non-root user
USER radar

# Set up volume for outputs
VOLUME ["/app/outputs", "/tmp/germany", "/tmp/slovakia", "/tmp/czechia", "/tmp/composite"]

# Default command - show help
CMD ["imeteo-radar", "--help"]

# Example commands:
# Fetch latest DWD data:
#   docker run -v $(pwd)/outputs:/app/outputs imeteo-radar imeteo-radar fetch --source dwd
#
# Fetch latest SHMU data:
#   docker run -v $(pwd)/outputs:/app/outputs imeteo-radar imeteo-radar fetch --source shmu
#
# Run with custom command:
#   docker run -v $(pwd)/outputs:/app/outputs imeteo-radar imeteo-radar fetch --source dwd --backload --hours 6
#
# Generate extent files:
#   docker run -v $(pwd)/outputs:/app/outputs imeteo-radar imeteo-radar extent --source all
#
# Run as cron job:
#   docker run -d --name radar-fetcher \
#     -v $(pwd)/outputs:/app/outputs \
#     -v $(pwd)/cron:/etc/cron.d \
#     imeteo-radar crond -f