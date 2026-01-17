# Dockerfile
# Production container for AutomatedTrading application
#
# Build: podman build -t trading-app:latest .
# Run: podman run --rm -it --env-file .env trading-app:latest

FROM docker.io/python:3.11-slim

# Metadata
LABEL maintainer="santhosh <skumarr53@gmail.com>"
LABEL version="1.0.0"
LABEL description="Automated Trading Application with Ray distributed processing"

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Application settings
    APP_VERSION=1.0.0 \
    RAY_ENABLED=true \
    HEALTH_PORT=8080

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster Python package management
RUN pip install uv

# Create app user (non-root)
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock* ./

# Install Python dependencies
RUN uv pip install --system -e . || pip install -e .

# Copy application code
COPY --chown=appuser:appuser . .

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/mlartifacts \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose ports
# 8080: Health API
# 8265: Ray dashboard (if running head node)
EXPOSE 8080 8265

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${HEALTH_PORT}/healthz || exit 1

# Default command
CMD ["python", "main.py"]
