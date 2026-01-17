#!/bin/bash
# scripts/check_containers.sh
# Checks if required containers are running and healthy
# Returns 0 if all healthy, 1 if any missing/unhealthy

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Required containers
REQUIRED_CONTAINERS=("trading-influxdb" "trading-mlflow")

# Check if podman is available
if ! command -v podman &> /dev/null; then
    echo "ERROR: podman not found. Please install podman."
    exit 1
fi

# Check if podman-compose is available
if ! command -v podman-compose &> /dev/null && ! command -v podman compose &> /dev/null; then
    echo "ERROR: podman-compose not found. Please install podman-compose."
    exit 1
fi

ALL_HEALTHY=true
MISSING_CONTAINERS=()

echo "Checking container health..."

for container in "${REQUIRED_CONTAINERS[@]}"; do
    if podman ps --format "{{.Names}}" | grep -q "^${container}$"; then
        # Check if container is healthy
        health_status=$(podman inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "none")
        
        if [ "$health_status" = "healthy" ] || [ "$health_status" = "none" ]; then
            echo "✓ $container is running"
        else
            echo "⚠ $container is running but unhealthy (status: $health_status)"
            ALL_HEALTHY=false
        fi
    else
        echo "✗ $container is not running"
        MISSING_CONTAINERS+=("$container")
        ALL_HEALTHY=false
    fi
done

if [ "$ALL_HEALTHY" = true ]; then
    echo ""
    echo "All containers are healthy ✓"
    exit 0
else
    echo ""
    echo "Some containers are missing or unhealthy:"
    printf '  - %s\n' "${MISSING_CONTAINERS[@]}"
    exit 1
fi
