#!/bin/bash
# Verify MLflow container can write artifacts
# This is a health check that can be run periodically

set -e

CONTAINER_NAME="trading-mlflow"
TEST_FILE="/mlflow/artifacts/.health_check_$(date +%s).txt"

echo "🔍 Verifying MLflow container write permissions..."

# Check if container is running
if ! podman ps --format "{{.Names}}" 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container '${CONTAINER_NAME}' is not running"
    exit 1
fi

# Test write permission
if podman exec "${CONTAINER_NAME}" sh -c "test -w /mlflow/artifacts" 2>/dev/null; then
    echo "✅ Container can write to /mlflow/artifacts"
else
    echo "❌ Container CANNOT write to /mlflow/artifacts"
    echo "   Fixing permissions..."
    
    # Fix from host
    bash "$(dirname "$0")/ensure_mlflow_permissions.sh" || {
        echo "❌ Failed to fix permissions"
        exit 1
    }
    
    # Verify again
    if podman exec "${CONTAINER_NAME}" sh -c "test -w /mlflow/artifacts" 2>/dev/null; then
        echo "✅ Permissions fixed, container can now write"
    else
        echo "❌ Permissions still incorrect after fix"
        exit 1
    fi
fi

# Test actual write
if podman exec "${CONTAINER_NAME}" sh -c "echo 'health_check' > ${TEST_FILE} && test -f ${TEST_FILE}" 2>/dev/null; then
    echo "✅ Container successfully wrote test file"
    # Cleanup
    podman exec "${CONTAINER_NAME}" rm -f "${TEST_FILE}" 2>/dev/null || true
    echo "✅ Write test passed"
    exit 0
else
    echo "❌ Container failed to write test file"
    exit 1
fi
