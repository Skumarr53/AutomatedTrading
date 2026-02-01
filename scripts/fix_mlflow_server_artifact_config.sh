#!/bin/bash
# Fix MLflow server configuration to use HTTP artifact URIs instead of local filesystem

set -euo pipefail

CONTAINER_NAME="trading-mlflow"

echo "🔧 Fixing MLflow Server Artifact Configuration"
echo "=" | tr '=' '='

# Check if container is running
if ! podman ps --format "{{.Names}}" 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
    echo "❌ Container '${CONTAINER_NAME}' is not running"
    echo "   Start it first: bash scripts/setup_infrastructure.sh"
    exit 1
fi

echo "✅ Container is running"

# Check current MLflow server command
echo ""
echo "Current MLflow Server Configuration:"
podman inspect "${CONTAINER_NAME}" --format '{{range .Config.Cmd}}{{.}} {{end}}' | tr ' ' '\n' | grep -E "(artifacts|default-artifact)" || true

# The issue: MLflow server must use --serve-artifacts to return HTTP artifact URIs
# If artifact URI is file:// or local path, client will try to write directly to filesystem

echo ""
echo "Checking MLflow server logs for artifact configuration..."
podman logs "${CONTAINER_NAME}" 2>&1 | grep -i "artifact\|serve" | tail -5 || echo "   (no artifact-related logs found)"

echo ""
echo "=" | tr '=' '='
echo "SOLUTION"
echo "=" | tr '=' '='

echo ""
echo "The MLflow server MUST be configured with --serve-artifacts flag."
echo "This makes the server return HTTP artifact URIs instead of file:// URIs."
echo ""
echo "Current server command should include:"
echo "  --artifacts-destination /mlflow/artifacts"
echo "  --serve-artifacts"
echo ""
echo "If --serve-artifacts is missing, the client will try to write directly"
echo "to /mlflow filesystem, causing permission errors."
echo ""
echo "To fix:"
echo "1. Stop the container: podman stop ${CONTAINER_NAME}"
echo "2. Update compose.yml or setup script to include --serve-artifacts"
echo "3. Restart the container"
echo ""
echo "The server is currently configured with:"
podman inspect "${CONTAINER_NAME}" --format '{{range .Config.Cmd}}{{.}} {{end}}' | tr ' ' '\n' | head -20
