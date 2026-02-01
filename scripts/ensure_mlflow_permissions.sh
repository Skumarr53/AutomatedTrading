#!/bin/bash
# Ensure MLflow directory permissions are correct for container writes
# This script fixes permissions and verifies they work

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MLFLOW_DIR="${PROJECT_ROOT}/data/mlflow"
ARTIFACTS_DIR="${PROJECT_ROOT}/data/mlartifacts"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}❌${NC} $1"
}

# Ensure directories exist
mkdir -p "${MLFLOW_DIR}"
mkdir -p "${ARTIFACTS_DIR}"

log_info "Ensuring MLflow directory permissions..."

# Fix permissions using podman unshare (works with rootless Podman)
# This ensures permissions are set in the container's namespace
log_info "Fixing permissions via podman unshare..."

if command -v podman >/dev/null 2>&1; then
    # Use podman unshare to set permissions in container namespace
    podman unshare chmod -R 777 "${MLFLOW_DIR}" 2>/dev/null || {
        log_warn "podman unshare failed, trying direct chmod..."
        chmod -R 777 "${MLFLOW_DIR}" 2>/dev/null || {
            log_error "Failed to set permissions on ${MLFLOW_DIR}"
            exit 1
        }
    }
    
    podman unshare chmod -R 777 "${ARTIFACTS_DIR}" 2>/dev/null || {
        log_warn "podman unshare failed, trying direct chmod..."
        chmod -R 777 "${ARTIFACTS_DIR}" 2>/dev/null || {
            log_error "Failed to set permissions on ${ARTIFACTS_DIR}"
            exit 1
        }
    }
    
    log_info "Permissions set via podman unshare"
else
    log_warn "podman not found, using direct chmod..."
    chmod -R 777 "${MLFLOW_DIR}" 2>/dev/null || {
        log_error "Failed to set permissions (may need sudo)"
        exit 1
    }
    chmod -R 777 "${ARTIFACTS_DIR}" 2>/dev/null || {
        log_error "Failed to set permissions (may need sudo)"
        exit 1
    }
fi

# Verify permissions
log_info "Verifying permissions..."

MLFLOW_PERMS=$(stat -c "%a" "${MLFLOW_DIR}" 2>/dev/null || echo "000")
ARTIFACTS_PERMS=$(stat -c "%a" "${ARTIFACTS_DIR}" 2>/dev/null || echo "000")

if [ "${MLFLOW_PERMS}" = "777" ] || [ "${MLFLOW_PERMS}" = "775" ]; then
    log_info "MLflow directory permissions: ${MLFLOW_PERMS} ✅"
else
    log_warn "MLflow directory permissions: ${MLFLOW_PERMS} (expected 777 or 775)"
fi

if [ "${ARTIFACTS_PERMS}" = "777" ] || [ "${ARTIFACTS_PERMS}" = "775" ]; then
    log_info "Artifacts directory permissions: ${ARTIFACTS_PERMS} ✅"
else
    log_warn "Artifacts directory permissions: ${ARTIFACTS_PERMS} (expected 777 or 775)"
fi

# Test write permissions if container is running
CONTAINER_NAME="trading-mlflow"
if podman ps --format "{{.Names}}" 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
    log_info "Testing write permissions in container..."
    
    # Test write in container
    if podman exec "${CONTAINER_NAME}" sh -c "test -w /mlflow/artifacts && echo 'WRITABLE'" 2>/dev/null | grep -q "WRITABLE"; then
        log_info "Container can write to /mlflow/artifacts ✅"
    else
        log_error "Container CANNOT write to /mlflow/artifacts"
        log_info "Attempting to fix permissions in container..."
        
        # Try to fix permissions from inside container
        podman exec "${CONTAINER_NAME}" sh -c "chmod -R 777 /mlflow /mlflow/artifacts" 2>/dev/null || {
            log_error "Failed to fix permissions from inside container"
            log_info "Try running: podman unshare chmod 777 ${MLFLOW_DIR} ${ARTIFACTS_DIR}"
            exit 1
        }
        
        log_info "Permissions fixed from inside container"
    fi
else
    log_warn "Container '${CONTAINER_NAME}' is not running (skipping container write test)"
fi

log_info "Permission check complete!"
echo ""
echo "Directories:"
ls -ld "${MLFLOW_DIR}"
ls -ld "${ARTIFACTS_DIR}"
