#!/bin/bash
# Fix MLflow directory permissions for rootless Podman

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MLFLOW_DIR="${PROJECT_ROOT}/data/mlflow"
ARTIFACTS_DIR="${PROJECT_ROOT}/data/mlartifacts"

echo "🔧 Fixing MLflow directory permissions for rootless Podman..."

# Ensure directories exist
mkdir -p "${MLFLOW_DIR}"
mkdir -p "${ARTIFACTS_DIR}"

# Get the UID/GID that Podman maps root (0) to on the host
# In rootless Podman, this is typically the user's UID + some offset
# We'll use a more permissive approach: make directories group-writable

# Get current user's UID/GID
CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

echo "   Current user: ${CURRENT_UID}:${CURRENT_GID}"

# Option 1: Make directories writable by owner and group
# This works because Podman often maps container root to the user's UID
chmod 775 "${MLFLOW_DIR}" 2>/dev/null || true
chmod 775 "${ARTIFACTS_DIR}" 2>/dev/null || true

# Option 2: If that doesn't work, make world-writable (less secure but works)
# Uncomment if Option 1 doesn't work:
# chmod 777 "${MLFLOW_DIR}"
# chmod 777 "${ARTIFACTS_DIR}"

# Option 3: Change ownership to current user (if running as root)
if [ "${CURRENT_UID}" = "0" ]; then
    # Running as root - change to a non-root user if available
    if [ -n "${SUDO_USER}" ]; then
        TARGET_USER="${SUDO_USER}"
    else
        # Find first non-root user
        TARGET_USER=$(getent passwd | awk -F: '$3 >= 1000 && $3 != 65534 {print $1; exit}')
    fi
    
    if [ -n "${TARGET_USER}" ]; then
        echo "   Changing ownership to ${TARGET_USER}..."
        chown -R "${TARGET_USER}:${TARGET_USER}" "${MLFLOW_DIR}" 2>/dev/null || true
        chown -R "${TARGET_USER}:${TARGET_USER}" "${ARTIFACTS_DIR}" 2>/dev/null || true
    fi
else
    # Running as regular user - ensure we own the directories
    chown -R "${CURRENT_UID}:${CURRENT_GID}" "${MLFLOW_DIR}" 2>/dev/null || true
    chown -R "${CURRENT_UID}:${CURRENT_GID}" "${ARTIFACTS_DIR}" 2>/dev/null || true
fi

# Use podman unshare to fix permissions (works with rootless Podman)
# Podman rootless maps container UIDs to high UIDs on host
# Using podman unshare ensures we're in the same namespace as the container
echo "   Using podman unshare to fix permissions..."
podman unshare chmod 777 "${MLFLOW_DIR}" 2>/dev/null || {
    echo "   ⚠️  Warning: Could not set permissions via podman unshare"
    echo "   Trying direct chmod..."
    chmod 777 "${MLFLOW_DIR}" 2>/dev/null || {
        echo "   ❌ Failed: You may need to run: sudo chmod 777 ${MLFLOW_DIR}"
    }
}

podman unshare chmod 777 "${ARTIFACTS_DIR}" 2>/dev/null || {
    echo "   ⚠️  Warning: Could not set permissions via podman unshare"
    echo "   Trying direct chmod..."
    chmod 777 "${ARTIFACTS_DIR}" 2>/dev/null || {
        echo "   ❌ Failed: You may need to run: sudo chmod 777 ${ARTIFACTS_DIR}"
    }
}

# Verify permissions
echo ""
echo "✅ Permissions fixed:"
ls -ld "${MLFLOW_DIR}"
ls -ld "${ARTIFACTS_DIR}"

echo ""
echo "💡 If issues persist, check Podman UID mapping:"
echo "   podman unshare cat /proc/self/uid_map"
