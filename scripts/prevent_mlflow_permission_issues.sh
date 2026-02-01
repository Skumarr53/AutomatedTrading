#!/bin/bash
# Prevent MLflow Permission Issues - Run this before starting containers
# This ensures permissions are always correct

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="${PROJECT_ROOT}/scripts"

echo "🛡️  Preventing MLflow Permission Issues..."

# Run permission fix script
bash "${SCRIPT_DIR}/ensure_mlflow_permissions.sh"

# Verify permissions are correct
MLFLOW_DIR="${PROJECT_ROOT}/data/mlflow"
ARTIFACTS_DIR="${PROJECT_ROOT}/data/mlartifacts"

MLFLOW_PERMS=$(stat -c "%a" "${MLFLOW_DIR}" 2>/dev/null || echo "000")
ARTIFACTS_PERMS=$(stat -c "%a" "${ARTIFACTS_DIR}" 2>/dev/null || echo "000")

if [ "${MLFLOW_PERMS}" != "777" ] && [ "${MLFLOW_PERMS}" != "775" ]; then
    echo "❌ MLflow directory permissions incorrect: ${MLFLOW_PERMS}"
    echo "   Expected: 777 or 775"
    exit 1
fi

if [ "${ARTIFACTS_PERMS}" != "777" ] && [ "${ARTIFACTS_PERMS}" != "775" ]; then
    echo "❌ Artifacts directory permissions incorrect: ${ARTIFACTS_PERMS}"
    echo "   Expected: 777 or 775"
    exit 1
fi

echo "✅ Permissions verified - ready to start containers"
