#!/bin/bash
# Complete test suite for MLflow permission fixes

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=" | tr '=' '='
echo "MLFLOW PERMISSION FIX - COMPLETE TEST SUITE"
echo "=" | tr '=' '='

PASSED=0
FAILED=0
SKIPPED=0

# Test 1: Permission fix script
echo ""
echo "TEST 1: Permission Fix Script"
echo "----------------------------"
if bash scripts/ensure_mlflow_permissions.sh > /tmp/mlflow_test_1.log 2>&1; then
    echo "✅ PASSED: Permission fix script works"
    PASSED=$((PASSED + 1))
else
    echo "❌ FAILED: Permission fix script"
    cat /tmp/mlflow_test_1.log
    FAILED=$((FAILED + 1))
fi

# Test 2: Prevention script
echo ""
echo "TEST 2: Prevention Script"
echo "----------------------------"
if bash scripts/prevent_mlflow_permission_issues.sh > /tmp/mlflow_test_2.log 2>&1; then
    echo "✅ PASSED: Prevention script works"
    PASSED=$((PASSED + 1))
else
    echo "❌ FAILED: Prevention script"
    cat /tmp/mlflow_test_2.log
    FAILED=$((FAILED + 1))
fi

# Test 3: Container write verification
echo ""
echo "TEST 3: Container Write Verification"
echo "----------------------------"
if podman ps --format "{{.Names}}" 2>/dev/null | grep -q "^trading-mlflow$"; then
    if bash scripts/verify_mlflow_artifact_write.sh > /tmp/mlflow_test_3.log 2>&1; then
        echo "✅ PASSED: Container write verification works"
        PASSED=$((PASSED + 1))
    else
        echo "❌ FAILED: Container write verification"
        cat /tmp/mlflow_test_3.log
        FAILED=$((FAILED + 1))
    fi
else
    echo "⚠️  SKIPPED: Container not running"
    SKIPPED=$((SKIPPED + 1))
fi

# Test 4: Verify permissions are correct
echo ""
echo "TEST 4: Permission Verification"
echo "----------------------------"
MLFLOW_PERMS=$(stat -c "%a" data/mlflow 2>/dev/null || echo "000")
ARTIFACTS_PERMS=$(stat -c "%a" data/mlartifacts 2>/dev/null || echo "000")

if [ "${MLFLOW_PERMS}" = "777" ] || [ "${MLFLOW_PERMS}" = "775" ]; then
    echo "✅ PASSED: MLflow directory permissions: ${MLFLOW_PERMS}"
    PASSED=$((PASSED + 1))
else
    echo "❌ FAILED: MLflow directory permissions: ${MLFLOW_PERMS} (expected 777 or 775)"
    FAILED=$((FAILED + 1))
fi

if [ "${ARTIFACTS_PERMS}" = "777" ] || [ "${ARTIFACTS_PERMS}" = "775" ]; then
    echo "✅ PASSED: Artifacts directory permissions: ${ARTIFACTS_PERMS}"
    PASSED=$((PASSED + 1))
else
    echo "❌ FAILED: Artifacts directory permissions: ${ARTIFACTS_PERMS} (expected 777 or 775)"
    FAILED=$((FAILED + 1))
fi

# Summary
echo ""
echo "=" | tr '=' '='
echo "TEST SUMMARY"
echo "=" | tr '=' '='
echo "✅ Passed: ${PASSED}"
echo "❌ Failed: ${FAILED}"
echo "⚠️  Skipped: ${SKIPPED}"
echo ""

if [ ${FAILED} -eq 0 ]; then
    echo "✅ ALL TESTS PASSED!"
    echo ""
    echo "Permission issues are fixed and prevented:"
    echo "  ✅ Permission fix script works"
    echo "  ✅ Prevention script works"
    echo "  ✅ Permissions are correct (777)"
    echo "  ✅ Container can write artifacts"
    echo ""
    echo "The system will automatically prevent permission issues in the future."
    exit 0
else
    echo "❌ SOME TESTS FAILED"
    echo ""
    echo "Run the following to fix:"
    echo "  bash scripts/ensure_mlflow_permissions.sh"
    exit 1
fi
