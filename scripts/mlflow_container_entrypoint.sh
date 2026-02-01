#!/bin/bash
# MLflow Container Entrypoint Script
# Ensures permissions are correct before starting MLflow server

set -e

echo "🔧 MLflow Container Entrypoint: Checking permissions..."

# Fix permissions on startup (in case they were changed)
if [ -d "/mlflow" ]; then
    chmod -R 777 /mlflow 2>/dev/null || true
    echo "✓ Fixed /mlflow permissions"
fi

if [ -d "/mlflow/artifacts" ]; then
    chmod -R 777 /mlflow/artifacts 2>/dev/null || true
    echo "✓ Fixed /mlflow/artifacts permissions"
fi

# Verify write permissions
if [ -w "/mlflow/artifacts" ]; then
    echo "✅ Write permissions verified: /mlflow/artifacts"
else
    echo "⚠️  Warning: Cannot write to /mlflow/artifacts"
    echo "   Attempting to fix..."
    chmod -R 777 /mlflow/artifacts 2>/dev/null || {
        echo "❌ Failed to fix permissions"
        exit 1
    }
fi

# Test write
test_file="/mlflow/artifacts/.permission_test"
if echo "test" > "$test_file" 2>/dev/null; then
    rm -f "$test_file"
    echo "✅ Write test successful"
else
    echo "❌ Write test failed - permissions are incorrect"
    exit 1
fi

echo "✅ Permission checks complete, starting MLflow server..."

# Execute the original command
exec "$@"
