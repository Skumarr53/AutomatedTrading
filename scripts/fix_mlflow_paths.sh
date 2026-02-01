#!/bin/bash
# Quick fix for MLflow path issues

set -e

PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "=========================================="
echo "MLflow Path Fix Script"
echo "=========================================="
echo "Project root: $PROJECT_ROOT"
echo ""

# Stop existing MLflow server
echo "1. Stopping existing MLflow server..."
pkill -f "mlflow server" || echo "   No MLflow server running"
sleep 2

# Find existing database
echo ""
echo "2. Searching for existing MLflow database..."
DB_LOCATIONS=$(find "$PROJECT_ROOT" -name "mlflow.db" -type f 2>/dev/null || true)
if [ -n "$DB_LOCATIONS" ]; then
    echo "   Found database(s):"
    echo "$DB_LOCATIONS" | while read -r db; do
        size=$(du -h "$db" | cut -f1)
        echo "     - $db ($size)"
    done
else
    echo "   No database found (will be created on first run)"
fi

# Find existing artifact directories
echo ""
echo "3. Searching for artifact directories..."
ARTIFACT_DIRS=$(find "$PROJECT_ROOT" -type d -name "mlruns" 2>/dev/null || true)
if [ -n "$ARTIFACT_DIRS" ]; then
    echo "   Found artifact directory(ies):"
    echo "$ARTIFACT_DIRS" | while read -r dir; do
        exp_count=$(find "$dir" -mindepth 1 -maxdepth 1 -type d | wc -l)
        echo "     - $dir ($exp_count experiments)"
    done
else
    echo "   No artifact directory found (will be created on first run)"
fi

# Start server with absolute paths
echo ""
echo "4. Starting MLflow server with absolute paths..."
cd "$PROJECT_ROOT"

DB_PATH="${PROJECT_ROOT}/mlflow.db"
ARTIFACT_PATH="${PROJECT_ROOT}/mlruns"

# Ensure artifact directory exists
mkdir -p "$ARTIFACT_PATH"

# Start server in background
nohup mlflow server \
  --backend-store-uri "sqlite:///${DB_PATH}" \
  --default-artifact-root "${ARTIFACT_PATH}" \
  --host 0.0.0.0 \
  --port 5000 > /tmp/mlflow_server.log 2>&1 &

SERVER_PID=$!
echo "   Server started (PID: $SERVER_PID)"
echo "   Database: $DB_PATH"
echo "   Artifacts: $ARTIFACT_PATH"
echo "   UI: http://localhost:5000"
echo "   Logs: /tmp/mlflow_server.log"

# Wait for server to start
echo ""
echo "5. Waiting for server to start..."
sleep 5

# Check if server is running
if curl -s http://localhost:5000/health > /dev/null 2>&1; then
    echo "   ✅ Server is running"
else
    echo "   ⚠️  Server may not be ready yet, check logs: tail -f /tmp/mlflow_server.log"
fi

echo ""
echo "=========================================="
echo "Next Steps:"
echo "1. Run: python scripts/diagnose_mlflow.py"
echo "2. Check UI: http://localhost:5000"
echo "3. If no experiments, run model training"
echo "=========================================="
