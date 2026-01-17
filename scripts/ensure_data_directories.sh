#!/bin/bash
# scripts/ensure_data_directories.sh
# Ensures all required data directories exist with proper permissions
# This script should be run before starting containers

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data"

echo "Creating data directories in project folder: $DATA_DIR"

# Create main data directory
mkdir -p "$DATA_DIR"

# Create subdirectories for each service
mkdir -p "$DATA_DIR/influxdb"
mkdir -p "$DATA_DIR/influxdb-config"
mkdir -p "$DATA_DIR/mlflow"
mkdir -p "$DATA_DIR/mlflow/artifacts"
mkdir -p "$DATA_DIR/backups"
mkdir -p "$DATA_DIR/grafana"
mkdir -p "$DATA_DIR/logs"

# Set permissions (read/write for owner, read for group/others)
chmod 755 "$DATA_DIR"
chmod 755 "$DATA_DIR/influxdb"
chmod 755 "$DATA_DIR/influxdb-config"
chmod 755 "$DATA_DIR/mlflow"
chmod 755 "$DATA_DIR/mlflow/artifacts"
chmod 755 "$DATA_DIR/backups"
chmod 755 "$DATA_DIR/grafana"
chmod 755 "$DATA_DIR/logs"

echo "✓ Data directories created successfully"
echo "  - InfluxDB: $DATA_DIR/influxdb"
echo "  - MLflow: $DATA_DIR/mlflow"
echo "  - Backups: $DATA_DIR/backups"
echo ""
echo "Note: These directories persist data across container restarts"
echo "      and are stored in the project folder, not in OS partition."
