# MLflow Troubleshooting Guide

## Issue: Models Not Showing in MLflow UI

If you can access MLflow UI at `http://0.0.0.0:5000/#/` but don't see any experiments or runs, this guide will help diagnose and fix the issue.

## 🔍 Quick Diagnosis

Run the diagnostic script:

```bash
python scripts/diagnose_mlflow.py
```

Or debug it in VS Code:
1. Open `scripts/diagnose_mlflow.py`
2. Set breakpoints
3. Press `F5` → Select "Debug: MLflow Diagnostic"

## Common Causes

### 1. **Path Mismatch** (Most Common)

**Problem**: MLflow server uses relative paths, so database location depends on where server was started.

**Symptoms**:
- Server runs but shows no experiments
- Database file exists but in wrong location
- Models logged but not visible in UI

**Solution**:
The `mlflow_server.py` now automatically resolves paths to absolute. However, if you started the server manually or it's running from a different location:

1. **Stop the current MLflow server**
2. **Check where database actually is**:
   ```bash
   find . -name "mlflow.db" -type f
   find . -name "mlruns" -type d
   ```

3. **Restart server with absolute paths**:
   ```bash
   # Get project root
   PROJECT_ROOT=$(pwd)
   
   # Start MLflow with absolute paths
   mlflow server \
     --backend-store-uri "sqlite:///${PROJECT_ROOT}/mlflow.db" \
     --default-artifact-root "${PROJECT_ROOT}/mlruns" \
     --host 0.0.0.0 \
     --port 5000
   ```

### 2. **Tracking URI Mismatch**

**Problem**: Code connects to different URI than server.

**Check**:
```bash
# What code uses
python -c "from src import config; print(config.mlflow_config.tracking_uri)"

# What server is running on
curl http://localhost:5000/health
```

**Fix**: Ensure both use same URI:
- Server: `http://0.0.0.0:5000` (for binding)
- Client: `http://localhost:5000` (for connection)

### 3. **Database Not Created**

**Problem**: Database file doesn't exist yet.

**Check**:
```bash
ls -la mlflow.db
ls -la mlruns/
```

**Fix**: Run model training once to create database:
```bash
python main.py  # This will log models and create database
```

### 4. **Wrong Working Directory**

**Problem**: Server started from different directory than code runs.

**Solution**: Always start server from project root:
```bash
cd /home/sanky/projects/AutomatedTrading
python -m src.mlflow_utils.mlflow_server  # Or use the start function
```

## 🔧 Step-by-Step Fix

### Step 1: Diagnose Current State

```bash
python scripts/diagnose_mlflow.py
```

**Look for**:
- ✅ Server running?
- ✅ Database exists?
- ✅ Experiments found?
- ⚠️ Path issues?

### Step 2: Find Actual Database Location

```bash
# Find MLflow database
find ~ -name "mlflow.db" 2>/dev/null

# Find artifact directories
find ~ -type d -name "mlruns" 2>/dev/null
```

### Step 3: Stop Current Server

```bash
# Find MLflow process
ps aux | grep mlflow

# Kill it
pkill -f "mlflow server"
```

### Step 4: Update Configuration (if needed)

If paths are wrong, update `src/config/custom.yaml`:

```yaml
mlflow_config:
  backend_store_uri: "sqlite:////home/sanky/projects/AutomatedTrading/mlflow.db"
  default_artifact_root: "/home/sanky/projects/AutomatedTrading/mlruns"
```

**OR** use relative paths (they'll be auto-resolved by the startup code).

### Step 5: Restart Server

**Option A: Use Python function** (Recommended)
```python
from src.mlflow_utils.mlflow_server import start_mlflow_server
start_mlflow_server()
```

**Option B: Manual command**
```bash
cd /home/sanky/projects/AutomatedTrading
mlflow server \
  --backend-store-uri "sqlite:///$(pwd)/mlflow.db" \
  --default-artifact-root "$(pwd)/mlruns" \
  --host 0.0.0.0 \
  --port 5000
```

### Step 6: Verify

1. **Check server is running**:
   ```bash
   curl http://localhost:5000/health
   ```

2. **Check database**:
   ```bash
   python scripts/diagnose_mlflow.py
   ```

3. **List experiments**:
   ```python
   import mlflow
   mlflow.set_tracking_uri("http://localhost:5000")
   experiments = mlflow.search_experiments()
   print(f"Found {len(experiments)} experiments")
   ```

## 🐛 Debugging in VS Code

### Debug MLflow Connection

1. **Set breakpoints** in `scripts/diagnose_mlflow.py`:
   - Line ~50: `check_server_running()`
   - Line ~80: `mlflow.set_tracking_uri()`
   - Line ~90: `client.search_experiments()`

2. **Press F5** → Select "Debug: MLflow Diagnostic"

3. **Watch variables**:
   - `tracking_uri`
   - `backend_uri`
   - `experiments`
   - `db_path`

### Debug Model Logging

1. **Set breakpoints** in `src/pipelines/base_pipeline.py`:
   - Line ~736: `mlflow.set_tracking_uri()`
   - Line ~738: `mlflow.start_run()`
   - Line ~803: `mlflow.log_params()`

2. **Run training** and step through to see:
   - Tracking URI being set
   - Run being created
   - Parameters/metrics being logged

## 📋 Verification Checklist

- [ ] MLflow server is running (`curl http://localhost:5000/health`)
- [ ] Database file exists (`ls mlflow.db`)
- [ ] Artifact directory exists (`ls mlruns/`)
- [ ] Tracking URI matches (`MLFLOW_TRACKING_URI=http://localhost:5000`)
- [ ] Can list experiments (`mlflow.search_experiments()`)
- [ ] Models are being logged (check training logs)

## 🔍 Advanced Debugging

### Check Database Contents

```python
import sqlite3

conn = sqlite3.connect('mlflow.db')
cursor = conn.cursor()

# List tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Tables:", cursor.fetchall())

# Check experiments
cursor.execute("SELECT * FROM experiments LIMIT 5;")
print("Experiments:", cursor.fetchall())

conn.close()
```

### Check Artifact Storage

```bash
# List experiment directories
ls -la mlruns/

# Check specific experiment
ls -la mlruns/0/  # Experiment ID 0
ls -la mlruns/0/*/artifacts/  # Model artifacts
```

### Test MLflow Client Connection

```python
import mlflow
from mlflow.tracking import MlflowClient

# Set tracking URI
mlflow.set_tracking_uri("http://localhost:5000")

# Create client
client = MlflowClient()

# List experiments
experiments = client.search_experiments()
print(f"Found {len(experiments)} experiments")

# List runs from first experiment
if experiments:
    runs = client.search_runs(experiment_ids=[experiments[0].experiment_id])
    print(f"Found {len(runs)} runs in {experiments[0].name}")
```

## 💡 Prevention

1. **Always use absolute paths** in config (or let code resolve them)
2. **Start server from project root**
3. **Use environment variable** for tracking URI: `export MLFLOW_TRACKING_URI=http://localhost:5000`
4. **Check paths match** before starting server

## 🚀 Quick Fix Script

Create `scripts/fix_mlflow_paths.sh`:

```bash
#!/bin/bash
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "Fixing MLflow paths..."
echo "Project root: $PROJECT_ROOT"

# Stop existing server
pkill -f "mlflow server" || true

# Start with absolute paths
cd "$PROJECT_ROOT"
mlflow server \
  --backend-store-uri "sqlite:///${PROJECT_ROOT}/mlflow.db" \
  --default-artifact-root "${PROJECT_ROOT}/mlruns" \
  --host 0.0.0.0 \
  --port 5000 &

echo "MLflow server started"
echo "Database: ${PROJECT_ROOT}/mlflow.db"
echo "Artifacts: ${PROJECT_ROOT}/mlruns"
echo "UI: http://localhost:5000"
```

---

**Next Steps**: Run `python scripts/diagnose_mlflow.py` to see current state and get specific recommendations.
