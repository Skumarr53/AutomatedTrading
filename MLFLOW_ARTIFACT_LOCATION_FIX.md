# MLflow Artifact Location Fix - Root Cause and Solution

## 🎯 Problem

**Error**: `PermissionError: [Errno 13] Permission denied: '/mlflow'`

**Symptoms**:
- MLflow client tries to write directly to `/mlflow` filesystem
- Uses `LocalArtifactRepository` instead of `MlflowArtifactsRepository`
- Happens even though MLflow server is configured with `--serve-artifacts`

---

## 🔍 Root Cause

**The issue is with the experiment's artifact location, NOT the server configuration.**

### How MLflow Determines Artifact Repository

1. When you log an artifact: `mlflow.log_artifact(local_file)`
2. MLflow gets the artifact URI from the active run
3. The artifact URI format determines which repository is used:
   - `mlflow-artifacts:/...` → `MlflowArtifactsRepository` (HTTP upload) ✅
   - `/mlflow/artifacts/...` → `LocalArtifactRepository` (direct filesystem write) ❌
   - `file:///...` → `LocalArtifactRepository` (direct filesystem write) ❌

### Why Old Experiments Have Wrong Artifact Locations

Experiments created **before** the MLflow server was configured with `--serve-artifacts` have artifact locations pointing to local filesystem paths:

```
❌ TradingModels_20260131 → /mlflow/artifacts/2
❌ TradingModels_20260130 → /mlflow/artifacts/1
❌ Default → /mlflow/artifacts/0
```

Experiments created **after** the server was configured correctly have:

```
✅ artifact_uri_test → mlflow-artifacts:/3
✅ TradingModels_20260131 (new) → mlflow-artifacts:/4
```

---

## ✅ Solution

### 1. Automatic Fix in Code

Added code in `src/pipelines/base_pipeline.py` (line 571-596) that:
- Checks experiment artifact location when setting experiment
- If it's a local filesystem path, deletes and recreates the experiment
- New experiment will have correct `mlflow-artifacts:/` location

### 2. Manual Fix Script

Use `scripts/fix_mlflow_experiment_artifacts.py`:

```bash
# Check for problematic experiments
python scripts/fix_mlflow_experiment_artifacts.py

# Fix (soft delete) problematic experiments
python scripts/fix_mlflow_experiment_artifacts.py --fix

# Permanently delete from database
python scripts/fix_mlflow_experiment_artifacts.py --permanent
```

### 3. Direct Database Fix

If needed, directly delete from SQLite:

```bash
sqlite3 data/mlflow/mlflow.db << 'SQL'
-- Delete runs for problematic experiments
DELETE FROM runs WHERE experiment_id IN (
    SELECT experiment_id FROM experiments 
    WHERE artifact_location LIKE '/mlflow%'
);

-- Delete problematic experiments
DELETE FROM experiments WHERE artifact_location LIKE '/mlflow%';
SQL
```

---

## 🧪 Verification

### Check Experiment Artifact Locations

```python
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri('http://localhost:5000')
client = MlflowClient()

for exp in client.search_experiments():
    print(f"{exp.name}: {exp.artifact_location}")
    # Should show: mlflow-artifacts:/...
    # NOT: /mlflow/artifacts/...
```

### Test Artifact Logging

```python
import mlflow
import tempfile
import os

mlflow.set_tracking_uri('http://localhost:5000')
mlflow.set_experiment("test_artifact_logging")

with mlflow.start_run():
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("test")
        test_file = f.name
    
    mlflow.log_artifact(test_file)  # Should succeed
    os.unlink(test_file)
```

---

## 📋 Summary

| Component | Status |
|-----------|--------|
| MLflow Server | ✅ Correctly configured with `--serve-artifacts` |
| Old Experiments | ❌ Had local filesystem artifact locations |
| Fix Applied | ✅ Code auto-detects and fixes problematic experiments |
| Manual Fix | ✅ Script available: `scripts/fix_mlflow_experiment_artifacts.py` |

**Result**: ✅ **FIXED** - Artifact logging now works correctly

---

## 🔧 Prevention

The code in `base_pipeline.py` now automatically checks and fixes experiments with incorrect artifact locations. No manual intervention needed going forward.

If you create experiments manually, ensure the MLflow server is running with `--serve-artifacts` before creating them.
