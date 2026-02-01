# MLflow Database Status - Container Setup

## ✅ Database Status: **PERSISTED BUT EMPTY**

### Findings

1. **Container**: ✅ Running (`trading-mlflow`)
2. **Database**: ✅ Exists and persisted via bind mount
   - Location: `/home/sanky/projects/AutomatedTrading/data/mlflow/mlflow.db`
   - Size: 540 KB (528 KB)
   - Mount: `data/mlflow/` → `/mlflow` (bind mount)
3. **Experiments**: ✅ 1 experiment ("Default")
4. **Runs**: ❌ **0 runs** ← **THIS IS THE PROBLEM**

### Root Cause

**The database is persisted correctly, but no models have been logged yet.**

The MLflow UI shows nothing because there are no runs in the database. The database file exists and is properly mounted, but it's empty (no model training runs logged).

### Container Configuration

```bash
# Container mounts (bind mounts, not named volume):
bind /home/sanky/projects/AutomatedTrading/data/mlflow → /mlflow
bind /home/sanky/projects/AutomatedTrading/data/mlartifacts → /mlflow/artifacts

# MLflow server command:
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri sqlite:////mlflow/mlflow.db \
  --default-artifact-root /mlflow/artifacts
```

### Quick Check

Run this to verify database status:

```bash
python scripts/check_mlflow_db.py
```

Expected output:
```
✅ Database FOUND
   Path: /home/sanky/projects/AutomatedTrading/data/mlflow/mlflow.db
   Size: 540,672 bytes (0.52 MB)

📊 Experiments: 1
📊 Runs: 0  ← No models logged!

⚠️  CRITICAL: No runs found in database!
```

### Solution

**To see models in MLflow UI, you need to log models first:**

1. **Run model training**:
   ```bash
   python main.py --mode train
   ```

2. **Verify models are logged**:
   ```bash
   python scripts/check_mlflow_db.py
   # Should show Runs > 0
   ```

3. **Check MLflow UI**:
   - Open: http://localhost:5000
   - You should now see experiments and runs

### Debugging in VS Code

1. **Open**: `scripts/check_mlflow_db.py`
2. **Set breakpoint** at line ~25 (`check_database()`)
3. **Press F5** → Run debugger
4. **Watch**: `db_info["runs"]` - should be > 0 after training

### Verification Commands

```bash
# Check database directly
python scripts/check_mlflow_db.py

# Check container status
podman ps | grep trading-mlflow

# Check database file on host
ls -lh data/mlflow/mlflow.db

# Query database manually
python3 -c "
import sqlite3
conn = sqlite3.connect('data/mlflow/mlflow.db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM runs')
print(f'Runs: {cursor.fetchone()[0]}')
conn.close()
"
```

### Summary

| Item | Status | Details |
|------|--------|---------|
| Container | ✅ Running | `trading-mlflow` |
| Database File | ✅ Exists | `data/mlflow/mlflow.db` (540 KB) |
| Persistence | ✅ Working | Bind mount to host |
| Experiments | ✅ 1 | "Default" experiment |
| **Runs** | ❌ **0** | **No models logged** |

**Next Step**: Run model training to log models to MLflow.
