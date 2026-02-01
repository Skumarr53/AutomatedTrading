# MLflow Container Database Debugging Guide

## Issue: Models Not Showing in MLflow UI (Container Setup)

When MLflow runs in a Podman container, the database persistence depends on volume mounts. This guide helps debug why models aren't visible.

## 🔍 Quick Diagnosis

### Step 1: Check Database Directly (Easiest - Works with Bind Mounts)

```bash
# Quick database check (works even if container commands fail)
python scripts/check_mlflow_db.py
```

This will show:
- ✅ Database exists and size
- ✅ Number of experiments
- ✅ Number of runs (CRITICAL - if 0, no models logged!)
- ✅ Experiment list
- ✅ Latest runs

### Step 2: Check Container Status

```bash
# Check if container is running
podman ps | grep trading-mlflow

# Check container logs
podman logs trading-mlflow

# Check container mounts
podman inspect trading-mlflow --format '{{range .Mounts}}{{.Type}} {{.Source}} {{.Destination}} {{end}}'
```

### Step 2: Verify Volume Mounts

```bash
# Inspect container mounts
podman inspect trading-mlflow --format '{{range .Mounts}}{{.Source}}:{{.Destination}} {{end}}'

# Check Podman volume location
podman volume inspect mlflow-data
```

### Step 3: Check Database in Container

```bash
# List files in /mlflow directory
podman exec trading-mlflow ls -la /mlflow

# Check if database exists
podman exec trading-mlflow test -f /mlflow/mlflow.db && echo "Database exists" || echo "Database NOT found"

# Check database size
podman exec trading-mlflow stat -c "%s" /mlflow/mlflow.db

# Query database (if sqlite3 available)
podman exec trading-mlflow sqlite3 /mlflow/mlflow.db "SELECT COUNT(*) FROM experiments;"
```

## 🐛 VS Code Debugging

### Debug Container Check Script

1. **Open** `scripts/check_mlflow_container.py`
2. **Set breakpoints** at:
   - Line ~50: `check_container_status()` - Check if container exists/running
   - Line ~80: `check_volume_mounts()` - Verify volume mounts
   - Line ~120: `check_database_in_container()` - Check database file
   - Line ~160: `check_database_contents()` - Query database
3. **Press F5** → Select "Debug: MLflow Container Check"
4. **Watch variables**:
   - `container_status` - Container existence and running state
   - `mounts["mlflow_volume"]` - Volume mount details
   - `db_check["database_exists"]` - Database file existence
   - `db_contents["experiments"]` - Number of experiments
5. **Step through** to see:
   - Container status check
   - Volume mount resolution
   - Database file verification
   - Database content queries

### Debug Database Query

**Goal**: See what's actually in the database

1. **Set breakpoint** at line ~160 in `check_database_contents()`
2. **Watch** the `queries` dictionary
3. **Step into** SQLite query execution
4. **Inspect** `result` dictionary for experiment/run counts

## 📋 Common Issues & Fixes

### Issue 1: Container Not Running

**Symptoms**:
- `podman ps` shows no `trading-mlflow`
- UI not accessible

**Debug**:
```python
# In VS Code debugger, check:
container_status = check_container_status()
# Watch: container_status["running"]
```

**Fix**:
```bash
podman start trading-mlflow
# Or
podman-compose up -d mlflow
```

### Issue 2: Volume Not Mounted

**Symptoms**:
- Container runs but database not found
- `/mlflow` directory empty

**Debug**:
```python
# In VS Code debugger:
mounts = check_volume_mounts()
# Watch: mounts["mlflow_volume"]
# Check: mounts["mlflow_volume"]["source"]
```

**Fix**:
```bash
# Check volume exists
podman volume ls | grep mlflow-data

# If missing, create it
podman volume create mlflow-data

# Restart container
podman restart trading-mlflow
```

### Issue 3: Database Empty (No Experiments)

**Symptoms**:
- Database file exists
- Size is small (< 100KB)
- No experiments when querying

**Debug**:
```python
# In VS Code debugger:
db_contents = check_database_contents()
# Watch: db_contents["experiments"]
# Should be > "0" if models were logged
```

**Fix**:
- Models haven't been logged yet
- Run model training: `python main.py` (training mode)
- Check training logs for MLflow logging errors

### Issue 4: Database in Wrong Location

**Symptoms**:
- Database exists but not in `/mlflow`
- Volume mounted to different path

**Debug**:
```bash
# Check where database actually is
podman exec trading-mlflow find / -name "mlflow.db" 2>/dev/null

# Check volume mount point
podman volume inspect mlflow-data | grep Mountpoint
```

**Fix**:
- Update container to mount volume correctly
- Or update MLflow server command to use correct path

### Issue 5: Volume Location on Host

**Symptoms**:
- Need to access database from host
- Want to backup database

**Find Volume Location**:
```bash
# Get Podman volume mountpoint
podman volume inspect mlflow-data

# Example output:
# [
#     {
#         "Name": "mlflow-data",
#         "Mountpoint": "/home/sanky/.local/share/containers/storage/volumes/mlflow-data/_data"
#     }
# ]

# Access database from host
sqlite3 ~/.local/share/containers/storage/volumes/mlflow-data/_data/mlflow.db "SELECT COUNT(*) FROM experiments;"
```

## 🔧 Step-by-Step Container Debugging

### 1. Check Container Status

**In VS Code**:
- Set breakpoint in `check_container_status()`
- Watch `container_status` variable
- Verify `running == True`

**Manual**:
```bash
podman ps --filter "name=trading-mlflow"
```

### 2. Inspect Volume Mounts

**In VS Code**:
- Set breakpoint in `check_volume_mounts()`
- Watch `mounts["mlflow_volume"]`
- Verify `destination == "/mlflow"`

**Manual**:
```bash
podman inspect trading-mlflow | grep -A 10 Mounts
```

### 3. Verify Database File

**In VS Code**:
- Set breakpoint in `check_database_in_container()`
- Watch `db_check["database_exists"]`
- Check `db_check["database_size_bytes"]`

**Manual**:
```bash
podman exec trading-mlflow ls -lh /mlflow/mlflow.db
```

### 4. Query Database Contents

**In VS Code**:
- Set breakpoint in `check_database_contents()`
- Watch `db_contents["experiments"]`
- Watch `db_contents["runs"]`

**Manual**:
```bash
podman exec trading-mlflow sqlite3 /mlflow/mlflow.db <<EOF
SELECT COUNT(*) as experiments FROM experiments;
SELECT COUNT(*) as runs FROM runs;
SELECT name FROM experiments LIMIT 5;
EOF
```

### 5. Check Server Configuration

**In VS Code**:
- Set breakpoint in container startup code
- Check environment variables
- Verify `--backend-store-uri` and `--default-artifact-root`

**Manual**:
```bash
podman exec trading-mlflow env | grep MLFLOW
podman exec trading-mlflow ps aux | grep mlflow
```

## 🎯 Quick Fixes

### Fix 1: Restart Container with Correct Mounts

```bash
# Stop container
podman stop trading-mlflow
podman rm trading-mlflow

# Ensure volume exists
podman volume create mlflow-data

# Start with correct mounts (check compose.yml or setup script)
podman-compose up -d mlflow
```

### Fix 2: Copy Database to Container

If database exists on host but not in container:

```bash
# Find database on host
find ~ -name "mlflow.db" 2>/dev/null

# Copy to volume (if using bind mount)
# Or exec into container and check
podman exec -it trading-mlflow /bin/bash
```

### Fix 3: Verify Container MLflow Command

Check how MLflow server is started in container:

```bash
# Check container command
podman inspect trading-mlflow --format '{{.Config.Cmd}}'

# Check logs for startup command
podman logs trading-mlflow | head -20
```

## 📊 Expected Output

When everything works:

```
Container Status: ✅ Running
Volume Mount: ✅ mlflow-data:/mlflow
Database: ✅ /mlflow/mlflow.db (2.5 MB)
Experiments: 5
Runs: 150
Server: ✅ Healthy
```

## 🚀 Debugging Workflow

1. **Run diagnostic**: `python scripts/check_mlflow_container.py`
2. **If container not running**: Start it
3. **If volume not mounted**: Fix volume mount
4. **If database empty**: Run model training
5. **If database exists but UI empty**: Check tracking URI
6. **Verify in UI**: http://localhost:5000

## 💡 Pro Tips

1. **Access container shell**:
   ```bash
   podman exec -it trading-mlflow /bin/bash
   ```

2. **Check volume from host**:
   ```bash
   VOLUME_PATH=$(podman volume inspect mlflow-data | grep Mountpoint | cut -d'"' -f4)
   ls -la "$VOLUME_PATH"
   ```

3. **Backup database**:
   ```bash
   podman exec trading-mlflow tar czf /tmp/mlflow_backup.tar.gz /mlflow
   podman cp trading-mlflow:/tmp/mlflow_backup.tar.gz .
   ```

4. **Watch container logs**:
   ```bash
   podman logs -f trading-mlflow
   ```

---

**Next Steps**: Run `python scripts/check_mlflow_container.py` to get detailed container diagnostics!
