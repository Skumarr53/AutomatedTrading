# MLflow Permission Fix & Prevention - Complete Solution

## 🎯 Problem Solved

**Issue**: Permission denied errors when MLflow server writes artifacts to `/mlflow/artifacts/`

**Root Cause**: MLflow server (running as root/UID 0) cannot write to directories owned by UID 1001 if permissions are 755

**Solution**: Ensure directories have 777 permissions so root can write

---

## ✅ Fixes Implemented

### 1. Permission Fix Script (`scripts/ensure_mlflow_permissions.sh`)

**Purpose**: Fixes permissions and verifies they work

**Features**:
- ✅ Uses `podman unshare` for rootless Podman compatibility
- ✅ Sets 777 permissions on directories
- ✅ Verifies permissions are correct
- ✅ Tests container write permissions if container is running
- ✅ Provides clear error messages

**Usage**:
```bash
bash scripts/ensure_mlflow_permissions.sh
```

### 2. Permission Prevention Script (`scripts/prevent_mlflow_permission_issues.sh`)

**Purpose**: Ensures permissions are correct before starting containers

**Features**:
- ✅ Runs permission fix automatically
- ✅ Verifies permissions before proceeding
- ✅ Fails fast if permissions can't be fixed

**Usage**:
```bash
bash scripts/prevent_mlflow_permission_issues.sh
```

### 3. Automatic Fix in Setup (`scripts/setup_infrastructure.sh`)

**Purpose**: Automatically fixes permissions when starting MLflow container

**Features**:
- ✅ Runs permission prevention before starting container
- ✅ Verifies container can write after startup
- ✅ Fixes permissions from inside container if needed

**Location**: Lines 259-300 in `setup_infrastructure.sh`

### 4. Container Write Verification (`scripts/verify_mlflow_artifact_write.sh`)

**Purpose**: Health check to verify container can write artifacts

**Features**:
- ✅ Tests container write permissions
- ✅ Attempts to fix if permissions are wrong
- ✅ Performs actual write test
- ✅ Can be run periodically or as health check

**Usage**:
```bash
bash scripts/verify_mlflow_artifact_write.sh
```

### 5. Comprehensive Test Suite (`scripts/test_mlflow_permissions.py`)

**Purpose**: Tests all aspects of permission flow

**Features**:
- ✅ Tests host-side temp directory creation
- ✅ Tests container-side write permissions
- ✅ Tests end-to-end MLflow artifact logging
- ✅ Provides detailed test results

**Usage**:
```bash
python3 scripts/test_mlflow_permissions.py
```

---

## 🛡️ Prevention Mechanisms

### Automatic Prevention

1. **On Container Start** (`setup_infrastructure.sh`):
   ```bash
   # Automatically runs before starting MLflow container
   bash scripts/prevent_mlflow_permission_issues.sh
   ```

2. **Permission Verification After Start**:
   ```bash
   # Verifies container can write after startup
   # Fixes permissions from inside container if needed
   ```

### Manual Prevention

Run before starting containers:
```bash
bash scripts/prevent_mlflow_permission_issues.sh
```

### Periodic Health Checks

Add to cron or monitoring:
```bash
# Run every hour to verify permissions
0 * * * * cd /path/to/project && bash scripts/verify_mlflow_artifact_write.sh
```

---

## 🧪 Testing

### Test Permission Fix:
```bash
bash scripts/ensure_mlflow_permissions.sh
```

**Expected Output**:
```
✓ Ensuring MLflow directory permissions...
✓ Fixing permissions via podman unshare...
✓ Permissions set via podman unshare
✓ Verifying permissions...
✓ MLflow directory permissions: 777 ✅
✓ Artifacts directory permissions: 777 ✅
✓ Testing write permissions in container...
✓ Container can write to /mlflow/artifacts ✅
```

### Test Full Flow:
```bash
python3 scripts/test_mlflow_permissions.py
```

**Expected Output**:
```
TEST SUMMARY
  HOST            ✅ PASSED
  CONTAINER       ✅ PASSED
  MLFLOW          ✅ PASSED

  Total: 3 passed, 0 failed, 0 skipped
✅ All tests passed!
```

### Test Container Write:
```bash
bash scripts/verify_mlflow_artifact_write.sh
```

**Expected Output**:
```
✅ Container can write to /mlflow/artifacts
✅ Container successfully wrote test file
✅ Write test passed
```

---

## 📋 Integration Points

### 1. Container Startup (`setup_infrastructure.sh`)

**Before starting container**:
```bash
bash scripts/prevent_mlflow_permission_issues.sh
```

**After container starts**:
```bash
# Verify container can write
podman exec trading-mlflow sh -c "test -w /mlflow/artifacts"
```

### 2. CI/CD Pipeline

Add to your CI/CD:
```bash
# Before deploying
bash scripts/prevent_mlflow_permission_issues.sh

# After deployment
python3 scripts/test_mlflow_permissions.py
```

### 3. Monitoring/Health Checks

Add to monitoring:
```bash
# Health check script
bash scripts/verify_mlflow_artifact_write.sh
```

---

## 🔧 Manual Fix (If Needed)

If permissions get messed up:

```bash
# Option 1: Use the fix script
bash scripts/ensure_mlflow_permissions.sh

# Option 2: Manual fix
podman unshare chmod -R 777 data/mlflow data/mlartifacts

# Option 3: Fix from inside container
podman exec trading-mlflow chmod -R 777 /mlflow /mlflow/artifacts
```

---

## 📊 Verification Checklist

After implementing fixes, verify:

- [ ] `bash scripts/ensure_mlflow_permissions.sh` succeeds
- [ ] `python3 scripts/test_mlflow_permissions.py` passes all tests
- [ ] `bash scripts/verify_mlflow_artifact_write.sh` succeeds
- [ ] Container can write: `podman exec trading-mlflow test -w /mlflow/artifacts`
- [ ] MLflow artifact logging works in actual training runs
- [ ] Artifacts appear in MLflow UI

---

## 🎯 Summary

### What Was Fixed:

1. ✅ **Permission Fix Script**: Robust script to fix permissions
2. ✅ **Prevention Script**: Ensures permissions before container start
3. ✅ **Automatic Fix**: Integrated into setup script
4. ✅ **Health Check**: Verifies permissions work
5. ✅ **Test Suite**: Comprehensive tests

### Prevention Mechanisms:

1. ✅ **Automatic**: Runs before container start
2. ✅ **Verification**: Checks permissions after start
3. ✅ **Health Checks**: Can be run periodically
4. ✅ **Manual**: Scripts available for manual fixes

### Result:

✅ **Permission issues are prevented and automatically fixed**

The system now:
- ✅ Fixes permissions automatically before starting containers
- ✅ Verifies permissions after container starts
- ✅ Provides health checks to catch issues early
- ✅ Has comprehensive tests to verify everything works

**Status**: ✅ **FIXED AND PREVENTED**
