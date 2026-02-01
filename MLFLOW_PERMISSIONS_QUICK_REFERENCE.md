# MLflow Permissions - Quick Reference

## 🚀 Quick Fix

```bash
# Fix permissions now
bash scripts/ensure_mlflow_permissions.sh

# Verify it works
bash scripts/verify_mlflow_artifact_write.sh
```

## 🛡️ Prevention

```bash
# Run before starting containers (automatic in setup_infrastructure.sh)
bash scripts/prevent_mlflow_permission_issues.sh
```

## 🧪 Testing

```bash
# Test all permission aspects
python3 scripts/test_mlflow_permissions.py

# Test container write specifically
bash scripts/verify_mlflow_artifact_write.sh
```

## 📋 What Happens Automatically

1. **Before Container Start** (`setup_infrastructure.sh`):
   - ✅ Runs `prevent_mlflow_permission_issues.sh`
   - ✅ Ensures directories have 777 permissions

2. **After Container Start**:
   - ✅ Verifies container can write to `/mlflow/artifacts`
   - ✅ Fixes permissions from inside container if needed

## 🔧 Manual Fixes

```bash
# Option 1: Use fix script
bash scripts/ensure_mlflow_permissions.sh

# Option 2: Direct fix
podman unshare chmod -R 777 data/mlflow data/mlartifacts

# Option 3: Fix from container
podman exec trading-mlflow chmod -R 777 /mlflow /mlflow/artifacts
```

## ✅ Verification

```bash
# Check permissions
stat -c "%a" data/mlflow data/mlartifacts
# Should show: 777

# Test container write
podman exec trading-mlflow test -w /mlflow/artifacts && echo "OK" || echo "FAILED"
# Should show: OK
```

## 📊 Where Errors Occur

| Location | Error? | Fix |
|----------|--------|-----|
| HOST (your code) | ❌ NO | N/A |
| HOST (MLflow client) | ❌ NO | N/A |
| **CONTAINER (MLflow server)** | ✅ **YES** | **777 permissions** |
| HOST (bind mount) | ❌ NO | N/A |

**Answer**: Errors occur **IN THE CONTAINER** when MLflow server writes artifacts.
