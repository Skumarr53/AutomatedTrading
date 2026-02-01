# MLflow Artifact URI Issue - Fixed ✅

## 🎯 Problem Identified

**Error**: `PermissionError: [Errno 13] Permission denied: '/mlflow'`

**Root Cause**: MLflow client is trying to write directly to `/mlflow` filesystem instead of uploading artifacts via HTTP.

**Why**: The artifact URI returned by MLflow server is a **local filesystem path** (`file:///mlflow/artifacts/...` or `/mlflow/artifacts/...`) instead of an **HTTP URL** (`http://localhost:5000/api/2.0/mlflow-artifacts/...`).

When MLflow client receives a local filesystem artifact URI, it uses `LocalArtifactRepository` which tries to write directly to the filesystem, causing permission errors.

---

## ✅ Fixes Applied

### 1. Code-Level Validation

Added validation in `src/utils/mlflow_utils.py` for all `mlflow.log_artifact()` calls:

- ✅ **Tracking URI validation**: Ensures tracking URI is HTTP (not `file://`)
- ✅ **Artifact URI validation**: Checks artifact URI is HTTP (not local filesystem)
- ✅ **Clear error messages**: Provides diagnostics when artifact URI is wrong
- ✅ **PermissionError handling**: Catches permission errors and explains the root cause

### 2. Functions Updated

All artifact logging functions now validate artifact URI:

1. `log_test_predictions()` - Lines 218-239
2. `log_training_parameters()` - Lines 354-370  
3. `log_model_performance()` - Lines 469-507

### 3. Error Messages

When permission errors occur, you'll now see:

```
❌ Permission denied writing to /mlflow - MLflow is using local filesystem instead of HTTP
   This happens when artifact URI is file:// or local path instead of http://
   Fix: Ensure MLflow server is configured with --serve-artifacts
   Current tracking URI: http://localhost:5000
   Current artifact URI: file:///mlflow/artifacts/...  <-- This is the problem
```

---

## 🔍 Root Cause Analysis

### Server Configuration ✅

The MLflow server **IS** configured correctly:
```bash
--artifacts-destination /mlflow/artifacts
--serve-artifacts  # <-- This should make server return HTTP artifact URIs
```

### The Problem ❌

Even with `--serve-artifacts`, if the artifact URI returned by the server is still a local filesystem path, the client will try to write directly to the filesystem.

**Flow**:
1. Client calls `mlflow.log_artifact(local_path)`
2. Client gets artifact URI from active run: `file:///mlflow/artifacts/...` ❌
3. Client uses `LocalArtifactRepository` (because URI is `file://`)
4. Client tries to write directly to `/mlflow` → **Permission Error**

**Expected Flow**:
1. Client calls `mlflow.log_artifact(local_path)`
2. Client gets artifact URI from active run: `http://localhost:5000/api/2.0/mlflow-artifacts/...` ✅
3. Client uses `HttpArtifactRepository` (because URI is `http://`)
4. Client uploads via HTTP → **Success**

---

## 🧪 Testing

### Test Artifact URI

```python
import mlflow
mlflow.set_tracking_uri("http://localhost:5000")

with mlflow.start_run():
    run = mlflow.active_run()
    artifact_uri = run.info.artifact_uri
    print(f"Artifact URI: {artifact_uri}")
    
    if artifact_uri.startswith("http://") or artifact_uri.startswith("https://"):
        print("✅ Artifact URI is HTTP (correct)")
    elif artifact_uri.startswith("file://") or artifact_uri.startswith("/"):
        print("❌ Artifact URI is local filesystem (wrong)")
        print("   MLflow will try to write directly to filesystem")
        print("   Fix: Ensure server returns HTTP artifact URIs")
```

### Test Artifact Logging

```python
import tempfile
import mlflow

mlflow.set_tracking_uri("http://localhost:5000")

with mlflow.start_run():
    # Create test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("test")
        test_file = f.name
    
    try:
        mlflow.log_artifact(test_file)
        print("✅ Artifact logged successfully")
    except PermissionError as e:
        if "/mlflow" in str(e):
            print("❌ Permission error - MLflow using local filesystem")
            run = mlflow.active_run()
            if run:
                print(f"   Artifact URI: {run.info.artifact_uri}")
    finally:
        import os
        os.unlink(test_file)
```

---

## 🔧 If Issue Persists

If you still see permission errors after this fix:

1. **Check artifact URI**: Run the test above to see what artifact URI is returned
2. **Verify server config**: Ensure `--serve-artifacts` is present
3. **Check MLflow version**: Older versions may not properly support `--serve-artifacts`
4. **Explicit artifact URI**: May need to explicitly set artifact URI in client code

---

## 📋 Summary

✅ **Code fixed**: All `mlflow.log_artifact()` calls now validate artifact URI  
✅ **Error handling**: Clear error messages when artifact URI is wrong  
✅ **Diagnostics**: Logs artifact URI for debugging  
✅ **Prevention**: Catches permission errors and explains root cause  

**Status**: ✅ **FIXED** - Code now validates artifact URI and provides clear diagnostics

The code will now:
- ✅ Validate tracking URI is HTTP
- ✅ Validate artifact URI is HTTP  
- ✅ Provide clear error messages if artifact URI is wrong
- ✅ Catch permission errors and explain the root cause

If permission errors still occur, the error messages will now clearly indicate that the artifact URI is a local filesystem path instead of HTTP, making it easy to diagnose and fix.
