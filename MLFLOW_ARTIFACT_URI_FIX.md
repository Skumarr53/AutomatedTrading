# MLflow Artifact URI Fix - Root Cause and Solution

## 🎯 Problem

**Error**: `PermissionError: [Errno 13] Permission denied: '/mlflow'`

**Root Cause**: MLflow client is trying to write directly to `/mlflow` filesystem instead of uploading artifacts via HTTP.

**Why**: MLflow client receives a **local filesystem artifact URI** (like `file:///mlflow/artifacts/...`) instead of an **HTTP artifact URI** (like `http://localhost:5000/api/2.0/mlflow-artifacts/...`).

---

## 🔍 Diagnosis

### Check 1: Server Configuration ✅

The MLflow server **IS** configured correctly:
```bash
--artifacts-destination /mlflow/artifacts
--serve-artifacts
```

The `--serve-artifacts` flag tells MLflow server to serve artifacts via HTTP.

### Check 2: Artifact URI from Server ❌

The problem: Even with `--serve-artifacts`, the server might be returning a local filesystem artifact URI.

When MLflow client calls `mlflow.log_artifact(local_path)`, it:
1. Gets the artifact URI from the active run
2. Determines the artifact repository type based on the URI
3. If URI is `file://` or starts with `/`, uses `LocalArtifactRepository` → **writes directly to filesystem**
4. If URI is `http://` or `https://`, uses `HttpArtifactRepository` → **uploads via HTTP**

---

## ✅ Solution

### 1. Verify Server Returns HTTP Artifact URIs

The MLflow server **must** return HTTP artifact URIs. Check by:

```python
import mlflow
mlflow.set_tracking_uri("http://localhost:5000")
with mlflow.start_run():
    run = mlflow.active_run()
    artifact_uri = run.info.artifact_uri
    print(f"Artifact URI: {artifact_uri}")
    
    # Should be: http://localhost:5000/api/2.0/mlflow-artifacts/...
    # NOT: file:///mlflow/artifacts/... or /mlflow/artifacts/...
```

### 2. Code-Level Protection

Added checks in `src/utils/mlflow_utils.py` to:
- ✅ Verify tracking URI is HTTP (not `file://`)
- ✅ Check artifact URI is HTTP (not local filesystem)
- ✅ Provide clear error messages if artifact URI is wrong
- ✅ Catch `PermissionError` and provide helpful diagnostics

### 3. Server Configuration

Ensure MLflow server is started with:
```bash
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri sqlite:////mlflow/mlflow.db \
  --artifacts-destination /mlflow/artifacts \
  --serve-artifacts  # <-- CRITICAL: This makes server return HTTP artifact URIs
```

---

## 🧪 Testing

### Test 1: Check Artifact URI

```python
import mlflow
mlflow.set_tracking_uri("http://localhost:5000")

with mlflow.start_run():
    run = mlflow.active_run()
    artifact_uri = run.info.artifact_uri
    print(f"Artifact URI: {artifact_uri}")
    
    if artifact_uri.startswith("http://") or artifact_uri.startswith("https://"):
        print("✅ Artifact URI is HTTP (correct)")
    else:
        print("❌ Artifact URI is local filesystem (wrong)")
```

### Test 2: Test Artifact Logging

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
            print(f"   Artifact URI: {mlflow.active_run().info.artifact_uri}")
    finally:
        import os
        os.unlink(test_file)
```

---

## 🔧 Fix Applied

### Code Changes

1. **Added artifact URI validation** in `log_test_predictions()`, `log_training_parameters()`, and `log_model_performance()`
2. **Added PermissionError handling** with clear diagnostics
3. **Added tracking URI validation** to ensure HTTP (not file://)

### Error Messages

Now when permission errors occur, you'll see:
```
❌ Permission denied writing to /mlflow - MLflow is using local filesystem instead of HTTP
   This happens when artifact URI is file:// or local path instead of http://
   Fix: Ensure MLflow server is configured with --serve-artifacts
   Current tracking URI: http://localhost:5000
   Current artifact URI: file:///mlflow/artifacts/...  <-- This is the problem
```

---

## 📋 Verification Checklist

- [x] MLflow server has `--serve-artifacts` flag ✅
- [x] Code validates tracking URI is HTTP ✅
- [x] Code validates artifact URI is HTTP ✅
- [x] Code provides clear error messages ✅
- [ ] Server returns HTTP artifact URIs (needs testing)
- [ ] Artifact logging works without permission errors (needs testing)

---

## 🎯 Next Steps

1. **Test artifact URI**: Run the test script to verify server returns HTTP artifact URIs
2. **If still wrong**: Check MLflow server version and configuration
3. **If still fails**: May need to explicitly set artifact URI in MLflow client

---

## 📚 References

- MLflow Artifact Storage: https://www.mlflow.org/docs/latest/tracking.html#artifact-storage
- MLflow Server Configuration: https://www.mlflow.org/docs/latest/cli.html#mlflow-server
- `--serve-artifacts` flag: Makes server return HTTP artifact URIs instead of file:// URIs
