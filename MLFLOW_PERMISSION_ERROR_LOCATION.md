# MLflow Permission Error Location - Complete Answer

## 🎯 Direct Answer

**Q: When we see '/mlflow' permission denied error, where is this write happening - within container server or host (local)?**

**A: IN THE CONTAINER (Server-Side)**

The permission error occurs **INSIDE THE CONTAINER** when the MLflow server receives an artifact upload via HTTP and tries to write it to `/mlflow/artifacts/`.

---

## 🔬 Investigation Results

### Current Container Status (Verified):

```bash
# Container user
podman exec trading-mlflow id
# uid=0(root) gid=0(root) groups=0(root)

# Directory permissions
podman exec trading-mlflow stat -c "%a" /mlflow/artifacts
# 777 (world-writable) ✅

# Write test
podman exec trading-mlflow sh -c "test -w /mlflow/artifacts && echo WRITABLE"
# WRITABLE ✅
```

**Current Status**: ✅ Permissions are correct (777), container can write

---

## 📊 Where Permission Errors Occur

### Complete Flow:

```
┌─────────────────────────────────────────────────────────────┐
│ STEP 1: HOST - Your Python Code                             │
│ Location: /tmp/tmpXXX/file.png                              │
│ Component: Your Python script                               │
│ Operation: Creates temp file                                │
│ Permission Error: ❌ NO                                      │
│                                                              │
│ ✅ Temp directory has correct permissions                   │
│ ✅ Your user can write to /tmp/                             │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ mlflow.log_artifact(file_path)
                    │ → MLflow Python Client
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 2: HOST - MLflow Python Client                         │
│ Location: Reads from /tmp/tmpXXX/file.png                   │
│ Component: mlflow Python library                            │
│ Operation: Reads file, uploads via HTTP POST               │
│ Permission Error: ❌ NO                                      │
│                                                              │
│ ✅ Just reading file (no write needed)                      │
│ ✅ HTTP upload doesn't require file permissions             │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ HTTP POST
                    │ POST http://localhost:5000/api/2.0/mlflow/artifacts/upload
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 3: CONTAINER - MLflow Server                           │
│ Location: /mlflow/artifacts/<exp>/<run>/artifacts/file.png  │
│ Component: MLflow server (FastAPI/Uvicorn)                  │
│ Operation: Writes uploaded file to disk                     │
│ Permission Error: ✅ YES - THIS IS WHERE IT FAILS           │
│                                                              │
│ ⚠️  PERMISSION CHECK:                                       │
│    Server process: root (UID 0)                             │
│    Directory: /mlflow/artifacts/                            │
│    Directory owner: UID 1001                                │
│    Directory permissions: 755 or 777?                        │
│                                                              │
│    If 755 (rwxr-xr-x):                                       │
│      - Owner (1001): rwx ✅                                  │
│      - Group: r-x ❌ (no write)                             │
│      - Others (root/0): r-x ❌ (no write)                    │
│      → Permission denied!                                    │
│                                                              │
│    If 777 (rwxrwxrwx):                                       │
│      - Owner (1001): rwx ✅                                  │
│      - Group: rwx ✅                                         │
│      - Others (root/0): rwx ✅                              │
│      → Success!                                             │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ Bind Mount
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 4: HOST - File Appears via Bind Mount                  │
│ Location: data/mlflow/artifacts/.../file.png                 │
│ Component: Podman bind mount                               │
│ Operation: File appears on host after container write       │
│ Permission Error: ❌ NO (just reading what container wrote)  │
│                                                              │
│ ✅ File appears automatically via bind mount                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 Root Cause Analysis

### The Permission Problem Explained:

**When MLflow server receives artifact upload**:

1. **HTTP Request Arrives**:
   ```
   POST /api/2.0/mlflow/artifacts/upload
   Content-Type: multipart/form-data
   Body: file content
   ```

2. **Server Processes Request**:
   - Extracts file content from request body
   - Determines storage path: `/mlflow/artifacts/<exp_id>/<run_id>/artifacts/file.png`
   - Creates directory structure if needed
   - **Tries to write file**

3. **Permission Check**:
   ```python
   # Inside MLflow server (container)
   file_path = "/mlflow/artifacts/1/abc123/artifacts/file.png"
   
   # Server process: root (UID 0)
   # Directory owner: UID 1001
   # Directory permissions: 755 or 777?
   
   with open(file_path, 'wb') as f:  # ← Permission check happens here
       f.write(file_content)
   ```

4. **If Permissions Are Wrong**:
   ```
   PermissionError: [Errno 13] Permission denied: '/mlflow/artifacts/1/abc123/artifacts/file.png'
   ```

---

## 🔧 Docker/Podman Configuration

### Container Setup:

**From `setup_infrastructure.sh`**:
```bash
# Container started via podman-compose or:
podman run -d \
    --name trading-mlflow \
    --network trading-network \
    -p 5000:5000 \
    -v /home/sanky/projects/AutomatedTrading/data/mlflow:/mlflow \
    -v /home/sanky/projects/AutomatedTrading/data/mlartifacts:/mlflow/artifacts \
    -e MLFLOW_TRACKING_URI=http://0.0.0.0:5000 \
    trading-mlflow:local
```

**Key Configuration**:
- **Bind mounts**: Host directories mounted into container
- **Container user**: `root` (UID 0) - default
- **MLflow server command**: `--default-artifact-root /mlflow/artifacts`

### Permission Fix:

```bash
# From setup_infrastructure.sh (lines 268-272):
podman unshare chmod 777 data/mlflow data/mlartifacts
```

**Why This Works**:
- `podman unshare` runs command in container's namespace
- Sets permissions that container sees as 777
- Allows root (MLflow server) to write to directories

---

## 📋 Verification

### Current Status (Verified):

```bash
# Container can write
podman exec trading-mlflow sh -c "test -w /mlflow/artifacts && echo WRITABLE"
# WRITABLE ✅

# Directory permissions
podman exec trading-mlflow stat -c "%a" /mlflow/artifacts
# 777 ✅

# Test file written by root exists
podman exec trading-mlflow ls -la /mlflow/artifacts/
# -rw-r--r-- 1 root root  0 Jan 31 14:01 test_write.txt ✅
```

**Conclusion**: Permissions are currently correct, but errors occur **IN THE CONTAINER** if permissions are wrong.

---

## 🎯 Final Answer

### **Permission errors occur IN THE CONTAINER when MLflow server writes artifacts**

**Evidence Chain**:

1. ✅ **HOST**: Your code writes to `/tmp/tmpXXX/` - **no errors** (temp dirs have correct permissions)
2. ✅ **HOST**: MLflow client uploads via HTTP - **no errors** (just reading and uploading)
3. ❌ **CONTAINER**: MLflow server writes to `/mlflow/artifacts/...` - **errors here if permissions wrong**
4. ✅ **HOST**: File appears via bind mount - **works if step 3 succeeds**

**Root Cause**:
- MLflow server (root/UID 0) tries to write to `/mlflow/artifacts/`
- Directory owned by UID 1001 (from bind mount)
- If directory permissions are 755 → root cannot write → **Permission denied**
- If directory permissions are 777 → root can write → **Success**

**Solution**:
```bash
podman unshare chmod 777 data/mlflow data/mlartifacts
```

This ensures directories are world-writable so root (MLflow server) can write to them.

---

## 📊 Summary Table

| Stage | Location | Component | Write Operation | Permission Error? |
|-------|----------|-----------|-----------------|-------------------|
| 1 | **HOST** | Your Python code | `/tmp/tmpXXX/file.png` | ❌ **NO** |
| 2 | **HOST** | MLflow client | Reads file, uploads HTTP | ❌ **NO** |
| 3 | **CONTAINER** | **MLflow server** | **`/mlflow/artifacts/.../file.png`** | ✅ **YES** |
| 4 | **HOST** | Bind mount | File appears after container write | ✅ Works if step 3 succeeds |

**Answer**: Permission errors happen **IN THE CONTAINER** at **STEP 3** when MLflow server writes artifacts to `/mlflow/artifacts/`.
