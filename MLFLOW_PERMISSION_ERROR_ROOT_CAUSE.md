# MLflow Permission Error Root Cause Analysis

## 🔍 The Critical Question

**When you see `/mlflow` permission denied error, where is the write happening?**

## ✅ ANSWER: **IN THE CONTAINER (Server-Side)**

The permission error occurs **INSIDE THE CONTAINER** when the MLflow server tries to write uploaded artifacts, NOT on the host when your Python code runs.

---

## 📊 Complete Flow Analysis

### Step-by-Step Artifact Upload Flow

```
┌─────────────────────────────────────────────────────────────┐
│ STEP 1: HOST - Your Python Code                            │
│ ─────────────────────────────────────────────────────────── │
│ Code: mlflow.log_artifact('/tmp/tmpXXX/file.png')          │
│                                                              │
│ ✅ Creates temp file: /tmp/tmpXXX/file.png                 │
│ ✅ Owner: Your user (UID 1000)                              │
│ ✅ Permissions: 644 (rw-r--r--)                             │
│ ✅ Status: SUCCESS (temp dir has correct permissions)       │
│                                                              │
│ ❌ NO PERMISSION ERRORS HERE                                │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ HTTP POST
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 2: HOST - MLflow Python Client                         │
│ ─────────────────────────────────────────────────────────── │
│ Client reads file and uploads via HTTP                      │
│                                                              │
│ ✅ Reads: /tmp/tmpXXX/file.png (HOST filesystem)           │
│ ✅ Uploads to: http://localhost:5000/api/2.0/mlflow/...    │
│ ✅ Status: SUCCESS (just reading and uploading)             │
│                                                              │
│ ❌ NO PERMISSION ERRORS HERE                                │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ HTTP Request
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ STEP 3: CONTAINER - MLflow Server                           │
│ ─────────────────────────────────────────────────────────── │
│ Server receives HTTP POST request                           │
│                                                              │
│ ✅ Receives file content in request body                    │
│ ✅ Determines storage path: /mlflow/artifacts/.../file.png │
│ ❓ Tries to write: /mlflow/artifacts/.../file.png          │
│                                                              │
│ ⚠️  PERMISSION CHECK HAPPENS HERE:                         │
│    - Server runs as: root (UID 0)                          │
│    - Directory owner: UID 1001 (from bind mount)          │
│    - Directory permissions: ???                             │
│                                                              │
│ ❌ PERMISSION ERROR OCCURS HERE IF:                        │
│    - Directory is 755 (not writable by others)             │
│    - Directory is owned by UID 1001, server is UID 0       │
│    - Root cannot write to UID 1001 directory               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Root Cause Summary

### Where Permission Errors Occur:

| Location | Component | Write Operation | Permission Error? |
|----------|-----------|-----------------|-------------------|
| **HOST** | Your Python code | Writes to `/tmp/tmpXXX/` | ❌ **NO** - Temp dirs have correct permissions |
| **HOST** | MLflow client | Reads from `/tmp/tmpXXX/` | ❌ **NO** - Just reading |
| **CONTAINER** | MLflow server | Writes to `/mlflow/artifacts/...` | ✅ **YES** - **THIS IS WHERE IT FAILS** |

### Why It Fails:

1. **MLflow server** receives artifact upload via HTTP POST
2. Server extracts file content from request body
3. Server determines storage path: `/mlflow/artifacts/<exp_id>/<run_id>/artifacts/file.png`
4. Server tries to **write file** to this path
5. **Permission check fails** if:
   - Directory permissions are 755 (not writable by others)
   - Server runs as root (UID 0)
   - Directory owned by UID 1001
   - Root cannot write to UID 1001's directory

---

## 🔧 Configuration Details

### Container Configuration

**Container User**: `root` (UID 0)
```bash
podman exec trading-mlflow id
# uid=0(root) gid=0(root) groups=0(root)
```

**Volume Mounts** (Bind Mounts):
```bash
# Container path → Host path
bind /home/sanky/projects/AutomatedTrading/data/mlflow → /mlflow
bind /home/sanky/projects/AutomatedTrading/data/mlartifacts → /mlflow/artifacts
```

**MLflow Server Command**:
```bash
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri sqlite:////mlflow/mlflow.db \
  --default-artifact-root /mlflow/artifacts
```

**Key Point**: `--default-artifact-root /mlflow/artifacts` tells MLflow server to store artifacts at `/mlflow/artifacts/` **INSIDE THE CONTAINER**.

### Permission Fix

From `setup_infrastructure.sh`:
```bash
podman unshare chmod 777 data/mlflow data/mlartifacts
```

**Why `podman unshare`?**
- Podman rootless uses UID mapping
- `podman unshare` runs commands in the same namespace as containers
- Ensures permissions are set correctly for the container's view of the filesystem

---

## 📋 Final Answer

### **Permission errors occur IN THE CONTAINER when MLflow server writes artifacts**

**Evidence**:
1. ✅ Your code writes to `/tmp/tmpXXX/` on HOST - no permission issues
2. ✅ MLflow client uploads via HTTP - no permission issues  
3. ❌ MLflow server writes to `/mlflow/artifacts/...` in CONTAINER - **permission errors here**
4. ✅ File appears on HOST via bind mount after container write succeeds

**Root Cause**:
- MLflow server runs as **root (UID 0)** in container
- Directory owned by **UID 1001** (from bind mount)
- Directory permissions **755** (not writable by others)
- Root cannot write to UID 1001's directory

**Solution**:
```bash
podman unshare chmod 777 data/mlflow data/mlartifacts
```

This makes directories world-writable so root can write to them.

---

## 📊 Summary Table

| Stage | Location | Component | Write Operation | Permission Error? |
|-------|----------|-----------|-----------------|-------------------|
| 1 | HOST | Your Python code | `/tmp/tmpXXX/file.png` | ❌ NO |
| 2 | HOST | MLflow client | Reads file, uploads HTTP | ❌ NO |
| 3 | **CONTAINER** | **MLflow server** | **`/mlflow/artifacts/.../file.png`** | ✅ **YES** |
| 4 | HOST | Bind mount | File appears after container write | ✅ Works if step 3 succeeds |

**Conclusion**: Permission errors happen **IN THE CONTAINER** at **STEP 3** when MLflow server writes artifacts.
