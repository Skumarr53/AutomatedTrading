# MLflow Permission Error - Complete Investigation

## 🎯 Direct Answer to Your Question

**Q: When we see '/mlflow' permission denied error, where is this write happening - within container server or host (local)?**

**A: IN THE CONTAINER (Server-Side)**

The write happens **INSIDE THE CONTAINER** when the MLflow server receives the artifact upload via HTTP and tries to write it to `/mlflow/artifacts/`.

---

## 🔬 Complete Investigation Results

### Container Configuration Verified

**Container**: `trading-mlflow`
- **Image**: `localhost/trading-mlflow:local` (or `bitnami/mlflow:2.11.3`)
- **Status**: Running (healthy)
- **User**: `root` (UID 0)
- **Port**: `5000:5000`

**Volume Mounts** (Bind Mounts):
```
bind: /home/sanky/projects/AutomatedTrading/data/mlflow → /mlflow
bind: /home/sanky/projects/AutomatedTrading/data/mlartifacts → /mlflow/artifacts
```

**MLflow Server Command** (Inside Container):
```bash
mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri sqlite:////mlflow/mlflow.db \
  --default-artifact-root /mlflow/artifacts \
  --artifacts-destination /mlflow/artifacts \
  --serve-artifacts
```

**Key Configuration**:
- `--default-artifact-root /mlflow/artifacts` → Tells server where to store artifacts
- Server runs as `root` (UID 0)
- Directory `/mlflow/artifacts` is bind-mounted from host

---

## 📊 Where Permission Errors Occur

### Flow Breakdown:

```
┌─────────────────────────────────────────────────────────────┐
│ HOST: Your Python Code                                       │
│ ─────────────────────────────────────────────────────────── │
│ mlflow.log_artifact('/tmp/tmpXXX/file.png')                 │
│                                                              │
│ ✅ Creates: /tmp/tmpXXX/file.png                            │
│ ✅ Owner: Your user (UID 1000)                               │
│ ✅ Permissions: 644                                          │
│ ✅ Status: SUCCESS                                           │
│                                                              │
│ ❌ NO PERMISSION ERRORS                                      │
└─────────────────────────────────────────────────────────────┘
                    │
                    │ HTTP POST
                    │ POST /api/2.0/mlflow/artifacts/upload
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ CONTAINER: MLflow Server                                     │
│ ─────────────────────────────────────────────────────────── │
│ Receives HTTP request with file content                     │
│                                                              │
│ ✅ Extracts file from request body                          │
│ ✅ Determines path: /mlflow/artifacts/<exp>/<run>/file.png │
│ ❓ Tries to write file                                       │
│                                                              │
│ Permission Check:                                           │
│   - Process UID: 0 (root)                                   │
│   - Directory: /mlflow/artifacts/                            │
│   - Directory owner: UID 1001                                │
│   - Directory permissions: ???                               │
│                                                              │
│ ❌ PERMISSION ERROR IF:                                     │
│    Directory is 755 (rwxr-xr-x)                             │
│    → Root (others) cannot write                              │
│                                                              │
│ ✅ SUCCESS IF:                                               │
│    Directory is 777 (rwxrwxrwx)                             │
│    → Root (others) can write                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 Root Cause Analysis

### The Permission Problem:

1. **MLflow Server Process**:
   - Runs as: `root` (UID 0)
   - Inside container: `trading-mlflow`

2. **Artifact Storage Directory**:
   - Container path: `/mlflow/artifacts/`
   - Host path: `data/mlflow/artifacts/`
   - Owner (in container): UID 1001
   - Owner (on host): UID 101000 (Podman rootless mapping)

3. **The Conflict**:
   ```
   Server (UID 0) tries to write to directory owned by UID 1001
   
   If directory permissions are 755 (rwxr-xr-x):
   - Owner (1001): rwx ✅
   - Group: r-x ❌ (no write)
   - Others (including root/0): r-x ❌ (no write)
   → Permission denied!
   
   If directory permissions are 777 (rwxrwxrwx):
   - Owner (1001): rwx ✅
   - Group: rwx ✅
   - Others (including root/0): rwx ✅
   → Success!
   ```

---

## 🔧 Docker/Podman Configuration Investigation

### Container Setup (from `setup_infrastructure.sh`):

```bash
# Container is started via podman-compose or:
podman run -d \
    --name trading-mlflow \
    --network trading-network \
    -p 5000:5000 \
    -v /home/sanky/projects/AutomatedTrading/data/mlflow:/mlflow \
    -v /home/sanky/projects/AutomatedTrading/data/mlartifacts:/mlflow/artifacts \
    -e MLFLOW_TRACKING_URI=http://0.0.0.0:5000 \
    trading-mlflow:local
```

**Key Points**:
- Uses **bind mounts** (not named volumes)
- Container runs as **root** (default)
- Host directories are bind-mounted to container paths

### Permission Fix Applied:

```bash
# From setup_infrastructure.sh (lines 268-272):
if [ -d "${PROJECT_ROOT}/data/mlflow" ]; then
    log_info "Fixing MLflow directory permissions for rootless Podman..."
    podman unshare chmod 777 "${PROJECT_ROOT}/data/mlflow" 2>/dev/null || true
    podman unshare chmod 777 "${PROJECT_ROOT}/data/mlartifacts" 2>/dev/null || true
fi
```

**Why `podman unshare`?**
- Podman rootless uses UID/GID mapping
- Container UID 0 (root) maps to host UID 1000+ (high UID)
- `podman unshare` runs commands in container's namespace
- Ensures permissions are set correctly for container's view

---

## 📋 Verification Steps

### To Confirm Where Errors Occur:

1. **Test HOST write** (should succeed):
   ```bash
   python3 -c "import tempfile, os; d=tempfile.mkdtemp(); f=os.path.join(d,'test.txt'); open(f,'w').write('test'); print('Host write: OK')"
   ```
   ✅ Should succeed

2. **Test CONTAINER write** (may fail):
   ```bash
   podman exec trading-mlflow sh -c "echo 'test' > /mlflow/artifacts/test.txt && ls -la /mlflow/artifacts/test.txt"
   ```
   ❌ Will fail if permissions are 755
   ✅ Will succeed if permissions are 777

3. **Check MLflow server logs**:
   ```bash
   podman logs trading-mlflow | grep -i "permission\|denied\|error" | tail -20
   ```
   ✅ Will show permission errors from server-side writes

---

## 🎯 Final Answer

### **Permission errors occur IN THE CONTAINER when MLflow server writes artifacts**

**Evidence**:
1. ✅ Your Python code writes to `/tmp/tmpXXX/` on HOST - **no errors**
2. ✅ MLflow client uploads via HTTP - **no errors**
3. ❌ MLflow server writes to `/mlflow/artifacts/...` in CONTAINER - **errors here**
4. ✅ File appears on HOST via bind mount after container write succeeds

**Root Cause**:
- MLflow server (running as root/UID 0) tries to write to `/mlflow/artifacts/`
- Directory owned by UID 1001 (from bind mount)
- If directory permissions are 755 → root cannot write → **Permission denied**
- If directory permissions are 777 → root can write → **Success**

**Solution**:
```bash
podman unshare chmod 777 data/mlflow data/mlartifacts
```

This makes directories world-writable so root (MLflow server) can write to them.

---

## 📊 Summary Table

| Stage | Location | Component | Operation | Permission Error? |
|-------|----------|-----------|-----------|-------------------|
| 1 | **HOST** | Your Python code | Write `/tmp/tmpXXX/file.png` | ❌ **NO** |
| 2 | **HOST** | MLflow client | Upload via HTTP | ❌ **NO** |
| 3 | **CONTAINER** | **MLflow server** | **Write `/mlflow/artifacts/.../file.png`** | ✅ **YES** |
| 4 | **HOST** | Bind mount | File appears after container write | ✅ Works if step 3 succeeds |

**Conclusion**: Permission errors happen **IN THE CONTAINER** at **STEP 3** when MLflow server writes artifacts to `/mlflow/artifacts/`.
