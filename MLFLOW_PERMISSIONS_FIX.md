# MLflow Rootless Podman Permissions Fix

## Issue

When MLflow runs in a rootless Podman container, you may encounter:

```
PermissionError: [Errno 13] Permission denied: '/mlflow'
```

This happens because:
1. Container runs as root (UID 0) inside
2. Podman rootless maps container UID 0 → host UID 1000 (your user)
3. Host directory owned by UID 101000 (Podman's namespace)
4. Container can't write to directory owned by different UID

## Quick Fix

Run the permission fix script:

```bash
bash scripts/fix_mlflow_permissions.sh
```

Or manually:

```bash
podman unshare chmod 777 data/mlflow data/mlartifacts
```

## Understanding Rootless Podman UID Mapping

Check your UID mapping:

```bash
podman unshare cat /proc/self/uid_map
```

Example output:
```
         0       1000          1    # Container UID 0 → Host UID 1000
         1     100000      65536   # Container UID 1+ → Host UID 100000+
```

This means:
- Container root (0) writes as host UID 1000
- But directories might be owned by 101000
- Permission denied!

## Solutions

### Solution 1: Make Directories World-Writable (Recommended)

```bash
# Use podman unshare to fix in container's namespace
podman unshare chmod 777 data/mlflow data/mlartifacts

# Verify
ls -ld data/mlflow data/mlartifacts
# Should show: drwxrwxrwx
```

**Pros**: Works immediately, no container restart needed
**Cons**: Less secure (world-writable)

### Solution 2: Change Ownership to Your User

```bash
# Change ownership to your user (UID 1000)
podman unshare chown -R 1000:1000 data/mlflow data/mlartifacts

# Then make group-writable
chmod 775 data/mlflow data/mlartifacts
```

**Pros**: More secure
**Cons**: May need to fix after container recreates directories

### Solution 3: Configure Container User

Update your container to run as a specific user that matches the host mapping.

In `compose.yml` or container startup:
```yaml
services:
  mlflow:
    user: "1000:1000"  # Match your host UID
```

**Pros**: Most secure, permanent fix
**Cons**: Requires container configuration changes

## Automatic Fix on Container Start

The setup script now automatically fixes permissions when starting MLflow:

```bash
scripts/setup_infrastructure.sh
```

Or manually:

```bash
# Before starting container
podman unshare chmod 777 data/mlflow data/mlartifacts

# Start container
podman-compose up -d mlflow
```

## Verification

After fixing permissions, verify:

```bash
# Check permissions
ls -ld data/mlflow data/mlartifacts
# Should show: drwxrwxrwx

# Test write from container
podman exec trading-mlflow touch /mlflow/test_write
podman exec trading-mlflow rm /mlflow/test_write

# Check MLflow can write
python scripts/check_mlflow_db.py
```

## Troubleshooting

### Still Getting Permission Errors?

1. **Check current permissions**:
   ```bash
   ls -ld data/mlflow data/mlartifacts
   ```

2. **Check container user**:
   ```bash
   podman exec trading-mlflow id
   ```

3. **Check UID mapping**:
   ```bash
   podman unshare cat /proc/self/uid_map
   ```

4. **Fix with podman unshare**:
   ```bash
   podman unshare chmod 777 data/mlflow data/mlartifacts
   ```

5. **Restart container** (if needed):
   ```bash
   podman restart trading-mlflow
   ```

### Directory Owned by Wrong User?

If directories are owned by UID 101000 (Podman namespace):

```bash
# Fix ownership
podman unshare chown -R 1000:1000 data/mlflow data/mlartifacts

# Or make world-writable
podman unshare chmod 777 data/mlflow data/mlartifacts
```

## Prevention

To prevent this issue:

1. **Run fix script before starting containers**:
   ```bash
   bash scripts/fix_mlflow_permissions.sh
   ```

2. **Ensure directories exist with correct permissions**:
   ```bash
   mkdir -p data/mlflow data/mlartifacts
   podman unshare chmod 777 data/mlflow data/mlartifacts
   ```

3. **Use setup script** (automatically fixes permissions):
   ```bash
   scripts/setup_infrastructure.sh
   ```

## Related Files

- `scripts/fix_mlflow_permissions.sh` - Permission fix script
- `scripts/setup_infrastructure.sh` - Setup script (auto-fixes permissions)
- `MLFLOW_CONTAINER_DEBUG.md` - Container debugging guide
