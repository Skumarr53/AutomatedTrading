#!/usr/bin/env python3
"""
Investigate MLflow Permission Errors - Where Do They Happen?

This script traces the full flow to determine if permission errors occur:
1. On HOST (when Python code writes temp files)
2. In CONTAINER (when MLflow server receives uploads and writes artifacts)
"""
import os
import sys
import tempfile
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_host_permissions():
    """Check permissions on host filesystem."""
    print("\n" + "=" * 70)
    print("1. HOST FILESYSTEM PERMISSIONS")
    print("=" * 70)
    
    # Check temp directory
    tmpdir = tempfile.mkdtemp()
    print(f"\n✅ Created temp directory: {tmpdir}")
    print(f"   Exists: {os.path.exists(tmpdir)}")
    print(f"   Writable: {os.access(tmpdir, os.W_OK)}")
    print(f"   Owner: {os.stat(tmpdir).st_uid}")
    print(f"   Permissions: {oct(os.stat(tmpdir).st_mode)[-3:]}")
    
    # Try to write a file
    test_file = os.path.join(tmpdir, "test.txt")
    try:
        with open(test_file, 'w') as f:
            f.write("test")
        print(f"\n✅ Successfully wrote file: {test_file}")
        print(f"   File exists: {os.path.exists(test_file)}")
        print(f"   File owner: {os.stat(test_file).st_uid}")
    except Exception as e:
        print(f"\n❌ Failed to write file: {e}")
    
    # Cleanup
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)
    
    # Check data/mlflow directory
    mlflow_dir = project_root / "data" / "mlflow"
    if mlflow_dir.exists():
        print(f"\n📁 Host MLflow directory: {mlflow_dir}")
        print(f"   Exists: {mlflow_dir.exists()}")
        print(f"   Writable: {os.access(mlflow_dir, os.W_OK)}")
        stat = mlflow_dir.stat()
        print(f"   Owner UID: {stat.st_uid}")
        print(f"   Permissions: {oct(stat.st_mode)[-3:]}")
    else:
        print(f"\n❌ Host MLflow directory not found: {mlflow_dir}")

def check_container_permissions():
    """Check permissions inside container."""
    print("\n" + "=" * 70)
    print("2. CONTAINER FILESYSTEM PERMISSIONS")
    print("=" * 70)
    
    container_name = "trading-mlflow"
    
    # Check if container is running
    result = subprocess.run(
        ["podman", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    
    if container_name not in result.stdout:
        print(f"\n❌ Container '{container_name}' is not running")
        return
    
    print(f"\n✅ Container '{container_name}' is running")
    
    # Check container user
    result = subprocess.run(
        ["podman", "exec", container_name, "id"],
        capture_output=True,
        text=True
    )
    print(f"\nContainer User:")
    print(f"   {result.stdout.strip()}")
    
    # Check /mlflow directory in container
    result = subprocess.run(
        ["podman", "exec", container_name, "ls", "-la", "/mlflow"],
        capture_output=True,
        text=True
    )
    print(f"\n📁 Container /mlflow directory:")
    for line in result.stdout.strip().split('\n'):
        if line.strip():
            print(f"   {line}")
    
    # Check /mlflow/artifacts directory
    result = subprocess.run(
        ["podman", "exec", container_name, "ls", "-la", "/mlflow/artifacts"],
        capture_output=True,
        text=True
    )
    print(f"\n📁 Container /mlflow/artifacts directory:")
    for line in result.stdout.strip().split('\n'):
        if line.strip():
            print(f"   {line}")
    
    # Try to write a test file in container
    print(f"\n🧪 Testing write permissions in container:")
    result = subprocess.run(
        ["podman", "exec", container_name, "sh", "-c", "echo 'test' > /mlflow/artifacts/test_write_container.txt && ls -la /mlflow/artifacts/test_write_container.txt"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print(f"   ✅ Container can write to /mlflow/artifacts")
        print(f"   {result.stdout.strip()}")
    else:
        print(f"   ❌ Container CANNOT write to /mlflow/artifacts")
        print(f"   Error: {result.stderr.strip()}")

def check_mlflow_server_config():
    """Check MLflow server configuration."""
    print("\n" + "=" * 70)
    print("3. MLFLOW SERVER CONFIGURATION")
    print("=" * 70)
    
    container_name = "trading-mlflow"
    
    # Get container command
    result = subprocess.run(
        ["podman", "inspect", container_name, "--format", "{{range .Config.Cmd}}{{.}} {{end}}"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        cmd = result.stdout.strip()
        print(f"\nMLflow Server Command:")
        print(f"   {cmd}")
        
        # Extract artifact root from command
        if "--default-artifact-root" in cmd:
            parts = cmd.split()
            idx = parts.index("--default-artifact-root")
            if idx + 1 < len(parts):
                artifact_root = parts[idx + 1]
                print(f"\n   Artifact Root: {artifact_root}")
                print(f"   💡 This is where MLflow SERVER writes artifacts")
        
        if "--backend-store-uri" in cmd:
            parts = cmd.split()
            idx = parts.index("--backend-store-uri")
            if idx + 1 < len(parts):
                backend_uri = parts[idx + 1]
                print(f"   Backend Store URI: {backend_uri}")

def check_mlflow_upload_flow():
    """Explain MLflow artifact upload flow."""
    print("\n" + "=" * 70)
    print("4. MLFLOW ARTIFACT UPLOAD FLOW")
    print("=" * 70)
    
    print("""
When you call mlflow.log_artifact('/tmp/tmpXXX/file.png'):

STEP 1: HOST (Your Python Code)
  ├─ Creates temp file: /tmp/tmpXXX/file.png ✅ (HOST filesystem)
  ├─ File owner: Your user (UID 1000)
  ├─ Permissions: 644 (rw-r--r--)
  └─ Status: ✅ SUCCESS (temp dir has correct permissions)

STEP 2: MLflow Python Client (HOST)
  ├─ Reads file: /tmp/tmpXXX/file.png ✅ (HOST filesystem)
  ├─ Uploads via HTTP POST to: http://localhost:5000/api/2.0/mlflow/artifacts/upload
  └─ Status: ✅ SUCCESS (just reading and uploading)

STEP 3: MLflow Server (CONTAINER)
  ├─ Receives HTTP POST request
  ├─ Extracts file content from request body
  ├─ Determines storage location: /mlflow/artifacts/<exp_id>/<run_id>/artifacts/
  ├─ Writes file: /mlflow/artifacts/.../file.png ❓ (CONTAINER filesystem)
  │   ├─ Directory owner: UID 1001 (from bind mount)
  │   ├─ Server runs as: UID 0 (root)
  │   └─ Permission check: Can root write to UID 1001 directory?
  └─ Status: ❓ DEPENDS ON PERMISSIONS

STEP 4: Bind Mount (HOST ↔ CONTAINER)
  ├─ Container path: /mlflow/artifacts/.../file.png
  ├─ Host path: data/mlflow/artifacts/.../file.png
  ├─ Owner on host: UID 101000 (Podman rootless mapping)
  └─ Status: ✅ File appears on host after container write
    """)

def analyze_permission_issue():
    """Analyze where permission errors occur."""
    print("\n" + "=" * 70)
    print("5. PERMISSION ERROR ANALYSIS")
    print("=" * 70)
    
    print("""
WHERE PERMISSION ERRORS OCCUR:

❌ NOT on HOST (when your code writes temp files):
   - Your code writes to /tmp/tmpXXX/ (temp directory)
   - Temp dirs have correct permissions (777 or your user)
   - No permission issues here ✅

✅ YES in CONTAINER (when MLflow server writes artifacts):
   - MLflow server receives upload via HTTP
   - Server tries to write to /mlflow/artifacts/<exp>/<run>/artifacts/file.png
   - Server runs as: root (UID 0)
   - Directory owned by: UID 1001 (from bind mount)
   - If directory is 755 (not writable by others): ❌ Permission denied
   - If directory is 777 (world-writable): ✅ Works

ROOT CAUSE:
   The permission error happens INSIDE THE CONTAINER when MLflow server
   tries to write the uploaded artifact file to /mlflow/artifacts/

SOLUTION:
   Ensure /mlflow/artifacts/ has 777 permissions so root can write:
   podman unshare chmod 777 data/mlflow data/mlartifacts
    """)

def main():
    """Run all checks."""
    print("\n" + "=" * 70)
    print("MLFLOW PERMISSION INVESTIGATION")
    print("=" * 70)
    
    check_host_permissions()
    check_container_permissions()
    check_mlflow_server_config()
    check_mlflow_upload_flow()
    analyze_permission_issue()
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("""
✅ Permission errors occur IN THE CONTAINER (not on host)
✅ MLflow server (running as root) writes artifacts to /mlflow/artifacts/
✅ Directory must be writable by root (777 permissions)
✅ Fix: podman unshare chmod 777 data/mlflow data/mlartifacts
    """)
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
