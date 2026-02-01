#!/usr/bin/env python3
"""
Complete MLflow Setup Verification

Checks:
1. Container status and health
2. Database persistence
3. Volume mounts and permissions
4. Configuration alignment
5. Artifact directory setup
"""
from __future__ import annotations

import os
import subprocess
import sqlite3
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

def run_cmd(cmd: list[str] | str) -> tuple[int, str, str]:
    """Run command and return exit code, stdout, stderr."""
    if isinstance(cmd, str):
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr

def check_container_status() -> dict:
    """Check if MLflow container is running."""
    result = {}
    
    code, stdout, stderr = run_cmd(["podman", "ps", "--format", "{{.Names}}\t{{.Status}}"])
    
    if code == 0 and "trading-mlflow" in stdout:
        result["exists"] = True
        result["running"] = "Up" in stdout
        result["status"] = "✅ Running" if result["running"] else "⚠️ Not running"
    else:
        result["exists"] = False
        result["running"] = False
        result["status"] = "❌ Not found"
    
    return result

def check_container_mounts() -> dict:
    """Check container volume mounts."""
    result = {}
    
    code, stdout, stderr = run_cmd([
        "podman", "inspect", "trading-mlflow",
        "--format", "{{json .Mounts}}"
    ])
    
    if code == 0:
        try:
            mounts = json.loads(stdout)
            result["mounts"] = []
            for mount in mounts:
                result["mounts"].append({
                    "type": mount["Type"],
                    "source": mount["Source"],
                    "destination": mount["Destination"]
                })
        except:
            result["error"] = "Failed to parse mounts"
    
    return result

def check_container_env() -> dict:
    """Check container environment variables."""
    result = {}
    
    code, stdout, stderr = run_cmd([
        "podman", "inspect", "trading-mlflow",
        "--format", "{{json .Config.Env}}"
    ])
    
    if code == 0:
        try:
            env = json.loads(stdout)
            mlflow_env = {}
            for var in env:
                if "MLFLOW" in var:
                    key, value = var.split("=", 1)
                    mlflow_env[key] = value
            result["mlflow_env"] = mlflow_env
        except:
            result["error"] = "Failed to parse env"
    
    return result

def check_database() -> dict:
    """Check MLflow database."""
    result = {}
    
    db_path = PROJECT_ROOT / "data" / "mlflow" / "mlflow.db"
    
    if not db_path.exists():
        result["exists"] = False
        result["status"] = "❌ Database not found"
        return result
    
    result["exists"] = True
    result["path"] = str(db_path)
    result["size_bytes"] = db_path.stat().st_size
    result["size_mb"] = result["size_bytes"] / 1024 / 1024
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Count experiments
        cursor.execute("SELECT COUNT(*) FROM experiments")
        result["experiments"] = cursor.fetchone()[0]
        
        # Count runs
        cursor.execute("SELECT COUNT(*) FROM runs")
        result["runs"] = cursor.fetchone()[0]
        
        # Count finished runs
        cursor.execute("SELECT COUNT(*) FROM runs WHERE status='FINISHED'")
        result["runs_finished"] = cursor.fetchone()[0]
        
        # Check for artifacts
        cursor.execute("SELECT COUNT(DISTINCT artifact_uri) FROM runs WHERE artifact_uri IS NOT NULL")
        result["run_artifacts"] = cursor.fetchone()[0]
        
        conn.close()
        result["status"] = "✅ Database OK"
    except Exception as e:
        result["error"] = str(e)
        result["status"] = f"⚠️  Error: {e}"
    
    return result

def check_permissions() -> dict:
    """Check directory permissions."""
    result = {}
    
    paths = {
        "data/mlflow": PROJECT_ROOT / "data" / "mlflow",
        "data/mlartifacts": PROJECT_ROOT / "data" / "mlartifacts",
    }
    
    for name, path in paths.items():
        if path.exists():
            mode = oct(path.stat().st_mode)[-3:]
            result[name] = {
                "exists": True,
                "permissions": mode,
                "writable": os.access(path, os.W_OK),
                "status": "✅ 777 (world-writable)" if mode == "777" else f"⚠️  {mode}"
            }
        else:
            result[name] = {
                "exists": False,
                "status": "❌ Not found"
            }
    
    return result

def check_artifacts_filesystem() -> dict:
    """Check artifacts on filesystem."""
    result = {}
    
    artifacts_dir = PROJECT_ROOT / "data" / "mlflow" / "artifacts"
    
    if not artifacts_dir.exists():
        result["status"] = "❌ Directory not found"
        return result
    
    result["exists"] = True
    
    # Count files
    all_files = []
    for root, dirs, files in os.walk(artifacts_dir):
        for file in files:
            all_files.append(os.path.join(root, file))
    
    result["total_files"] = len(all_files)
    result["status"] = f"✅ {len(all_files)} files"
    
    return result

def print_report(
    container: dict,
    mounts: dict,
    env: dict,
    db: dict,
    perms: dict,
    artifacts: dict
) -> None:
    """Print comprehensive report."""
    print("\n" + "=" * 80)
    print("MLFLOW SETUP VERIFICATION REPORT")
    print("=" * 80)
    
    # Container Status
    print("\n1️⃣  CONTAINER STATUS")
    print(f"   {container['status']}")
    
    # Container Mounts
    print("\n2️⃣  CONTAINER MOUNTS")
    if "mounts" in mounts:
        for mount in mounts["mounts"]:
            print(f"   {mount['type'].upper()}: {mount['source']} → {mount['destination']}")
    else:
        print("   ❌ Could not retrieve mounts")
    
    # Container Environment
    print("\n3️⃣  MLFLOW ENVIRONMENT (In Container)")
    if "mlflow_env" in env:
        for key, value in env["mlflow_env"].items():
            # Truncate long values
            display_value = value if len(value) < 50 else value[:47] + "..."
            print(f"   {key}={display_value}")
    else:
        print("   ⚠️  Could not retrieve environment")
    
    # Database
    print("\n4️⃣  DATABASE")
    print(f"   {db['status']}")
    if db.get("exists"):
        print(f"   Path: {db['path']}")
        print(f"   Size: {db['size_bytes']:,} bytes ({db['size_mb']:.2f} MB)")
        print(f"   Experiments: {db.get('experiments', 0)}")
        print(f"   Runs: {db.get('runs', 0)} (Finished: {db.get('runs_finished', 0)})")
        print(f"   Run Artifacts: {db.get('run_artifacts', 0)}")
    
    # Permissions
    print("\n5️⃣  PERMISSIONS")
    for name, perm_info in perms.items():
        if perm_info.get("exists"):
            print(f"   {name}: {perm_info['status']}")
        else:
            print(f"   {name}: {perm_info['status']}")
    
    # Artifacts Filesystem
    print("\n6️⃣  ARTIFACTS FILESYSTEM")
    print(f"   {artifacts['status']}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    checks = [
        ("Container", container.get("running", False)),
        ("Database", db.get("exists", False)),
        ("Permissions", all(p.get("writable", False) for p in perms.values() if p.get("exists"))),
        ("Environment", "mlflow_env" in env),
    ]
    
    all_good = True
    for name, status in checks:
        icon = "✅" if status else "❌"
        print(f"   {icon} {name}")
        if not status:
            all_good = False
    
    print("\n" + "=" * 80)
    if all_good:
        print("✅ ALL CHECKS PASSED")
    else:
        print("⚠️  SOME CHECKS FAILED - Review above for details")
    print("=" * 80 + "\n")

def main():
    """Run all checks."""
    container = check_container_status()
    mounts = check_container_mounts()
    env = check_container_env()
    db = check_database()
    perms = check_permissions()
    artifacts = check_artifacts_filesystem()
    
    print_report(container, mounts, env, db, perms, artifacts)
    
    # Return exit code based on container status
    return 0 if container.get("running") else 1

if __name__ == "__main__":
    exit(main())
