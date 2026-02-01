#!/usr/bin/env python3
"""
MLflow Container Database Check

Checks if MLflow container is running and verifies database persistence:
1. Container status
2. Volume mounts
3. Database file existence in volume
4. Database contents (experiments/runs)
5. Artifact storage

Usage:
    python scripts/check_mlflow_container.py
    python scripts/check_mlflow_container.py --exec-shell  # Open shell in container
"""
from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str] | str, capture_output: bool = True, shell: bool = False) -> tuple[int, str, str]:
    """Run shell command and return exit code, stdout, stderr."""
    try:
        # For podman commands, use shell=True to inherit proper environment
        if isinstance(cmd, list) and len(cmd) > 0 and "podman" in cmd[0]:
            shell = True
            cmd = " ".join(cmd)
        
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=False,
            shell=shell
        )
        stdout = result.stdout if capture_output else ""
        stderr = result.stderr if capture_output else ""
        return result.returncode, stdout, stderr
    except Exception as e:
        return 1, "", str(e)


def check_container_status(container_name: str = "trading-mlflow") -> dict:
    """Check if MLflow container is running."""
    result = {}
    
    # Try to inspect container - if it exists, this will succeed
    code, _, _ = run_command(["podman", "inspect", container_name])
    result["exists"] = (code == 0)
    
    # Check if running by inspecting state
    if result["exists"]:
        code, stdout, _ = run_command([
            "podman", "inspect", container_name, "--format", "{{.State.Running}}"
        ])
        if code == 0:
            result["running"] = stdout.strip().lower() == "true"
        else:
            result["running"] = False
    else:
        result["running"] = False
    
    return result


def get_container_info(container_name: str = "trading-mlflow") -> dict:
    """Get detailed container information."""
    info = {}
    
    # Get container inspect
    code, stdout, stderr = run_command([
        "podman", "inspect", container_name
    ])
    
    if code != 0:
        return {"error": stderr}
    
    # Parse key info (simplified - would need json parsing for full)
    # Get volume mounts
    code, mounts, _ = run_command([
        "podman", "inspect", container_name, "--format", "{{range .Mounts}}{{.Source}}:{{.Destination}} {{end}}"
    ])
    info["mounts"] = mounts.strip().split() if mounts.strip() else []
    
    # Get environment variables
    code, env, _ = run_command([
        "podman", "inspect", container_name, "--format", "{{range .Config.Env}}{{println .}}{{end}}"
    ])
    info["env"] = [e for e in env.strip().split("\n") if e]
    
    # Get port mappings
    code, ports, _ = run_command([
        "podman", "inspect", container_name, "--format", "{{range $p, $conf := .NetworkSettings.Ports}}{{$p}} {{end}}"
    ])
    info["ports"] = ports.strip()
    
    return info


def check_volume_mounts(container_name: str = "trading-mlflow") -> dict:
    """Check volume mounts for MLflow container."""
    mounts_info = {}
    
    # Get all mounts
    code, stdout, _ = run_command([
        "podman", "inspect", container_name, "--format", 
        "{{range .Mounts}}{{.Type}}\t{{.Source}}\t{{.Destination}}\n{{end}}"
    ])
    
    if code == 0 and stdout:
        mounts = []
        for line in stdout.strip().split("\n"):
            if line.strip():
                parts = line.split("\t")
                if len(parts) >= 3:
                    mount_type, source, dest = parts[0], parts[1], parts[2]
                    mounts.append({
                        "type": mount_type,
                        "source": source,
                        "destination": dest
                    })
                    if "/mlflow" in dest.lower() or "mlflow" in source.lower():
                        mounts_info["mlflow_volume"] = {
                            "type": mount_type,
                            "source": source,
                            "destination": dest
                        }
        mounts_info["all_mounts"] = mounts
    
    return mounts_info


def check_database_in_container(container_name: str = "trading-mlflow") -> dict:
    """Check database file inside container."""
    result = {}
    
    # Check if database exists in /mlflow
    code, stdout, stderr = run_command([
        "podman", "exec", container_name, "ls", "-la", "/mlflow"
    ])
    
    if code == 0:
        result["mlflow_dir_exists"] = True
        result["mlflow_dir_contents"] = stdout
        
        # Check for database file
        code2, db_check, _ = run_command([
            "podman", "exec", container_name, "test", "-f", "/mlflow/mlflow.db"
        ])
        result["database_exists"] = (code2 == 0)
        
        if result["database_exists"]:
            # Get database size
            code3, size, _ = run_command([
                "podman", "exec", container_name, "stat", "-c", "%s", "/mlflow/mlflow.db"
            ])
            if code3 == 0:
                result["database_size_bytes"] = int(size.strip()) if size.strip().isdigit() else 0
        
        # Check for mlruns directory
        code4, runs_check, _ = run_command([
            "podman", "exec", container_name, "test", "-d", "/mlflow/mlruns"
        ])
        result["mlruns_exists"] = (code4 == 0)
        
        if result["mlruns_exists"]:
            # Count experiments
            code5, exp_count, _ = run_command([
                "podman", "exec", container_name, "find", "/mlflow/mlruns", "-mindepth", "1", "-maxdepth", "1", "-type", "d", "|", "wc", "-l"
            ])
            if code5 == 0:
                result["experiment_count"] = int(exp_count.strip()) if exp_count.strip().isdigit() else 0
    else:
        result["mlflow_dir_exists"] = False
        result["error"] = stderr
    
    return result


def check_database_contents(container_name: str = "trading-mlflow", mount_info: dict = None) -> dict:
    """Check database contents using Python SQLite (works with bind mounts)."""
    result = {}
    
    # Determine database path
    db_path = None
    
    # If bind mount, use host path
    if mount_info and mount_info.get("mlflow_volume"):
        vol = mount_info["mlflow_volume"]
        if vol.get("type") == "bind":
            # Bind mount: database is on host at source/mlflow.db
            source = vol.get("source", "")
            db_path = Path(source) / "mlflow.db"
        else:
            # Named volume: need to query from container
            db_path = None
    
    # Try Python-based query (works from host for bind mounts)
    if db_path and db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM experiments")
            result["experiments"] = str(cursor.fetchone()[0])
            
            cursor.execute("SELECT COUNT(*) FROM runs")
            result["runs"] = str(cursor.fetchone()[0])
            
            cursor.execute("SELECT name FROM experiments ORDER BY experiment_id DESC LIMIT 1")
            exp_result = cursor.fetchone()
            result["latest_experiment"] = exp_result[0] if exp_result else "N/A"
            
            # Get experiment details
            cursor.execute("SELECT experiment_id, name FROM experiments")
            result["experiment_list"] = [{"id": r[0], "name": r[1]} for r in cursor.fetchall()]
            
            conn.close()
            result["query_method"] = "host_python"
            return result
        except Exception as e:
            result["error"] = str(e)
            result["query_method"] = "failed"
            return result
    
    # Fallback: Try sqlite3 in container (may not be available)
    code, _, _ = run_command([
        "podman", "exec", container_name, "which", "sqlite3"
    ])
    
    if code != 0:
        result["sqlite3_available"] = False
        result["error"] = "Cannot query database: sqlite3 not in container and no bind mount detected"
        return result
    
    result["sqlite3_available"] = True
    
    # Query database via container
    queries = {
        "experiments": "SELECT COUNT(*) FROM experiments;",
        "runs": "SELECT COUNT(*) FROM runs;",
        "latest_experiment": "SELECT name FROM experiments ORDER BY experiment_id DESC LIMIT 1;",
    }
    
    for key, query in queries.items():
        # Escape query for shell
        escaped_query = query.replace('"', '\\"')
        code, stdout, stderr = run_command([
            "podman", "exec", container_name,
            "sqlite3", "/mlflow/mlflow.db", f'"{escaped_query}"'
        ])
        if code == 0:
            result[key] = stdout.strip()
        else:
            result[f"{key}_error"] = stderr
    
    result["query_method"] = "container_sqlite3"
    return result


def check_mlflow_server_health(container_name: str = "trading-mlflow") -> dict:
    """Check MLflow server health endpoint."""
    import requests
    
    result = {}
    
    try:
        response = requests.get("http://localhost:5000/health", timeout=5)
        result["status_code"] = response.status_code
        result["healthy"] = response.status_code == 200
        if response.status_code == 200:
            result["response"] = response.text
    except requests.exceptions.ConnectionError:
        result["healthy"] = False
        result["error"] = "Cannot connect to MLflow server"
    except Exception as e:
        result["healthy"] = False
        result["error"] = str(e)
    
    return result


def get_podman_volume_location(volume_name: str = "mlflow-data") -> dict:
    """Get Podman volume location on host."""
    result = {}
    
    # List volumes
    code, stdout, stderr = run_command([
        "podman", "volume", "ls", "--format", "{{.Name}}\t{{.Mountpoint}}"
    ])
    
    if code == 0:
        for line in stdout.strip().split("\n"):
            if volume_name in line:
                parts = line.split("\t")
                if len(parts) >= 2:
                    result["volume_name"] = parts[0]
                    result["mountpoint"] = parts[1]
                    result["found"] = True
                    break
        
        if not result.get("found"):
            result["found"] = False
            result["available_volumes"] = stdout.strip().split("\n")
    else:
        result["error"] = stderr
    
    return result


def print_report(container_status: dict, container_info: dict, mounts: dict, 
                db_check: dict, db_contents: dict, health: dict, volume_info: dict) -> None:
    """Print comprehensive report."""
    print("\n" + "=" * 70)
    print("MLFLOW CONTAINER DIAGNOSTIC REPORT")
    print("=" * 70)
    
    # Container Status
    print("\n1. Container Status:")
    if container_status.get("exists"):
        if container_status.get("running"):
            print("   ✅ Container 'trading-mlflow' is RUNNING")
        else:
            print("   ⚠️  Container 'trading-mlflow' exists but is NOT running")
            print("   💡 Start it: podman start trading-mlflow")
    else:
        print("   ❌ Container 'trading-mlflow' does NOT exist")
        print("   💡 Create it: podman-compose up -d mlflow")
        return
    
    # Volume Information
    print("\n2. Volume Mounts:")
    if mounts.get("mlflow_volume"):
        vol = mounts["mlflow_volume"]
        print(f"   ✅ MLflow volume found:")
        print(f"      Type: {vol['type']}")
        print(f"      Source: {vol['source']}")
        print(f"      Destination: {vol['destination']}")
        
        if vol['type'] == 'volume':
            # Named volume - check Podman volume location
            if volume_info.get("found"):
                print(f"   📍 Volume location on host: {volume_info.get('mountpoint')}")
        elif vol['type'] == 'bind':
            # Bind mount - database is directly on host
            db_path = Path(vol['source']) / "mlflow.db"
            if db_path.exists():
                size = db_path.stat().st_size
                print(f"   📍 Database on host: {db_path} ({size:,} bytes)")
    else:
        print("   ⚠️  No MLflow volume mount found")
    
    # Database Check
    print("\n3. Database in Container:")
    if db_check.get("mlflow_dir_exists"):
        print("   ✅ /mlflow directory exists")
        
        if db_check.get("database_exists"):
            size = db_check.get("database_size_bytes", 0)
            size_mb = size / 1024 / 1024
            print(f"   ✅ Database file exists: /mlflow/mlflow.db")
            print(f"      Size: {size:,} bytes ({size_mb:.2f} MB)")
        else:
            print("   ⚠️  Database file NOT found in /mlflow/mlflow.db")
            print("   💡 Database will be created on first model training")
        
        if db_check.get("mlruns_exists"):
            exp_count = db_check.get("experiment_count", 0)
            print(f"   ✅ Artifact directory exists: /mlflow/mlruns")
            print(f"      Experiments: {exp_count}")
        else:
            print("   ⚠️  Artifact directory NOT found")
    else:
        print("   ❌ /mlflow directory not accessible")
        if db_check.get("error"):
            print(f"      Error: {db_check['error']}")
    
    # Database Contents
    print("\n4. Database Contents:")
    query_method = db_contents.get("query_method", "unknown")
    
    if query_method in ["host_python", "container_sqlite3"]:
        exp_count = db_contents.get("experiments", "0")
        runs_count = db_contents.get("runs", "0")
        latest_exp = db_contents.get("latest_experiment", "N/A")
        
        print(f"   Query method: {query_method}")
        print(f"   Experiments in database: {exp_count}")
        print(f"   Runs in database: {runs_count}")
        
        if latest_exp and latest_exp != "N/A":
            print(f"   Latest experiment: {latest_exp}")
        
        # Show experiment list if available
        if db_contents.get("experiment_list"):
            print(f"   Experiment list:")
            for exp in db_contents["experiment_list"]:
                print(f"      - {exp['name']} (ID: {exp['id']})")
        
        if runs_count == "0":
            print("\n   ⚠️  CRITICAL: No runs found in database!")
            print("   💡 This is why models don't show in UI")
            print("   💡 Run model training to log models:")
            print("      python main.py --mode train")
        elif exp_count == "0":
            print("   ⚠️  No experiments found in database")
            print("   💡 Run model training to create experiments")
    elif db_contents.get("error"):
        print(f"   ❌ Cannot query database: {db_contents['error']}")
    else:
        print("   ⚠️  Cannot query database (sqlite3 not available and no bind mount detected)")
    
    # Server Health
    print("\n5. MLflow Server Health:")
    if health.get("healthy"):
        print("   ✅ Server is healthy and responding")
    else:
        print("   ❌ Server is not responding")
        if health.get("error"):
            print(f"      Error: {health['error']}")
    
    # Recommendations
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS:")
    
    issues = []
    if not container_status.get("running"):
        issues.append("Container is not running")
    if not db_check.get("database_exists"):
        issues.append("Database file not found")
    if db_contents.get("experiments") == "0":
        issues.append("No experiments in database")
    if not health.get("healthy"):
        issues.append("Server not responding")
    
    if issues:
        print("\n   Issues found:")
        for issue in issues:
            print(f"   - {issue}")
        
        print("\n   Fix steps:")
        if not container_status.get("running"):
            print("   1. Start container: podman start trading-mlflow")
        if db_contents.get("experiments") == "0":
            print("   2. Run model training to create experiments")
        if not health.get("healthy"):
            print("   3. Check container logs: podman logs trading-mlflow")
            print("   4. Restart container: podman restart trading-mlflow")
    else:
        print("\n   ✅ All checks passed!")
        print("   If models still don't show in UI:")
        print("   1. Check tracking URI matches: http://localhost:5000")
        print("   2. Verify experiments exist: python scripts/diagnose_mlflow.py")
        print("   3. Check browser cache or try incognito mode")
    
    print("=" * 70 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Check MLflow container and database")
    parser.add_argument("--container", default="trading-mlflow", help="Container name")
    parser.add_argument("--volume", default="mlflow-data", help="Volume name")
    parser.add_argument("--exec-shell", action="store_true", help="Open shell in container")
    
    args = parser.parse_args()
    
    # Check container status
    container_status = check_container_status(args.container)
    
    if not container_status.get("exists"):
        print(f"❌ Container '{args.container}' does not exist")
        print("💡 Create it: podman-compose up -d mlflow")
        sys.exit(1)
    
    if not container_status.get("running"):
        print(f"⚠️  Container '{args.container}' is not running")
        print("💡 Start it: podman start trading-mlflow")
        sys.exit(1)
    
    # Get container info
    container_info = get_container_info(args.container)
    mounts = check_volume_mounts(args.container)
    db_check = check_database_in_container(args.container)
    db_contents = check_database_contents(args.container, mount_info=mounts)
    health = check_mlflow_server_health(args.container)
    volume_info = get_podman_volume_location(args.volume)
    
    # Print report
    print_report(container_status, container_info, mounts, db_check, db_contents, health, volume_info)
    
    # Exec shell if requested
    if args.exec_shell:
        print("Opening shell in container...")
        subprocess.run(["podman", "exec", "-it", args.container, "/bin/bash"])


if __name__ == "__main__":
    main()
