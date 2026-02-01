#!/usr/bin/env python3
"""
MLflow Diagnostic Script

Diagnoses MLflow setup issues:
1. Checks if MLflow server is running
2. Verifies database location and accessibility
3. Lists experiments and runs
4. Checks tracking URI configuration
5. Verifies artifact storage location

Usage:
    python scripts/diagnose_mlflow.py
    python scripts/diagnose_mlflow.py --fix-paths  # Update config with absolute paths
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import mlflow
    from mlflow.tracking import MlflowClient
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logger.error("MLflow not installed")
    sys.exit(1)

from src import config


def check_server_running(url: str = "http://localhost:5000") -> bool:
    """Check if MLflow server is running."""
    try:
        response = requests.get(url, timeout=5)
        return response.status_code == 200
    except Exception as e:
        logger.debug(f"Server check failed: {e}")
        return False


def get_database_location(backend_uri: str) -> Optional[str]:
    """Get actual database file location from SQLite URI."""
    if backend_uri.startswith("sqlite:///"):
        # sqlite:///path/to/db.db
        db_path = backend_uri.replace("sqlite:///", "")
        if not os.path.isabs(db_path):
            # Relative path - resolve from current directory
            db_path = os.path.abspath(db_path)
        return db_path
    return None


def check_container_mlflow() -> dict:
    """Check if MLflow is running in container."""
    import subprocess
    
    result = {"is_container": False, "container_name": None, "running": False}
    
    # Check for trading-mlflow container
    try:
        code, stdout, _ = subprocess.run(
            ["podman", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=False
        )
        if "trading-mlflow" in stdout:
            result["is_container"] = True
            result["container_name"] = "trading-mlflow"
            result["running"] = True
        else:
            # Check if exists but not running
            code2, stdout2, _ = subprocess.run(
                ["podman", "ps", "-a", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=False
            )
            if "trading-mlflow" in stdout2:
                result["is_container"] = True
                result["container_name"] = "trading-mlflow"
                result["running"] = False
    except Exception:
        pass
    
    return result


def diagnose_mlflow(fix_paths: bool = False) -> None:
    """Run MLflow diagnostics."""
    print("\n" + "=" * 70)
    print("MLFLOW DIAGNOSTIC REPORT")
    print("=" * 70)
    
    # Check if running in container
    container_info = check_container_mlflow()
    if container_info["is_container"]:
        print("\n📦 MLflow is running in CONTAINER")
        print(f"   Container: {container_info['container_name']}")
        print(f"   Status: {'✅ Running' if container_info['running'] else '❌ Not running'}")
        if container_info["running"]:
            print("\n   💡 For detailed container check, run:")
            print("      python scripts/check_mlflow_container.py")
        print("\n" + "-" * 70)
    
    # Load environment
    load_dotenv()
    
    # Get configuration
    ml_config = config.mlflow_config
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", ml_config.get("tracking_uri", "http://localhost:5000"))
    backend_uri = ml_config.get("backend_store_uri", "sqlite:///mlflow.db")
    artifact_root = ml_config.get("default_artifact_root", "./mlruns")
    
    print(f"\n1. Configuration:")
    print(f"   Tracking URI: {tracking_uri}")
    print(f"   Backend Store URI: {backend_uri}")
    print(f"   Artifact Root: {artifact_root}")
    print(f"   Server Host: {ml_config.get('host', '0.0.0.0')}")
    print(f"   Server Port: {ml_config.get('port', 5000)}")
    
    # Check server
    server_url = f"http://{ml_config.get('host', '0.0.0.0')}:{ml_config.get('port', 5000)}"
    print(f"\n2. Server Status:")
    if check_server_running(server_url):
        print(f"   ✅ MLflow server is running at {server_url}")
    else:
        print(f"   ❌ MLflow server is NOT running at {server_url}")
        print(f"   💡 Start it with: mlflow server --backend-store-uri {backend_uri} --default-artifact-root {artifact_root} --host {ml_config.get('host', '0.0.0.0')} --port {ml_config.get('port', 5000)}")
    
    # Check database location
    print(f"\n3. Database Location:")
    db_path = get_database_location(backend_uri)
    if db_path:
        if os.path.exists(db_path):
            size = os.path.getsize(db_path)
            print(f"   ✅ Database exists: {db_path}")
            print(f"   📊 Size: {size:,} bytes ({size / 1024 / 1024:.2f} MB)")
        else:
            print(f"   ⚠️  Database file not found: {db_path}")
            print(f"   💡 Database will be created on first run")
    else:
        print(f"   ℹ️  Backend URI: {backend_uri} (not SQLite or absolute path)")
    
    # Check artifact root
    print(f"\n4. Artifact Storage:")
    artifact_path = Path(artifact_root).resolve()
    if artifact_path.exists():
        print(f"   ✅ Artifact root exists: {artifact_path}")
        # Count experiments
        experiments = list(artifact_path.glob("*/"))
        print(f"   📁 Found {len(experiments)} experiment directories")
    else:
        print(f"   ⚠️  Artifact root not found: {artifact_path}")
        print(f"   💡 Will be created on first run")
    
    # Try to connect and list experiments
    print(f"\n5. MLflow Client Connection:")
    try:
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
        
        try:
            experiments = client.search_experiments()
            print(f"   ✅ Connected successfully")
            print(f"   📊 Found {len(experiments)} experiments")
            
            if experiments:
                print(f"\n   Experiments:")
                for exp in experiments[:10]:  # Show first 10
                    run_count = len(client.search_runs(experiment_ids=[exp.experiment_id]))
                    print(f"      - {exp.name} (ID: {exp.experiment_id[:8]}...) - {run_count} runs")
                
                # Get runs from first experiment
                if experiments:
                    first_exp = experiments[0]
                    runs = client.search_runs(
                        experiment_ids=[first_exp.experiment_id],
                        max_results=5
                    )
                    if runs:
                        print(f"\n   Recent runs in '{first_exp.name}':")
                        for run in runs:
                            status = run.info.status
                            print(f"      - {run.info.run_name} ({status}) - {run.info.run_id[:8]}...")
            else:
                print(f"   ⚠️  No experiments found")
                print(f"   💡 Run model training to create experiments")
        
        except Exception as e:
            print(f"   ❌ Error querying experiments: {e}")
            print(f"   💡 Check if database is accessible and server is running")
    
    except Exception as e:
        print(f"   ❌ Failed to connect: {e}")
        print(f"   💡 Verify tracking URI and server status")
    
    # Check for path issues
    print(f"\n6. Path Issues:")
    issues = []
    
    if backend_uri.startswith("sqlite:///") and not os.path.isabs(backend_uri.replace("sqlite:///", "")):
        issues.append(f"Backend URI is relative: {backend_uri}")
    
    if not os.path.isabs(artifact_root):
        issues.append(f"Artifact root is relative: {artifact_root}")
    
    if issues:
        print(f"   ⚠️  Found {len(issues)} potential issues:")
        for issue in issues:
            print(f"      - {issue}")
        print(f"\n   💡 Recommendation: Use absolute paths for consistency")
        
        if fix_paths:
            print(f"\n   🔧 Fixing paths...")
            project_root = Path(__file__).parent.parent
            abs_backend = f"sqlite:///{project_root / 'mlflow.db'}"
            abs_artifact = str(project_root / "mlruns")
            
            print(f"      Backend URI: {backend_uri} → {abs_backend}")
            print(f"      Artifact Root: {artifact_root} → {abs_artifact}")
            print(f"\n   ⚠️  Update src/config/custom.yaml manually:")
            print(f"      backend_store_uri: \"{abs_backend}\"")
            print(f"      default_artifact_root: \"{abs_artifact}\"")
    else:
        print(f"   ✅ All paths are absolute")
    
    # Check environment variable
    print(f"\n7. Environment Variables:")
    env_uri = os.getenv("MLFLOW_TRACKING_URI")
    if env_uri:
        print(f"   MLFLOW_TRACKING_URI: {env_uri}")
        if env_uri != tracking_uri:
            print(f"   ⚠️  Environment variable differs from config!")
            print(f"      Config: {tracking_uri}")
            print(f"      Env: {env_uri}")
    else:
        print(f"   MLFLOW_TRACKING_URI: Not set (using config)")
    
    print("\n" + "=" * 70)
    print("\n💡 Troubleshooting Tips:")
    print("   1. Ensure MLflow server is running with correct paths")
    print("   2. Check that tracking URI matches server URL")
    print("   3. Verify database file exists and is readable")
    print("   4. Check artifact root directory permissions")
    print("   5. Restart MLflow server after changing paths")
    print("=" * 70 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Diagnose MLflow setup")
    parser.add_argument("--fix-paths", action="store_true", help="Show how to fix relative paths")
    args = parser.parse_args()
    
    diagnose_mlflow(fix_paths=args.fix_paths)


if __name__ == "__main__":
    main()
