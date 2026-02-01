#!/usr/bin/env python3
"""
Quick MLflow Database Check

Checks MLflow database directly (works with bind mounts).
Since container uses bind mount to data/mlflow/, we can check the database directly.

Usage:
    python scripts/check_mlflow_db.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "mlflow" / "mlflow.db"


def check_database() -> dict:
    """Check database contents."""
    result = {}
    
    if not DB_PATH.exists():
        result["exists"] = False
        result["error"] = f"Database not found at {DB_PATH}"
        return result
    
    result["exists"] = True
    result["path"] = str(DB_PATH)
    result["size_bytes"] = DB_PATH.stat().st_size
    result["size_mb"] = result["size_bytes"] / 1024 / 1024
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        # Check experiments
        cursor.execute("SELECT COUNT(*) FROM experiments")
        result["experiments"] = cursor.fetchone()[0]
        
        # Check runs
        cursor.execute("SELECT COUNT(*) FROM runs")
        result["runs"] = cursor.fetchone()[0]
        
        # Get experiment list
        cursor.execute("SELECT experiment_id, name FROM experiments ORDER BY experiment_id")
        result["experiment_list"] = [
            {"id": r[0], "name": r[1]} for r in cursor.fetchall()
        ]
        
        # Get latest runs
        if result["runs"] > 0:
            cursor.execute("""
                SELECT run_id, experiment_id, status, start_time, end_time 
                FROM runs 
                ORDER BY start_time DESC 
                LIMIT 5
            """)
            result["latest_runs"] = [
                {
                    "run_id": r[0],
                    "experiment_id": r[1],
                    "status": r[2],
                    "start_time": r[3],
                    "end_time": r[4]
                }
                for r in cursor.fetchall()
            ]
        else:
            result["latest_runs"] = []
        
        conn.close()
        result["query_success"] = True
        
    except Exception as e:
        result["query_success"] = False
        result["error"] = str(e)
    
    return result


def print_report(db_info: dict) -> None:
    """Print diagnostic report."""
    print("\n" + "=" * 70)
    print("MLFLOW DATABASE CHECK")
    print("=" * 70)
    
    if not db_info.get("exists"):
        print(f"\n❌ Database NOT FOUND")
        print(f"   Expected: {DB_PATH}")
        print("\n💡 Database will be created when you:")
        print("   1. Start MLflow container")
        print("   2. Run model training")
        print("=" * 70 + "\n")
        return
    
    print(f"\n✅ Database FOUND")
    print(f"   Path: {db_info['path']}")
    print(f"   Size: {db_info['size_bytes']:,} bytes ({db_info['size_mb']:.2f} MB)")
    
    if not db_info.get("query_success"):
        print(f"\n❌ Cannot query database: {db_info.get('error')}")
        print("=" * 70 + "\n")
        return
    
    print("\n" + "-" * 70)
    print("DATABASE CONTENTS:")
    print("-" * 70)
    
    exp_count = db_info.get("experiments", 0)
    runs_count = db_info.get("runs", 0)
    
    print(f"\n📊 Experiments: {exp_count}")
    print(f"📊 Runs: {runs_count}")
    
    if exp_count > 0:
        print("\n📁 Experiment List:")
        for exp in db_info.get("experiment_list", []):
            print(f"   - {exp['name']} (ID: {exp['id']})")
    
    if runs_count > 0:
        print("\n🔄 Latest Runs:")
        for run in db_info.get("latest_runs", [])[:5]:
            status_icon = "✅" if run["status"] == "FINISHED" else "⏳" if run["status"] == "RUNNING" else "❌"
            print(f"   {status_icon} Run {run['run_id'][:8]}... (Exp: {run['experiment_id']}, Status: {run['status']})")
    
    print("\n" + "=" * 70)
    print("DIAGNOSIS:")
    print("=" * 70)
    
    if runs_count == 0:
        print("\n⚠️  CRITICAL: No runs found in database!")
        print("\n   This is why models don't show in MLflow UI.")
        print("   The database exists but is empty (no models logged).")
        print("\n💡 To fix:")
        print("   1. Run model training to log models:")
        print("      python main.py --mode train")
        print("\n   2. Or check if training is configured to log to MLflow:")
        print("      - Check MLFLOW_TRACKING_URI environment variable")
        print("      - Check config in src/config/custom.yaml")
        print("      - Verify MLflow server is running: curl http://localhost:5000/health")
    elif exp_count == 0:
        print("\n⚠️  No experiments found")
        print("   💡 Run model training to create experiments")
    else:
        print("\n✅ Database contains data")
        print("   If models still don't show in UI:")
        print("   1. Check MLflow UI: http://localhost:5000")
        print("   2. Verify tracking URI matches: http://localhost:5000")
        print("   3. Check browser cache or try incognito mode")
        print("   4. Check container logs: podman logs trading-mlflow")
    
    print("=" * 70 + "\n")


def main():
    """Main entry point."""
    db_info = check_database()
    print_report(db_info)
    
    # Exit with error if database empty
    if db_info.get("runs", 0) == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
