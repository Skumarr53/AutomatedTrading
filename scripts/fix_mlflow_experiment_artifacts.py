#!/usr/bin/env python3
"""
Fix MLflow experiments with incorrect artifact locations.

The Problem:
- Old experiments may have artifact locations like '/mlflow/artifacts/...'
- This causes MLflow client to use LocalArtifactRepository
- Which tries to write directly to /mlflow filesystem, causing permission errors

The Fix:
- Delete experiments with local filesystem artifact locations
- They will be recreated with correct 'mlflow-artifacts:/' scheme
"""
import os
import sys
import argparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import mlflow
    from mlflow.tracking import MlflowClient
    from mlflow.entities import ViewType
except ImportError:
    print("❌ MLflow not installed")
    sys.exit(1)


def check_experiments(tracking_uri: str = "http://localhost:5000"):
    """Check all experiments for incorrect artifact locations."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    
    print(f"Checking MLflow experiments at {tracking_uri}...")
    print("=" * 80)
    
    # Get all experiments including deleted
    all_exps = client.search_experiments(view_type=ViewType.ALL)
    
    problematic = []
    ok = []
    
    for exp in all_exps:
        artifact_loc = exp.artifact_location
        status = "active" if exp.lifecycle_stage == "active" else "deleted"
        
        if artifact_loc.startswith('/mlflow') or artifact_loc.startswith('file://'):
            problematic.append(exp)
            print(f"❌ {exp.name} (ID: {exp.experiment_id}) [{status}]")
            print(f"   Artifact Location: {artifact_loc}")
            print(f"   Problem: Local filesystem - will cause permission errors")
        else:
            ok.append(exp)
            print(f"✅ {exp.name} (ID: {exp.experiment_id}) [{status}]")
            print(f"   Artifact Location: {artifact_loc}")
    
    print("\n" + "=" * 80)
    print(f"Summary: {len(ok)} OK, {len(problematic)} problematic")
    
    return problematic


def fix_experiments(tracking_uri: str = "http://localhost:5000", dry_run: bool = True):
    """Fix experiments with incorrect artifact locations."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    
    print(f"\n{'DRY RUN - ' if dry_run else ''}Fixing MLflow experiments at {tracking_uri}...")
    print("=" * 80)
    
    # Get all experiments including deleted
    all_exps = client.search_experiments(view_type=ViewType.ALL)
    
    fixed_count = 0
    
    for exp in all_exps:
        artifact_loc = exp.artifact_location
        
        if artifact_loc.startswith('/mlflow') or artifact_loc.startswith('file://'):
            print(f"\nFixing: {exp.name} (ID: {exp.experiment_id})")
            print(f"  Current artifact location: {artifact_loc}")
            
            if dry_run:
                print(f"  [DRY RUN] Would delete experiment and all its runs")
            else:
                # Delete all runs in the experiment
                runs = client.search_runs(experiment_ids=[exp.experiment_id])
                for run in runs:
                    print(f"  Deleting run: {run.info.run_id}")
                    client.delete_run(run.info.run_id)
                
                # Delete the experiment
                if exp.lifecycle_stage == "active":
                    client.delete_experiment(exp.experiment_id)
                    print(f"  ✅ Deleted experiment (soft delete)")
                
                # Note: MLflow doesn't have permanent delete API
                # Would need direct database access for permanent delete
                
            fixed_count += 1
    
    print("\n" + "=" * 80)
    if dry_run:
        print(f"DRY RUN: Would fix {fixed_count} experiments")
        print("Run with --fix to actually delete them")
    else:
        print(f"Fixed {fixed_count} experiments (soft deleted)")
        print("Note: Experiments are soft-deleted. For permanent deletion, use database cleanup.")


def permanent_delete_from_db():
    """Permanently delete problematic experiments from the database."""
    import sqlite3
    
    db_path = os.path.join(project_root, "data", "mlflow", "mlflow.db")
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return False
    
    print(f"Permanently deleting from database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find experiments with local artifact locations
    cursor.execute("""
        SELECT experiment_id, name, artifact_location 
        FROM experiments 
        WHERE artifact_location LIKE '/mlflow%' OR artifact_location LIKE 'file:%'
    """)
    
    experiments = cursor.fetchall()
    
    if not experiments:
        print("✅ No experiments with local artifact locations found")
        conn.close()
        return True
    
    print(f"Found {len(experiments)} experiments to delete:")
    for exp_id, name, artifact_loc in experiments:
        print(f"  - {name} (ID: {exp_id}): {artifact_loc}")
    
    # Delete runs
    cursor.execute("""
        DELETE FROM runs 
        WHERE experiment_id IN (
            SELECT experiment_id FROM experiments 
            WHERE artifact_location LIKE '/mlflow%' OR artifact_location LIKE 'file:%'
        )
    """)
    print(f"Deleted {cursor.rowcount} runs")
    
    # Delete experiments
    cursor.execute("""
        DELETE FROM experiments 
        WHERE artifact_location LIKE '/mlflow%' OR artifact_location LIKE 'file:%'
    """)
    print(f"Deleted {cursor.rowcount} experiments")
    
    conn.commit()
    conn.close()
    
    print("✅ Permanent deletion complete")
    return True


def main():
    parser = argparse.ArgumentParser(description="Fix MLflow experiments with incorrect artifact locations")
    parser.add_argument("--tracking-uri", default="http://localhost:5000", help="MLflow tracking URI")
    parser.add_argument("--fix", action="store_true", help="Actually fix (delete) problematic experiments")
    parser.add_argument("--permanent", action="store_true", help="Permanently delete from database")
    
    args = parser.parse_args()
    
    if args.permanent:
        permanent_delete_from_db()
    elif args.fix:
        fix_experiments(args.tracking_uri, dry_run=False)
    else:
        problematic = check_experiments(args.tracking_uri)
        if problematic:
            print("\nTo fix these experiments, run:")
            print(f"  python {__file__} --fix")
            print("\nOr for permanent deletion from database:")
            print(f"  python {__file__} --permanent")


if __name__ == "__main__":
    main()
