#!/usr/bin/env python3
"""
Debug MLflow artifact logging flow to trace where files are actually written.

This script will:
1. Show MLflow configuration
2. Create a test artifact
3. Trace the full logging flow
4. Show where files are actually written
"""
import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import mlflow
import matplotlib.pyplot as plt
import numpy as np

def debug_mlflow_config():
    """Show MLflow configuration."""
    print("\n" + "=" * 70)
    print("MLFLOW CONFIGURATION")
    print("=" * 70)
    
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    
    print(f"Tracking URI: {mlflow.get_tracking_uri()}")
    
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient()
        
        # Get active run
        active_run = mlflow.active_run()
        if active_run:
            print(f"\nActive Run ID: {active_run.info.run_id}")
            print(f"Active Run Artifact URI: {active_run.info.artifact_uri}")
        else:
            print("\nNo active run")
            
        # List experiments
        experiments = client.search_experiments()
        if experiments:
            exp = experiments[0]
            print(f"\nFirst Experiment: {exp.name} (ID: {exp.experiment_id})")
            print(f"Experiment Artifact Location: {exp.artifact_location}")
            
            # Get latest run
            runs = client.search_runs(experiment_ids=[exp.experiment_id], max_results=1)
            if runs:
                run = runs[0]
                print(f"\nLatest Run ID: {run.info.run_id}")
                print(f"Run Artifact URI: {run.info.artifact_uri}")
                
    except Exception as e:
        print(f"Error getting MLflow info: {e}")
        import traceback
        traceback.print_exc()

def trace_artifact_logging():
    """Trace the full artifact logging flow."""
    print("\n" + "=" * 70)
    print("TRACING ARTIFACT LOGGING FLOW")
    print("=" * 70)
    
    # Create test experiment
    experiment_name = "debug_artifact_flow"
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"\n✅ Created experiment: {experiment_name} (ID: {experiment_id})")
    except Exception:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment:
            experiment_id = experiment.experiment_id
            print(f"\n✅ Using existing experiment: {experiment_name} (ID: {experiment_id})")
        else:
            raise
    
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name="debug_run"):
        print("\n" + "-" * 70)
        print("STEP 1: Create temporary directory")
        print("-" * 70)
        
        tmpdir = tempfile.mkdtemp()
        print(f"✅ Created temp directory: {tmpdir}")
        print(f"   Exists: {os.path.exists(tmpdir)}")
        print(f"   Writable: {os.access(tmpdir, os.W_OK)}")
        
        try:
            print("\n" + "-" * 70)
            print("STEP 2: Create test file")
            print("-" * 70)
            
            test_file = os.path.join(tmpdir, "test_artifact.txt")
            with open(test_file, 'w') as f:
                f.write("Test artifact content\n")
            
            print(f"✅ Created file: {test_file}")
            print(f"   Exists: {os.path.exists(test_file)}")
            print(f"   Size: {os.path.getsize(test_file)} bytes")
            print(f"   Absolute path: {os.path.abspath(test_file)}")
            
            print("\n" + "-" * 70)
            print("STEP 3: Check MLflow active run")
            print("-" * 70)
            
            active_run = mlflow.active_run()
            if active_run:
                print(f"✅ Active run: {active_run.info.run_id}")
                print(f"   Artifact URI: {active_run.info.artifact_uri}")
                print(f"   Note: This is where MLflow will STORE the artifact")
                print(f"   (This is the server-side path, not where code writes)")
            
            print("\n" + "-" * 70)
            print("STEP 4: Log artifact to MLflow")
            print("-" * 70)
            
            print(f"Calling: mlflow.log_artifact('{test_file}')")
            print(f"   Source file: {test_file}")
            print(f"   Source exists: {os.path.exists(test_file)}")
            
            mlflow.log_artifact(test_file)
            
            print(f"✅ Successfully called mlflow.log_artifact()")
            print(f"   MLflow client uploads file to server")
            print(f"   Server stores at: {active_run.info.artifact_uri}/test_artifact.txt")
            
            print("\n" + "-" * 70)
            print("STEP 5: Verify artifact was logged")
            print("-" * 70)
            
            from mlflow.tracking import MlflowClient
            client = MlflowClient()
            artifacts = client.list_artifacts(active_run.info.run_id)
            
            print(f"✅ Artifacts in run:")
            for artifact in artifacts:
                print(f"   - {artifact.path} (size: {artifact.file_size if hasattr(artifact, 'file_size') else 'N/A'})")
            
            print("\n" + "-" * 70)
            print("STEP 6: Check what MLflow shows")
            print("-" * 70)
            
            run_info = client.get_run(active_run.info.run_id)
            print(f"Run artifact URI: {run_info.info.artifact_uri}")
            print(f"\n💡 This URI shows where MLflow STORES artifacts on the server")
            print(f"   It may show '/mlflow/artifacts/...' - this is CORRECT")
            print(f"   This is the server-side path, not where your code writes")
            
        finally:
            print("\n" + "-" * 70)
            print("STEP 7: Cleanup")
            print("-" * 70)
            
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"✅ Cleaned up temp directory: {tmpdir}")
            print(f"   Still exists: {os.path.exists(tmpdir)}")

def check_actual_file_locations():
    """Check where files are actually being written."""
    print("\n" + "=" * 70)
    print("CHECKING ACTUAL FILE LOCATIONS")
    print("=" * 70)
    
    # Check if /mlflow exists on host
    mlflow_paths = [
        "/mlflow",
        "/mlflow/artifacts",
        "./mlruns",
        "data/mlflow",
        "data/mlflow/artifacts"
    ]
    
    print("\nChecking paths on HOST filesystem:")
    for path in mlflow_paths:
        exists = os.path.exists(path)
        is_dir = os.path.isdir(path) if exists else False
        status = "✅ EXISTS" if exists else "❌ NOT FOUND"
        print(f"   {path:30} {status} {'(dir)' if is_dir else ''}")
    
    # Check temp directory
    tmpdir = tempfile.gettempdir()
    print(f"\nTemp directory: {tmpdir}")
    print(f"   Exists: {os.path.exists(tmpdir)}")
    print(f"   Writable: {os.access(tmpdir, os.W_OK)}")

def main():
    """Run all debug checks."""
    print("\n" + "=" * 70)
    print("MLFLOW ARTIFACT LOGGING FLOW DEBUG")
    print("=" * 70)
    
    debug_mlflow_config()
    check_actual_file_locations()
    trace_artifact_logging()
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("""
KEY INSIGHT:
When you call mlflow.log_artifact('/tmp/tmpXXX/file.png'):
  1. Your code writes to: /tmp/tmpXXX/file.png (HOST)
  2. MLflow client uploads to server
  3. MLflow server stores at: /mlflow/artifacts/<exp>/<run>/artifacts/file.png (CONTAINER)
  
If you see '/mlflow' in logs, it's the SERVER-SIDE path, which is CORRECT.
Your code should NOT write directly to /mlflow - that's what we fixed!
    """)
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
