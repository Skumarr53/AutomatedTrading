#!/usr/bin/env python3
"""
Diagnose MLflow artifact URI configuration.

The issue: MLflow client is trying to write directly to /mlflow instead of uploading via HTTP.
This happens when the artifact URI is a local filesystem path instead of an HTTP URL.
"""
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import mlflow
    from mlflow.tracking import MlflowClient
except ImportError:
    print("❌ MLflow not installed")
    sys.exit(1)

print("=" * 70)
print("MLFLOW ARTIFACT URI DIAGNOSIS")
print("=" * 70)

# Check tracking URI
tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
print(f"\n1. Tracking URI Configuration:")
print(f"   Environment: {os.getenv('MLFLOW_TRACKING_URI', 'Not set')}")
print(f"   Using: {tracking_uri}")

mlflow.set_tracking_uri(tracking_uri)
actual_tracking_uri = mlflow.get_tracking_uri()
print(f"   MLflow client tracking URI: {actual_tracking_uri}")

# Check if server is accessible
print(f"\n2. Server Accessibility:")
try:
    import requests
    response = requests.get(f"{tracking_uri}/health", timeout=5)
    if response.status_code == 200:
        print(f"   ✅ Server is accessible")
    else:
        print(f"   ⚠️  Server returned status {response.status_code}")
except Exception as e:
    print(f"   ❌ Cannot connect to server: {e}")
    sys.exit(1)

# Check artifact URI from server
print(f"\n3. Artifact URI from Server:")
try:
    client = MlflowClient(tracking_uri=tracking_uri)
    
    # Try to get experiments to see artifact URI
    experiments = client.search_experiments(max_results=1)
    if experiments:
        exp = experiments[0]
        print(f"   Experiment: {exp.name}")
        print(f"   Experiment ID: {exp.experiment_id}")
        
        # Get a run to see artifact URI
        runs = client.search_runs(experiment_ids=[exp.experiment_id], max_results=1)
        if runs:
            run = runs[0]
            artifact_uri = run.info.artifact_uri
            print(f"   Run artifact URI: {artifact_uri}")
            
            # Check if it's a local filesystem path
            if artifact_uri.startswith("file://") or artifact_uri.startswith("/"):
                print(f"\n   ❌ PROBLEM: Artifact URI is a local filesystem path!")
                print(f"      This causes MLflow client to write directly to filesystem")
                print(f"      instead of uploading via HTTP.")
                print(f"\n   Expected: http://localhost:5000/api/2.0/mlflow-artifacts/...")
                print(f"   Actual: {artifact_uri}")
                print(f"\n   SOLUTION: MLflow server must be configured with HTTP artifact URI")
            elif artifact_uri.startswith("http://") or artifact_uri.startswith("https://"):
                print(f"   ✅ Artifact URI uses HTTP (correct)")
            else:
                print(f"   ⚠️  Unknown artifact URI format: {artifact_uri}")
        else:
            print(f"   ⚠️  No runs found in experiment")
    else:
        print(f"   ⚠️  No experiments found")
        
except Exception as e:
    print(f"   ❌ Error checking artifact URI: {e}")
    import traceback
    traceback.print_exc()

# Check what artifact repository MLflow would use
print(f"\n4. Artifact Repository Type:")
try:
    from mlflow.tracking.artifact_utils import get_artifact_repo
    from mlflow.tracking import _get_or_start_run
    
    # Try to get active run
    try:
        run = _get_or_start_run()
        artifact_uri = run.info.artifact_uri
        print(f"   Active run artifact URI: {artifact_uri}")
        
        # Get artifact repository
        artifact_repo = get_artifact_repo(artifact_uri)
        repo_type = type(artifact_repo).__name__
        print(f"   Artifact repository type: {repo_type}")
        
        if "LocalArtifactRepository" in repo_type:
            print(f"\n   ❌ PROBLEM: Using LocalArtifactRepository!")
            print(f"      This means MLflow will try to write directly to filesystem")
            print(f"      instead of uploading via HTTP.")
        elif "HttpArtifactRepository" in repo_type or "RestStore" in repo_type:
            print(f"   ✅ Using HTTP artifact repository (correct)")
        else:
            print(f"   ⚠️  Unknown repository type: {repo_type}")
            
    except Exception as e:
        print(f"   ⚠️  Could not get active run: {e}")
        
except Exception as e:
    print(f"   ⚠️  Could not check artifact repository: {e}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("\nIf artifact URI is a local filesystem path (file:// or /), MLflow client")
print("will try to write directly to that path, causing permission errors.")
print("\nThe MLflow server must be configured to return HTTP artifact URIs.")
print("Check the server's --default-artifact-root configuration.")
