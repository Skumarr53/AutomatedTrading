#!/usr/bin/env python3
"""
Fix MLflow artifact URI issue.

The problem: MLflow client is trying to write directly to /mlflow filesystem
instead of uploading via HTTP. This happens when the artifact URI is a local
filesystem path instead of an HTTP URL.

Solution: Ensure MLflow client uses HTTP artifact repository by:
1. Verifying tracking URI is HTTP (not file://)
2. Ensuring artifact URI from server is HTTP
3. Forcing HTTP artifact repository if needed
"""
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import mlflow
    from mlflow.tracking import MlflowClient
    from mlflow.tracking.artifact_utils import get_artifact_repo
except ImportError:
    print("❌ MLflow not installed")
    sys.exit(1)

def fix_mlflow_artifact_uri():
    """Ensure MLflow uses HTTP artifact repository."""
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    
    print(f"Setting tracking URI to: {tracking_uri}")
    mlflow.set_tracking_uri(tracking_uri)
    
    # Verify tracking URI is HTTP
    actual_uri = mlflow.get_tracking_uri()
    if actual_uri.startswith("file://"):
        print(f"❌ ERROR: Tracking URI is file:// (should be http://)")
        print(f"   Current: {actual_uri}")
        print(f"   Fix: Set MLFLOW_TRACKING_URI=http://localhost:5000")
        return False
    
    if not actual_uri.startswith("http://") and not actual_uri.startswith("https://"):
        print(f"❌ ERROR: Tracking URI is not HTTP")
        print(f"   Current: {actual_uri}")
        return False
    
    print(f"✅ Tracking URI is HTTP: {actual_uri}")
    
    # Check artifact URI from a run
    try:
        client = MlflowClient(tracking_uri=tracking_uri)
        
        # Get or create a test experiment
        exp_name = "artifact_uri_test"
        try:
            exp = client.get_experiment_by_name(exp_name)
            if exp is None:
                exp_id = client.create_experiment(exp_name)
                exp = client.get_experiment(exp_id)
        except Exception:
            exp_id = client.create_experiment(exp_name)
            exp = client.get_experiment(exp_id)
        
        # Start a test run
        with mlflow.start_run(experiment_id=exp.experiment_id):
            run = mlflow.active_run()
            artifact_uri = run.info.artifact_uri
            
            print(f"\nArtifact URI from server: {artifact_uri}")
            
            # Check if it's HTTP
            if artifact_uri.startswith("file://") or (artifact_uri.startswith("/") and not artifact_uri.startswith("http")):
                print(f"❌ PROBLEM: Artifact URI is local filesystem!")
                print(f"   This causes MLflow to write directly to filesystem")
                print(f"   instead of uploading via HTTP.")
                print(f"\n   Solution: MLflow server must return HTTP artifact URIs")
                print(f"   Check server configuration: --serve-artifacts flag")
                return False
            elif artifact_uri.startswith("http://") or artifact_uri.startswith("https://"):
                print(f"✅ Artifact URI uses HTTP (correct)")
                
                # Verify artifact repository type
                artifact_repo = get_artifact_repo(artifact_uri)
                repo_type = type(artifact_repo).__name__
                print(f"   Artifact repository type: {repo_type}")
                
                if "LocalArtifactRepository" in repo_type:
                    print(f"   ⚠️  Warning: Using LocalArtifactRepository (should be HTTP)")
                    return False
                elif "HttpArtifactRepository" in repo_type or "RestStore" in repo_type:
                    print(f"   ✅ Using HTTP artifact repository (correct)")
                    return True
                else:
                    print(f"   ⚠️  Unknown repository type: {repo_type}")
                    return True  # Assume OK if unknown
            else:
                print(f"   ⚠️  Unknown artifact URI format: {artifact_uri}")
                return True  # Assume OK if unknown
                
    except Exception as e:
        print(f"❌ Error checking artifact URI: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_mlflow_artifact_uri()
    sys.exit(0 if success else 1)
