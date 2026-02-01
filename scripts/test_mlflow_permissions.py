#!/usr/bin/env python3
"""
Test MLflow permissions to ensure artifact logging works.

This script:
1. Tests host-side temp directory creation
2. Tests MLflow client upload
3. Tests container-side write permissions
4. Verifies artifact appears in MLflow
"""
import os
import sys
import tempfile
import shutil
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_host_permissions():
    """Test 1: Host-side temp directory creation."""
    print("\n" + "=" * 70)
    print("TEST 1: Host-Side Temp Directory Creation")
    print("=" * 70)
    
    tmpdir = tempfile.mkdtemp()
    try:
        test_file = os.path.join(tmpdir, "test.txt")
        
        # Test write
        with open(test_file, 'w') as f:
            f.write("test")
        
        if os.path.exists(test_file):
            print(f"✅ SUCCESS: Created file: {test_file}")
            print(f"   Directory exists: {os.path.exists(tmpdir)}")
            print(f"   Directory writable: {os.access(tmpdir, os.W_OK)}")
            print(f"   File exists: {os.path.exists(test_file)}")
            return True
        else:
            print(f"❌ FAILED: File not created")
            return False
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def test_container_permissions():
    """Test 2: Container-side write permissions."""
    print("\n" + "=" * 70)
    print("TEST 2: Container-Side Write Permissions")
    print("=" * 70)
    
    container_name = "trading-mlflow"
    
    # Check if container is running
    result = subprocess.run(
        ["podman", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    
    if container_name not in result.stdout:
        print(f"⚠️  Container '{container_name}' is not running")
        print("   Skipping container permission test")
        return None
    
    print(f"✅ Container '{container_name}' is running")
    
    # Test write permission
    result = subprocess.run(
        ["podman", "exec", container_name, "sh", "-c", 
         "test -w /mlflow/artifacts && echo 'WRITABLE' || echo 'NOT_WRITABLE'"],
        capture_output=True,
        text=True
    )
    
    if "WRITABLE" in result.stdout:
        print("✅ SUCCESS: Container can write to /mlflow/artifacts")
        
        # Try actual write
        result = subprocess.run(
            ["podman", "exec", container_name, "sh", "-c",
             "echo 'test' > /mlflow/artifacts/permission_test.txt && ls -la /mlflow/artifacts/permission_test.txt"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ SUCCESS: Container successfully wrote test file")
            print(f"   {result.stdout.strip()}")
            
            # Cleanup
            subprocess.run(
                ["podman", "exec", container_name, "rm", "-f", "/mlflow/artifacts/permission_test.txt"],
                capture_output=True
            )
            return True
        else:
            print(f"❌ FAILED: Container cannot write file")
            print(f"   Error: {result.stderr.strip()}")
            return False
    else:
        print("❌ FAILED: Container cannot write to /mlflow/artifacts")
        print("   Directory is not writable by container user")
        return False

def test_mlflow_artifact_logging():
    """Test 3: End-to-end MLflow artifact logging."""
    print("\n" + "=" * 70)
    print("TEST 3: End-to-End MLflow Artifact Logging")
    print("=" * 70)
    
    try:
        import mlflow
        import matplotlib
        matplotlib.use('Agg')  # Non-interactive backend
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as e:
        print(f"⚠️  MLflow/matplotlib not available: {e}")
        print("   Skipping MLflow artifact logging test")
        return None
    
    # Set tracking URI
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    
    print(f"Tracking URI: {mlflow.get_tracking_uri()}")
    
    # Check if server is accessible
    try:
        import requests
        response = requests.get(f"{tracking_uri}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ MLflow server not healthy (status: {response.status_code})")
            return False
        print("✅ MLflow server is healthy")
    except Exception as e:
        print(f"❌ Cannot connect to MLflow server: {e}")
        return False
    
    # Create test experiment
    experiment_name = "permission_test"
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"✅ Created experiment: {experiment_name} (ID: {experiment_id})")
    except Exception:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment:
            experiment_id = experiment.experiment_id
            print(f"✅ Using existing experiment: {experiment_name} (ID: {experiment_id})")
        else:
            print("❌ Failed to get/create experiment")
            return False
    
    mlflow.set_experiment(experiment_name)
    
    # Test artifact logging
    tmpdir = tempfile.mkdtemp()
    try:
        # Create test file
        test_file = os.path.join(tmpdir, "test_artifact.txt")
        with open(test_file, 'w') as f:
            f.write("This is a test artifact for permission verification.\n")
        
        # Create test plot
        plot_file = os.path.join(tmpdir, "test_plot.png")
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot([1, 2, 3], [1, 2, 3])
        ax.set_title("Permission Test Plot")
        plt.savefig(plot_file)
        plt.close()
        
        print(f"✅ Created test files in temp directory: {tmpdir}")
        
        # Start run and log artifacts
        with mlflow.start_run(run_name="permission_test_run"):
            print("✅ Started MLflow run")
            
            try:
                # Log text artifact
                mlflow.log_artifact(test_file)
                print("✅ Successfully logged text artifact")
                
                # Log plot artifact
                mlflow.log_artifact(plot_file)
                print("✅ Successfully logged plot artifact")
                
                # Verify artifacts were logged
                from mlflow.tracking import MlflowClient
                client = MlflowClient()
                active_run = mlflow.active_run()
                
                if active_run:
                    artifacts = client.list_artifacts(active_run.info.run_id)
                    artifact_names = [a.path for a in artifacts]
                    
                    if "test_artifact.txt" in artifact_names and "test_plot.png" in artifact_names:
                        print("✅ Artifacts verified in MLflow")
                        print(f"   Artifacts: {artifact_names}")
                        return True
                    else:
                        print(f"⚠️  Artifacts not found in MLflow")
                        print(f"   Found: {artifact_names}")
                        return False
                else:
                    print("❌ No active run found")
                    return False
                    
            except Exception as e:
                print(f"❌ Failed to log artifacts: {e}")
                import traceback
                traceback.print_exc()
                return False
                
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("MLFLOW PERMISSION TEST SUITE")
    print("=" * 70)
    
    results = {}
    
    # Test 1: Host permissions
    results['host'] = test_host_permissions()
    
    # Test 2: Container permissions
    results['container'] = test_container_permissions()
    
    # Test 3: MLflow artifact logging
    results['mlflow'] = test_mlflow_artifact_logging()
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    for test_name, result in results.items():
        if result is None:
            status = "⚠️  SKIPPED"
        elif result:
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        print(f"  {test_name.upper():15} {status}")
    
    # Overall result
    passed = sum(1 for r in results.values() if r is True)
    failed = sum(1 for r in results.values() if r is False)
    skipped = sum(1 for r in results.values() if r is None)
    
    print(f"\n  Total: {passed} passed, {failed} failed, {skipped} skipped")
    
    if failed > 0:
        print("\n❌ Some tests failed. Check permissions:")
        print("   bash scripts/ensure_mlflow_permissions.sh")
        return 1
    elif passed > 0:
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n⚠️  All tests skipped (container not running or MLflow not available)")
        return 0

if __name__ == "__main__":
    sys.exit(main())
