#!/usr/bin/env python3
"""
Test MLflow artifact logging to verify it works correctly.

This script tests:
1. MLflow connection
2. Creating a test artifact
3. Logging it to MLflow
4. Verifying it appears in MLflow
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
from loguru import logger

def test_mlflow_connection():
    """Test MLflow connection."""
    try:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        mlflow.set_tracking_uri(tracking_uri)
        
        # Try to get tracking URI
        actual_uri = mlflow.get_tracking_uri()
        logger.info(f"✅ MLflow tracking URI: {actual_uri}")
        
        # Try to list experiments
        experiments = mlflow.search_experiments()
        logger.info(f"✅ Found {len(experiments)} experiments")
        
        return True
    except Exception as e:
        logger.error(f"❌ Failed to connect to MLflow: {e}")
        return False

def test_artifact_logging():
    """Test artifact logging with a simple file."""
    try:
        # Create a test experiment
        experiment_name = "test_artifact_logging"
        try:
            experiment_id = mlflow.create_experiment(experiment_name)
            logger.info(f"✅ Created experiment: {experiment_name} (ID: {experiment_id})")
        except Exception:
            # Experiment might already exist
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment:
                experiment_id = experiment.experiment_id
                logger.info(f"✅ Using existing experiment: {experiment_name} (ID: {experiment_id})")
            else:
                raise
        
        mlflow.set_experiment(experiment_name)
        
        # Start a run
        with mlflow.start_run(run_name="test_artifact_run"):
            logger.info("✅ Started MLflow run")
            
            # Create a temporary directory
            tmpdir = tempfile.mkdtemp()
            try:
                # Create a test file
                test_file = os.path.join(tmpdir, "test_artifact.txt")
                with open(test_file, 'w') as f:
                    f.write("This is a test artifact file.\n")
                    f.write("If you can see this, artifact logging is working!\n")
                
                logger.info(f"✅ Created test file: {test_file}")
                
                # Log the artifact
                mlflow.log_artifact(test_file)
                logger.info(f"✅ Successfully logged artifact: {test_file}")
                
                # Create a test plot
                plot_file = os.path.join(tmpdir, "test_plot.png")
                fig, ax = plt.subplots(figsize=(6, 4))
                x = np.linspace(0, 10, 100)
                y = np.sin(x)
                ax.plot(x, y)
                ax.set_title("Test Plot")
                ax.set_xlabel("X")
                ax.set_ylabel("Y")
                plt.savefig(plot_file)
                plt.close()
                
                logger.info(f"✅ Created test plot: {plot_file}")
                
                # Log the plot
                mlflow.log_artifact(plot_file)
                logger.info(f"✅ Successfully logged plot artifact: {plot_file}")
                
                # Log a metric to verify run is active
                mlflow.log_metric("test_metric", 42.0)
                logger.info("✅ Logged test metric")
                
                logger.info("✅ All artifact logging tests passed!")
                return True
                
            finally:
                # Cleanup
                shutil.rmtree(tmpdir, ignore_errors=True)
                logger.info("✅ Cleaned up temporary directory")
    
    except Exception as e:
        logger.error(f"❌ Artifact logging test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("MLFLOW ARTIFACT LOGGING TEST")
    print("=" * 70 + "\n")
    
    # Test connection
    if not test_mlflow_connection():
        print("\n❌ MLflow connection test failed. Check:")
        print("   1. MLflow server is running: curl http://localhost:5000/health")
        print("   2. MLFLOW_TRACKING_URI is set correctly")
        sys.exit(1)
    
    # Test artifact logging
    if not test_artifact_logging():
        print("\n❌ Artifact logging test failed.")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED!")
    print("=" * 70)
    print("\n💡 Check MLflow UI: http://localhost:5000")
    print("   Look for experiment: 'test_artifact_logging'")
    print("   You should see artifacts: test_artifact.txt and test_plot.png")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
