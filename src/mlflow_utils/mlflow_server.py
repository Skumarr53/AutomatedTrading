from src import config
import subprocess
import requests
import time
import mlflow
import os
from pathlib import Path

def is_mlflow_server_running(url="http://0.0.0.0:5000"):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.ConnectionError:
        return False

def _resolve_path(path: str, base_dir: Path) -> str:
    """Resolve relative paths to absolute paths."""
    if os.path.isabs(path):
        return path
    
    # Handle SQLite URI
    if path.startswith("sqlite:///"):
        db_path = path.replace("sqlite:///", "")
        if not os.path.isabs(db_path):
            db_path = str(base_dir / db_path)
        return f"sqlite:///{db_path}"
    
    # Regular path
    return str(base_dir / path)

def start_mlflow_server():
    """
    Start MLflow server with resolved absolute paths.
    
    Ensures database and artifacts are stored in project directory
    regardless of where the server is started from.
    """
    ml_config = config.mlflow_config
    
    # Get project root directory
    project_root = Path(__file__).parent.parent.parent
    
    # Resolve paths to absolute
    backend_uri = ml_config.get("backend_store_uri", "sqlite:///mlflow.db")
    artifact_root = ml_config.get("default_artifact_root", "./mlruns")
    
    backend_uri_abs = _resolve_path(backend_uri, project_root)
    artifact_root_abs = _resolve_path(artifact_root, project_root)
    
    # Ensure artifact directory exists
    artifact_dir = Path(artifact_root_abs.replace("sqlite:///", ""))
    artifact_dir.mkdir(parents=True, exist_ok=True)
    
    command = [
        "mlflow", "server",
        "--backend-store-uri", backend_uri_abs,
        "--default-artifact-root", artifact_root_abs,
        "--host", ml_config.get("host", "0.0.0.0"),
        "--port", str(ml_config.get("port", 5000))
    ]
    
    mlflow.set_tracking_uri(ml_config.get("tracking_uri", "http://localhost:5000"))
    
    print(f"Starting MLflow server:")
    print(f"  Backend URI: {backend_uri_abs}")
    print(f"  Artifact Root: {artifact_root_abs}")
    print(f"  Host: {ml_config.get('host', '0.0.0.0')}")
    print(f"  Port: {ml_config.get('port', 5000)}")
    
    subprocess.Popen(command)
    time.sleep(5)  # Give the server some time to start

