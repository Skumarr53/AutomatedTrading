from src import config
import subprocess
import requests
import time
import mlflow

def is_mlflow_server_running(url="http://0.0.0.0:5000"):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.ConnectionError:
        return False

def start_mlflow_server():
    ml_config = config.mlflow_config
    command = [
        "mlflow", "server",
        "--backend-store-uri", ml_config.get("backend_store_uri", "sqlite:///mlflow.db"),
        "--default-artifact-root", ml_config.get("default_artifact_root", "./mlruns"),
        "--host", ml_config.get("host", "0.0.0.0"),
        "--port", str(ml_config.get("port", 5000))
    ]
    
    mlflow.set_tracking_uri(ml_config.get("tracking_uri", "http://localhost:5000"))
    subprocess.Popen(command)
    time.sleep(5)  # Give the server some time to start

