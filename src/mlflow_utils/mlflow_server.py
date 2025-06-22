import subprocess
import requests
import time

def is_mlflow_server_running(url="http://0.0.0.0:5000"):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.ConnectionError:
        return False

def start_mlflow_server():
    command = [
        "mlflow", "server",
        "--backend-store-uri", "sqlite:///mlflow.db",
        "--default-artifact-root", "./mlruns",
        "--host", "0.0.0.0",
        "--port", "5000"
    ]
    subprocess.Popen(command)
    time.sleep(5)  # Give the server some time to start

