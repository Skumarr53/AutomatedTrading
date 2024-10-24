#!/bin/bash

# Set the MLflow tracking URI
# export MLFLOW_TRACKING_URI=sqlite:///mlflow.db  # Change this to your desired URI
export MLFLOW_TRACKING_URI=./mlruns

# Start the MLflow server
mlflow server \
    --backend-store-uri $MLFLOW_TRACKING_URI \
    --default-artifact-root $MLFLOW_TRACKING_URI \
    --host 0.0.0.0 \
    --port 5000