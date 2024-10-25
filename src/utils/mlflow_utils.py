# src/utils/mlflow_utils.py

import mlflow
import mlflow.sklearn
from sklearn.metrics import (f1_score, accuracy_score, 
                             precision_score, recall_score, 
                             confusion_matrix)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def log_model_performance(y_true, y_pred, model, X_trans):
    """
    Logs model performance metrics, confusion matrix, and feature importance to MLflow.

    Args:
        y_true (pd.Series): True labels.
        y_pred (pd.Series): Predicted labels.
        model (Pipeline): Trained model pipeline.
        X_trans (pd.DataFrame): Transformed input features.
    """
    # Calculate metrics
    f1 = f1_score(y_true, y_pred, average='weighted')
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average='weighted', zero_division=0)
    recall = recall_score(y_true, y_pred, average='weighted', zero_division=0)

    # Log metrics
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)

    # Confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(10, 7))
    sns.heatmap(cm, annot=True, fmt='d')
    plt.title('Confusion Matrix')
    plt.ylabel('Actual')
    plt.xlabel('Predicted')
    plt.savefig('mlflow_reports/confusion_matrix.png')
    mlflow.log_artifact('mlflow_reports/confusion_matrix.png')
    plt.close()

    # Feature importance
    if hasattr(model.named_steps['model_fit'], 'feature_importances_'):
        feature_importances = model.named_steps['model_fit'].feature_importances_
        feature_names = model.named_steps['feature_selection'].selector_.get_feature_names_out()
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': feature_importances
        })
        importance_df.sort_values(by='importance', ascending=False, inplace=True)
        importance_df.to_csv('mlflow_reports/feature_importance.csv', index=False)
        mlflow.log_artifact('mlflow_reports/feature_importance.csv')
