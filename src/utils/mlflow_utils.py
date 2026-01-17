# src/utils/mlflow_utils.py
"""
MLflow utilities for logging model performance and artifacts.
uv syn
Includes both ML metrics (accuracy, F1, etc.) and financial trading metrics
(Sharpe ratio, profit factor, max drawdown) for trading model evaluation.
"""

import os
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import seaborn as sns
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from loguru import logger


def calculate_financial_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: Optional[np.ndarray] = None,
    target_encode_dict: Optional[dict] = None,
) -> dict:
    """
    Calculate trading-relevant financial metrics from classification predictions.
    
    These metrics help evaluate model usefulness for actual trading beyond
    standard ML metrics like accuracy.
    
    Args:
        y_true: Actual target labels (encoded as integers)
        y_pred: Predicted labels (encoded as integers)
        y_proba: Prediction probabilities (optional, for confidence metrics)
        target_encode_dict: Mapping of categories to numeric values, e.g.,
                           {'Low': -2, 'Medium Low': -1, 'Neutral': 0, 'Medium High': 1, 'High': 2}
    
    Returns:
        Dictionary of financial metrics
    """
    metrics = {}
    
    # Default encoding if not provided
    if target_encode_dict is None:
        target_encode_dict = {
            'Low': -2,
            'Medium Low': -1,
            'Neutral': 0,
            'Medium High': 1,
            'High': 2
        }
    
    # Convert to numpy arrays for consistency
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    
    # 1. Directional Accuracy: Did we predict the correct direction (up/down/neutral)?
    # Map encoded values to direction: negative = -1, neutral = 0, positive = 1
    def to_direction(arr):
        return np.sign(arr)
    
    true_direction = to_direction(y_true)
    pred_direction = to_direction(y_pred)
    
    directional_accuracy = np.mean(true_direction == pred_direction)
    metrics['directional_accuracy'] = float(directional_accuracy)
    
    # 2. Profit Factor: Ratio of profits to losses
    # Simulate: if prediction matches direction, it's a "win"
    # Weight by the magnitude of actual movement
    wins = np.abs(y_true[true_direction == pred_direction]).sum()
    losses = np.abs(y_true[true_direction != pred_direction]).sum()
    
    if losses > 0:
        profit_factor = wins / losses
    else:
        profit_factor = float('inf') if wins > 0 else 1.0
    metrics['profit_factor'] = float(min(profit_factor, 100))  # Cap at 100 for display
    
    # 3. Simulated Returns and Sharpe Ratio
    # Assume: correct prediction = gain proportional to |actual|, wrong = loss
    simulated_returns = np.where(
        true_direction == pred_direction,
        np.abs(y_true) * 0.01,  # Win: small % gain
        -np.abs(y_true) * 0.01   # Loss: small % loss
    )
    
    if len(simulated_returns) > 1 and np.std(simulated_returns) > 0:
        # Annualized Sharpe (assuming 252 trading days * ~78 5-min periods per day)
        periods_per_year = 252 * 78
        mean_return = np.mean(simulated_returns)
        std_return = np.std(simulated_returns)
        sharpe_ratio = (mean_return / std_return) * np.sqrt(periods_per_year)
        metrics['sharpe_ratio'] = float(np.clip(sharpe_ratio, -10, 10))  # Clip extreme values
    else:
        metrics['sharpe_ratio'] = 0.0
    
    # 4. Max Drawdown: Largest peak-to-trough decline in cumulative returns
    cumulative_returns = np.cumsum(simulated_returns)
    rolling_max = np.maximum.accumulate(cumulative_returns)
    drawdowns = cumulative_returns - rolling_max
    max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0.0
    metrics['max_drawdown'] = float(max_drawdown)
    
    # 5. Win Rate: Percentage of correct directional predictions (excluding neutral)
    non_neutral_mask = (true_direction != 0) | (pred_direction != 0)
    if np.sum(non_neutral_mask) > 0:
        win_rate = np.mean((true_direction == pred_direction)[non_neutral_mask])
        metrics['win_rate'] = float(win_rate)
    else:
        metrics['win_rate'] = 0.5
    
    # 6. Confidence-weighted Accuracy (if probabilities available)
    if y_proba is not None:
        try:
            max_proba = np.max(y_proba, axis=1)
            # High confidence predictions (>0.6 probability)
            high_conf_mask = max_proba > 0.6
            if np.sum(high_conf_mask) > 0:
                high_conf_accuracy = np.mean(y_true[high_conf_mask] == y_pred[high_conf_mask])
                metrics['high_confidence_accuracy'] = float(high_conf_accuracy)
                metrics['high_confidence_pct'] = float(np.mean(high_conf_mask))
        except Exception as e:
            logger.debug(f"Could not calculate confidence metrics: {e}")
    
    # 7. Extreme Movement Detection: Accuracy on High/Low predictions
    extreme_mask = np.abs(y_true) >= 2  # "High" or "Low" categories
    if np.sum(extreme_mask) > 0:
        extreme_accuracy = np.mean(y_true[extreme_mask] == y_pred[extreme_mask])
        metrics['extreme_accuracy'] = float(extreme_accuracy)
    
    return metrics


def log_model_performance(y_true, y_pred, model, X_test=None):
    """
    Logs model performance metrics, confusion matrix, and financial trading metrics to MLflow.
    
    This function logs both standard ML metrics and trading-specific financial metrics
    that are more relevant for evaluating trading model usefulness.

    Args:
        y_true (pd.Series): True labels.
        y_pred (pd.Series): Predicted labels.
        model (Pipeline): Trained model pipeline.
        X_test (pd.DataFrame): Test input features for ROC-AUC calculation.
    """
    # Calculate standard ML metrics
    f1 = f1_score(y_true, y_pred, average="weighted")
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average="weighted", zero_division=0)
    recall = recall_score(y_true, y_pred, average="weighted", zero_division=0)

    roc_auc = None
    y_proba = None
    if X_test is not None and hasattr(model, "predict_proba"):
        try:
            y_proba = model.predict_proba(X_test)
            roc_auc = roc_auc_score(y_true, y_proba, multi_class="ovr")
        except Exception:
            roc_auc = None

    # Log standard ML metrics
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    if roc_auc is not None:
        mlflow.log_metric("roc_auc", roc_auc)
    if hasattr(model, "cv_results_"):
        cv_score = model.cv_results_["mean_test_score"][model.best_index_]
        mlflow.log_metric("cv_mean_score", cv_score)

    # Calculate and log financial trading metrics
    try:
        financial_metrics = calculate_financial_metrics(
            y_true=np.asarray(y_true),
            y_pred=np.asarray(y_pred),
            y_proba=y_proba,
        )
        
        for metric_name, metric_value in financial_metrics.items():
            if not np.isnan(metric_value) and not np.isinf(metric_value):
                mlflow.log_metric(metric_name, metric_value)
        
        logger.info(f"Financial metrics: Sharpe={financial_metrics.get('sharpe_ratio', 'N/A'):.2f}, "
                   f"MaxDD={financial_metrics.get('max_drawdown', 'N/A'):.4f}, "
                   f"WinRate={financial_metrics.get('win_rate', 'N/A'):.2%}, "
                   f"ProfitFactor={financial_metrics.get('profit_factor', 'N/A'):.2f}")
    except Exception as e:
        logger.warning(f"Could not calculate financial metrics: {e}")

    # Generate and log confusion matrix
    plot_path = "./mlruns/confusion_matrix.png"
    os.makedirs(os.path.dirname(plot_path), exist_ok=True)

    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(10, 7))
    sns.heatmap(cm, annot=True, fmt="d")
    plt.title("Confusion Matrix")
    plt.ylabel("Actual")
    plt.xlabel("Predicted")
    plt.savefig(plot_path)
    mlflow.log_artifact(plot_path)

    # remove the figure after logging the artifact
    os.remove(plot_path)
    plt.close()

    # Feature importance (commented out for now)
    # if hasattr(model.named_steps['model_fit'], 'feature_importances_'):
    #     feature_importances = model.named_steps['model_fit'].feature_importances_
    #     feature_names = model.named_steps['feature_selection'].selector_.get_feature_names_out()
    #     importance_df = pd.DataFrame({
    #         'feature': feature_names,
    #         'importance': feature_importances
    #     })
    #     importance_df.sort_values(by='importance', ascending=False, inplace=True)
    #     importance_df.to_csv('WeeklyReports/mlflow_reports/feature_importance.csv', index=False)
    #     mlflow.log_artifact('WeeklyReports/mlflow_reports/feature_importance.csv')
