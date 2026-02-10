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
    
    func = np.vectorize(lambda x: target_encode_dict.get(x, x))
    true_direction = to_direction(func(y_true))
    pred_direction = to_direction(func(y_pred))
    
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


def log_test_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: Optional[np.ndarray] = None,
    X_test: Optional[pd.DataFrame] = None,
    model_name: str = "model",
    class_labels: Optional[list] = None,
) -> str:
    """
    Log test set predictions as a CSV artifact to MLflow.
    
    This allows post-hoc analysis of model predictions, error patterns,
    and calibration without needing to re-run inference.
    
    Args:
        y_true: Actual target labels
        y_pred: Predicted labels
        y_proba: Prediction probabilities from predict_proba (optional)
        X_test: Test features for context (optional, subset of columns logged)
        model_name: Name prefix for the artifact file
        class_labels: List of class labels for probability columns
        
    Returns:
        Path to the saved predictions CSV file
    """
    predictions_df = pd.DataFrame({
        'y_true': y_true,
        'y_pred': y_pred,
        'correct': np.asarray(y_true) == np.asarray(y_pred),
    })
    
    # Add confidence (max probability) and per-class probabilities
    if y_proba is not None:
        try:
            predictions_df['confidence'] = np.max(y_proba, axis=1)
            
            # Add individual class probabilities
            if class_labels is not None:
                for i, label in enumerate(class_labels):
                    predictions_df[f'prob_{label}'] = y_proba[:, i]
            else:
                for i in range(y_proba.shape[1]):
                    predictions_df[f'prob_class_{i}'] = y_proba[:, i]
        except Exception as e:
            logger.warning(f"Could not add probability columns: {e}")
    
    # Add selected features from X_test for context (limit to avoid bloat)
    if X_test is not None:
        try:
            # Select a subset of important columns if too many
            max_feature_cols = 20
            feature_cols = list(X_test.columns)[:max_feature_cols]
            for col in feature_cols:
                predictions_df[f'feature_{col}'] = X_test[col].values
        except Exception as e:
            logger.warning(f"Could not add feature columns: {e}")
    
    # Add index for reference
    predictions_df['sample_idx'] = range(len(y_true))
    
    # Save to file and log as artifact
    import tempfile
    import shutil
    # Use temporary directory for artifacts (works on host, not container path)
    tmpdir = tempfile.mkdtemp()
    try:
        artifact_path = os.path.join(tmpdir, f"{model_name}_test_predictions.csv")
        predictions_df.to_csv(artifact_path, index=False)
        
        logger.debug(f"Created test predictions file: {artifact_path}")
        logger.debug(f"File exists: {os.path.exists(artifact_path)}")
        logger.debug(f"File size: {os.path.getsize(artifact_path)} bytes")
        
        # Log the artifact to MLflow
        try:
            # Ensure we're using HTTP tracking URI (not file://)
            tracking_uri = mlflow.get_tracking_uri()
            if tracking_uri.startswith("file://"):
                logger.error(f"MLflow tracking URI is file:// (should be http://): {tracking_uri}")
                raise ValueError(f"MLflow tracking URI must be HTTP, not file://. Current: {tracking_uri}")
            
            # Verify artifact URI is HTTP (not local filesystem)
            active_run = mlflow.active_run()
            if active_run:
                artifact_uri = active_run.info.artifact_uri
                logger.debug(f"Artifact URI: {artifact_uri}")
                
                # If artifact URI is local filesystem, MLflow will try to write directly
                # This causes permission errors. Force HTTP upload.
                if artifact_uri.startswith("file://") or (artifact_uri.startswith("/") and not artifact_uri.startswith("http")):
                    logger.warning(f"Artifact URI is local filesystem: {artifact_uri}")
                    logger.warning("MLflow server may not be configured with --serve-artifacts")
                    logger.warning("This will cause permission errors. Check server configuration.")
            
            mlflow.log_artifact(artifact_path)
            logger.info(f"Logged test predictions artifact: {artifact_path} ({len(predictions_df)} samples)")
            logger.debug(f"MLflow will store artifact at server-side path (shown in MLflow UI)")
        except PermissionError as e:
            if "/mlflow" in str(e):
                logger.error(f"Permission denied writing to /mlflow - MLflow is using local filesystem instead of HTTP")
                logger.error(f"This happens when artifact URI is file:// or local path instead of http://")
                logger.error(f"Fix: Ensure MLflow server is configured with --serve-artifacts")
                logger.error(f"Current tracking URI: {mlflow.get_tracking_uri()}")
                if active_run:
                    logger.error(f"Current artifact URI: {active_run.info.artifact_uri}")
            raise
        except Exception as e:
            logger.error(f"Failed to log test predictions artifact: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise
    finally:
        # Cleanup temporary directory
        shutil.rmtree(tmpdir, ignore_errors=True)
    
    return artifact_path


def log_training_parameters(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    config_dict: dict,
    run_id: str,
    symbol: str,
    target: str,
    hardware_info: Optional[dict] = None,
) -> None:
    """
    Log comprehensive training parameters and metadata to MLflow.
    
    Captures data characteristics, configuration, and hardware context
    for full reproducibility and model auditing.
    
    Args:
        X_train: Training features
        X_test: Test features
        y_train: Training target
        config_dict: Configuration dictionary
        run_id: Timeframe identifier (e.g., '5min', '1h')
        symbol: Stock symbol
        target: Target variable name (e.g., 'PctChange', 'ATR')
        hardware_info: Optional hardware detection info
    """
    # === Data Parameters ===
    mlflow.log_param("n_samples_train", X_train.shape[0])
    mlflow.log_param("n_samples_test", X_test.shape[0])
    mlflow.log_param("n_features", X_train.shape[1])
    mlflow.log_param("train_test_ratio", round(X_train.shape[0] / (X_train.shape[0] + X_test.shape[0]), 3))
    
    # Class distribution
    class_dist = y_train.value_counts().to_dict()
    mlflow.log_param("class_distribution", str(class_dist))
    mlflow.log_param("n_classes", len(class_dist))
    
    # Check class imbalance ratio
    if len(class_dist) > 1:
        max_class = max(class_dist.values())
        min_class = min(class_dist.values())
        imbalance_ratio = max_class / min_class if min_class > 0 else float('inf')
        mlflow.log_metric("class_imbalance_ratio", min(imbalance_ratio, 100))
    
    # === Training Context ===
    mlflow.log_param("run_id_timeframe", run_id)
    mlflow.log_param("symbol", symbol)
    mlflow.log_param("target_variable", target)
    
    # === Configuration Parameters ===
    # Log key configuration items (avoid logging entire nested config)
    if 'model_settings' in config_dict:
        model_settings = config_dict['model_settings']
        mlflow.log_param("shuffle", model_settings.get('shuffle', 'unknown'))
        mlflow.log_param("temporal_split", model_settings.get('temporal_split', 'unknown'))
        mlflow.log_param("model_targets", str(model_settings.get('model_targets', [])))
    
    if 'hardware_optimization' in config_dict:
        hw_config = config_dict['hardware_optimization']
        mlflow.log_param("hw_auto_detect", hw_config.get('auto_detect', False))
        mlflow.log_param("hw_mode", hw_config.get('mode', 'unknown'))
    
    # === Hardware Context ===
    if hardware_info:
        mlflow.log_param("hw_tier", hardware_info.get('tier', 'unknown'))
        mlflow.log_param("hw_cpu_cores", hardware_info.get('cpu_physical', 'unknown'))
        mlflow.log_param("hw_ram_gb", hardware_info.get('ram_total_gb', 'unknown'))
        mlflow.log_param("hw_gpu_available", hardware_info.get('gpu_available', False))
    
    # === Feature List (as artifact) ===
    import tempfile
    import shutil
    # Use temporary directory for artifacts (works on host, not container path)
    tmpdir = tempfile.mkdtemp()
    try:
        feature_list_path = os.path.join(tmpdir, "feature_list.txt")
        
        logger.debug(f"Creating feature list file at: {feature_list_path}")
        logger.debug(f"Temp directory exists: {os.path.exists(tmpdir)}")
        logger.debug(f"Temp directory writable: {os.access(tmpdir, os.W_OK)}")
        
        with open(feature_list_path, 'w') as f:
            f.write(f"# Features for {symbol}_{run_id}_{target}\n")
            f.write(f"# Total: {X_train.shape[1]} features\n\n")
            for col in X_train.columns:
                f.write(f"{col}\n")
        
        logger.debug(f"Feature list file created: {feature_list_path}")
        logger.debug(f"File exists: {os.path.exists(feature_list_path)}")
        logger.debug(f"File size: {os.path.getsize(feature_list_path)} bytes")
        
        # Log the artifact to MLflow
        try:
            # Ensure we're using HTTP tracking URI (not file://)
            tracking_uri = mlflow.get_tracking_uri()
            if tracking_uri.startswith("file://"):
                logger.error(f"MLflow tracking URI is file:// (should be http://): {tracking_uri}")
                raise ValueError(f"MLflow tracking URI must be HTTP, not file://. Current: {tracking_uri}")
            
            # Get active run info for debugging
            active_run = mlflow.active_run()
            if active_run:
                logger.debug(f"MLflow active run ID: {active_run.info.run_id}")
                artifact_uri = active_run.info.artifact_uri
                logger.debug(f"MLflow artifact URI (server-side): {artifact_uri}")
                logger.debug(f"Note: Artifact URI shows server storage location, not where code writes")
                
                # Check if artifact URI is local filesystem (causes permission errors)
                if artifact_uri.startswith("file://") or (artifact_uri.startswith("/") and not artifact_uri.startswith("http")):
                    logger.error(f"Artifact URI is local filesystem: {artifact_uri}")
                    logger.error("MLflow server is not returning HTTP artifact URIs despite --serve-artifacts")
                    logger.error("This causes client to write directly to filesystem, causing permission errors.")
                    logger.error("Fix: Ensure MLflow server is properly configured with --serve-artifacts")
                    raise ValueError(f"Artifact URI must be HTTP, not local filesystem. Current: {artifact_uri}")
            
            mlflow.log_artifact(feature_list_path)
            logger.info(f"Logged feature list artifact: {feature_list_path}")
            logger.debug(f"MLflow client uploaded file to server")
            logger.debug(f"Server stores at: {active_run.info.artifact_uri if active_run else 'N/A'}/feature_list.txt")
        except PermissionError as e:
            if "/mlflow" in str(e):
                logger.error(f"Permission denied writing to /mlflow - MLflow is using local filesystem instead of HTTP")
                logger.error(f"This happens when artifact URI is file:// or local path instead of http://")
                logger.error(f"Fix: Ensure MLflow server is configured with --serve-artifacts")
                logger.error(f"Current tracking URI: {mlflow.get_tracking_uri()}")
                if active_run:
                    logger.error(f"Current artifact URI: {active_run.info.artifact_uri}")
            raise
        except Exception as e:
            logger.error(f"Failed to log feature list artifact: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise
    finally:
        # Cleanup temporary directory
        logger.debug(f"Cleaning up temp directory: {tmpdir}")
        shutil.rmtree(tmpdir, ignore_errors=True)
        logger.debug(f"Temp directory cleaned: {not os.path.exists(tmpdir)}")
    
    logger.info(f"Logged training parameters: {X_train.shape[0]} train, {X_test.shape[0]} test, {X_train.shape[1]} features")


def _log_feature_importance(model) -> None:
    """
    Log feature importance from the trained model pipeline to MLflow.
    
    Handles multiple scenarios:
    - Model with built-in feature_importances_ (LGBM, XGBoost, RandomForest)
    - Pipeline with feature_selection step that has feature importance
    - Multi-stage feature selectors
    
    Args:
        model: Trained model or pipeline (can be RandomizedSearchCV, Pipeline, or estimator)
    """
    import tempfile
    import shutil
    
    try:
        # Extract the actual estimator from RandomizedSearchCV or GridSearchCV
        if hasattr(model, 'best_estimator_'):
            pipeline = model.best_estimator_
        else:
            pipeline = model
        
        importance_data = []
        
        # Check for feature selection step in pipeline
        if hasattr(pipeline, 'named_steps'):
            # Check for feature_selection step
            if 'feature_selection' in pipeline.named_steps:
                selector = pipeline.named_steps['feature_selection']
                
                # Get feature importances from selector
                if hasattr(selector, 'get_feature_importances'):
                    try:
                        importances = selector.get_feature_importances()
                        for feature, importance in importances.items():
                            importance_data.append({
                                'feature': feature,
                                'importance': importance,
                                'source': 'feature_selector'
                            })
                        logger.debug(f"Got {len(importance_data)} feature importances from feature selector")
                    except Exception as e:
                        logger.debug(f"Could not get importances from feature selector: {e}")
                
                # Get selected feature names
                if hasattr(selector, 'get_feature_names_out'):
                    try:
                        selected_features = selector.get_feature_names_out()
                        mlflow.log_param("n_features_selected", len(selected_features))
                        logger.info(f"Feature selection: {len(selected_features)} features selected")
                    except Exception as e:
                        logger.debug(f"Could not get selected feature names: {e}")
                
                # For MultiStageFeatureSelector, log stage results
                if hasattr(selector, 'stage_results_'):
                    for stage_name, results in selector.stage_results_.items():
                        mlflow.log_metric(f"fs_{stage_name}_features_in", results.get('n_features_in', 0))
                        mlflow.log_metric(f"fs_{stage_name}_features_out", results.get('n_features_out', 0))
                        mlflow.log_metric(f"fs_{stage_name}_reduction_pct", results.get('reduction_pct', 0))
            
            # Check for model_fit step with feature_importances_
            if 'model_fit' in pipeline.named_steps:
                model_fit = pipeline.named_steps['model_fit']
                
                if hasattr(model_fit, 'feature_importances_'):
                    # Get feature names
                    feature_names = None
                    
                    # Try to get feature names from feature_selection step
                    if 'feature_selection' in pipeline.named_steps:
                        selector = pipeline.named_steps['feature_selection']
                        if hasattr(selector, 'get_feature_names_out'):
                            try:
                                feature_names = list(selector.get_feature_names_out())
                            except Exception:
                                pass
                    
                    # Fallback: try to get from features step
                    if feature_names is None and 'features' in pipeline.named_steps:
                        try:
                            features_step = pipeline.named_steps['features']
                            if hasattr(features_step, 'get_feature_names_out'):
                                feature_names = list(features_step.get_feature_names_out())
                        except Exception:
                            pass
                    
                    # Get importances
                    importances = model_fit.feature_importances_
                    
                    if feature_names is not None and len(feature_names) == len(importances):
                        for feature, importance in zip(feature_names, importances):
                            importance_data.append({
                                'feature': feature,
                                'importance': float(importance),
                                'source': 'model'
                            })
                        logger.debug(f"Got {len(importances)} feature importances from model")
                    else:
                        # Use generic feature names if no names available
                        for i, importance in enumerate(importances):
                            importance_data.append({
                                'feature': f'feature_{i}',
                                'importance': float(importance),
                                'source': 'model'
                            })
                        logger.debug(f"Got {len(importances)} feature importances with generic names")
        
        # Direct model with feature_importances_ (not in pipeline)
        elif hasattr(pipeline, 'feature_importances_'):
            importances = pipeline.feature_importances_
            for i, importance in enumerate(importances):
                importance_data.append({
                    'feature': f'feature_{i}',
                    'importance': float(importance),
                    'source': 'model'
                })
        
        # If we have importance data, log it
        if importance_data:
            importance_df = pd.DataFrame(importance_data)
            
            # Sort by importance descending
            importance_df = importance_df.sort_values('importance', ascending=False)
            
            # Log top features as metrics for quick comparison in MLflow UI
            top_features = importance_df.head(10)
            for idx, row in top_features.iterrows():
                # Sanitize feature name for metric name
                feature_name = str(row['feature'])[:50].replace(' ', '_').replace('.', '_')
                mlflow.log_metric(f"imp_{feature_name}", row['importance'])
            
            # Log total number of important features (importance > 0.01)
            significant_features = len(importance_df[importance_df['importance'] > 0.01])
            mlflow.log_metric("n_significant_features", significant_features)
            
            # Save as artifact
            tmpdir = tempfile.mkdtemp()
            try:
                artifact_path = os.path.join(tmpdir, "feature_importance.csv")
                importance_df.to_csv(artifact_path, index=False)
                
                mlflow.log_artifact(artifact_path)
                logger.info(f"Logged feature importance artifact ({len(importance_df)} features)")
                
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        else:
            logger.debug("No feature importance data available to log")
            
    except Exception as e:
        logger.warning(f"Could not log feature importance: {e}")
        import traceback
        logger.debug(traceback.format_exc())


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
    import tempfile
    import shutil
    # Use temporary directory for artifacts (works on host, not container path)
    tmpdir = tempfile.mkdtemp()
    try:
        plot_path = os.path.join(tmpdir, "confusion_matrix.png")
        
        logger.debug(f"Creating confusion matrix plot at: {plot_path}")
        logger.debug(f"Temp directory exists: {os.path.exists(tmpdir)}")
        logger.debug(f"Temp directory writable: {os.access(tmpdir, os.W_OK)}")
        
        # Ensure directory exists (defensive - mkdtemp creates it, but be safe)
        plot_dir = os.path.dirname(plot_path)
        os.makedirs(plot_dir, exist_ok=True)
        
        # Verify directory is ready
        if not os.path.exists(plot_dir):
            raise RuntimeError(f"Failed to create directory: {plot_dir}")
        if not os.access(plot_dir, os.W_OK):
            raise PermissionError(f"Directory not writable: {plot_dir}")
        
        logger.debug(f"Directory verified: {plot_dir} (exists: {os.path.exists(plot_dir)}, writable: {os.access(plot_dir, os.W_OK)})")
        
        cm = confusion_matrix(y_true, y_pred)
        plt.figure(figsize=(10, 7))
        sns.heatmap(cm, annot=True, fmt="d")
        plt.title("Confusion Matrix")
        plt.ylabel("Actual")
        plt.xlabel("Predicted")
        
        # Save plot with error handling
        try:
            plt.savefig(plot_path)
            logger.debug(f"Plot saved successfully: {plot_path}")
        except Exception as save_error:
            logger.error(f"Failed to save plot to {plot_path}: {save_error}")
            logger.debug(f"Directory exists: {os.path.exists(plot_dir)}")
            logger.debug(f"Directory writable: {os.access(plot_dir, os.W_OK)}")
            logger.debug(f"Full path: {os.path.abspath(plot_path)}")
            raise
        
        # Verify file was actually created
        if not os.path.exists(plot_path):
            error_msg = (
                f"Plot file was not created after plt.savefig(): {plot_path}\n"
                f"  Directory exists: {os.path.exists(plot_dir)}\n"
                f"  Directory writable: {os.access(plot_dir, os.W_OK)}\n"
                f"  Full absolute path: {os.path.abspath(plot_path)}\n"
                f"  Temp dir: {tmpdir}"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        file_size = os.path.getsize(plot_path)
        logger.debug(f"Plot file verified: {plot_path} (size: {file_size} bytes)")
        
        if file_size == 0:
            logger.warning(f"Plot file is empty (0 bytes): {plot_path}")
        
        # Log the artifact to MLflow
        try:
            # Ensure we're using HTTP tracking URI (not file://)
            tracking_uri = mlflow.get_tracking_uri()
            if tracking_uri.startswith("file://"):
                logger.error(f"MLflow tracking URI is file:// (should be http://): {tracking_uri}")
                raise ValueError(f"MLflow tracking URI must be HTTP, not file://. Current: {tracking_uri}")
            
            # Get active run info for debugging
            active_run = mlflow.active_run()
            if active_run:
                logger.debug(f"MLflow active run ID: {active_run.info.run_id}")
                artifact_uri = active_run.info.artifact_uri
                logger.debug(f"MLflow artifact URI (server-side): {artifact_uri}")
                logger.debug(f"Note: Artifact URI shows server storage location, not where code writes")
                
                # If artifact URI is local filesystem, MLflow will try to write directly
                # This causes permission errors. Force HTTP upload.
                if artifact_uri.startswith("file://") or (artifact_uri.startswith("/") and not artifact_uri.startswith("http")):
                    logger.warning(f"Artifact URI is local filesystem: {artifact_uri}")
                    logger.warning("MLflow server may not be configured with --serve-artifacts")
                    logger.warning("This will cause permission errors. Check server configuration.")
            
            mlflow.log_artifact(plot_path)
            logger.info(f"Logged confusion matrix artifact: {plot_path}")
            logger.debug(f"MLflow client uploaded file to server")
            logger.debug(f"Server stores at: {active_run.info.artifact_uri if active_run else 'N/A'}/confusion_matrix.png")
        except PermissionError as e:
            if "/mlflow" in str(e):
                logger.error(f"Permission denied writing to /mlflow - MLflow is using local filesystem instead of HTTP")
                logger.error(f"This happens when artifact URI is file:// or local path instead of http://")
                logger.error(f"Fix: Ensure MLflow server is configured with --serve-artifacts")
                logger.error(f"Current tracking URI: {mlflow.get_tracking_uri()}")
                if active_run:
                    logger.error(f"Current artifact URI: {active_run.info.artifact_uri}")
            raise
        except Exception as e:
            logger.error(f"Failed to log confusion matrix artifact: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise
        
        plt.close()
    finally:
        # Cleanup temporary directory
        logger.debug(f"Cleaning up temp directory: {tmpdir}")
        shutil.rmtree(tmpdir, ignore_errors=True)
        logger.debug(f"Temp directory cleaned: {not os.path.exists(tmpdir)}")

    # Log feature importance if available
    _log_feature_importance(model)
