# src/pipelines/base_pipeline.py

from typing import Any, Dict, Union, List, Optional, Tuple
from src.config import pipeline_configs 
import joblib
import os
import pandas as pd
from datetime import datetime
from loguru import logger
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.base import clone
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score
from src.config.vars import CLOSE
from src import config
from src.feature_engineering.custom_target_tranform import TargetTransform
from src.utils.mlflow_utils import log_model_performance
from src.pipelines.custom_pipelines import CustomModelPipeline  # Import custom pipeline class


class MLPipelineBase:

    def __init__(self) -> None:
        """
        Initializes the MLPipelineBase instance.

        Sets up the necessary attributes. In LIVE mode, it attempts to load pre-trained models.
        In BACKTEST mode, models are defined and trained during execution.
        """

        self.model_id: str = datetime.now().strftime('%Y%m%d%H%M%S')
        self.features: Optional[List[str]] = config.columns.custom_cs_cols if config.model_settings.model_type == 'COMB' else []
        self.pipelines: Optional[Pipeline] =  []
        self.model: Optional[GridSearchCV] = None
        self.best_model_dict: Dict[str, Any] = (
            self._load_models()
            if config.trading_config.trade_mode == 'LIVE'
            else {}
        )
        self.mode: str = config.trading_config.trade_mode
        self.target_transform = TargetTransform()
        self.setup_all_pipelines() 

    def setup(self) -> None:
        """
        Sets up the pipeline by defining the model and pipeline components.

        In BACKTEST mode, it defines the model using GridSearchCV.
        In LIVE mode, it relies on pre-loaded models.
        """
        self.define_pipeline()
        if self.mode != 'LIVE':
            self.model = self.define_model()
        

    def define_model(self) -> GridSearchCV:
        """
        Defines the machine learning model using GridSearchCV for hyperparameter tuning.

        Returns:
            GridSearchCV: An instance of GridSearchCV configured with the pipeline and parameter grid.
        """
        if not self.pipeline:
            raise ValueError("Pipeline must be defined before defining the model.")

        return GridSearchCV(
            self.pipeline,
            # TODO
            param_grid=dict(config.model.model_params),
            scoring='f1_weighted',
            n_jobs=5,
            cv=5,
            verbose=1,
            return_train_score=True
        )

    def define_pipeline(self) -> None:
        """
        Defines the machine learning pipeline.

        This method should be implemented by subclasses to specify the sequence of transformations
        and the estimator.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError(
            "Subclasses must implement the define_pipeline method."
        )

    def _load_models(self) -> Dict[str, Any]:
        """
        Loads pre-trained models from MLflow Model Registry.

        Returns:
            Dict[str, Any]: A dictionary mapping symbols to their corresponding models and run IDs.
        """
        best_model_dict = {}
        run_ids = config.model_settings.run_ids

        for run_id in run_ids:
            for symbol in config.symbols:
                for target in config.model_settings.model_targets:
                    registered_model_name = f"{symbol}_{run_id}_{target}"
                    model_uri = f"models:/{registered_model_name}/Production"

                    try:
                        loaded_model = mlflow.sklearn.load_model(model_uri)
                        if symbol not in best_model_dict:
                            best_model_dict[symbol] = {}
                        if run_id not in best_model_dict[symbol]:
                            best_model_dict[symbol][run_id] = {}
                        best_model_dict[symbol][run_id][target] = loaded_model
                    except mlflow.exceptions.RestException as e:
                        logger.error(f"Model {registered_model_name} not found in MLflow Model Registry.")
                        raise e

        return best_model_dict
    
    def run(self, X: pd.DataFrame) -> None:
        """
        Executes the pipeline based on the operational mode (BACKTEST or LIVE).

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data and a 'symbol' column.
        """
        if 'symbol' not in X.columns:
            raise KeyError("Input DataFrame must contain a 'symbol' column.")

        symbol = X['symbol'].iloc[0]

        if self.mode == 'LIVE':
            if symbol not in self.best_model_dict:
                raise ValueError(f"No models found for symbol '{symbol}' in LIVE mode.")
            self.predict(X, symbol)
        elif self.mode == 'BACKTEST':
            self.train(X, symbol)
        else:
            raise ValueError(f"Unsupported mode '{self.mode}'. Supported modes are 'BACKTEST' and 'LIVE'.")



    def train(self, X: pd.DataFrame, symbol: str) -> None:
        """
        Trains models for the given symbol using the provided data.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data.
            symbol (str): The stock symbol.
        """

        experiment_name = f"TradingModels_{self.model_id}"
        mlflow.set_experiment(experiment_name)

        run_ids = config.model_settings.run_ids
        if not run_ids:
            raise ValueError("run_ids must be set for BACKTEST mode.")
        for run_id in run_ids:
            for target in config.model_settings.model_targets:
                try:
                    logger.info(f"Training model for {symbol} {run_id} {target}")
                    with mlflow.start_run(run_name=f"{symbol}_{run_id}_{target}_{self.model_id}"):
                        mlflow.set_tag("mode", self.mode)
                        mlflow.set_tag("model_id", self.model_id)
                        mlflow.log_param("symbol", symbol)
                        mlflow.log_param("run_id", run_id)
                        mlflow.log_param("target", target)

                        # Prepare target variable
                        X_trans, y_trans = self.prepare_input_and_target(X, target, run_id)

                        if y_trans is None:
                            logger.warning(f"Target {target} could not be prepared for {symbol} {run_id}")
                            continue

                        if self.model is None:
                            raise ValueError("Model has not been defined. Call setup() before running.")
                        
                        ## drop expirt column
                        if 'expiry' in X_trans: X_trans = X_trans.drop(['expiry'], axis=1)

                        rows_to_drop = X_trans.isna().any(axis=1)
                        X_trans, y_trans = X_trans[~rows_to_drop], y_trans[~rows_to_drop]
                         
                        # Fit the model
                        self.model.fit(X_trans, y_trans)

                        # Log best parameters
                        mlflow.log_params(self.model.best_params_)

                        # Predict on training data
                        y_pred = self.model.predict(X_trans)

                        # Log performance metrics and artifacts
                        log_model_performance(y_trans, y_pred, self.model.best_estimator_, X_trans)

                        # Log the model
                        registered_model_name = f"{symbol}_{run_id}_{target}"
                        mlflow.sklearn.log_model(
                            sk_model=self.model.best_estimator_,
                            artifact_path="model",
                            registered_model_name=registered_model_name
                        )

                        # Store the best estimator
                        if symbol not in self.best_model_dict:
                            self.best_model_dict[symbol] = {}
                        if run_id not in self.best_model_dict[symbol]:
                            self.best_model_dict[symbol][run_id] = {}
                        self.best_model_dict[symbol][run_id][target] = clone(self.model.best_estimator_)
                except Exception as e:
                    mlflow.log_param("error", str(e))
                    logger.error(f"Error encountered while training: {symbol}_{run_id}_{target}_{self.model_id}")
                    raise e

    def predict(self, X: pd.DataFrame, symbol: str) -> None:
        """
        Makes predictions using the pre-loaded models in LIVE mode.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data.
            symbol (str): The stock symbol.
        """
        run_ids = config.model_settings.run_ids
        model_fit_dict = self.best_model_dict.get(symbol, {})
        if not model_fit_dict:
            logger.warning(f"No models available for symbol '{symbol}' in LIVE mode.")
            return

        for run_id in run_ids:
            for target in ['pct_change', 'atr']:
                model = model_fit_dict.get(run_id, {}).get(target, None)
                if model is None:
                    logger.warning(f"No model found for {symbol} {run_id} {target}")
                    continue
                with mlflow.start_run(run_name=f"{symbol}_{run_id}_{target}_{self.model_id}_LIVE"):
                    mlflow.set_tag("mode", self.mode)
                    mlflow.log_param("symbol", symbol)
                    mlflow.log_param("run_id", run_id)
                    mlflow.log_param("target", target)

                    prediction = model.predict(X)
                    X.loc[:, f'prediction_{run_id}_{target}'] = prediction

                    # Log prediction (optional)
                    mlflow.log_metric("prediction", prediction[0])  # Logging first prediction as an example

    def prepare_input_and_target(self, X: pd.DataFrame, target: str, run_id: str) -> Optional[pd.Series]:
        """
        Prepares the target variable based on the specified target type.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data.
            target (str): The target type ('pct_change' or 'atr').
            run_id (str): The run identifier.

        Returns:
            pd.Series or None: The prepared target variable or None if target is unknown.
        """
        if target == 'pct_change':
            x_trans, y_trans = self.target_transform.categorize_percent_change(X, run_id)
        elif target == 'atr':
            x_trans, y_trans = self.target_transform.categorize_atr(X, run_id)
        else:
            logger.error(f"Unknown target '{target}'")
            raise f"Unknown target '{target}'"
        return x_trans, y_trans