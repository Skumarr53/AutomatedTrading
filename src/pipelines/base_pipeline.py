# src/pipelines/base_pipeline.py

from typing import Any, Dict, Union, List, Optional, Tuple
import joblib
import os
import pandas as pd
from datetime import datetime
from loguru import logger
from sklearn.model_selection import GridSearchCV, train_test_split
from imblearn.pipeline import Pipeline
from sklearn.base import clone
import pandas as pd
import mlflow
import mlflow.sklearn
from imblearn.over_sampling import SMOTE
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score
from src.config.vars import CLOSE
from joblib import Memory  # NEW: Import Memory for caching
from src import config, pp
from src.feature_engineering.custom_target_tranform import TargetTransform
from src.utils.mlflow_utils import log_model_performance
from src.pipelines.custom_pipelines import CustomModelPipeline  # Import custom pipeline class

cache_dir = './pipeline_cache'
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
memory = Memory(location=cache_dir, verbose=0) 


class MLPipelineBase:

    def __init__(self) -> None:
        """
        Initializes the MLPipelineBase instance.

        Sets up the necessary attributes. In LIVE mode, it attempts to load pre-trained models.
        In BACKTEST mode, models are defined and trained during execution.
        """

        self.model_id: str = datetime.now().strftime('%Y%m%d')
        self.features: Optional[List[str]] = config.columns.custom_cs_cols if config.model_settings.model_type == 'COMB' else []
        self.pipelines: Optional[Pipeline] =  []
        self.best_model_dict: Dict[str, Any] = (
            self._load_models()
            if config.trading_config.trade_mode == 'LIVE'
            else {}
        )
        self.mode: str = config.trading_config.trade_mode
        self.target_transform = TargetTransform()
        self.setup_all_pipelines()

    def setup_all_pipelines(self) -> None:
        """
        Sets up multiple pipelines based on the provided configurations.
        """
        for pp_name, p_config in config.model.pipeline_configs.items():
            pipeline = CustomModelPipeline(p_config)
            pipeline.define_model(memory)
            self.pipelines.append(pipeline)

    # def setup(self) -> None:
    #     """
    #     Sets up the pipeline by defining the model and pipeline components.

    #     In BACKTEST mode, it defines the model using GridSearchCV.
    #     In LIVE mode, it relies on pre-loaded models.
    #     """
    #     self.define_pipeline()
    #     if self.mode != 'LIVE':
    #         self.model = self.define_model()
        

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
        
    @staticmethod
    def get_cleaned_data(df: pd.DataFrame, target: pd.Series) -> pd.DataFrame:

        # Drop unnecessary columns if present
        drop_cols = [col for col in ['expiry', 'symbol','open_interest_flag'] if col in df.columns]
        df = df.drop(columns=drop_cols, errors='ignore')

        # Remove rows with any NaN values
        rows_to_drop = df.isna().any(axis=1)
        df, target = df[~rows_to_drop], target[~rows_to_drop]
        df = df.infer_objects()
        df = df.replace({True: 1, False: 0})
        
        
        # cat_cols = list(getattr(config.columns, 'cat_cols', []))
        # if cat_cols:
        #     df[cat_cols] = df[cat_cols].astype(int, errors='ignore')
        obj_cols = df.columns[df.dtypes=='object']
        df[obj_cols] = df[obj_cols].astype('float')
        return df, target

    def transform_and_split(self, 
                    X: pd.DataFrame, 
                    target: pd.Series, 
                    run_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Transform input data by preparing target variables, handling missing data, 
        encoding categorical features, applying optional shuffling, splitting into 
        train/test sets, and addressing class imbalance with SMOTE.

        Parameters:
        - X (pd.DataFrame): Input features.
        - target (pd.Series): Target variable.
        - run_id (str): Identifier for the current run.

        Returns:
        - X_train (pd.DataFrame): Training features after transformation.
        - X_test (pd.DataFrame): Test features after transformation.
        - y_train (pd.Series): Training targets after transformation.
        - y_test (pd.Series): Test targets after transformation.
        """
        # Prepare input and target
        X_trans, y_trans = self.prepare_input_and_target(X, target, run_id)

        X_trans, y_trans = self.get_cleaned_data(X_trans, y_trans)


        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X_trans, y_trans, test_size=0.2, random_state=42
        )

        return X_train, X_test, y_train, y_test


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
        
        
        for pipeline in self.pipelines:
            for run_id in run_ids:
                for target in config.model_settings.model_targets:
                    # try:
                    #self.model = self.define_model(pipeline)
                    logger.info(f"Training model for {symbol} {run_id} {target} with config: {pp.pformat(pipeline.params)}")
                    with mlflow.start_run(run_name=f"{symbol}_{run_id}_{target}_{self.model_id}"):
                        mlflow.set_tag("mode", self.mode)
                        mlflow.set_tag("model_id", self.model_id)
                        # mlflow.log_params(pipeline.params) 
                        mlflow.log_param("symbol", symbol)
                        mlflow.log_param("run_id", run_id)
                        mlflow.log_param("target", target)

                        X_train, X_test, y_train, y_test = self.transform_and_split(X, target, run_id)

                        if y_train is None:
                            logger.warning(f"Target {target} could not be prepared for {symbol} {run_id}")
                            continue

                        if pipeline.model is None:
                            raise ValueError("Model has not been defined. Call setup() before running.")
                        
                        # Fit the model
                        pipeline.model.fit(X_train, y_train)

                        # Log best parameters
                        mlflow.log_params(pipeline.model.best_params_)

                        # Predict on training data
                        y_pred = pipeline.model.predict(X_test)

                        # Log performance metrics and artifacts
                        log_model_performance(y_test, y_pred, pipeline.model.best_estimator_)

                        # Log the model
                        registered_model_name = f"{symbol}_{run_id}_{target}"
                        mlflow.sklearn.log_model(
                            sk_model=pipeline.model.best_estimator_,
                            artifact_path="model",
                            registered_model_name=registered_model_name
                        )

                        # Store the best estimator
                        if symbol not in self.best_model_dict:
                            self.best_model_dict[symbol] = {}
                        if run_id not in self.best_model_dict[symbol]:
                            self.best_model_dict[symbol][run_id] = {}
                        self.best_model_dict[symbol][run_id][target] = clone(pipeline.model.best_estimator_)
                    # except Exception as e:
                    #     mlflow.log_param("error", str(e))
                    #     logger.error(f"Error encountered while training: {symbol}_{run_id}_{target}_{self.model_id} \n {str(e)}")
                    #     raise e

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