# src/pipelines/base_pipeline.py

import traceback
from typing import Any, Dict, List, Optional, Tuple
import os
from pathlib import Path
import warnings
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.impute import SimpleImputer
from imblearn.pipeline import Pipeline
from sklearn.base import clone
# Parallel processing libraries:
# - joblib: Used for sklearn's internal parallelism (model training, cross-validation)
#   This is the standard library for sklearn and handles sklearn-specific optimizations.
# - Ray: Used for distributed data ingestion and signal generation across 100+ symbols
#   Ray provides distributed computing capabilities beyond sklearn's scope.
# Both libraries serve different purposes and can coexist without conflicts.
from joblib import Parallel, delayed
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from joblib import Memory  # NEW: Import Memory for caching
from src import config, pp
from src.feature_engineering.custom_target_tranform import TargetTransform
from src.utils.mlflow_utils import log_model_performance
from src.mlflow_utils.mlflow_server import start_mlflow_server, is_mlflow_server_running
from src.pipelines.custom_pipelines import CustomModelPipeline  # Import custom pipeline class
from src.preprocessing.custom_transformers import TargetLabelEncoder
from src.utils.hardware_detector import get_hardware_detector

# Suppress warnings in this module
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', message='.*resource_tracker.*')

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
        # Set MLflow tracking URI early to prevent local file storage issues
        mlflow.set_tracking_uri(config.mlflow_config.tracking_uri)
        logger.debug(f"MLflow tracking URI set to: {mlflow.get_tracking_uri()}")
        self.model_id: str = datetime.now().strftime('%Y%m%d')
        self.features: Optional[List[str]] = config.columns.custom_cs_cols if config.model_settings.model_type == 'COMB' else []
        self.pipelines: Optional[Pipeline] =  []
        self.mlflow_client = MlflowClient()
        self.target_encoder = TargetLabelEncoder()
        self.best_model_dict: Dict[str, Any] = (
            self._load_models()
            if config.trading_config.trade_mode == 'LIVE'
            else {}
        )
        self.mode: str = config.trading_config.trade_mode
        self.target_transform = TargetTransform()
        self._training_cache_days: int = 7  # Skip training if recent successful run exists
        self.setup_all_pipelines()

    def setup_all_pipelines(self) -> None:
        """
        Sets up multiple pipelines based on the provided configurations.
        
        Note: Uses TimeSeriesSplit for cross-validation with a default gap
        that covers the longest prediction horizon (3h = 36 periods at 5-min intervals).
        Hardware-aware optimization is applied automatically.
        """
        # Hardware detection and logging
        hw_detector = get_hardware_detector()
        hw_detector.log_hardware_info()
        
        # Default CV gap: covers longest prediction horizon to prevent leakage
        # 3h = 180min / 5min = 36 periods
        cv_gap = 36
        
        # Get hardware-aware settings
        hw_config = config.hardware_optimization
        mode = hw_config.mode if hw_config.auto_detect else 'balanced'
        
        # Auto-detect optimal n_splits (will be overridden per dataset size later)
        n_splits = 5
        
        for pp_name, p_config in config.model.pipeline_configs.items():
            pipeline = CustomModelPipeline(p_config)
            # Pass None for n_iter and n_jobs to trigger auto-detection
            pipeline.define_model(memory, n_splits=n_splits, gap=cv_gap, 
                                 n_iter=None, n_jobs=None)
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
    def get_cleaned_data(df: pd.DataFrame, target: pd.Series) -> tuple[pd.DataFrame, pd.Series]:
        """
        Final Fix: Ensures strict numeric types for sklearn/SMOTE and 
        maintains row synchronization between features and target.
        """
        # 1. SYNCHRONIZE: Remove rows where target is NaN
        mask = target.notna()
        df = df[mask].copy()
        target = target[mask].copy()

        if len(df) == 0:
            logger.error("All rows dropped: target contained only NaNs.")
            raise ValueError("Target variable is empty after NaN removal.")

        # 2. DROP UNNECESSARY COLUMNS
        drop_cols = ['expiry', 'open_interest_flag']
        if not getattr(config.training, 'combine_all_symbols', False):
            drop_cols.append('symbol')
        
        df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')

        # 3. CLEAN TYPES: Fix <NA>, Booleans, and mixed Strings
        # Use standard np.nan immediately. DO NOT use 'None' or 'object' type.
        df = df.replace({pd.NA: np.nan, pd.NaT: np.nan})
        
        # Convert boolean columns to int
        bool_cols = df.select_dtypes(include=['bool', 'boolean']).columns
        if len(bool_cols) > 0:
            df[bool_cols] = df[bool_cols].astype(int)
        
        for col in df.columns:
            if col == 'symbol':
                df[col] = df[col].astype(str)
            else:
                # Force everything to float. This fixes the '1.4' (str) issue.
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            
            # 4. CRITICAL FOR SMOTE: Fill all NaN values
            # SMOTE (in ResamplerTransformer) will crash if it sees even one NaN.
            # We fill with 0.0 or -1.0 to ensure a valid numeric matrix.
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(0.0)

            # 5. ENFORCE HOMOGENEOUS TYPES
            # This prevents the 'setting an array element with a sequence' error.
            # We ensure all numeric columns are float64.
            df[numeric_cols] = df[numeric_cols].astype('float64')
            
            # Ensure target is also strictly typed (numeric or string)
            if pd.api.types.is_numeric_dtype(target):
                target = target.fillna(0).astype('float64')
            else:
                target = target.astype(str)

            logger.success(
                f"Cleaning complete. Features: {df.shape}, Target: {target.shape}. "
                f"All NaNs filled. Numeric dtypes: float64."
            )
        # Remove duplicate columns if any
        df = df.loc[:, ~df.columns.duplicated()]
        
        return df, target

    def transform_and_split(self, 
                    X: pd.DataFrame, 
                    target: pd.Series, 
                    run_id: str,
                    test_size: float = 0.2,
                    embargo_periods: Optional[int] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Transform input data by preparing target variables, handling missing data, 
        encoding categorical features, and splitting into train/test sets using 
        TEMPORAL (chronological) split to prevent data leakage.

        IMPORTANT: For time series data, random splitting causes future data to leak
        into training. This method uses chronological splitting with an optional
        embargo period to prevent boundary leakage.

        Parameters:
        - X (pd.DataFrame): Input features.
        - target (pd.Series): Target variable.
        - run_id (str): Identifier for the current run.
        - test_size (float): Proportion of data for testing (default 0.2).
        - embargo_periods (int, optional): Number of periods to skip between train/test
          to prevent information leakage at the boundary. Default is prediction window.

        Returns:
        - X_train (pd.DataFrame): Training features after transformation.
        - X_test (pd.DataFrame): Test features after transformation.
        - y_train (pd.Series): Training targets after transformation.
        - y_test (pd.Series): Test targets after transformation.
        """
        # Prepare input and target
        X_trans, y_trans = self.prepare_input_and_target(X, target, run_id)

        if os.environ.get("DEBUG_TARGET_TRANSFORM") == "1":
            logger.debug("DEBUG_TARGET_TRANSFORM active; inspecting targets before cleaning/split.")
            logger.debug(f"Target sample (first 10): {y_trans.head(10).tolist()}")
            breakpoint()

        X_trans, y_trans = self.get_cleaned_data(X_trans, y_trans)

        # TEMPORAL SPLIT: Use chronological order, NOT random
        # This prevents future data from leaking into training
        n_samples = len(X_trans)
        split_idx = int(n_samples * (1 - test_size))
        
        # Calculate embargo periods if not specified (based on prediction window)
        if embargo_periods is None:
            # Extract window from run_id (e.g., '5min' -> 1 period, '1h' -> 12 periods at 5-min intervals)
            embargo_periods = self._get_embargo_periods(run_id)
        
        # Apply embargo: skip 'embargo_periods' samples between train and test
        train_end_idx = split_idx
        test_start_idx = split_idx + embargo_periods
        
        if test_start_idx >= n_samples:
            logger.warning(f"Embargo period ({embargo_periods}) too large for dataset size ({n_samples}). "
                          f"Reducing embargo to maintain at least 10% test data.")
            test_start_idx = min(split_idx + 1, int(n_samples * 0.9))
        
        X_train = X_trans.iloc[:train_end_idx].copy()
        X_test = X_trans.iloc[test_start_idx:].copy()
        y_train = y_trans.iloc[:train_end_idx].copy()
        y_test = y_trans.iloc[test_start_idx:].copy()
        
        logger.info(f"Temporal split: Train={len(X_train)} samples, Test={len(X_test)} samples, "
                   f"Embargo={embargo_periods} periods ({test_start_idx - split_idx} samples skipped)")

        return X_train, X_test, y_train, y_test
    
    def _get_embargo_periods(self, run_id: str) -> int:
        """
        Calculate embargo periods based on prediction horizon.
        
        The embargo should be at least as long as the prediction window to prevent
        overlapping information between train and test sets.
        
        Args:
            run_id: String like '5min', '15min', '1h', '3h'
            
        Returns:
            Number of 5-minute periods to skip
        """
        import re
        match = re.match(r'(\d+)(min|h)', run_id)
        if not match:
            logger.warning(f"Could not parse run_id '{run_id}' for embargo calculation. Using default of 12 periods.")
            return 12  # Default: 1 hour at 5-min intervals
        
        value, unit = match.groups()
        window_minutes = int(value) * (60 if unit == 'h' else 1)
        
        # Convert to number of 5-minute periods
        interval_min = getattr(config.scheduler, 'data_fetch_cron_interval_min', 5)
        embargo_periods = window_minutes // interval_min
        
        # Minimum embargo of 1 period
        return max(1, embargo_periods)
    
    @staticmethod
    def start_mlflow_server_if_not_running():
        url = config.mlflow_config.tracking_uri
        if not is_mlflow_server_running(url):
            logger.info("MLflow server is not running. Starting server...")
            start_mlflow_server()
        else:
            logger.info("MLflow server is already running.")
    
    def log_and_register_model(self, X_test, y_test, pipeline, symbol, run_id, target):
        """
        Logs model performance, logs the model, and registers it in the production stage.

        Parameters:
        - y_test: Actual target values for the test set.
        - y_pred: Predicted target values from the model.
        - best_estimator: The best estimator from the model pipeline.
        - symbol: A symbol or identifier for the model.
        - run_id: Unique run identifier.
        - target: The target variable name.

        Returns:
        - registered_model_name: The name of the registered model.
        """
        # Log model performance (assumed to be defined elsewhere)
        mlflow.log_params(pipeline.model.best_params_)
        
        # Predict on training data
        y_pred = pipeline.model.predict(X_test)

        # Log performance metrics and artifacts
        log_model_performance(y_test, y_pred, pipeline.model.best_estimator_)

        # Create the registered model name
        registered_model_name = f"{symbol}_{run_id}_{target}"

        # Log the model
        mlflow.sklearn.log_model(
            sk_model=pipeline.model.best_estimator_,
            artifact_path="model",
            registered_model_name=registered_model_name,
        )

        latest_model_versions = self.mlflow_client.get_latest_versions(registered_model_name, stages=["None"])

        if latest_model_versions:
            latest_version = latest_model_versions[0].version
        else:
            raise ValueError(f"No versions found for model '{registered_model_name}'.")

        # Register the model in the production stage
        model_uri = f"models:/{registered_model_name}/{latest_version}"  # Adjust the version accordingly
        mlflow.register_model(
            model_uri=model_uri,
            name=registered_model_name,
            tags={"stage": "Production"}
        )
        self.mlflow_client.transition_model_version_stage(
            name=registered_model_name,
            version=latest_version,
            stage="Production",
            archive_existing_versions=True
        )

        return registered_model_name


    def _load_training_data_from_influxdb(self, symbol: str) -> pd.DataFrame:
        """
        Load historical training data from InfluxDB.
        
        This method loads both ticker and orderbook data from InfluxDB,
        then uses DataAggregator to combine them into the expected feature format
        (same as the CSV-based flow).
        
        Args:
            symbol: Stock symbol to load data for (or "ALL_SYMBOLS" for all)
            
        Returns:
            DataFrame with aggregated features for training
        """
        try:
            from src.utils.influx_client import (
                InfluxDBClient_Wrapper,
                InfluxDBConfig,
                create_influx_config_from_hydra,
            )
            from src.feature_engineering.feature_aggregator import DataAggregator
            
            # Get InfluxDB config from Hydra
            influx_config = create_influx_config_from_hydra(config)
            if not influx_config.enabled:
                logger.warning("InfluxDB not enabled, cannot load training data")
                return pd.DataFrame()
            
            # Create client and load data
            client = InfluxDBClient_Wrapper(influx_config)
            
            # Calculate time range (default: 3 years of data)
            end_time = datetime.now()
            start_time = end_time - timedelta(days=365*3)
            
            # Query data
            import asyncio
            loop = asyncio.new_event_loop()
            connect_success = loop.run_until_complete(client.connect())
            if not connect_success:
                loop.close()
                logger.warning("Failed to connect to InfluxDB client for training data")
                return pd.DataFrame()
            
            # Load ticker data
            ticker_df = loop.run_until_complete(
                client.query_training_data([symbol], start_time, end_time)
            )
            
            # Load orderbook data
            orderbook_df = loop.run_until_complete(
                client.query_orderbook_data([symbol], start_time, end_time)
            )
            
            loop.run_until_complete(client.close())
            loop.close()
            
            if ticker_df.empty:
                logger.warning(f"No ticker data found in InfluxDB for {symbol}")
                return pd.DataFrame()
            
            # Convert timestamp columns to expected format
            if "timestamp" in ticker_df.columns:
                ticker_df = ticker_df.rename(columns={"timestamp": "date"})
            if "date" in ticker_df.columns:
                ticker_df["epoch_time"] = pd.to_datetime(ticker_df["date"]).astype("int64") // 10**9
            
            logger.info(f"Loaded {len(ticker_df)} ticker records from InfluxDB for {symbol}")
            
            # If we have orderbook data, aggregate features like the CSV flow
            if not orderbook_df.empty:
                logger.info(f"Loaded {len(orderbook_df)} orderbook records from InfluxDB for {symbol}")
                
                try:
                    # Use DataAggregator to combine ticker and orderbook data
                    # This generates all the features (technical indicators, orderbook features, etc.)
                    data_aggregator = DataAggregator()
                    
                    # Process per symbol if we have multiple symbols
                    if symbol == "ALL_SYMBOLS":
                        # Group by symbol and aggregate separately
                        combined_dfs = []
                        unique_symbols = ticker_df['symbol'].unique()
                        
                        for sym in unique_symbols:
                            sym_ticker = ticker_df[ticker_df['symbol'] == sym].copy()
                            sym_orderbook = orderbook_df[orderbook_df['symbol'] == sym].copy() if 'symbol' in orderbook_df.columns else pd.DataFrame()
                            
                            if not sym_orderbook.empty:
                                try:
                                    aggregated = data_aggregator.aggregate_features(sym_ticker, sym_orderbook)
                                    aggregated['symbol'] = sym
                                    combined_dfs.append(aggregated)
                                except Exception as e:
                                    logger.warning(f"Failed to aggregate features for {sym}: {e}")
                                    # Fall back to ticker data only
                                    combined_dfs.append(sym_ticker)
                            else:
                                combined_dfs.append(sym_ticker)
                        
                        if combined_dfs:
                            df = pd.concat(combined_dfs, ignore_index=True)
                        else:
                            df = ticker_df
                    else:
                        # Single symbol
                        try:
                            df = data_aggregator.aggregate_features(ticker_df, orderbook_df)
                            df['symbol'] = symbol
                        except Exception as e:
                            logger.warning(f"Failed to aggregate features: {e}, using ticker data only")
                            df = ticker_df
                            
                except Exception as e:
                    logger.warning(f"Error during feature aggregation: {e}, using ticker data only")
                    df = ticker_df
            else:
                logger.warning(f"No orderbook data found in InfluxDB for {symbol}, using ticker data only")
                df = ticker_df
            
            return df
            
        except ImportError as e:
            logger.warning(f"Required module not available: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to load training data from InfluxDB for {symbol}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()

    def train(self, X: pd.DataFrame, symbol: str) -> None:
        """
        Trains models for the given symbol using the provided data.
        
        If X is empty or None, attempts to load data from InfluxDB.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data.
            symbol (str): The stock symbol.
        """
        # Load from InfluxDB if X is empty
        if X is None or X.empty:
            logger.info(f"Input data is empty for {symbol}, loading from InfluxDB...")
            X = self._load_training_data_from_influxdb(symbol)
            if X.empty:
                logger.error(f"No training data available for {symbol} (neither provided nor in InfluxDB)")
                return

        experiment_name = f"TradingModels_{self.model_id}"
        mlflow.set_experiment(experiment_name)

        run_ids = config.model_settings.run_ids
        if not run_ids:
            raise ValueError("run_ids must be set for BACKTEST mode.")
        
        
        debug_serial = any(
            os.environ.get(flag) == "1"
            for flag in ("DEBUG_TARGET_TRANSFORM", "DEBUG_TARGET_FIT", "PIPELINE_DEBUG_SERIAL")
        )

        for pipeline in self.pipelines:
            if debug_serial:
                logger.info("Debug mode active; running training sequentially to allow breakpoints.")
                for run_id in run_ids:
                    for target in config.model_settings.model_targets:
                        self._train_single(pipeline, run_id, target, X, symbol)
            else:
                # Use joblib for parallel model training (sklearn-optimized)
                # This runs multiple model training jobs in parallel on local CPU cores
                # Ray is used separately for distributed data ingestion (see distributed/coordinator.py)
                
                # Hardware-aware parallel job count
                hw_detector = get_hardware_detector()
                hw_config = config.hardware_optimization
                if hw_config.auto_detect and hw_config.n_jobs is None:
                    mode = hw_config.mode or 'balanced'
                    reserve = hw_config.reserve_cores or 2
                    optimal_n_jobs = hw_detector.get_optimal_n_jobs(mode=mode, reserve_cores=reserve)
                else:
                    optimal_n_jobs = hw_config.n_jobs or 4
                
                logger.info(f"Using {optimal_n_jobs} parallel jobs for model training")
                Parallel(n_jobs=optimal_n_jobs)(
                    delayed(self._train_single)(pipeline, run_id, target, X, symbol)
                    for run_id in run_ids
                    for target in config.model_settings.model_targets
                )
            # self._train_single(pipeline, run_ids[0], config.model_settings.model_targets[0], X, symbol)

    def _has_recent_successful_run(self, symbol: str, run_id: str, target: str, days: int = 7) -> Tuple[bool, Optional[str]]:
        """
        Check if a recent successful training run exists for the given parameters.
        
        This prevents redundant training when a model was already trained within the
        specified time period.
        
        Args:
            symbol: Stock symbol
            run_id: Run identifier (time period like '5m', '15m')
            target: Target variable name ('PctChange', 'ATR')
            days: Number of days to look back (default 7)
            
        Returns:
            Tuple of (has_recent_run: bool, existing_run_id: Optional[str])
        """
        try:
            experiment_name = f"TradingModels_{self.model_id}"
            experiment = mlflow.get_experiment_by_name(experiment_name)
            
            if experiment is None:
                return False, None
            
            # Calculate the cutoff time
            cutoff_time = datetime.now() - timedelta(days=days)
            cutoff_timestamp = int(cutoff_time.timestamp() * 1000)  # MLflow uses milliseconds
            
            # Search for recent successful runs with matching parameters
            # Using filter_string for efficient server-side filtering
            filter_string = (
                f"params.symbol = '{symbol}' and "
                f"params.run_id = '{run_id}' and "
                f"params.target = '{target}' and "
                f"attributes.status = 'FINISHED'"
            )
            
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=filter_string,
                order_by=["attributes.end_time DESC"],
                max_results=1
            )
            
            if runs.empty:
                return False, None
            
            # Check if the most recent run is within our time window
            latest_run = runs.iloc[0]
            end_time = latest_run.get("end_time")
            
            if end_time is not None:
                # end_time is already a datetime or timestamp
                if isinstance(end_time, (int, float)):
                    run_end_time = datetime.fromtimestamp(end_time / 1000)
                else:
                    run_end_time = end_time
                
                if run_end_time >= cutoff_time:
                    existing_run_id = latest_run.get("run_id")
                    logger.info(
                        f"Skipping training for {symbol}/{run_id}/{target}: "
                        f"Recent successful run found (ended {run_end_time.strftime('%Y-%m-%d %H:%M')})"
                    )
                    return True, existing_run_id
            
            return False, None
            
        except Exception as e:
            # Don't let MLflow query errors prevent training
            logger.warning(f"Error checking for recent runs: {e}. Proceeding with training.")
            return False, None

    def _load_model_from_run(self, run_id: str, symbol: str, time_period: str, target: str) -> bool:
        """
        Load a model from an existing MLflow run into the best_model_dict.
        
        Args:
            run_id: MLflow run ID
            symbol: Stock symbol
            time_period: Time period (run_id in training context)
            target: Target variable name
            
        Returns:
            True if model was loaded successfully, False otherwise
        """
        try:
            model_uri = f"runs:/{run_id}/model"
            model = mlflow.sklearn.load_model(model_uri)
            
            if symbol not in self.best_model_dict:
                self.best_model_dict[symbol] = {}
            if time_period not in self.best_model_dict[symbol]:
                self.best_model_dict[symbol][time_period] = {}
            
            self.best_model_dict[symbol][time_period][target] = model
            logger.info(f"Loaded existing model for {symbol}/{time_period}/{target} from run {run_id}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to load model from run {run_id}: {e}")
            return False

    def _train_single(self, pipeline: CustomModelPipeline, run_id: str, target: str, X: pd.DataFrame, symbol: str) -> None:
        try:
            # Check for recent successful training run before proceeding
            # This prevents redundant training when a model was trained within the last N days
            has_recent, existing_run_id = self._has_recent_successful_run(
                symbol=symbol,
                run_id=run_id,
                target=target,
                days=self._training_cache_days
            )
            
            if has_recent and existing_run_id:
                # Load the existing model instead of retraining
                if self._load_model_from_run(existing_run_id, symbol, run_id, target):
                    return  # Successfully loaded existing model, skip training
                # If loading failed, proceed with training
                logger.info(f"Could not load cached model, proceeding with training for {symbol}/{run_id}/{target}")
            
            logger.info(
                f"Training model for {symbol} {run_id} {target} with config: {pp.pformat(pipeline.params)}"
            )
            
            # Ensure MLflow tracking URI is set correctly (redundant but safe)
            mlflow.set_tracking_uri(config.mlflow_config.tracking_uri)
            
            with mlflow.start_run(run_name=f"{symbol}_{run_id}_{target}_{self.model_id}"):
                mlflow.set_tag("mode", self.mode)
                mlflow.set_tag("model_id", self.model_id)
                mlflow.log_param("symbol", symbol)
                mlflow.log_param("run_id", run_id) 
                mlflow.log_param("target", target)
                
                X_train, X_test, y_train, y_test = self.transform_and_split(X, target, run_id)

                if y_train is None:
                    logger.warning(f"Target {target} could not be prepared for {symbol} {run_id}")
                    return

                if pipeline.model is None:
                    raise ValueError("Model has not been defined. Call setup() before running.")
                
                # self.target_encoder.fit_transform(y_train)
                # y_train = y_train.map(config.target_encode_dict)
                if os.environ.get("DEBUG_TARGET_FIT") == "1":
                    logger.debug("DEBUG_TARGET_FIT active; inspecting data prior to pipeline.fit.")
                    logger.debug(f"y_train unique categories: {y_train.unique().tolist()[:10]}")
                    breakpoint()
                
                # Validate data before fitting (catch sequence issues early)
                # from src.utils.data_validation import validate_data_for_model_fit
                # try:
                #     validate_data_for_model_fit(
                #         X_train, 
                #         y_train, 
                #         stage_name=f"pre-fit_{symbol}_{run_id}_{target}",
                #         raise_on_error=True
                #     )
                # except ValueError as validation_error:
                #     logger.error(f"Data validation failed before model fit: {validation_error}")
                #     # Try to identify problematic columns
                #     import numpy as np
                #     problematic_cols = []
                #     for col in X_train.columns:
                #         try:
                #             # Try to convert to numpy array - this will fail if column has sequences
                #             test_array = np.asarray(X_train[col].head(100))
                #             if test_array.dtype == object:
                #                 # Check if it's sequences
                #                 for val in X_train[col].head(10):
                #                     if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                #                         problematic_cols.append(col)
                #                         break
                #         except (ValueError, TypeError) as e:
                #             problematic_cols.append(col)
                #             logger.error(f"   Column '{col}' cannot be converted to array: {e}")
                    
                #     if problematic_cols:
                #         logger.error(f"Problematic columns with sequences: {problematic_cols}")
                #         # Show sample data from problematic columns
                #         for col in problematic_cols[:5]:  # Show first 5
                #             logger.error(f"   Column '{col}' sample values:")
                #             for idx, val in X_train[col].head(3).items():
                #                 logger.error(f"      Row {idx}: type={type(val)}, value={val}")
                    
                #     raise ValueError(
                #         f"Data validation failed: {validation_error}. "
                #         f"Problematic columns: {problematic_cols[:10]}"
                #     )
                
                pipeline.fit(X_train, y_train)
                mlflow.log_params(pipeline.model.best_params_)
                y_pred = pipeline.predict(X_test)

                log_model_performance(y_test, y_pred, pipeline.model.best_estimator_, X_test)

                registered_model_name = f"{symbol}_{run_id}_{target}"
                mlflow.sklearn.log_model(
                    sk_model=pipeline.model.best_estimator_,
                    artifact_path="model",
                    registered_model_name=registered_model_name,
                )

                if symbol not in self.best_model_dict:
                    self.best_model_dict[symbol] = {}
                if run_id not in self.best_model_dict[symbol]:
                    self.best_model_dict[symbol][run_id] = {}
                self.best_model_dict[symbol][run_id][target] = clone(pipeline.model.best_estimator_)
        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(f"Training error: {error_msg}")
            raise ValueError(f"Training failed: {str(e)}\n{error_msg}")

    def predict(self, X: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Makes predictions using the pre-loaded models in LIVE mode.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data.
            symbol (str): The stock symbol.
        """
        run_ids = config.model_settings.run_ids
        targets = config.model_settings.model_targets
        model_fit_dict = self.best_model_dict.get(symbol, {})
        if not model_fit_dict:
            logger.warning(f"No models available for symbol '{symbol}' in LIVE mode.")
            return X

        for run_id in run_ids:
            for target in targets:
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

                    if hasattr(model, "predict_proba"):
                        probas = model.predict_proba(X)
                        for idx, class_label in enumerate(model.classes_):
                            X.loc[:, f'prob_{run_id}_{target}_{class_label}'] = probas[:, idx]

                    # Log prediction (optional)
                    mlflow.log_metric("prediction", prediction[0])  # Logging first prediction as an example

        return X

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
        X = X.loc[:, ~X.columns.duplicated()]
        if target == 'PctChange':
            x_trans, y_trans = self.target_transform.categorize_percent_change(X, run_id)
        elif target == 'ATR':
            x_trans, y_trans = self.target_transform.categorize_atr(X, run_id)
        else:
            logger.error(f"Unknown target '{target}'")
            raise f"Unknown target '{target}'"
        return x_trans, y_trans