from src import config
from src.utils.utils import load_symbols
from loguru import logger

import mlflow
import mlflow.sklearn
import logging
import pandas as pd
import time
import concurrent.futures
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import yaml
from functools import lru_cache


class ModelCache:
    """Custom model cache for efficient model reuse."""
    
    def __init__(self, max_cache_size: int = 500):
        # Cache the most recent 10 models
        self.cache = {}
        self.max_cache_size = max_cache_size

    def get(self, model_name: str):
        """Retrieve the model from the cache if available."""
        if model_name in self.cache:
            logging.info(f"Cache hit for {model_name}")
            return self.cache[model_name]
        logging.info(f"Cache miss for {model_name}")
        return None

    def add(self, model_name: str, model):
        """Add a model to the cache, removing the oldest if cache size exceeds limit."""
        if len(self.cache) >= self.max_cache_size:
            self.cache.pop(next(iter(self.cache)))  # Remove the oldest item
        self.cache[model_name] = model

class MLflowModelLoader:
    """
    Load models from MLflow for given stock, time period, and metric.
    
    Supports configurable experiment selection, model stages, and staleness checks
    for production-ready model loading.
    """

    def __init__(
        self, 
        config: dict, 
        model_cache: ModelCache, 
        experiment_name: str = None, 
        tracking_uri: str = "http://localhost:5000",
        live_trading_config: Optional[dict] = None,
    ):
        self.config = config
        self.model_cache = model_cache
        
        # Load live_trading config from main config or use provided
        self._live_config = live_trading_config or {}
        if hasattr(config, 'live_trading'):
            self._live_config = dict(config.live_trading)
        
        # Determine experiment name (priority: explicit > config > default)
        self.experiment_name = (
            experiment_name or 
            self._live_config.get('experiment_name') or 
            "TradingModels_Production"
        )
        
        # Model loading settings
        self.model_stage = self._live_config.get('model_stage', 'Production')
        self.fallback_experiment = self._live_config.get('fallback_experiment')
        self.max_model_age_days = self._live_config.get('max_model_age_days', 7)
        self.require_registered_model = self._live_config.get('require_registered_model', True)
        self.cache_ttl_minutes = self._live_config.get('cache_ttl_minutes', 60)
        
        # Track model load times for cache invalidation
        self._model_load_times: Dict[str, datetime] = {}
        
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(tracking_uri)
        logger.info(f"MLflow tracking URI set to: {tracking_uri}")
        logger.info(f"Live trading config: experiment={self.experiment_name}, stage={self.model_stage}")

    def _get_model_name(self, stock_symbol: str, time_period: str, metric: str) -> str:
        """Generate model name based on stock symbol, time period, and metric."""
        return f"{stock_symbol}_{time_period}_{metric}"

    def _get_latest_experiment(self):
        """Get the experiment from MLflow, with optional fallback."""
        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        
        if not experiment and self.fallback_experiment:
            logger.warning(f"Primary experiment '{self.experiment_name}' not found, trying fallback '{self.fallback_experiment}'")
            experiment = mlflow.get_experiment_by_name(self.fallback_experiment)
        
        if not experiment:
            raise ValueError(f"Experiment '{self.experiment_name}' not found in MLflow.")
        
        return experiment

    def is_model_stale(self, model_name: str) -> bool:
        """
        Check if a model is older than the configured threshold.
        
        Args:
            model_name: Name of the registered model
            
        Returns:
            True if model is stale and should not be used
        """
        if self.max_model_age_days <= 0:
            return False  # No age limit configured
        
        try:
            client = mlflow.tracking.MlflowClient()
            
            # Get model version info
            if self.model_stage == "None" or self.model_stage is None:
                # Get latest version
                versions = client.get_latest_versions(model_name)
            else:
                # Get version for specific stage
                versions = client.get_latest_versions(model_name, stages=[self.model_stage])
            
            if not versions:
                logger.warning(f"No versions found for model '{model_name}'")
                return True
            
            latest_version = versions[0]
            
            # Check creation time
            creation_timestamp = latest_version.creation_timestamp / 1000  # Convert ms to seconds
            creation_time = datetime.fromtimestamp(creation_timestamp)
            age = datetime.now() - creation_time
            
            is_stale = age > timedelta(days=self.max_model_age_days)
            
            if is_stale:
                logger.warning(
                    f"Model '{model_name}' is stale: created {creation_time.strftime('%Y-%m-%d')}, "
                    f"age={age.days} days, max_age={self.max_model_age_days} days"
                )
            
            return is_stale
            
        except Exception as e:
            logger.warning(f"Could not check model staleness for '{model_name}': {e}")
            return False  # Don't block on staleness check failure

    def _is_cache_valid(self, model_name: str) -> bool:
        """Check if cached model is still valid (within TTL)."""
        if model_name not in self._model_load_times:
            return False
        
        load_time = self._model_load_times[model_name]
        age = datetime.now() - load_time
        
        return age < timedelta(minutes=self.cache_ttl_minutes)

    def load_model(self, stock_symbol: str, time_period: str, metric: str, skip_staleness_check: bool = False):
        """
        Load model from MLflow for the given stock, time period, and metric.
        
        Args:
            stock_symbol: Stock symbol (e.g., 'RELIANCE', 'ALL_SYMBOLS')
            time_period: Timeframe (e.g., '5min', '1h')
            metric: Target metric (e.g., 'PctChange', 'ATR')
            skip_staleness_check: If True, skip model age validation
            
        Returns:
            Loaded sklearn pipeline model
            
        Raises:
            ValueError: If model not found or is stale
        """
        model_name = self._get_model_name(stock_symbol, time_period, metric)
        
        # Check cache validity (TTL-based)
        cached_model = self.model_cache.get(model_name)
        if cached_model and self._is_cache_valid(model_name):
            return cached_model

        # Check staleness before loading
        if not skip_staleness_check and self.is_model_stale(model_name):
            raise ValueError(
                f"Model '{model_name}' is stale (older than {self.max_model_age_days} days). "
                f"Please retrain or set skip_staleness_check=True."
            )

        # Load model from MLflow
        try:
            logger.info(f"Loading model: {model_name} (stage={self.model_stage})")
            
            # Build model URI based on stage
            if self.model_stage == "None" or self.model_stage is None:
                model_uri = f"models:/{model_name}/latest"
            else:
                model_uri = f"models:/{model_name}/{self.model_stage}"
            
            loaded_pipeline = mlflow.sklearn.load_model(model_uri)
            
            # Cache the loaded model and track load time
            self.model_cache.add(model_name, loaded_pipeline)
            self._model_load_times[model_name] = datetime.now()
            
            logger.info(f"Model {model_name} loaded and cached successfully")
            return loaded_pipeline
            
        except Exception as e:
            logger.error(f"Failed to load model {model_name}: {e}")
            raise

class AsyncModelLoader:
    """
    Async parallel model loader for faster startup.
    
    Loads multiple models concurrently using ThreadPoolExecutor,
    significantly reducing startup time when many models need to be loaded.
    """
    
    def __init__(self, model_loader: MLflowModelLoader, max_workers: int = 4):
        """
        Initialize async loader.
        
        Args:
            model_loader: Base MLflowModelLoader instance
            max_workers: Maximum parallel threads for loading
        """
        self.model_loader = model_loader
        self.max_workers = max_workers
        self._load_errors: Dict[str, str] = {}
    
    def load_models_parallel(
        self, 
        symbols: List[str], 
        timeframes: List[str], 
        metrics: List[str],
        skip_staleness_check: bool = False,
    ) -> Dict[str, Any]:
        """
        Load multiple models in parallel.
        
        Args:
            symbols: List of stock symbols
            timeframes: List of timeframes (e.g., ['5min', '15min', '1h'])
            metrics: List of target metrics (e.g., ['PctChange', 'ATR'])
            skip_staleness_check: If True, skip model age validation
            
        Returns:
            Dict of {model_key: loaded_model} for successful loads
        """
        self._load_errors = {}
        loaded_models = {}
        
        # Build list of (symbol, timeframe, metric) tuples to load
        load_tasks = []
        for symbol in symbols:
            for tf in timeframes:
                for metric in metrics:
                    load_tasks.append((symbol, tf, metric))
        
        logger.info(f"Starting parallel model loading: {len(load_tasks)} models, {self.max_workers} workers")
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all load tasks
            future_to_task = {
                executor.submit(
                    self._load_single_model, 
                    symbol, tf, metric, skip_staleness_check
                ): (symbol, tf, metric)
                for symbol, tf, metric in load_tasks
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_task):
                symbol, tf, metric = future_to_task[future]
                model_key = f"{symbol}_{tf}_{metric}"
                
                try:
                    model = future.result()
                    if model is not None:
                        loaded_models[model_key] = model
                except Exception as e:
                    error_msg = str(e)
                    self._load_errors[model_key] = error_msg
                    logger.error(f"Failed to load model {model_key}: {error_msg}")
        
        elapsed = time.time() - start_time
        success_count = len(loaded_models)
        error_count = len(self._load_errors)
        
        logger.info(
            f"Parallel model loading complete: {success_count} loaded, {error_count} failed "
            f"in {elapsed:.2f}s ({elapsed/len(load_tasks):.3f}s per model avg)"
        )
        
        return loaded_models
    
    def _load_single_model(
        self, 
        symbol: str, 
        timeframe: str, 
        metric: str,
        skip_staleness_check: bool,
    ) -> Optional[Any]:
        """Load a single model with error handling."""
        try:
            return self.model_loader.load_model(
                symbol, timeframe, metric, 
                skip_staleness_check=skip_staleness_check
            )
        except Exception as e:
            logger.debug(f"Error loading {symbol}_{timeframe}_{metric}: {e}")
            raise
    
    def get_load_errors(self) -> Dict[str, str]:
        """Get dictionary of model keys that failed to load and their errors."""
        return self._load_errors.copy()


class PredictionExecutor:
    """Class to execute predictions for a list of stocks."""

    def __init__(self, model_loader: MLflowModelLoader, data: pd.DataFrame):
        self.model_loader = model_loader
        self.data = data  # Assuming data is a pandas DataFrame

    def _predict_for_stock(self, stock_symbol: str, time_periods: List[str], metrics: List[str]) -> Dict[str, float]:
        """Run predictions for each stock symbol across different time periods and metrics."""
        predictions = {}
        self.data.fillna(0, inplace=True)
        for time_period in config.model_settings.run_ids:
            for metric in metrics:
                start_time = time.time()  # Start timer for performance profiling
                try:
                    model = self.model_loader.load_model(stock_symbol, time_period, metric)
                    prediction = model.predict(self.data)  # Predict using the pipeline model
                    predictions[f"{time_period}_{metric}"] = prediction[0]  # Assuming single row prediction
                    end_time = time.time()  # End timer
                    logger.debug(f"Prediction for {stock_symbol}, {time_period}, {metric} took {end_time - start_time:.4f} seconds.")
                except Exception as e:
                    logger.error(f"Error in making prediction for {stock_symbol}, {time_period}, {metric}: {e}")
        return predictions

    def run_predictions(self, stock_symbols: List[str], time_periods: List[str], metrics: List[str], actual_symbol: str = None) -> Dict[str, Dict[str, float]]:
        """Run predictions for all stocks in parallel using ThreadPoolExecutor.
        
        Args:
            stock_symbols: List of symbol identifiers for model loading (can be 'ALL_SYMBOLS')
            time_periods: List of time periods for predictions
            metrics: List of metrics to predict
            actual_symbol: The actual stock symbol for output (used when stock_symbols contains 'ALL_SYMBOLS')
        """
        predictions = {}
        
        # Determine which symbol to use for output keys
        output_symbol = actual_symbol if actual_symbol else stock_symbols[0]
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_stock = {executor.submit(self._predict_for_stock, stock, time_periods, metrics): stock for stock in stock_symbols}
            for future in concurrent.futures.as_completed(future_to_stock):
                stock_symbol = future_to_stock[future]
                try:
                    predictions[output_symbol] = future.result()
                except Exception as e:
                    logger.error(f"Error occurred for stock {stock_symbol}: {e}")
        
        return predictions


# Example usage
def main(config_path: str):
    # Load configuration
    # config = load_config(config_path)

    # Stock symbols, time periods, and metrics are now sourced from config
    stock_symbols = ["PNB", "IND"]
    time_periods = config['model_settings']['run_ids']
    metrics = config['model_settings']['model_targets']

    # Example data: Each stock has a list of feature values
    # Assuming data is a Pandas DataFrame with one row of feature values
    data = pd.DataFrame({
        'feature1': [1.2],  # Replace with actual feature names
        'feature2': [0.5]
    })

    # Initialize the model cache
    model_cache = ModelCache(max_cache_size=10)
    
    # Initialize loader and executor
    model_loader = MLflowModelLoader(experiment_name="TradingModels_20250102", config=config, model_cache=model_cache)
    prediction_executor = PredictionExecutor(model_loader=model_loader, data=data)

    # Run predictions
    predictions = prediction_executor.run_predictions(stock_symbols, time_periods, metrics)
    logging.info(f"Predictions: {predictions}")

if __name__ == "__main__":
    main(config_path='config.yaml')
