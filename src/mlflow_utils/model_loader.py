from src import config
from src.utils.utils import load_symbols
from loguru import logger

import mlflow
import mlflow.sklearn
import logging
import pandas as pd
import time
import concurrent.futures
from typing import Dict, List
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
    """Class to load models from MLflow for given stock, time period, and metric."""

    def __init__(self, config: dict, model_cache: ModelCache, experiment_name: str = None, tracking_uri: str = "http://localhost:5000"):
        self.config = config
        self.model_cache = model_cache
        self.experiment_name = experiment_name
        
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(tracking_uri)
        logger.info(f"MLflow tracking URI set to: {tracking_uri}")

    def _get_model_name(self, stock_symbol: str, time_period: str, metric: str) -> str:
        """Generate model name based on stock symbol, time period, and metric."""
        return f"{stock_symbol}_{time_period}_{metric}"

    def _get_latest_experiment(self):
        """Get the latest experiment from MLflow."""
        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        if not experiment:
            raise ValueError(f"Experiment '{self.experiment_name}' not found in MLflow.")
        return experiment

    def load_model(self, stock_symbol: str, time_period: str, metric: str):
        """Load the most recent model from MLflow for the given stock, time period, and metric."""
        model_name = self._get_model_name(stock_symbol, time_period, metric)
        
        # Try to fetch from cache
        cached_model = self.model_cache.get(model_name)
        if cached_model:
            return cached_model

        # If cache miss, load the model from MLflow
        try:
            logging.info(f"Loading model: {model_name}")
            
            # Load the pipeline model from MLflow
            model_uri = f"models:/{model_name}/latest"
            loaded_pipeline = mlflow.sklearn.load_model(model_uri)
            
            # Cache the loaded model
            self.model_cache.add(model_name, loaded_pipeline)
            
            logging.info(f"Model {model_name} loaded and cached successfully")
            return loaded_pipeline
        except Exception as e:
            logging.error(f"Failed to load model {model_name}: {e}")
            raise

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
                # try:
                model = self.model_loader.load_model(stock_symbol, time_period, metric)
                prediction = model.predict(self.data)  # Predict using the pipeline model
                predictions[f"{time_period}_{metric}"] = prediction[0]  # Assuming single row prediction
                end_time = time.time()  # End timer
                logging.info(f"Prediction for {stock_symbol}, {time_period}, {metric} took {end_time - start_time:.4f} seconds.")
                # except Exception as e:
                #     logging.error(f"Error in making prediction for {stock_symbol}, {time_period}, {metric}: {e}")
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
                    logging.error(f"Error occurred for stock {stock_symbol}: {e}")
        
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
