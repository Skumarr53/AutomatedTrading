# src/distributed/actors/signal_generator.py
"""
Ray Actor for parallel signal generation and ML inference.

Processes market data through ML models to generate trading signals.
Designed to process 100+ symbols within a 5-minute window.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from loguru import logger
from pydantic import Field

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None

from src.distributed.actors.base import BaseActor, BaseActorConfig


class SignalGeneratorConfig(BaseActorConfig):
    """Configuration for SignalGeneratorActor."""
    
    # Model settings
    mlflow_tracking_uri: str = Field(default="http://localhost:5000")
    model_cache_size: int = Field(default=100, ge=10)
    
    # Processing settings
    batch_size: int = Field(default=25, ge=1, le=100)
    prediction_timeout_seconds: float = Field(default=30.0, ge=5.0)
    
    # Signal thresholds
    buy_score_threshold: float = Field(default=3.0)
    sell_score_threshold: float = Field(default=2.0)
    
    # Weights for different timeframes
    timeframe_weights: dict[str, float] = Field(
        default_factory=lambda: {"5m": 1.0, "15m": 2.0, "1h": 4.0}
    )


class TradingSignal:
    """Represents a trading signal for a symbol."""
    
    def __init__(
        self,
        symbol: str,
        signal: str,  # BUY, SELL, HOLD
        score: float,
        confidence: float,
        predictions: dict[str, Any],
        timestamp: datetime,
    ) -> None:
        self.symbol = symbol
        self.signal = signal
        self.score = score
        self.confidence = confidence
        self.predictions = predictions
        self.timestamp = timestamp
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "signal": self.signal,
            "score": self.score,
            "confidence": self.confidence,
            "predictions": self.predictions,
            "timestamp": self.timestamp.isoformat(),
        }


def create_signal_generator_actor(config: SignalGeneratorConfig):
    """
    Factory function to create a SignalGeneratorActor.
    
    Args:
        config: Actor configuration
        
    Returns:
        SignalGeneratorActor instance (Ray actor handle or regular object)
    """
    if RAY_AVAILABLE and ray.is_initialized():
        # Wrap class with ray.remote() at call time, then configure options
        return ray.remote(SignalGeneratorActor).options(
            name=f"signal_generator_{config.actor_id}",
            lifetime="detached",
            max_restarts=3,
        ).remote(config)
    else:
        return SignalGeneratorActor(config)


class SignalGeneratorActor(BaseActor):
    """
    Ray Actor for ML-based signal generation.
    
    Responsibilities:
    - Load and cache ML models from MLflow
    - Generate predictions for assigned symbols
    - Compute weighted trading signals
    - Report prediction latency metrics
    
    Usage:
        config = SignalGeneratorConfig(actor_id="signal_1")
        actor = create_signal_generator_actor(config)
        
        # Assign symbols and data
        ray.get(actor.assign_symbols.remote(["RELIANCE", "TCS"]))
        
        # Generate signals
        signals = ray.get(actor.generate_signals.remote(data_dict))
    """
    
    def __init__(self, config: SignalGeneratorConfig) -> None:
        """Initialize SignalGeneratorActor."""
        super().__init__(config)
        self._signal_config = config
        self._model_loader = None
        self._model_cache = None
        self._prediction_executor = None
        self._feature_aggregator = None
    
    async def _initialize(self) -> None:
        """Initialize ML model loader and feature aggregator."""
        try:
            # Import here to avoid circular dependencies
            from src.mlflow_utils.model_loader import MLflowModelLoader, ModelCache
            from src.feature_engineering.feature_aggregator import DataAggregator
            from src import config
            
            # Initialize model cache and loader
            self._model_cache = ModelCache(max_cache_size=self._signal_config.model_cache_size)
            self._model_loader = MLflowModelLoader(
                config=config,
                model_cache=self._model_cache,
                tracking_uri=self._signal_config.mlflow_tracking_uri,
            )
            
            # Initialize feature aggregator
            self._feature_aggregator = DataAggregator()
            
            logger.info(f"SignalGeneratorActor {self._actor_id} initialized")
            
        except Exception as e:
            logger.error(f"Actor {self._actor_id}: Initialization failed: {e}")
            raise
    
    async def _process(self, *args: Any, **kwargs: Any) -> list[TradingSignal]:
        """
        Main processing - generate signals for provided data.
        
        Expected kwargs:
            ticker_data: dict[str, pd.DataFrame] - Ticker data per symbol
            orderbook_data: dict[str, pd.DataFrame] - Order book data per symbol
        
        Returns:
            List of TradingSignal objects
        """
        ticker_data = kwargs.get("ticker_data", {})
        orderbook_data = kwargs.get("orderbook_data", {})
        
        return await self.generate_signals(ticker_data, orderbook_data)
    
    async def _cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        if self._model_cache:
            self._model_cache.clear()
        logger.info(f"Actor {self._actor_id}: Resources cleaned up")
    
    async def generate_signals(
        self,
        ticker_data: dict[str, pd.DataFrame],
        orderbook_data: dict[str, pd.DataFrame],
    ) -> list[TradingSignal]:
        """
        Generate trading signals for all assigned symbols.
        
        Args:
            ticker_data: Dictionary of ticker DataFrames per symbol
            orderbook_data: Dictionary of order book DataFrames per symbol
            
        Returns:
            List of TradingSignal objects
        """
        signals: list[TradingSignal] = []
        
        # Process symbols in batches
        for i in range(0, len(self._assigned_symbols), self._signal_config.batch_size):
            batch = self._assigned_symbols[i:i + self._signal_config.batch_size]
            
            batch_signals = await self._process_symbol_batch(
                batch, ticker_data, orderbook_data
            )
            signals.extend(batch_signals)
        
        logger.info(
            f"Actor {self._actor_id}: Generated {len(signals)} signals for "
            f"{len(self._assigned_symbols)} symbols"
        )
        
        return signals
    
    async def _process_symbol_batch(
        self,
        symbols: list[str],
        ticker_data: dict[str, pd.DataFrame],
        orderbook_data: dict[str, pd.DataFrame],
    ) -> list[TradingSignal]:
        """Process a batch of symbols concurrently."""
        tasks = []
        
        for symbol in symbols:
            if symbol not in ticker_data:
                logger.warning(f"Actor {self._actor_id}: No data for {symbol}")
                continue
            
            task = self._generate_signal_for_symbol(
                symbol,
                ticker_data.get(symbol, pd.DataFrame()),
                orderbook_data.get(symbol, pd.DataFrame()),
            )
            tasks.append(task)
        
        # Execute concurrently with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=self._signal_config.prediction_timeout_seconds,
            )
            
            # Filter out exceptions
            return [r for r in results if isinstance(r, TradingSignal)]
            
        except asyncio.TimeoutError:
            logger.error(f"Actor {self._actor_id}: Batch processing timed out")
            return []
    
    async def _generate_signal_for_symbol(
        self,
        symbol: str,
        ticker_df: pd.DataFrame,
        orderbook_df: pd.DataFrame,
    ) -> Optional[TradingSignal]:
        """Generate trading signal for a single symbol."""
        try:
            if ticker_df.empty:
                return None
            
            # Aggregate features
            if self._feature_aggregator:
                data_agg = self._feature_aggregator.aggregate_features(ticker_df, orderbook_df)
            else:
                data_agg = ticker_df
            
            # Generate predictions (run in executor for blocking ML calls)
            loop = asyncio.get_event_loop()
            predictions = await loop.run_in_executor(
                None,
                lambda: self._get_predictions(symbol, data_agg)
            )
            
            # Compute weighted score
            score = self._compute_weighted_score(predictions)
            
            # Determine signal
            if score >= self._signal_config.buy_score_threshold:
                signal_type = "BUY"
            elif score <= self._signal_config.sell_score_threshold:
                signal_type = "SELL"
            else:
                signal_type = "HOLD"
            
            # Compute confidence based on score distance from thresholds
            if signal_type == "BUY":
                confidence = min((score - self._signal_config.buy_score_threshold) / 2.0, 1.0)
            elif signal_type == "SELL":
                confidence = min((self._signal_config.sell_score_threshold - score) / 2.0, 1.0)
            else:
                confidence = 0.5
            
            return TradingSignal(
                symbol=symbol,
                signal=signal_type,
                score=score,
                confidence=max(0.0, confidence),
                predictions=predictions,
                timestamp=datetime.now(),
            )
            
        except Exception as e:
            logger.error(f"Actor {self._actor_id}: Signal generation failed for {symbol}: {e}")
            return None
    
    def _get_predictions(self, symbol: str, data: pd.DataFrame) -> dict[str, Any]:
        """
        Get ML model predictions for a symbol.
        
        Args:
            symbol: Stock symbol
            data: Aggregated feature DataFrame
            
        Returns:
            Dictionary of predictions per timeframe/model
        """
        predictions: dict[str, Any] = {}
        
        if not self._model_loader:
            logger.warning(f"Actor {self._actor_id}: Model loader not initialized")
            return predictions
        
        try:
            from src import config
            from src.mlflow_utils.model_loader import PredictionExecutor
            
            # Create prediction executor
            executor = PredictionExecutor(
                model_loader=self._model_loader,
                data=data
            )
            
            # Get predictions for configured time periods and targets
            time_periods = getattr(config.model_settings, 'run_ids', {})
            targets = getattr(config.model_settings, 'model_targets', [])
            
            raw_predictions = executor.run_predictions(
                stock_symbols=[symbol],
                time_periods=time_periods,
                metrics=targets,
                actual_symbol=symbol
            )
            
            # Extract predictions for this symbol
            if symbol in raw_predictions:
                predictions = raw_predictions[symbol]
            
        except Exception as e:
            logger.error(f"Actor {self._actor_id}: Prediction error for {symbol}: {e}")
        
        return predictions
    
    def _compute_weighted_score(self, predictions: dict[str, Any]) -> float:
        """
        Compute weighted score from predictions.
        
        Uses exponential weighting based on timeframe (longer = higher weight).
        
        Args:
            predictions: Dictionary of predictions per timeframe
            
        Returns:
            Weighted score (0-5 scale typically)
        """
        if not predictions:
            return 2.5  # Neutral score
        
        # Score mapping for prediction categories
        score_mapping = {
            "Low": 1.0,
            "Medium Low": 2.0,
            "Neutral": 2.5,
            "Medium High": 3.0,
            "High": 4.0,
        }
        
        total_weight = 0.0
        weighted_sum = 0.0
        
        for key, value in predictions.items():
            # Extract timeframe from key (e.g., "5m_PctChange" -> "5m")
            timeframe = key.split("_")[0] if "_" in key else key
            
            weight = self._signal_config.timeframe_weights.get(timeframe, 1.0)
            
            # Handle different prediction formats
            if isinstance(value, str):
                score = score_mapping.get(value, 2.5)
            elif isinstance(value, (int, float)):
                # Assume numeric value is already a score
                score = float(value)
            else:
                continue
            
            weighted_sum += score * weight
            total_weight += weight
        
        if total_weight == 0:
            return 2.5
        
        return weighted_sum / total_weight
    
    def get_signal_for_symbol(self, symbol: str) -> Optional[TradingSignal]:
        """
        Get the most recent signal for a symbol.
        
        This is a synchronous convenience method for testing.
        """
        # This would need a signal cache to be useful in production
        return None


# Keep original class for non-Ray usage
# The factory function create_signal_generator_actor handles Ray wrapping
# DO NOT apply ray.remote() here - it causes double-wrapping errors

# Debug & Verify
# ==============
# Run: python -c "from src.distributed.actors.signal_generator import SignalGeneratorConfig; print(SignalGeneratorConfig(actor_id='test').model_dump())"
# Verify: No import errors
