# src/backtesting/backtest_engine.py
"""
Backtesting Engine for Trading Models

Provides historical backtesting capabilities:
- Load historical data from InfluxDB
- Run MLflow models on historical data
- Simulate trades using TradeDecisionMaker logic
- Calculate performance metrics

Usage:
    engine = BacktestEngine(start_date, end_date, initial_capital=100000)
    result = await engine.run_backtest(symbol="RELIANCE", experiment_name="TradingModels_Production")
    print(result.summary())
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

# Project imports
try:
    from src import config
    from src.utils.influx_client import (
        DataBucket,
        InfluxDBClient_Wrapper,
        InfluxDBConfig,
        create_influx_config_from_hydra,
    )
    from src.feature_engineering.feature_aggregator import DataAggregator
    from src.trading_logic.trade_decision_maker import TradeDecisionMaker, TradeDecision
    from src.trading_logic.trade_simulator import TradeSimulator
    from src.backtesting.backtest_result import (
        BacktestResult,
        TradeRecord,
        TradeDirection,
        TradeStatus,
        PerformanceMetrics,
        ComparisonReport,
    )
    IMPORTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Some imports not available: {e}")
    IMPORTS_AVAILABLE = False

# MLflow imports
try:
    import mlflow
    import mlflow.sklearn
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logger.warning("MLflow not available, model loading will fail")


class BacktestEngine:
    """
    Engine for running historical backtests on trading models.
    
    Features:
    - Load historical data from InfluxDB
    - Load models from MLflow experiments
    - Walk-forward simulation (no lookahead bias)
    - Performance metric calculation
    - Model comparison
    
    Example:
        engine = BacktestEngine(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            initial_capital=100000
        )
        
        result = await engine.run_backtest(
            symbol="RELIANCE",
            experiment_name="TradingModels_Production"
        )
        
        print(f"Return: {result.metrics.total_return_pct:.2f}%")
        print(f"Sharpe: {result.metrics.sharpe_ratio:.2f}")
    """
    
    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        initial_capital: float = 100000.0,
        transaction_cost: float = 20.0,
        risk_per_trade_pct: float = 2.0,
        max_position_size: int = 100,
        mlflow_tracking_uri: str = "http://localhost:5000",
    ):
        """
        Initialize backtest engine.
        
        Args:
            start_date: Start of backtest period
            end_date: End of backtest period
            initial_capital: Starting capital
            transaction_cost: Cost per transaction (fees, slippage)
            risk_per_trade_pct: Risk percentage per trade for position sizing
            max_position_size: Maximum shares per trade
            mlflow_tracking_uri: MLflow server URI
        """
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.risk_per_trade_pct = risk_per_trade_pct
        self.max_position_size = max_position_size
        self.mlflow_tracking_uri = mlflow_tracking_uri
        
        # Initialize components
        self._influx_client: Optional[InfluxDBClient_Wrapper] = None
        self._decision_maker: Optional[TradeDecisionMaker] = None
        self._data_aggregator: Optional[DataAggregator] = None
        self._models: Dict[str, Any] = {}  # Cache for loaded models
        
        # Set MLflow tracking URI
        if MLFLOW_AVAILABLE:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        
        logger.info(
            f"BacktestEngine initialized: {start_date.date()} to {end_date.date()}, "
            f"capital=${initial_capital:,.0f}"
        )
    
    async def initialize(self) -> bool:
        """
        Initialize connections and components.
        
        Returns:
            True if initialization successful
        """
        try:
            # Initialize InfluxDB client
            influx_config = self._get_influx_config()
            if influx_config:
                self._influx_client = InfluxDBClient_Wrapper(influx_config)
                connected = await self._influx_client.connect()
                if not connected:
                    logger.error("Failed to connect to InfluxDB")
                    return False
                logger.info("Connected to InfluxDB")
            else:
                logger.error("Could not get InfluxDB configuration")
                return False
            
            # Initialize decision maker
            self._decision_maker = TradeDecisionMaker()
            
            # Initialize data aggregator
            if IMPORTS_AVAILABLE:
                self._data_aggregator = DataAggregator()
            
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False
    
    async def close(self) -> None:
        """Close connections."""
        if self._influx_client:
            await self._influx_client.close()
    
    def _get_influx_config(self) -> Optional[InfluxDBConfig]:
        """Get InfluxDB configuration."""
        from dotenv import load_dotenv
        load_dotenv()
        
        influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        influx_token = os.getenv("INFLUXDB_TOKEN")
        influx_org = os.getenv("INFLUXDB_ORG", "trading")
        
        if not influx_token:
            logger.error("INFLUXDB_TOKEN not set")
            return None
        
        return InfluxDBConfig(
            url=influx_url,
            token=influx_token,
            org=influx_org,
        )
    
    async def load_historical_data(
        self,
        symbol: str,
        include_orderbook: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load historical data from InfluxDB.
        
        Args:
            symbol: Stock symbol
            include_orderbook: Whether to load orderbook data
            
        Returns:
            Tuple of (ticker_df, orderbook_df)
        """
        if not self._influx_client:
            raise RuntimeError("InfluxDB client not initialized")
        
        # Calculate hours from date range
        hours = int((self.end_date - self.start_date).total_seconds() / 3600) + 24
        
        logger.info(f"Loading historical data for {symbol} ({hours} hours)...")
        
        # Load ticker data
        ticker_df = await self._influx_client.query_ticker_data(symbol, hours=hours)
        
        if ticker_df is None or ticker_df.empty:
            logger.warning(f"No ticker data found for {symbol}")
            ticker_df = pd.DataFrame()
        else:
            # Filter to date range
            if 'timestamp' in ticker_df.columns:
                ticker_df['timestamp'] = pd.to_datetime(ticker_df['timestamp'])
                ticker_df = ticker_df[
                    (ticker_df['timestamp'] >= self.start_date) & 
                    (ticker_df['timestamp'] <= self.end_date)
                ]
            logger.info(f"Loaded {len(ticker_df)} ticker records for {symbol}")
        
        # Load orderbook data
        orderbook_df = pd.DataFrame()
        if include_orderbook:
            try:
                # query_orderbook_data expects list of symbols, start_time, and end_time
                orderbook_df = await self._influx_client.query_orderbook_data(
                    symbols=[symbol],
                    start_time=self.start_date,
                    end_time=self.end_date
                )
                if orderbook_df is not None and not orderbook_df.empty:
                    # Filter to this symbol if multiple symbols returned
                    if 'symbol' in orderbook_df.columns:
                        orderbook_df = orderbook_df[orderbook_df['symbol'] == symbol]
                    
                    if 'timestamp' in orderbook_df.columns:
                        orderbook_df['timestamp'] = pd.to_datetime(orderbook_df['timestamp'])
                        orderbook_df = orderbook_df[
                            (orderbook_df['timestamp'] >= self.start_date) & 
                            (orderbook_df['timestamp'] <= self.end_date)
                        ]
                    logger.info(f"Loaded {len(orderbook_df)} orderbook records for {symbol}")
            except Exception as e:
                logger.debug(f"Could not load orderbook data: {e}")
        
        return ticker_df, orderbook_df
    
    def load_models(
        self,
        experiment_name: str,
        model_stage: str = "Production",
        symbol: str = "ALL_SYMBOLS",
        timeframes: Optional[List[str]] = None,
        metrics: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Load models from MLflow experiment.
        
        Args:
            experiment_name: MLflow experiment name
            model_stage: Model stage (Production, Staging, None)
            symbol: Symbol for model lookup
            timeframes: List of timeframes (default from config)
            metrics: List of target metrics (default from config)
            
        Returns:
            Dictionary of loaded models keyed by "{timeframe}_{metric}"
        """
        if not MLFLOW_AVAILABLE:
            raise RuntimeError("MLflow not available")
        
        # Get timeframes and metrics from config if not provided
        if timeframes is None:
            if IMPORTS_AVAILABLE:
                timeframes = list(config.model_settings.run_ids)
            else:
                timeframes = ["5min", "15min", "1h"]
        
        if metrics is None:
            if IMPORTS_AVAILABLE:
                metrics = list(config.model_settings.model_targets)
            else:
                metrics = ["PctChange", "ATR"]
        
        models = {}
        
        for tf in timeframes:
            for metric in metrics:
                model_name = f"{symbol}_{tf}_{metric}"
                
                try:
                    # Build model URI
                    if model_stage == "None" or model_stage is None:
                        model_uri = f"models:/{model_name}/latest"
                    else:
                        model_uri = f"models:/{model_name}/{model_stage}"
                    
                    logger.debug(f"Loading model: {model_uri}")
                    model = mlflow.sklearn.load_model(model_uri)
                    models[f"{tf}_{metric}"] = model
                    logger.info(f"Loaded model: {model_name}")
                    
                except Exception as e:
                    logger.warning(f"Could not load model {model_name}: {e}")
        
        if not models:
            logger.warning(f"No models loaded for experiment {experiment_name}")
        
        self._models = models
        return models
    
    def _generate_predictions(
        self,
        data: pd.DataFrame,
        models: Dict[str, Any],
    ) -> Dict[str, str]:
        """
        Generate predictions using loaded models.
        
        Args:
            data: Feature data (single row or batch)
            models: Dictionary of loaded models
            
        Returns:
            Dictionary of predictions keyed by model key
        """
        predictions = {}
        
        for key, model in models.items():
            try:
                # Prepare data for model
                # Note: Model expects specific columns based on training
                pred = model.predict(data)
                predictions[key] = pred[0] if len(pred) > 0 else "Neutral"
            except Exception as e:
                logger.debug(f"Prediction failed for {key}: {e}")
                predictions[key] = "Neutral"
        
        return predictions
    
    async def run_backtest(
        self,
        symbol: str,
        experiment_name: str = "TradingModels_Production",
        model_stage: str = "Production",
    ) -> BacktestResult:
        """
        Run a complete backtest for a symbol.
        
        Args:
            symbol: Stock symbol to backtest
            experiment_name: MLflow experiment to use
            model_stage: Model stage to load
            
        Returns:
            BacktestResult with trades and metrics
        """
        import time
        start_time = time.time()
        
        # Create result object
        backtest_id = str(uuid.uuid4())[:8]
        result = BacktestResult(
            backtest_id=backtest_id,
            symbol=symbol,
            model_name=experiment_name,
            experiment_name=experiment_name,
            start_date=self.start_date,
            end_date=self.end_date,
            initial_capital=self.initial_capital,
            transaction_cost=self.transaction_cost,
        )
        
        try:
            # Initialize if needed
            if not self._influx_client:
                if not await self.initialize():
                    result.errors.append("Failed to initialize")
                    return result
            
            # Load historical data
            ticker_df, orderbook_df = await self.load_historical_data(symbol)
            
            if ticker_df.empty:
                result.errors.append(f"No historical data for {symbol}")
                return result
            
            # Load models
            model_symbol = "ALL_SYMBOLS" if getattr(config, 'training', {}).get('combine_all_symbols', False) else symbol
            models = self.load_models(
                experiment_name=experiment_name,
                model_stage=model_stage,
                symbol=model_symbol,
            )
            
            if not models:
                result.errors.append("No models loaded")
                return result
            
            # Prepare feature data
            if self._data_aggregator and not orderbook_df.empty:
                try:
                    feature_data = self._data_aggregator.aggregate_features(ticker_df, orderbook_df)
                except Exception as e:
                    logger.warning(f"Feature aggregation failed: {e}, using ticker data only")
                    feature_data = ticker_df.copy()
            else:
                feature_data = ticker_df.copy()
            
            # Add symbol column if using combined model
            if model_symbol == "ALL_SYMBOLS":
                feature_data['symbol'] = symbol
            
            # Sort by timestamp for walk-forward simulation
            if 'timestamp' in feature_data.columns:
                feature_data = feature_data.sort_values('timestamp').reset_index(drop=True)
            
            # Run walk-forward simulation
            result = self._run_simulation(
                result=result,
                feature_data=feature_data,
                models=models,
            )
            
            # Calculate metrics
            result.calculate_metrics()
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            result.errors.append(str(e))
        
        result.execution_time_seconds = time.time() - start_time
        logger.info(
            f"Backtest complete: {symbol}, Return={result.metrics.total_return_pct:.2f}%, "
            f"Sharpe={result.metrics.sharpe_ratio:.2f}, Trades={result.metrics.total_trades}"
        )
        
        return result
    
    def _run_simulation(
        self,
        result: BacktestResult,
        feature_data: pd.DataFrame,
        models: Dict[str, Any],
    ) -> BacktestResult:
        """
        Run walk-forward simulation.
        
        Args:
            result: BacktestResult to populate
            feature_data: Historical feature data
            models: Loaded models
            
        Returns:
            Updated BacktestResult
        """
        # State variables
        capital = self.initial_capital
        position: Optional[TradeRecord] = None
        equity_values = []
        equity_times = []
        trade_count = 0
        
        # Get timestamp column
        time_col = 'timestamp' if 'timestamp' in feature_data.columns else feature_data.index.name or 'index'
        
        # Iterate through data (walk-forward)
        for idx in range(len(feature_data)):
            row = feature_data.iloc[[idx]]  # Keep as DataFrame
            
            # Get timestamp
            if time_col in row.columns:
                timestamp = pd.to_datetime(row[time_col].iloc[0])
            else:
                timestamp = datetime.now()
            
            # Get current price
            price = row['close'].iloc[0] if 'close' in row.columns else 0
            
            if price <= 0:
                continue
            
            # Generate predictions
            try:
                predictions = self._generate_predictions(row, models)
            except Exception as e:
                logger.debug(f"Prediction error at {timestamp}: {e}")
                predictions = {}
            
            # Filter to PctChange predictions for decision making
            pct_predictions = {
                k.split('_')[0]: v for k, v in predictions.items()
                if 'PctChange' in k
            }
            
            # Get ATR predictions for volatility
            atr_predictions = {
                k.split('_')[0]: v for k, v in predictions.items()
                if 'ATR' in k
            }
            
            # Make trade decision
            if self._decision_maker and pct_predictions:
                decision = self._decision_maker.make_decision(
                    pct_predictions=pct_predictions,
                    atr_predictions=atr_predictions if atr_predictions else None,
                )
            else:
                # Fallback: simple signal based on predictions
                decision = self._simple_decision(pct_predictions)
            
            # Execute trade logic
            if position is not None:
                # Check if we should close position
                should_close = False
                close_reason = "signal"
                
                if decision.direction == TradeDirection.HOLD.value:
                    pass  # Keep position
                elif (position.direction == TradeDirection.LONG and decision.direction == "SHORT"):
                    should_close = True
                    close_reason = "reverse_signal"
                elif (position.direction == TradeDirection.SHORT and decision.direction == "LONG"):
                    should_close = True
                    close_reason = "reverse_signal"
                
                # Check stop loss
                if position.stop_loss_price:
                    if (position.direction == TradeDirection.LONG and price <= position.stop_loss_price):
                        should_close = True
                        close_reason = "stop_loss"
                    elif (position.direction == TradeDirection.SHORT and price >= position.stop_loss_price):
                        should_close = True
                        close_reason = "stop_loss"
                
                if should_close:
                    # Close position
                    position.close(
                        exit_time=timestamp,
                        exit_price=price,
                        transaction_cost=self.transaction_cost,
                        exit_reason=close_reason,
                    )
                    capital += position.net_pnl
                    result.trades.append(position)
                    position = None
            
            # Open new position if no current position
            if position is None and decision.direction in ["LONG", "SHORT"]:
                # Calculate position size
                qty = self._calculate_position_size(price, capital)
                
                if qty > 0:
                    trade_count += 1
                    position = TradeRecord(
                        trade_id=f"{result.backtest_id}_{trade_count}",
                        symbol=result.symbol,
                        direction=TradeDirection(decision.direction),
                        entry_time=timestamp,
                        entry_price=price,
                        quantity=qty,
                        position_value=price * qty,
                        model_name=result.model_name,
                        prediction_confidence=decision.confidence,
                        timeframe_agreement=decision.timeframe_agreement,
                    )
                    
                    # Set stop loss based on ATR or fixed percentage
                    sl_multiplier = decision.stop_loss_atr_multiplier if hasattr(decision, 'stop_loss_atr_multiplier') else 2.0
                    sl_pct = 0.02 * sl_multiplier  # 2% base * multiplier
                    
                    if position.direction == TradeDirection.LONG:
                        position.stop_loss_price = price * (1 - sl_pct)
                    else:
                        position.stop_loss_price = price * (1 + sl_pct)
            
            # Calculate equity
            equity = capital
            if position:
                # Add unrealized P&L
                if position.direction == TradeDirection.LONG:
                    unrealized = (price - position.entry_price) * position.quantity
                else:
                    unrealized = (position.entry_price - price) * position.quantity
                equity += unrealized
            
            equity_values.append(equity)
            equity_times.append(timestamp)
        
        # Close any remaining position at end
        if position:
            final_price = feature_data['close'].iloc[-1] if 'close' in feature_data.columns else position.entry_price
            final_time = (
                pd.to_datetime(feature_data[time_col].iloc[-1]) 
                if time_col in feature_data.columns else datetime.now()
            )
            position.close(
                exit_time=final_time,
                exit_price=final_price,
                transaction_cost=self.transaction_cost,
                exit_reason="end_of_backtest",
            )
            capital += position.net_pnl
            result.trades.append(position)
        
        # Create equity curve
        if equity_times:
            result.equity_curve = pd.Series(
                equity_values,
                index=pd.DatetimeIndex(equity_times),
                name='equity'
            )
        
        return result
    
    def _simple_decision(self, predictions: Dict[str, str]) -> Any:
        """Simple decision based on predictions (fallback)."""
        from dataclasses import dataclass
        
        @dataclass
        class SimpleDecision:
            direction: str = "HOLD"
            confidence: float = 0.5
            timeframe_agreement: int = 0
            stop_loss_atr_multiplier: float = 2.0
        
        if not predictions:
            return SimpleDecision()
        
        # Count bullish/bearish predictions
        bullish = sum(1 for p in predictions.values() if p in ['High', 'Medium High'])
        bearish = sum(1 for p in predictions.values() if p in ['Low', 'Medium Low'])
        
        if bullish > bearish and bullish >= 2:
            return SimpleDecision(direction="LONG", confidence=bullish/len(predictions), timeframe_agreement=bullish)
        elif bearish > bullish and bearish >= 2:
            return SimpleDecision(direction="SHORT", confidence=bearish/len(predictions), timeframe_agreement=bearish)
        
        return SimpleDecision()
    
    def _calculate_position_size(self, price: float, capital: float) -> int:
        """Calculate position size based on risk management."""
        risk_amount = capital * (self.risk_per_trade_pct / 100)
        qty = int(risk_amount / price)
        return min(qty, self.max_position_size)
    
    async def compare_models(
        self,
        symbols: List[str],
        experiment_names: List[str],
        model_stage: str = "Production",
    ) -> ComparisonReport:
        """
        Compare multiple models across symbols.
        
        Args:
            symbols: List of symbols to test
            experiment_names: List of MLflow experiments to compare
            model_stage: Model stage to load
            
        Returns:
            ComparisonReport with all results
        """
        report = ComparisonReport(
            report_id=str(uuid.uuid4())[:8],
            comparison_type="model",
        )
        
        for experiment in experiment_names:
            for symbol in symbols:
                logger.info(f"Running backtest: {experiment} on {symbol}")
                
                result = await self.run_backtest(
                    symbol=symbol,
                    experiment_name=experiment,
                    model_stage=model_stage,
                )
                
                report.add_result(result)
        
        logger.info(f"Comparison complete: {len(report.results)} backtests")
        return report


# Convenience function for quick backtesting
async def quick_backtest(
    symbol: str,
    days: int = 30,
    initial_capital: float = 100000,
    experiment_name: str = "TradingModels_Production",
) -> BacktestResult:
    """
    Run a quick backtest with minimal configuration.
    
    Args:
        symbol: Stock symbol
        days: Number of days to backtest
        initial_capital: Starting capital
        experiment_name: MLflow experiment
        
    Returns:
        BacktestResult
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    engine = BacktestEngine(
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
    )
    
    try:
        result = await engine.run_backtest(symbol, experiment_name)
    finally:
        await engine.close()
    
    return result
