"""
AutomatedTrading Main Application

Supports two execution modes:
- Distributed (Ray): Parallel processing for 100+ symbols (recommended for production)
- Sequential: Single-threaded processing (for development/debugging)

Set RAY_ENABLED=true in environment or config to enable distributed mode.
"""
from __future__ import annotations

from src import config

# ============================================================================
# WARNING SUPPRESSION CONFIGURATION
# ============================================================================
# Suppress common warnings to reduce log clutter
# Set SUPPRESS_WARNINGS=0 in environment to enable warnings for debugging
# Set VERBOSE_LOGGING=1 in environment to enable verbose library logging

from src.utils.warning_config import configure_all_warnings
configure_all_warnings()

# ============================================================================

import os
import time
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

# Core imports
from src.auth.fyers_auth import AuthCodeGenerator
from src.data.company_metadata import CompanyMetadataFetcher
from src.data.data_fetcher import DataHandler
from src.data.order_book_handler import OrderBookHandler
from src.feature_engineering.feature_aggregator import DataAggregator
from src.feature_engineering.technical_indicators import TechnicalIndicators
from src.financial_analysis.trading_strategies import TradingStrategies
from src.mlflow_utils.mlflow_server import is_mlflow_server_running, start_mlflow_server
from src.mlflow_utils.model_loader import MLflowModelLoader, ModelCache, PredictionExecutor
from src.pipelines.base_pipeline import MLPipelineBase
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
from src.trading_logic.trade_decision_maker import TradeDecisionMaker
from src.utils.utils import determine_mode, get_NSE_symbol, get_timezone, load_symbols

# Distributed computing imports (optional)
try:
    import ray  # Import ray module for direct access
    from src.distributed import (
        RayClusterConfig,
        get_ray_status,
        init_ray_cluster,
        is_ray_initialized,
        shutdown_ray_cluster,
    )
    from src.distributed.actors.data_ingestor import create_data_ingestor_actor, DataIngestorConfig
    from src.distributed.actors.signal_generator import create_signal_generator_actor, SignalGeneratorConfig
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None  # Set to None so checks like `if ray:` work
    logger.info("Ray not available, running in sequential mode")

# Optional Slack notifications
try:
    from scripts.slack_notifier import send_slack_message, send_trading_alert
    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False
    def send_slack_message(*args, **kwargs) -> bool:
        return False
    def send_trading_alert(*args, **kwargs) -> bool:
        return False

# Prometheus metrics for live trading
try:
    from src.metrics.performance_metrics import get_ingestion_metrics
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    get_ingestion_metrics = None

# Live trading metrics (module-level to persist across calls)
_trading_metrics = None
_trading_start_time = None

def get_trading_metrics():
    """Get or create trading metrics instance."""
    global _trading_metrics, _trading_start_time
    if _trading_metrics is None and METRICS_AVAILABLE:
        _trading_metrics = get_ingestion_metrics()
        _trading_start_time = datetime.now()
    return _trading_metrics

# Legacy Telegram support (deprecated, use Slack instead)
TELEGRAM_AVAILABLE = False
def send_telegram_message(*args, **kwargs) -> None:
    """Deprecated: Use send_slack_message instead."""
    pass


def is_distributed_mode_enabled() -> bool:
    """Check if distributed (Ray) mode should be enabled."""
    # Check environment variable first
    env_enabled = os.getenv("RAY_ENABLED", "").lower() in ("true", "1", "yes")
    
    # Check config
    config_enabled = getattr(config, "ray", {}).get("enabled", False)
    
    return RAY_AVAILABLE and (env_enabled or config_enabled)


logger.info(f"Distributed mode: {'enabled' if is_distributed_mode_enabled() else 'disabled'}")

## TODO: use the following snippet for alert across 
# send_telegram_message(
# type: Good or Bad 
# message=str('Service has started')
# )

class MarketAnalysisApp:
    """
    Market Analysis Application for handling authorization, data fetching,
    computing technical indicators, executing trading strategies, and
    handling order book data.
    
    Supports two execution modes:
    - Distributed (Ray): Parallel processing for 100+ symbols
    - Sequential: Single-threaded processing for development
    
    Attributes:
        trading_mode: 'LIVE' or 'BACKTEST'
        distributed_mode: Whether Ray distributed processing is enabled
        fyers_instance: Authenticated Fyers API client
        data_ingestor_actors: Ray actors for parallel data fetching (if distributed)
        signal_generator_actors: Ray actors for parallel ML inference (if distributed)
    """
    
    def __init__(self, distributed: Optional[bool] = None) -> None:
        """
        Initialize MarketAnalysisApp.
        
        Args:
            distributed: Override distributed mode setting. If None, uses config/env.
        """
        self.trading_mode = config.trading_config.trade_mode or determine_mode()
        self.distributed_mode = distributed if distributed is not None else is_distributed_mode_enabled()
        
        # Ray actors (initialized if distributed mode)
        self.data_ingestor_actors: list = []
        self.signal_generator_actors: list = []
        self._ray_initialized = False
        
        # Trading coordinator (for distributed mode)
        self._trading_coordinator: Optional[Any] = None
        self._trade_executor_actor: Optional[Any] = None
        
        self.setup_based_on_mode()
    
    def setup_based_on_mode(self) -> None:
        """Setup application based on trading mode and distributed setting."""
        # Initialize Ray cluster if distributed mode
        if self.distributed_mode:
            self._initialize_ray_cluster()
        
        # Authorization
        self.generator = AuthCodeGenerator()
        self._setup_authorization()
        
        # Initialize company metadata if enabled
        self._initialize_company_metadata()
        
        self.scheduler = BackgroundScheduler() if self.trading_mode == 'LIVE' else None
        self._setup_data_handling()
        self.order_data_handler = OrderBookHandler(
            self.fyers_instance, self.scheduler
        )
        self.data_aggregator = DataAggregator()
        self.strategy_module = TradingStrategies()
        self.last_data_collection_time: Optional[datetime] = None
        self.custom_model = MLPipelineBase()
        
        if not is_mlflow_server_running():
            start_mlflow_server()

        # Initialize ML components
        self.model_cache = ModelCache(max_cache_size=500)
        self.model_loader = MLflowModelLoader(
            config=config,
            model_cache=self.model_cache,
            tracking_uri="http://localhost:5000"
        )
        self.trade_decision_maker = TradeDecisionMaker(cooldown_minutes=5)
        
        # Trade executor only for LIVE mode
        if self.trading_mode == 'LIVE':
            self.trade_executor = FyersTradeExecutor(fyers=self.fyers_instance)
            logger.info("Trade executor initialized successfully")
        else:
            self.trade_executor = None
        
        # Initialize distributed actors if enabled
        if self.distributed_mode and self._ray_initialized:
            self._initialize_distributed_actors()
        
        # Log startup configuration
        self._log_startup_config()
    
    def _initialize_ray_cluster(self) -> None:
        """Initialize Ray cluster for distributed processing."""
        if not RAY_AVAILABLE:
            logger.warning("Ray not available, falling back to sequential mode")
            self.distributed_mode = False
            return
        
        try:
            # Create config from Hydra config or defaults
            ray_cfg = getattr(config, 'ray', {})
            
            # Check RAY_ADDRESS environment variable - it can interfere with local cluster start
            ray_address_env = os.getenv("RAY_ADDRESS")
            if ray_address_env:
                logger.debug(f"RAY_ADDRESS environment variable is set to: {ray_address_env}")
                # If RAY_ADDRESS is set but we want local cluster, don't pass address
                # The init_ray_cluster function will handle unsetting it
            
            ray_config = RayClusterConfig(
                num_cpus=ray_cfg.get('num_cpus'),
                dashboard_host=ray_cfg.get('dashboard_host', '0.0.0.0'),
                dashboard_port=ray_cfg.get('dashboard_port', 8265),
                include_dashboard=True,
                namespace="trading",
                # Explicitly set address=None to start local cluster (overrides RAY_ADDRESS env var)
                address=None,  # Force local cluster start
            )
            
            logger.info("Initializing Ray cluster...")
            success = init_ray_cluster(ray_config)
            
            if success:
                # Verify Ray is actually initialized and connected
                if not RAY_AVAILABLE or ray is None or not ray.is_initialized():
                    logger.error("Ray init_ray_cluster returned True but ray.is_initialized() is False")
                    self._ray_initialized = False
                    self.distributed_mode = False
                    return
                
                # Check worker connection
                try:
                    if RAY_AVAILABLE and ray is not None:
                        worker = ray._private.worker.global_worker
                        if not worker.connected:
                            logger.error("Ray initialized but worker.connected is False")
                            self._ray_initialized = False
                            self.distributed_mode = False
                            return
                except Exception as worker_check_error:
                    logger.warning(f"Could not verify Ray worker connection: {worker_check_error}")
                
                self._ray_initialized = True
                status = get_ray_status()
                logger.success(
                    f"Ray cluster initialized successfully: {status.num_cpus} CPUs, "
                    f"Dashboard: {status.dashboard_url or 'http://localhost:8265'}"
                )
            else:
                logger.error("Ray initialization failed (init_ray_cluster returned False), falling back to sequential mode")
                logger.error("Check logs above for initialization errors")
                self._ray_initialized = False
                self.distributed_mode = False
                
        except Exception as e:
            logger.error(f"Ray initialization error: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            self._ray_initialized = False
            self.distributed_mode = False
    
    def _initialize_distributed_actors(self) -> None:
        """Initialize Ray actors for parallel processing."""
        try:
            # ray is already imported at module level
            from src.distributed.actors.data_ingestor import DataIngestorConfig
            from src.distributed.actors.signal_generator import SignalGeneratorConfig
            
            # Get actor configuration
            ray_cfg = getattr(config, 'ray', {})
            actors_cfg = ray_cfg.get('actors', {})
            
            num_ingestors = actors_cfg.get('data_ingestor', {}).get('num_actors', 5)
            symbols_per_actor = actors_cfg.get('data_ingestor', {}).get('symbols_per_actor', 20)
            num_signal_generators = actors_cfg.get('signal_generator', {}).get('num_actors', 4)
            
            # Distribute symbols across data ingestor actors
            symbols = list(config.symbols)
            symbol_chunks = [
                symbols[i:i + symbols_per_actor] 
                for i in range(0, len(symbols), symbols_per_actor)
            ]
            
            # Create data ingestor actors
            influx_cfg = getattr(config, 'influxdb', {})
            
            for i, symbol_chunk in enumerate(symbol_chunks[:num_ingestors]):
                actor_config = DataIngestorConfig(
                    actor_id=f"ingestor_{i}",
                    influx_url=influx_cfg.get('url', 'http://localhost:8086'),
                    influx_token=os.getenv('INFLUXDB_TOKEN', ''),
                    influx_org=influx_cfg.get('org', 'trading'),
                )
                
                # Create Ray actor using factory function
                actor = create_data_ingestor_actor(actor_config)
                
                # Initialize and assign symbols (check if this is a Ray actor handle)
                if RAY_AVAILABLE and ray is not None and ray.is_initialized():
                    ray.get(actor.initialize.remote())
                    ray.get(actor.assign_symbols.remote(symbol_chunk))
                    # Extract credentials from stored token to avoid pickling logger issues
                    # FyersModel contains logger that can't be pickled, so we recreate it in actor
                    from src import config
                    client_id = config.environment.app_settings.client_id
                    access_token = getattr(self, 'fyers_access_token', None)
                    if access_token:
                        ray.get(actor.set_fyers_credentials.remote(client_id, access_token))
                    else:
                        logger.warning(f"Access token not available for actor {i}, actor will not be able to fetch data")
                else:
                    # Local instance - call methods directly
                    import asyncio
                    asyncio.get_event_loop().run_until_complete(actor.initialize())
                    actor.assign_symbols(symbol_chunk)
                    actor.set_fyers_instance(self.fyers_instance)
                
                self.data_ingestor_actors.append(actor)
            
            # Create signal generator actors
            signal_chunks = [
                symbols[i:i + len(symbols) // num_signal_generators]
                for i in range(0, len(symbols), len(symbols) // num_signal_generators)
            ]
            
            for i, symbol_chunk in enumerate(signal_chunks[:num_signal_generators]):
                actor_config = SignalGeneratorConfig(
                    actor_id=f"signal_gen_{i}",
                    mlflow_tracking_uri="http://localhost:5000",
                )
                
                # Create Signal Generator actor using factory function
                actor = create_signal_generator_actor(actor_config)
                
                # Initialize and assign symbols
                if RAY_AVAILABLE and ray is not None and ray.is_initialized():
                    ray.get(actor.initialize.remote())
                    ray.get(actor.assign_symbols.remote(symbol_chunk))
                else:
                    # Local instance - call methods directly
                    import asyncio
                    asyncio.get_event_loop().run_until_complete(actor.initialize())
                    actor.assign_symbols(symbol_chunk)
                
                self.signal_generator_actors.append(actor)
            
            logger.info(
                f"Initialized {len(self.data_ingestor_actors)} data ingestors, "
                f"{len(self.signal_generator_actors)} signal generators"
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize distributed actors: {e}")
            self.distributed_mode = False
    
    def _log_startup_config(self) -> None:
        """Log startup configuration summary."""
        logger.info("=" * 60)
        logger.info("MarketAnalysisApp Configuration")
        logger.info("=" * 60)
        logger.info(f"  Trading Mode: {self.trading_mode}")
        logger.info(f"  Distributed Mode: {self.distributed_mode}")
        logger.info(f"  Symbols: {len(config.symbols)}")
        if self.distributed_mode:
            logger.info(f"  Data Ingestor Actors: {len(self.data_ingestor_actors)}")
            logger.info(f"  Signal Generator Actors: {len(self.signal_generator_actors)}")
        logger.info("=" * 60)

    def generate_live_predictions(self, data_agg: pd.DataFrame, symbol: str) -> dict:
        """Generate predictions using loaded models."""
        try:
            # Add symbol column if using combined model
            if getattr(config.training, 'combine_all_symbols', False):
                data_agg_with_symbol = data_agg.copy()
                data_agg_with_symbol['symbol'] = symbol
                prediction_symbol = 'ALL_SYMBOLS'
            else:
                data_agg_with_symbol = data_agg
                prediction_symbol = symbol
                
            prediction_executor = PredictionExecutor(
                model_loader=self.model_loader,
                data=data_agg_with_symbol
            )
            return prediction_executor.run_predictions(
                stock_symbols=[prediction_symbol],
                time_periods=config.model_settings.run_ids,
                metrics=config.model_settings.model_targets,
                actual_symbol=symbol  # Pass actual symbol for output
            )
        except Exception as e:
            logger.error(f"Prediction failed for {symbol}: {str(e)}")
            raise
    
    def execute_strategies(self, indicators_data: dict, predictions: dict) -> None:
        """Execute trading strategies combining indicators and model predictions."""
        try:
            # Merge technical indicators with model predictions
            enhanced_data = {
                **indicators_data,
                'model_predictions': predictions
            } 
            
            strategy_decisions = self.strategy_module.execute_technical_strategy(enhanced_data)
            self._execute_trades_based_on_decisions(strategy_decisions)
        except Exception as e:
            logger.error("Strategy execution failed")
            raise

    def _setup_data_handling(self):
        self.indicators = TechnicalIndicators()
        self.ticker_data_handler = DataHandler(self.fyers_instance, self.scheduler)
        self.ticker_data_handler.register_callback(
                self.indicators.get_stock_indicators)
        # self.indicators.register_callback(self.execute_strategies)
    
    def execute_strategies(self, indicators_data):
        """
        Execute trading strategies based on the indicators data.
        """
        try:
            strategy_decisions = self.strategy_module.execute_technical_strategy(
                indicators_data)
            # Process the strategy decisions further as needed
        except Exception as e:
            logger.error("Strategy execution failed")
            raise


    def _schedule_job(self, func: Callable, job_id: str) -> None:
        """Schedules a single job with a delay mechanism."""
        self.scheduler.add_job(
            func,
            'cron',
            day_of_week=config.scheduler.day_of_week,
            hour=config.scheduler.hour,
            minute=f'*/{config.scheduler.data_fetch_cron_interval_min}',
            timezone=get_timezone(),
            id=job_id,
            max_instances=config.scheduler.max_instances
        )
        logger.info(f"Scheduled {job_id} every {config.scheduler.data_fetch_cron_interval_min} minutes.")

    def configure_scheduler(self) -> None:
        """
        Schedule regular data updates during trading hours.
        
        Uses distributed coordinator if in distributed mode, otherwise
        falls back to sequential processing.
        """
        if self.distributed_mode and self._ray_initialized:
            # Use distributed trading with coordinator
            self._schedule_job(
                self.data_collection_distributed, "data_collection_distributed"
            )
        else:
            # Use sequential trading
            self._schedule_job(
                self.data_collection, "data_collection"
            )
        
        self.scheduler.start()

    def data_collection(self) -> None:
        """Sequential data collection and trading (non-distributed mode)."""
        self.order_data_handler.fetch_order_book_data()
        self.ticker_data_handler.update_data_regularly()
        self.last_data_collection_time = datetime.now()
        self.start_live_trading()
    
    def data_collection_distributed(self) -> None:
        """
        Distributed data collection and trading using Ray coordinator.
        
        Runs a single trading cycle through the TradingCoordinator,
        which orchestrates parallel data fetching, signal generation,
        and trade execution across Ray actors.
        """
        import asyncio
        
        if not hasattr(self, '_trading_coordinator') or self._trading_coordinator is None:
            self._initialize_trading_coordinator()
        
        if self._trading_coordinator is None:
            logger.warning("Trading coordinator not available, falling back to sequential")
            self.data_collection()
            return
        
        try:
            # Run single cycle through coordinator
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            metrics = loop.run_until_complete(self._trading_coordinator.run_cycle())
            
            loop.close()
            
            self.last_data_collection_time = datetime.now()
            
            # Log performance vs target
            target_ms = config.scheduler.data_fetch_cron_interval_min * 60 * 1000 * 0.6  # 60% of interval
            if metrics.total_duration_ms > target_ms:
                logger.warning(
                    f"Cycle duration {metrics.total_duration_ms:.0f}ms exceeds target {target_ms:.0f}ms"
                )
            
        except Exception as e:
            logger.error(f"Distributed data collection failed: {e}")
            # Fallback to sequential
            self.data_collection()
    
    def _initialize_trading_coordinator(self) -> None:
        """Initialize the TradingCoordinator for distributed trading."""
        try:
            from src.distributed import TradingCoordinator, CoordinatorConfig
            from src.distributed.actors.trade_executor import (
                TradeExecutorActor, 
                TradeExecutorConfig,
                create_trade_executor_actor
            )
            import ray
            
            # Create trade executor actor
            executor_config = TradeExecutorConfig(
                actor_id="trade_executor",
                dry_run_mode=getattr(config, 'execution', {}).get('dry_run', False),
                max_requests_per_second=8.0,
            )
            
            # Create executor actor (single instance)
            self._trade_executor_actor = create_trade_executor_actor(executor_config)
            if RAY_AVAILABLE and ray is not None and ray.is_initialized():
                ray.get(self._trade_executor_actor.initialize.remote())
                if self.trade_executor:
                    ray.get(self._trade_executor_actor.set_fyers_executor.remote(self.trade_executor))
            
            # Create coordinator config
            coord_config = CoordinatorConfig(
                cycle_timeout_seconds=180.0,
                target_cycle_duration_seconds=180.0,
                dry_run_mode=getattr(config, 'execution', {}).get('dry_run', False),
            )
            
            # Initialize coordinator
            self._trading_coordinator = TradingCoordinator(
                coord_config=coord_config,
                data_ingestor_actors=self.data_ingestor_actors,
                signal_generator_actors=self.signal_generator_actors,
                trade_executor_actor=self._trade_executor_actor,
                fyers_instance=self.fyers_instance,
            )
            
            logger.info("TradingCoordinator initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize TradingCoordinator: {e}")
            self._trading_coordinator = None

    def start_live_trading(self):
        """
        Execute live trading for all symbols:
        1. Get latest data and aggregate features
        2. Generate ML model predictions
        3. Compute technical indicators
        4. Make trading decision using weighted signals
        5. Execute trades based on decision
        
        Includes:
        - Prometheus metrics for latency and success/failure tracking
        - Slack/Telegram alerts for trade execution and errors
        """
        import time as time_module
        iteration_start = time_module.time()
        
        current_time = datetime.now()
        
        # Get metrics instance
        metrics = get_trading_metrics()
        
        # Track iteration stats
        symbols_processed = 0
        signals_generated = 0
        trades_executed = 0
        errors_count = 0
        
        logger.info(f"=== Live Trading Iteration Started at {current_time.strftime('%H:%M:%S')} ===")
        
        for symbol in config.symbols:
            try:
                logger.info(f"Processing trading signals for {symbol}")
                
                # Get data for symbol
                ticker_data = self.ticker_data_handler.data[symbol]
                order_book_data = self.order_data_handler.data[symbol]
                
                # Aggregate features
                data_agg = self.data_aggregator.aggregate_features(ticker_data, order_book_data)
                
                # Generate ML predictions for multiple timeframes
                predictions = self.generate_live_predictions(data_agg, symbol)
                symbol_predictions = predictions.get(symbol, {})
                
                # Filter predictions to get only PctChange predictions for decision making
                pct_change_predictions = {
                    k.split('_')[0]: v for k, v in symbol_predictions.items() 
                    if 'PctChange' in k
                }
                
                # Compute weighted signal score from predictions
                weighted_score = self.trade_decision_maker.compute_weighted_signal(pct_change_predictions)
                
                # Determine trade signal based on weighted score
                # Score > 3.0 indicates bullish (BUY), < 2.0 indicates bearish (SELL)
                if weighted_score >= 3.0:
                    trade_signal = "BUY"
                elif weighted_score <= 2.0:
                    trade_signal = "SELL"
                else:
                    trade_signal = "HOLD"
                
                # Format symbol for Fyers API (NSE:SYMBOL-EQ)
                fyers_symbol = get_NSE_symbol(symbol)
                
                # Check current position for the symbol
                current_position = self.trade_executor.get_position_for_symbol(fyers_symbol)
                position_qty = current_position.get('netQty', 0) if current_position else 0
                
                # Determine action based on signal and current position
                action_taken = False
                
                if trade_signal == "BUY" and position_qty <= 0:
                    # Want to BUY and don't have a long position
                    if position_qty < 0:
                        logger.info(f"{symbol}: Closing SHORT position before going LONG")
                        self.trade_executor.exit_position(fyers_symbol)
                    
                    # Check cooldown period before executing
                    if not self.trade_decision_maker.is_cooldown_over(current_time):
                        logger.info(f"{symbol}: BUY signal {trade_signal} (score: {weighted_score:.2f}) - Cooldown active, skipping")
                        continue
                    
                    # Confirm signal (requires consistency across checks)
                    signal_confirmed = self.trade_decision_maker.confirm_signal(trade_signal, current_time)
                    
                    if signal_confirmed:
                        # Get current price from data
                        current_price = data_agg['close'].iloc[-1] if 'close' in data_agg.columns else None
                        
                        if current_price:
                            ## EDIT: buy at market price avaiable as current_price may not exactly match and assuming the diff wont drastic
                            # Calculate position size based on risk management
                            qty = self.trade_executor.calculate_position_size(
                                symbol=fyers_symbol,
                                price=current_price,
                                risk_percent=2.0,  # Risk 2% of account
                                max_position_size=100
                            )
                            
                            if qty > 0:
                                logger.warning(f"{symbol}: EXECUTING BUY - Qty: {qty}, Price: ₹{current_price:.2f}, Score: {weighted_score:.2f}")
                                response = self.trade_executor.place_market_order(
                                    symbol=fyers_symbol,
                                    qty=qty,
                                    side="BUY",
                                    current_price=current_price
                                )
                                
                                if response:
                                    self.trade_decision_maker.last_trade_time = current_time
                                    action_taken = True
                                    trades_executed += 1
                                    logger.success(f"✓ {symbol}: BUY order executed successfully")
                                    
                                    # Send alert for trade execution
                                    if SLACK_AVAILABLE:
                                        send_trading_alert(
                                            alert_type="Trade Executed",
                                            symbol=symbol,
                                            message=f"BUY order placed",
                                            details={
                                                "Quantity": qty,
                                                "Price": f"₹{current_price:.2f}",
                                                "Score": f"{weighted_score:.2f}",
                                                "Time": current_time.strftime("%H:%M:%S"),
                                            }
                                        )
                                else:
                                    errors_count += 1
                                    logger.error(f"{symbol}: BUY order failed")
                                    if SLACK_AVAILABLE:
                                        send_slack_message("Bad", f"{symbol}: BUY order FAILED - Score: {weighted_score:.2f}")
                            else:
                                logger.warning(f"{symbol}: Insufficient funds or calculated qty is 0")
                        else:
                            logger.error(f"{symbol}: Cannot determine current price")
                    else:
                        logger.info(f"{symbol}: BUY signal not confirmed yet, waiting for consistency")
                
                elif trade_signal == "SELL" and position_qty >= 0:
                    # Want to SELL and don't have a short position
                    if position_qty > 0:
                        logger.info(f"{symbol}: Closing LONG position")
                        
                        # Check cooldown period before executing
                        if not self.trade_decision_maker.is_cooldown_over(current_time):
                            logger.info(f"{symbol}: SELL signal (score: {weighted_score:.2f}) - Cooldown active, skipping")
                            continue
                        
                        # Confirm signal
                        signal_confirmed = self.trade_decision_maker.confirm_signal(trade_signal, current_time)
                        
                        if signal_confirmed:
                            logger.warning(f"{symbol}: EXECUTING SELL (Exit) - Score: {weighted_score:.2f}")
                            success = self.trade_executor.exit_position(fyers_symbol, current_price=current_price)
                            
                            if success:
                                self.trade_decision_maker.last_trade_time = current_time
                                action_taken = True
                                trades_executed += 1
                                logger.success(f"✓ {symbol}: SELL (Exit) executed successfully")
                                
                                # Send alert for trade execution
                                if SLACK_AVAILABLE:
                                    send_trading_alert(
                                        alert_type="Trade Executed",
                                        symbol=symbol,
                                        message=f"SELL (Exit) order placed",
                                        details={
                                            "Position": position_qty,
                                            "Score": f"{weighted_score:.2f}",
                                            "Time": current_time.strftime("%H:%M:%S"),
                                        }
                                    )
                            else:
                                errors_count += 1
                                logger.error(f"{symbol}: SELL (Exit) order failed")
                                if SLACK_AVAILABLE:
                                    send_slack_message("Bad", f"{symbol}: SELL (Exit) order FAILED")
                        else:
                            logger.info(f"{symbol}: SELL signal not confirmed yet, waiting for consistency")
                    else:
                        # No position, could initiate SHORT if strategy allows
                        logger.info(f"{symbol}: SELL signal but no LONG position to exit (SHORT trading not implemented)")
                
                elif trade_signal == "HOLD":
                    logger.debug(f"{symbol}: HOLD signal - no action taken")
                
                # Log the decision summary
                if not action_taken:
                    logger.info(f"{symbol}: Signal={trade_signal}, Score={weighted_score:.2f}, Position={position_qty}, Action=NONE")
                
                logger.debug(f"{symbol}: Predictions = {symbol_predictions}")
                
                # Track successful processing
                symbols_processed += 1
                if trade_signal in ["BUY", "SELL"]:
                    signals_generated += 1
                    
            except Exception as e:
                errors_count += 1
                logger.error(f"Error processing {symbol}: {str(e)}")
                
                # Send alert for critical errors
                if SLACK_AVAILABLE:
                    send_slack_message("Bad", f"Error processing {symbol}: {str(e)[:100]}")
                continue
        
        # === Iteration Summary ===
        iteration_duration = time_module.time() - iteration_start
        
        logger.info(
            f"=== Live Trading Iteration Complete ===\n"
            f"  Duration: {iteration_duration:.2f}s\n"
            f"  Symbols: {symbols_processed}/{len(config.symbols)}\n"
            f"  Signals: {signals_generated}\n"
            f"  Trades: {trades_executed}\n"
            f"  Errors: {errors_count}"
        )
        
        # Record metrics if available
        if metrics and METRICS_AVAILABLE:
            try:
                # Record iteration latency
                metrics.record_api_latency("live_trading_iteration", iteration_duration)
            except Exception:
                pass
        
        # Send summary alert if trades were executed
        if trades_executed > 0 and SLACK_AVAILABLE:
            send_slack_message(
                "Good",
                f"Trading iteration complete: {trades_executed} trades executed for {symbols_processed} symbols"
            )

    def start_backtesting(self):
        """Run backtesting for all configured symbols."""
        if getattr(config.training, 'combine_all_symbols', False):
            combined = []
            for symbol in config.symbols:
                try: 
                    data_agg = self.data_aggregator.aggregate_features(
                        self.ticker_data_handler.data[symbol],
                        self.order_data_handler.data[symbol]
                    )
                    data_agg['symbol'] = symbol
                    combined.append(data_agg)
                except Exception as e:
                    logger.error(f"Error in backtesting for {symbol}: {e}")
                    pass
                

            if combined:
                combined_df = pd.concat(combined, ignore_index=False)
                self.custom_model.train(combined_df, 'ALL_SYMBOLS')
        else:
            for symbol in config.symbols:
                data_agg = self.data_aggregator.aggregate_features(
                    self.ticker_data_handler.data[symbol],
                    self.order_data_handler.data[symbol]
                )
                self.custom_model.train(data_agg, symbol)
    
    def _setup_authorization(self):
        """
        Setup authorization for Fyers API.
        """
        try:
            self.fyers_instance = self.generator.initialize_fyers_model()
            # Store access token for Ray actor distribution (avoids pickling logger issues)
            # Get token from generator (stored during initialize_fyers_model)
            self.fyers_access_token = getattr(self.generator, 'access_token', None)
            # Fallback: try to get from fyers_instance attributes
            if not self.fyers_access_token:
                self.fyers_access_token = (
                    getattr(self.fyers_instance, 'token', None) or
                    getattr(self.fyers_instance, 'access_token', None)
                )
            logger.info("Authorization successful.")
        except Exception as e:
            logger.error("Authorization failed")
            raise
    
    def _initialize_company_metadata(self):
        """
        Initialize company metadata cache if enabled in config.
        This will fetch metadata for new symbols and refresh stale entries.
        """
        try:
            # Check if metadata is enabled
            if not getattr(config.metadata, 'enabled', False):
                logger.info("Company metadata is disabled in config")
                return
            
            logger.info("Initializing company metadata cache...")
            
            # Create metadata fetcher
            cache_dir = getattr(config.metadata.cache, 'directory', './data/cache')
            fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
            
            # Update cache with current symbols if auto_update is enabled
            if getattr(config.metadata.fetcher, 'auto_update_on_startup', True):
                logger.info("Auto-updating metadata cache with current symbols...")
                fetcher.update_symbols(config.symbols)
            
            # Refresh stale entries if auto_refresh is enabled
            if getattr(config.metadata.fetcher, 'auto_refresh_stale', True):
                logger.info("Auto-refreshing stale metadata entries...")
                fetcher.refresh_stale_entries()
            
            # Log cache statistics
            stats = fetcher.get_cache_stats()
            logger.info(f"Metadata cache initialized: {stats['total_entries']} entries, "
                       f"{stats['stale_entries']} stale")
            
        except Exception as e:
            logger.warning(f"Failed to initialize company metadata: {e}")
            logger.warning("Continuing without metadata features...")



def main() -> None:
    """
    Main entry point for the AutomatedTrading application.
    
    Supports command-line arguments:
        --sequential: Force sequential (non-distributed) mode
        --distributed: Force distributed (Ray) mode
        --backtest: Run in backtest mode regardless of config
    """
    import argparse
    import signal
    import sys
    
    parser = argparse.ArgumentParser(description="AutomatedTrading Application")
    parser.add_argument(
        "--sequential", 
        action="store_true", 
        help="Force sequential (non-distributed) mode"
    )
    parser.add_argument(
        "--distributed", 
        action="store_true", 
        help="Force distributed (Ray) mode"
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run in backtest mode"
    )
    
    args = parser.parse_args()
    
    # Determine distributed mode
    distributed = None
    if args.sequential:
        distributed = False
    elif args.distributed:
        distributed = True
    
    # Check and start required containers
    try:
        from scripts.start_services import ensure_containers_running
        logger.info("Checking container health...")
        # if not ensure_containers_running():
        #     logger.error("Required containers are not healthy. Please check logs and fix issues.")
        #     logger.error("You can manually start containers with: podman-compose up -d")
        #     sys.exit(1)
    except ImportError as e:
        logger.warning(f"Could not import container check script: {e}")
        logger.warning("Continuing without container health check...")
    except Exception as e:
        logger.warning(f"Container health check failed: {e}")
        logger.warning("Continuing anyway, but services may not be available...")
    
    app: Optional[MarketAnalysisApp] = None
    
    def shutdown_handler(signum: int, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        
        if app:
            # Cleanup data handlers
            if hasattr(app, 'ticker_data_handler') and app.ticker_data_handler:
                app.ticker_data_handler.close()
            
            # Shutdown Ray if running
            if app.distributed_mode and RAY_AVAILABLE:
                shutdown_ray_cluster(graceful=True)
        
        logger.info("Shutdown complete")
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    # Health server thread
    health_server_thread = None
    
    try:
        # Initialize application
        app = MarketAnalysisApp(distributed=distributed)
        
        # Start health API server
        health_port = int(os.getenv("HEALTH_PORT", "8080"))
        try:
            from src.api.server import run_health_server
            
            # Create shutdown callback that properly captures app reference
            def health_shutdown_callback():
                """Shutdown callback for health API server."""
                shutdown_handler(signal.SIGTERM, None)
            
            health_server_thread = run_health_server(
                host="0.0.0.0",
                port=health_port,
                app_instance=app,
                shutdown_callback=health_shutdown_callback
            )
            logger.info(f"Health API running at http://0.0.0.0:{health_port}")
        except ImportError:
            logger.warning("Health API server not available (fastapi/uvicorn not installed)")
        except Exception as e:
            logger.warning(f"Failed to start health API server: {e}")
        
        # Send startup notification
        if SLACK_AVAILABLE:
            send_slack_message(
                type="Good",
                message=f"Trading service started - Mode: {app.trading_mode}, Distributed: {app.distributed_mode}"
            )
        
        # Run based on mode
        if app.trading_mode == 'LIVE':
            app.configure_scheduler()
            logger.info("Live trading scheduler started")
        else:
            logger.info("Starting backtesting...")
            app.start_backtesting()
        
        # Keep application running
        logger.info("Application running. Press Ctrl+C to stop.")
        while True:
            time.sleep(5)
            
            # Periodic health check in distributed mode
            if app.distributed_mode and RAY_AVAILABLE and app._ray_initialized:
                status = get_ray_status()
                if not status.is_healthy:
                    logger.warning(f"Ray cluster health issue: {status.errors}")
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Application error: {e}")
        if SLACK_AVAILABLE:
            send_slack_message(
                type="Bad",
                message=f"Trading service error: {str(e)}"
            )
        raise
    finally:
        # Cleanup
        if app and app.distributed_mode and RAY_AVAILABLE:
            shutdown_ray_cluster(graceful=True)


if __name__ == "__main__":
    main()
