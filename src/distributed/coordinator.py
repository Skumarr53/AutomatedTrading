# src/distributed/coordinator.py
"""
Distributed Trading Coordinator

Orchestrates parallel data ingestion, signal generation, and trade execution
across Ray actors. Designed to process 100+ symbols within a 5-minute window.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None

from src import config


@dataclass
class CycleMetrics:
    """Metrics for a single processing cycle."""
    cycle_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    symbols_processed: int = 0
    symbols_failed: int = 0
    signals_generated: int = 0
    trades_executed: int = 0
    data_fetch_duration_ms: float = 0.0
    signal_gen_duration_ms: float = 0.0
    execution_duration_ms: float = 0.0
    total_duration_ms: float = 0.0
    errors: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "cycle_id": self.cycle_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "symbols_processed": self.symbols_processed,
            "symbols_failed": self.symbols_failed,
            "signals_generated": self.signals_generated,
            "trades_executed": self.trades_executed,
            "data_fetch_duration_ms": self.data_fetch_duration_ms,
            "signal_gen_duration_ms": self.signal_gen_duration_ms,
            "execution_duration_ms": self.execution_duration_ms,
            "total_duration_ms": self.total_duration_ms,
            "errors": self.errors,
        }


class CoordinatorConfig(BaseModel):
    """Configuration for the trading coordinator."""
    
    # Timing constraints
    cycle_timeout_seconds: float = Field(default=180.0, ge=30.0, le=300.0)
    target_cycle_duration_seconds: float = Field(default=180.0, ge=60.0)
    
    # Parallelism settings
    max_concurrent_fetches: int = Field(default=20, ge=1, le=100)
    max_concurrent_signals: int = Field(default=10, ge=1, le=50)
    
    # Retry settings
    max_retries: int = Field(default=2, ge=0, le=5)
    retry_delay_seconds: float = Field(default=1.0, ge=0.1)
    
    # Feature flags
    enable_trade_execution: bool = Field(default=True)
    dry_run_mode: bool = Field(default=False)


class TradingCoordinator:
    """
    Coordinates distributed trading operations across Ray actors.
    
    Responsibilities:
    - Distribute symbols across data ingestor actors
    - Collect and aggregate fetched data
    - Distribute data to signal generator actors
    - Collect signals and route to trade executor
    - Monitor cycle timing and health
    
    Usage:
        coordinator = TradingCoordinator(config, data_actors, signal_actors, executor)
        
        # Run single cycle
        metrics = await coordinator.run_cycle()
        
        # Run continuous trading
        await coordinator.start_continuous_trading()
    """
    
    def __init__(
        self,
        coord_config: CoordinatorConfig,
        data_ingestor_actors: list,
        signal_generator_actors: list,
        trade_executor_actor: Optional[Any] = None,
        fyers_instance: Optional[Any] = None,
    ) -> None:
        """
        Initialize the trading coordinator.
        
        Args:
            coord_config: Coordinator configuration
            data_ingestor_actors: List of Ray actor handles for data ingestion
            signal_generator_actors: List of Ray actor handles for signal generation
            trade_executor_actor: Optional Ray actor handle for trade execution
            fyers_instance: Fyers API instance for passing to actors
        """
        self._config = coord_config
        self._data_actors = data_ingestor_actors
        self._signal_actors = signal_generator_actors
        self._executor_actor = trade_executor_actor
        self._fyers = fyers_instance
        
        self._cycle_count = 0
        self._last_cycle_metrics: Optional[CycleMetrics] = None
        self._is_running = False
        self._shutdown_requested = False
        
        # Data cache for current cycle
        self._ticker_data: dict[str, pd.DataFrame] = {}
        self._orderbook_data: dict[str, pd.DataFrame] = {}
        
        logger.info(
            f"TradingCoordinator initialized with {len(data_ingestor_actors)} data actors, "
            f"{len(signal_generator_actors)} signal actors"
        )
    
    @property
    def is_running(self) -> bool:
        """Check if coordinator is running."""
        return self._is_running
    
    @property
    def last_metrics(self) -> Optional[CycleMetrics]:
        """Get last cycle metrics."""
        return self._last_cycle_metrics
    
    async def run_cycle(self) -> CycleMetrics:
        """
        Execute a single trading cycle.
        
        Returns:
            CycleMetrics with cycle statistics
        """
        self._cycle_count += 1
        cycle_id = f"cycle_{self._cycle_count}_{int(time.time())}"
        metrics = CycleMetrics(cycle_id=cycle_id, start_time=datetime.now())
        
        cycle_start = time.perf_counter()
        
        try:
            # Phase 1: Parallel data fetching
            fetch_start = time.perf_counter()
            fetch_results = await self._fetch_all_data()
            metrics.data_fetch_duration_ms = (time.perf_counter() - fetch_start) * 1000
            
            # Count successful fetches
            for actor_result in fetch_results:
                if isinstance(actor_result, dict):
                    for symbol, result in actor_result.items():
                        if isinstance(result, dict) and result.get("success"):
                            metrics.symbols_processed += 1
                        else:
                            metrics.symbols_failed += 1
            
            # Phase 2: Parallel signal generation
            signal_start = time.perf_counter()
            signals = await self._generate_all_signals()
            metrics.signal_gen_duration_ms = (time.perf_counter() - signal_start) * 1000
            metrics.signals_generated = len(signals)
            
            # Phase 3: Trade execution (if enabled)
            if self._config.enable_trade_execution and signals:
                exec_start = time.perf_counter()
                trades = await self._execute_trades(signals)
                metrics.execution_duration_ms = (time.perf_counter() - exec_start) * 1000
                metrics.trades_executed = trades
            
        except asyncio.TimeoutError:
            metrics.errors.append(f"Cycle timeout after {self._config.cycle_timeout_seconds}s")
            logger.error(f"Cycle {cycle_id} timed out")
        except Exception as e:
            metrics.errors.append(f"Cycle error: {str(e)}")
            logger.error(f"Cycle {cycle_id} failed: {e}")
        
        metrics.end_time = datetime.now()
        metrics.total_duration_ms = (time.perf_counter() - cycle_start) * 1000
        
        self._last_cycle_metrics = metrics
        self._log_cycle_summary(metrics)
        
        return metrics
    
    async def _fetch_all_data(self) -> list[dict[str, Any]]:
        """
        Fetch data from all data ingestor actors in parallel.
        
        Returns:
            List of results from each actor
        """
        if not RAY_AVAILABLE or not self._data_actors:
            logger.warning("No data actors available, skipping fetch")
            return []
        
        try:
            # Trigger fetch on all actors in parallel
            futures = [
                actor.fetch_all_symbols.remote()
                for actor in self._data_actors
            ]
            
            # Wait for all with timeout
            results = await asyncio.wait_for(
                asyncio.gather(*[self._ray_get_async(f) for f in futures]),
                timeout=self._config.cycle_timeout_seconds * 0.5  # Use half timeout for fetch
            )
            
            # Collect data from actors
            await self._collect_cached_data()
            
            return list(results)
            
        except asyncio.TimeoutError:
            logger.error("Data fetch timed out")
            return []
        except Exception as e:
            logger.error(f"Data fetch failed: {e}")
            return []
    
    async def _collect_cached_data(self) -> None:
        """Collect cached data from all data ingestor actors."""
        self._ticker_data.clear()
        
        if not RAY_AVAILABLE or not self._data_actors:
            return
        
        try:
            # Get cached data from each actor
            futures = [
                actor.get_all_cached_data.remote()
                for actor in self._data_actors
            ]
            
            results = await asyncio.gather(
                *[self._ray_get_async(f) for f in futures],
                return_exceptions=True
            )
            
            # Merge results
            for result in results:
                if isinstance(result, dict):
                    self._ticker_data.update(result)
                    
            logger.debug(f"Collected data for {len(self._ticker_data)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to collect cached data: {e}")
    
    async def _generate_all_signals(self) -> list:
        """
        Generate signals from all signal generator actors in parallel.
        
        Returns:
            List of TradingSignal objects
        """
        if not RAY_AVAILABLE or not self._signal_actors:
            logger.warning("No signal actors available, skipping signal generation")
            return []
        
        if not self._ticker_data:
            logger.warning("No ticker data available for signal generation")
            return []
        
        try:
            # Distribute data to signal actors
            futures = [
                actor.process.remote(
                    ticker_data=self._ticker_data,
                    orderbook_data=self._orderbook_data
                )
                for actor in self._signal_actors
            ]
            
            # Wait for all with timeout
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[self._ray_get_async(f) for f in futures],
                    return_exceptions=True
                ),
                timeout=self._config.cycle_timeout_seconds * 0.4
            )
            
            # Flatten signals from all actors
            all_signals = []
            for result in results:
                if isinstance(result, list):
                    all_signals.extend(result)
                elif isinstance(result, Exception):
                    logger.warning(f"Signal generation error: {result}")
            
            logger.info(f"Generated {len(all_signals)} signals from {len(self._signal_actors)} actors")
            return all_signals
            
        except asyncio.TimeoutError:
            logger.error("Signal generation timed out")
            return []
        except Exception as e:
            logger.error(f"Signal generation failed: {e}")
            return []
    
    async def _execute_trades(self, signals: list) -> int:
        """
        Execute trades based on generated signals.
        
        Args:
            signals: List of TradingSignal objects
            
        Returns:
            Number of trades executed
        """
        if self._config.dry_run_mode:
            logger.info(f"[DRY RUN] Would execute {len(signals)} trades")
            return 0
        
        if not self._executor_actor:
            logger.warning("No executor actor available")
            return 0
        
        # Filter actionable signals (BUY/SELL, not HOLD)
        actionable = [s for s in signals if hasattr(s, 'signal') and s.signal in ('BUY', 'SELL')]
        
        if not actionable:
            logger.debug("No actionable signals to execute")
            return 0
        
        try:
            # Execute through executor actor
            result = await self._ray_get_async(
                self._executor_actor.execute_signals.remote(actionable)
            )
            return result if isinstance(result, int) else 0
            
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            return 0
    
    async def _ray_get_async(self, future) -> Any:
        """Async wrapper for ray.get()."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: ray.get(future))
    
    def _log_cycle_summary(self, metrics: CycleMetrics) -> None:
        """Log cycle summary."""
        status = "OK" if not metrics.errors else "ERROR"
        logger.info(
            f"Cycle {metrics.cycle_id} [{status}]: "
            f"Processed={metrics.symbols_processed}, "
            f"Failed={metrics.symbols_failed}, "
            f"Signals={metrics.signals_generated}, "
            f"Trades={metrics.trades_executed}, "
            f"Duration={metrics.total_duration_ms:.0f}ms "
            f"(fetch={metrics.data_fetch_duration_ms:.0f}ms, "
            f"signal={metrics.signal_gen_duration_ms:.0f}ms, "
            f"exec={metrics.execution_duration_ms:.0f}ms)"
        )
        
        if metrics.errors:
            for error in metrics.errors:
                logger.error(f"  Error: {error}")
    
    async def start_continuous_trading(self, interval_seconds: float = 300.0) -> None:
        """
        Start continuous trading loop.
        
        Args:
            interval_seconds: Time between cycles (default: 5 minutes)
        """
        self._is_running = True
        self._shutdown_requested = False
        
        logger.info(f"Starting continuous trading with {interval_seconds}s interval")
        
        while not self._shutdown_requested:
            cycle_start = time.time()
            
            try:
                metrics = await self.run_cycle()
                
                # Calculate sleep time to maintain interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, interval_seconds - elapsed)
                
                if sleep_time > 0:
                    logger.debug(f"Sleeping {sleep_time:.1f}s until next cycle")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(
                        f"Cycle took {elapsed:.1f}s, exceeding interval of {interval_seconds}s"
                    )
                    
            except asyncio.CancelledError:
                logger.info("Trading loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in trading loop: {e}")
                await asyncio.sleep(10)  # Brief pause before retry
        
        self._is_running = False
        logger.info("Continuous trading stopped")
    
    def request_shutdown(self) -> None:
        """Request graceful shutdown of trading loop."""
        logger.info("Shutdown requested")
        self._shutdown_requested = True
    
    def get_status(self) -> dict[str, Any]:
        """Get coordinator status."""
        return {
            "is_running": self._is_running,
            "cycle_count": self._cycle_count,
            "data_actors": len(self._data_actors),
            "signal_actors": len(self._signal_actors),
            "has_executor": self._executor_actor is not None,
            "cached_symbols": len(self._ticker_data),
            "last_cycle": self._last_cycle_metrics.to_dict() if self._last_cycle_metrics else None,
        }


# Debug & Verify
# ==============
# Run: python -c "from src.distributed.coordinator import TradingCoordinator, CoordinatorConfig; print(CoordinatorConfig().model_dump())"
# Verify: No import errors
