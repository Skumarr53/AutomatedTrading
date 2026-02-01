# src/distributed/actors/data_ingestor.py
"""
Ray Actor for parallel data ingestion across multiple symbols.

Fetches market data from Fyers API and persists to InfluxDB.
Designed to handle ~20 symbols per actor for optimal parallelism.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
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

# Lazy imports for optional dependencies
try:
    from src.utils.influx_client import (
        DataBucket,
        InfluxDBClient_Wrapper,
        InfluxDBConfig,
        TickerDataPoint,
    )
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False

# Import metrics
try:
    from src.metrics.performance_metrics import get_ingestion_metrics, DataIngestionMetrics
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    get_ingestion_metrics = None
import time


class DataIngestorConfig(BaseActorConfig):
    """Configuration for DataIngestorActor."""
    
    # InfluxDB settings
    influx_url: str = Field(default="http://localhost:8086")
    influx_token: str = Field(default="")
    influx_org: str = Field(default="trading")
    
    # Data fetching settings
    fetch_interval_minutes: int = Field(default=5, ge=1, le=60)
    batch_size: int = Field(default=1000, ge=100, le=10000)
    api_rate_limit_per_second: float = Field(default=10.0, ge=1.0)
    
    # CSV backup (optional, InfluxDB is primary)
    csv_backup_enabled: bool = Field(default=False, description="Enable CSV backup (optional, InfluxDB is primary)")
    csv_backup_path: str = Field(default="backups/TickerData")


def create_data_ingestor_actor(config: DataIngestorConfig):
    """
    Factory function to create a DataIngestorActor.
    
    Creates a Ray actor if Ray is available, otherwise returns a regular instance.
    
    Args:
        config: Actor configuration
        
    Returns:
        DataIngestorActor instance (Ray actor handle or regular object)
    """
    if RAY_AVAILABLE and ray.is_initialized():
        # Wrap class with ray.remote() at call time, then configure options
        return ray.remote(DataIngestorActor).options(
            name=f"data_ingestor_{config.actor_id}",
            lifetime="detached",
            max_restarts=3,
        ).remote(config)
    else:
        # Create as regular object for testing/non-distributed mode
        return DataIngestorActor(config)


class DataIngestorActor(BaseActor):
    """
    Ray Actor for fetching and storing market data.
    
    Responsibilities:
    - Fetch ticker data from Fyers API for assigned symbols
    - Persist data to InfluxDB (primary) and CSV (backup)
    - Handle rate limiting and API errors
    - Report health metrics
    
    Usage:
        config = DataIngestorConfig(actor_id="ingestor_1", influx_token="...")
        actor = create_data_ingestor_actor(config)
        
        # Assign symbols
        ray.get(actor.assign_symbols.remote(["RELIANCE", "TCS", "INFY"]))
        
        # Fetch data
        result = ray.get(actor.fetch_all_symbols.remote())
    """
    
    def __init__(self, config: DataIngestorConfig) -> None:
        """Initialize DataIngestorActor."""
        super().__init__(config)
        self._ingestor_config = config
        self._influx_client: Optional[InfluxDBClient_Wrapper] = None
        self._fyers_instance = None
        self._data_cache: dict[str, pd.DataFrame] = {}
        self._last_fetch_time: dict[str, datetime] = {}
        
        # Initialize metrics
        self._metrics: Optional[DataIngestionMetrics] = None
        if METRICS_AVAILABLE and get_ingestion_metrics:
            self._metrics = get_ingestion_metrics()
        
    async def _initialize(self) -> None:
        """Initialize InfluxDB connection and Fyers client."""
        # Initialize InfluxDB if available
        if INFLUX_AVAILABLE and self._ingestor_config.influx_token:
            try:
                influx_config = InfluxDBConfig(
                    url=self._ingestor_config.influx_url,
                    token=self._ingestor_config.influx_token,
                    org=self._ingestor_config.influx_org,
                    batch_size=self._ingestor_config.batch_size,
                )
                self._influx_client = InfluxDBClient_Wrapper(influx_config)
                await self._influx_client.connect()
                logger.info(f"Actor {self._actor_id}: InfluxDB connected")
                
                # Update active connections metric
                if self._metrics:
                    self._metrics.update_active_connections('influxdb', 1)
            except Exception as e:
                logger.warning(f"Actor {self._actor_id}: InfluxDB connection failed: {e}")
                
                # Update active connections metric (0 connections)
                if self._metrics:
                    self._metrics.update_active_connections('influxdb', 0)
        
        # Create CSV backup directory
        if self._ingestor_config.csv_backup_enabled:
            os.makedirs(self._ingestor_config.csv_backup_path, exist_ok=True)
        
        logger.info(f"DataIngestorActor {self._actor_id} initialized")
    
    async def _process(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Main processing - fetch data for all assigned symbols.
        
        Returns:
            Dictionary with fetch results per symbol
        """
        return await self.fetch_all_symbols()
    
    async def _cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        # Flush any pending data
        if self._influx_client:
            await self._influx_client.close()
            logger.info(f"Actor {self._actor_id}: InfluxDB connection closed")
            
            # Update active connections metric
            if self._metrics:
                self._metrics.update_active_connections('influxdb', 0)
        
        # Save cached data to CSV
        if self._ingestor_config.csv_backup_enabled:
            for symbol, df in self._data_cache.items():
                if not df.empty:
                    self._save_to_csv(symbol, df)
    
    def set_fyers_instance(self, fyers_instance: Any) -> None:
        """
        Set the Fyers API instance for data fetching.
        
        Args:
            fyers_instance: Initialized Fyers API client
        """
        self._fyers_instance = fyers_instance
        logger.info(f"Actor {self._actor_id}: Fyers instance set")
    
    def set_fyers_credentials(self, client_id: str, access_token: Optional[str] = None) -> None:
        """
        Set Fyers credentials and recreate instance in actor to avoid pickling issues.
        
        This method recreates the FyersModel instance inside the actor to avoid
        pickling errors caused by logger objects in the original instance.
        
        Args:
            client_id: Fyers API client ID
            access_token: Access token (if None, actor will need to handle auth separately)
        """
        
        if access_token:
            try:
                from fyers_apiv3 import fyersModel
                import os
                # Recreate FyersModel in actor without logger issues
                # Use /tmp for log_path to avoid permission issues
                self._fyers_instance = fyersModel.FyersModel(
                    client_id=client_id,
                    is_async=False,
                    token=access_token,
                    log_path="/tmp"  # Use /tmp instead of os.getcwd() to avoid logger pickling
                )
                # #region agent log
                with open(debug_log_path, "a") as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"data_ingestor.py:set_fyers_credentials","message":"FyersModel recreated successfully","data":{"fyers_type":str(type(self._fyers_instance))},"timestamp":int(datetime.now().timestamp()*1000)})+"\n")
                # #endregion
                logger.info(f"Actor {self._actor_id}: Fyers instance recreated from credentials")
            except Exception as e:
                # #region agent log
                with open(debug_log_path, "a") as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"data_ingestor.py:set_fyers_credentials","message":"Failed to recreate FyersModel","data":{"error":str(e)},"timestamp":int(datetime.now().timestamp()*1000)})+"\n")
                # #endregion
                logger.error(f"Actor {self._actor_id}: Failed to recreate Fyers instance: {e}")
                raise
        else:
            logger.warning(f"Actor {self._actor_id}: No access token provided, Fyers instance not set")
    
    async def fetch_all_symbols(self) -> dict[str, Any]:
        """
        Fetch data for all assigned symbols.
        
        Returns:
            Dictionary with results per symbol:
            {
                "symbol": {
                    "success": bool,
                    "records": int,
                    "error": Optional[str]
                }
            }
        """
        results: dict[str, Any] = {}
        
        if not self._fyers_instance:
            logger.error(f"Actor {self._actor_id}: Fyers instance not set")
            return {"error": "Fyers instance not set"}
        
        # Update queue size metric
        if self._metrics:
            self._metrics.update_queue_size(self._actor_id, len(self._assigned_symbols))
        
        # Rate limiting: spread requests over time
        delay_between_requests = 1.0 / self._ingestor_config.api_rate_limit_per_second
        
        for symbol in self._assigned_symbols:
            try:
                start_time = time.time()
                result = await self._fetch_symbol_data(symbol)
                latency = time.time() - start_time
                results[symbol] = result
                
                # Record metrics
                if self._metrics:
                    if result.get("success", False):
                        self._metrics.record_ingestion_success(
                            symbol=symbol,
                            actor_id=self._actor_id,
                            records=result.get("records", 0),
                            latency_seconds=latency
                        )
                    else:
                        error_type = result.get("error", "unknown")[:50]  # Truncate error
                        self._metrics.record_ingestion_failure(
                            symbol=symbol,
                            actor_id=self._actor_id,
                            error_type=error_type
                        )
                
                # Rate limiting delay
                await asyncio.sleep(delay_between_requests)
                
            except Exception as e:
                logger.error(f"Actor {self._actor_id}: Error fetching {symbol}: {e}")
                results[symbol] = {"success": False, "records": 0, "error": str(e)}
                
                # Record failure metric
                if self._metrics:
                    self._metrics.record_ingestion_failure(
                        symbol=symbol,
                        actor_id=self._actor_id,
                        error_type=type(e).__name__
                    )
        
        # Update queue size to 0 (done)
        if self._metrics:
            self._metrics.update_queue_size(self._actor_id, 0)
        
        successful = sum(1 for r in results.values() if r.get("success", False))
        logger.info(
            f"Actor {self._actor_id}: Fetched {successful}/{len(self._assigned_symbols)} symbols"
        )
        
        return results
    
    async def _fetch_symbol_data(self, symbol: str) -> dict[str, Any]:
        """
        Fetch data for a single symbol.
        
        Args:
            symbol: Stock symbol to fetch
            
        Returns:
            Result dictionary with success status and record count
        """
        from src.utils.utils import get_NSE_symbol
        from src import config
        
        try:
            # Determine time range
            now = datetime.now()
            last_fetch = self._last_fetch_time.get(symbol)
            
            if last_fetch:
                start_time = last_fetch.timestamp()
            else:
                # Fetch last 24 hours for initial load
                start_time = (now - timedelta(hours=24)).timestamp()
            
            end_time = now.timestamp()
            
            # Build API payload
            fyers_symbol = get_NSE_symbol(symbol)
            payload = {
                "symbol": fyers_symbol,
                "resolution": str(self._ingestor_config.fetch_interval_minutes),
                "date_format": "0",
                "range_from": str(int(start_time)),
                "range_to": str(int(end_time)),
                "cont_flag": "1",
            }
            
            # Execute API call (blocking, run in executor)
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, lambda: self._fyers_instance.history(payload)
            )
            
            if not response or response.get("code") != 200:
                error_msg = response.get("message", "Unknown API error") if response else "No response"
                return {"success": False, "records": 0, "error": error_msg}
            
            # Parse candle data
            candles = response.get("candles", [])
            if not candles:
                return {"success": True, "records": 0}
            
            # Convert to DataFrame
            df = pd.DataFrame(candles, columns=["epoch_time", "open", "high", "low", "close", "volume"])
            df["date"] = pd.to_datetime(df["epoch_time"], unit="s")
            df["symbol"] = symbol
            
            # Update cache
            if symbol in self._data_cache:
                self._data_cache[symbol] = pd.concat([self._data_cache[symbol], df]).drop_duplicates(
                    subset=["epoch_time"]
                ).reset_index(drop=True)
            else:
                self._data_cache[symbol] = df
            
            self._last_fetch_time[symbol] = now
            
            # Persist to storage
            await self._persist_symbol_data(symbol, df)
            
            return {"success": True, "records": len(df)}
            
        except Exception as e:
            logger.error(f"Actor {self._actor_id}: Fetch error for {symbol}: {e}")
            return {"success": False, "records": 0, "error": str(e)}
    
    async def _persist_symbol_data(self, symbol: str, df: pd.DataFrame) -> None:
        """Persist symbol data to configured storage backends."""
        if df.empty:
            return
        
        # Write to InfluxDB
        influx_success = False
        if self._influx_client:
            try:
                data_points = self._df_to_ticker_points(symbol, df)
                if data_points:
                    write_start = time.time()
                    await self._influx_client.write_ticker_batch(data_points)
                    influx_success = True
                    
                    # Update data freshness metric
                    if self._metrics and 'date' in df.columns:
                        try:
                            latest_time = df['date'].max()
                            if hasattr(latest_time, 'to_pydatetime'):
                                latest_time = latest_time.to_pydatetime()
                            age_seconds = (datetime.now() - latest_time).total_seconds()
                            self._metrics.update_data_freshness(symbol, 'ticker', age_seconds)
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"Actor {self._actor_id}: InfluxDB write failed for {symbol}: {e}")
                
                # Record failure metric
                if self._metrics:
                    self._metrics.record_ingestion_failure(
                        symbol=symbol,
                        actor_id=self._actor_id,
                        error_type=f"influxdb_write:{type(e).__name__}"
                    )
        
        # Write to CSV backup
        if self._ingestor_config.csv_backup_enabled:
            self._save_to_csv(symbol, df)
    
    def _df_to_ticker_points(self, symbol: str, df: pd.DataFrame) -> list:
        """Convert DataFrame to TickerDataPoint objects."""
        if not INFLUX_AVAILABLE:
            return []
        
        data_points = []
        for _, row in df.iterrows():
            try:
                timestamp = pd.to_datetime(row.get("date") or row.get("epoch_time"), unit="s" if "epoch_time" in row else None)
                data_points.append(TickerDataPoint(
                    symbol=symbol,
                    timestamp=timestamp.to_pydatetime(),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(row["volume"]),
                    exchange="NSE"
                ))
            except (ValueError, KeyError):
                continue
        
        return data_points
    
    def _save_to_csv(self, symbol: str, df: pd.DataFrame) -> None:
        """Save DataFrame to CSV file."""
        try:
            file_path = os.path.join(
                self._ingestor_config.csv_backup_path,
                f"{symbol}_ticker_data.csv"
            )
            
            # Append to existing or create new
            if os.path.exists(file_path):
                existing = pd.read_csv(file_path, on_bad_lines="skip")
                combined = pd.concat([existing, df]).drop_duplicates(subset=["epoch_time"])
                combined.to_csv(file_path, index=False)
            else:
                df.to_csv(file_path, index=False)
                
        except Exception as e:
            logger.error(f"Actor {self._actor_id}: CSV save error for {symbol}: {e}")
    
    def get_cached_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get cached data for a symbol."""
        return self._data_cache.get(symbol)
    
    def get_all_cached_data(self) -> dict[str, pd.DataFrame]:
        """Get all cached data."""
        return self._data_cache.copy()


# Keep original class for non-Ray usage
# The factory function create_data_ingestor_actor handles Ray wrapping
# DO NOT apply ray.remote() here - it causes double-wrapping errors

# Debug & Verify
# ==============
# Run: python -c "from src.distributed.actors.data_ingestor import DataIngestorConfig; print(DataIngestorConfig(actor_id='test').model_dump())"
# Verify: No import errors
