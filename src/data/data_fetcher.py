# src/data/data_fetcher.py
"""
Data fetching and storage module with InfluxDB support.

Handles fetching market data from Fyers API and persisting to InfluxDB
with CSV fallback for reliability.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, time as _time, timedelta
from enum import Enum
from typing import Callable, Optional

import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from fyers_apiv3 import fyersModel
from loguru import logger
from pydantic import BaseModel, Field
from tqdm import tqdm

from src import config
from src.utils.utils import get_NSE_symbol, get_timezone

# Lazy import for InfluxDB to handle missing dependency gracefully
try:
    from src.utils.influx_client import (
        DataBucket,
        InfluxDBClient_Wrapper,
        InfluxDBConfig,
        TickerDataPoint,
        create_influx_config_from_hydra,
    )
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    logger.warning("InfluxDB client not available, using CSV-only mode")


class StorageBackend(str, Enum):
    """Available storage backends for data persistence."""
    INFLUXDB = "influxdb"
    CSV = "csv"
    HYBRID = "hybrid"  # InfluxDB primary, CSV backup


class DataHandlerConfig(BaseModel):
    """Configuration for DataHandler."""
    storage_backend: StorageBackend = Field(default=StorageBackend.INFLUXDB)
    csv_backup_enabled: bool = Field(default=False, description="Enable CSV backup (optional, not for loading)")
    batch_write_size: int = Field(default=1000, ge=100, le=10000)
    max_api_retries: int = Field(default=3, ge=1)
    api_retry_delay_seconds: int = Field(default=10, ge=1)


class DataHandler:
    """
    Handles fetching, loading, updating, and backing up trading data for automated trading.
    
    Supports multiple storage backends:
    - InfluxDB: High-performance time-series storage (primary for production)
    - CSV: File-based storage (fallback/backup)
    - Hybrid: InfluxDB primary with CSV backup
    
    Attributes:
        trading_mode: Mode of trading, either 'BACKTEST' or 'LIVE'.
        fyers: Instance of the Fyers API client.
        file_path: Path to the directory where ticker CSV data is stored.
        data: Dictionary storing data for each symbol (in-memory cache).
        data_len: Duration of data to maintain, in seconds.
        callback: Callback function to execute after loading data.
        scheduler: Scheduler instance for managing jobs.
        influx_client: InfluxDB client wrapper (if available).
        storage_backend: Current storage backend configuration.
    """
    
    def __init__(
        self, 
        fyers_instance: fyersModel, 
        scheduler: Optional[BackgroundScheduler],
        handler_config: Optional[DataHandlerConfig] = None,
    ) -> None:
        """
        Initialize the DataHandler with Fyers instance and storage configuration.

        Args:
            fyers_instance: An instance of the Fyers API client.
            scheduler: Scheduler instance for managing jobs (None for backtest mode).
            handler_config: Optional configuration for storage backend.
        """
        self.trading_mode: str = config.trading_config.trade_mode
        self._fyers_instance = fyers_instance
        self.file_path: str = config.paths.ticker_filename
        self.data: dict[str, pd.DataFrame] = {symbol: pd.DataFrame() for symbol in config.symbols}
        self.data_len: int = config.backtest_data_load.backtest_data_length_years * 12 * 30 * 24 * 60 * 60
        self.callback: Optional[Callable[[dict[str, pd.DataFrame]], None]] = None
        self.scheduler = scheduler
        
        # Storage configuration
        self._config = handler_config or DataHandlerConfig()
        self._influx_client: Optional[InfluxDBClient_Wrapper] = None
        self._influx_connected = False
        
        # Initialize InfluxDB if available and enabled
        self._setup_storage_backend()
        
        # Load data for all symbols
        for symbol in tqdm(config.symbols, desc="Loading symbol data"):
            self.load_or_initialize_data(symbol)

        if self.trading_mode == "BACKTEST":
            self.load_historical_data()
        elif self.trading_mode == "LIVE":
            self.configure_scheduler()
    
    def _setup_storage_backend(self) -> None:
        """Initialize storage backend based on configuration."""
        if not INFLUX_AVAILABLE:
            logger.info("InfluxDB not available, using CSV storage")
            self._config.storage_backend = StorageBackend.CSV
            return
            
        # Check if InfluxDB is enabled in config
        influx_enabled = getattr(config, 'influxdb', {}).get('enabled', False)
        if not influx_enabled:
            logger.info("InfluxDB disabled in config, using CSV storage")
            self._config.storage_backend = StorageBackend.CSV
            return
            
        try:
            # Create InfluxDB config from Hydra config
            influx_config = create_influx_config_from_hydra(config)
            self._influx_client = InfluxDBClient_Wrapper(influx_config)
            
            # Try to connect (synchronously for initialization)
            loop = asyncio.new_event_loop()
            self._influx_connected = loop.run_until_complete(self._influx_client.connect())
            loop.close()
            
            if self._influx_connected:
                logger.info("InfluxDB connected successfully, using hybrid storage")
                self._config.storage_backend = StorageBackend.HYBRID
            else:
                logger.warning("InfluxDB connection failed, falling back to CSV")
                self._config.storage_backend = StorageBackend.CSV
                
        except Exception as e:
            logger.warning(f"InfluxDB setup failed: {e}, using CSV storage")
            self._config.storage_backend = StorageBackend.CSV
    
    @property
    def storage_backend(self) -> StorageBackend:
        """Get current storage backend."""
        return self._config.storage_backend
    
    @property 
    def is_influx_connected(self) -> bool:
        """Check if InfluxDB is connected."""
        return self._influx_connected
    
    # Legacy property for backward compatibility
    @property
    def fyres(self) -> fyersModel:
        """Backward compatibility alias for fyers."""
        return self._fyers_instance
    
    @fyres.setter
    def fyres(self, value: fyersModel) -> None:
        """Backward compatibility setter for fyers."""
        self._fyers_instance = value

    def register_callback(self, callback: Callable[[Dict[str, pd.DataFrame]], None]) -> None:
        """
        Registers a callback function to be called after loading historical data.

        Args:
            callback (Callable[[Dict[str, pd.DataFrame]], None]): 
                A function that takes the data dictionary as input.
        """
        self.callback = callback

    def load_historical_data(self) -> None:
        """
        Loads historical data and invokes the registered callback if available.
        """
        if self.callback:
            self.callback(self.data)

    def get_scheduler(self) -> 'BackgroundScheduler':
        """
        Retrieves the scheduler instance.

        Returns:
            Scheduler: The scheduler instance used for managing jobs.
        """
        return self.scheduler

    def configure_scheduler(self) -> None:
        """
        Configures the scheduler by adding necessary jobs, such as data backup.
        """
        # Adding jobs to the scheduler
        self.scheduler.add_job(
            self.backup_data,
            'interval',
            hours=config.scheduler.backup_interval_hours,
            id='backup_data_job'
        )
        # self.schedule_data_updates()

    def load_or_initialize_data(self, symbol: str) -> pd.DataFrame:
        """
        Load existing data for a symbol from storage or initialize by fetching.
        
        Primary source: InfluxDB (if available and configured)
        Fallback: Fetch from API if InfluxDB unavailable or empty
        CSV: Only used as optional backup for writes, not for loading

        Args:
            symbol: The trading symbol to load data for.

        Returns:
            DataFrame with loaded or fetched data for the symbol.
        """
        df = pd.DataFrame()
        data_loaded_from_influxdb = False
        
        # Primary: Load from InfluxDB if connected and configured
        if self._influx_connected and self._influx_client:
            if self._config.storage_backend in (StorageBackend.INFLUXDB, StorageBackend.HYBRID):
                df = self._load_from_influxdb(symbol)
                if not df.empty:
                    logger.info(f"Loaded {len(df)} records from InfluxDB for {symbol}")
                    data_loaded_from_influxdb = True
                else:
                    logger.debug(f"No data found in InfluxDB for {symbol}")
        
        # Fallback: Fetch from API if InfluxDB unavailable or empty
        if df.empty:
            logger.info(f"No existing data in InfluxDB for {symbol}, fetching from API")
            df = self.fetch_full_year_data(symbol)
            data_loaded_from_influxdb = False  # New data fetched, needs persistence
        
        # Track size before update to detect if new records were added
        size_before = len(df)
        
        # Update in-memory cache (may fetch missing records)
        self.update_data(symbol, df)
        
        # Check if update_data added new records
        size_after = len(self.data[symbol])
        new_records_added = size_after > size_before
        
        # Only persist if:
        # 1. Data was fetched from API (new data), OR
        # 2. update_data() added new records (missing data fetched)
        should_persist = not data_loaded_from_influxdb or new_records_added
        
        if should_persist:
            # Persist to storage backends (InfluxDB primary, CSV optional backup)
            if new_records_added:
                logger.debug(f"Persisting {symbol}: {size_after - size_before} new records added")
            self._persist_data(symbol, self.data[symbol])
        else:
            logger.debug(f"Skipping persistence for {symbol} - data already in InfluxDB and up-to-date")
        
        return self.data[symbol]
    
    def _load_from_influxdb(self, symbol: str) -> pd.DataFrame:
        """
        Load ticker data from InfluxDB.
        
        Args:
            symbol: Stock symbol to load
            
        Returns:
            DataFrame with ticker data or empty DataFrame on failure
        """
        if not self._influx_client or not self._influx_connected:
            return pd.DataFrame()
            
        try:
            # Calculate hours to query based on data_len
            hours = int(self.data_len / 3600)
            
            loop = asyncio.new_event_loop()
            df = loop.run_until_complete(
                self._influx_client.query_ticker_data(symbol, hours=hours)
            )
            loop.close()
            
            if not df.empty:
                # Convert to expected format
                df = df.rename(columns={"timestamp": "date"})
                if "date" in df.columns:
                    df["epoch_time"] = df["date"].astype("int64") // 10**9
                    
            return df
            
        except Exception as e:
            logger.warning(f"Failed to load from InfluxDB for {symbol}: {e}")
            return pd.DataFrame()
    
    def _load_from_csv(self, symbol: str) -> pd.DataFrame:
        """
        Load ticker data from CSV file.
        
        Args:
            symbol: Stock symbol to load
            
        Returns:
            DataFrame with ticker data or empty DataFrame if file not found
        """
        symbol_file = os.path.join(
            self.file_path, f"{symbol}_{config.backtest_data_load.ticker_file_suffix}.csv"
        )
        
        if not os.path.exists(symbol_file):
            return pd.DataFrame()
            
        try:
            df = pd.read_csv(symbol_file, on_bad_lines="skip", engine="python")
            logger.debug(f"Loaded {len(df)} records from CSV for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV data for {symbol}: {e}")
            return pd.DataFrame()
    
    def _persist_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Persist data to configured storage backends.
        
        Args:
            symbol: Stock symbol
            df: DataFrame to persist
        """
        if df.empty:
            return
            
        # Always write to CSV if backup enabled or CSV-only mode
        if self._config.csv_backup_enabled or self._config.storage_backend == StorageBackend.CSV:
            self._save_to_csv(symbol, df)
        
        # Write to InfluxDB if connected
        if self._influx_connected and self._config.storage_backend in (
            StorageBackend.INFLUXDB, StorageBackend.HYBRID
        ):
            self._save_to_influxdb(symbol, df)
    
    def _save_to_csv(self, symbol: str, df: pd.DataFrame) -> None:
        """Save DataFrame to CSV file."""
        try:
            symbol_file = os.path.join(
                self.file_path, f"{symbol}_{config.backtest_data_load.ticker_file_suffix}.csv"
            )
            os.makedirs(os.path.dirname(symbol_file), exist_ok=True)
            df.to_csv(symbol_file, index=False)
            logger.debug(f"Saved {len(df)} records to CSV for {symbol}")
        except Exception as e:
            logger.error(f"Error saving CSV for {symbol}: {e}")
    
    def _save_to_influxdb(self, symbol: str, df: pd.DataFrame) -> None:
        """Save DataFrame to InfluxDB."""
        if not self._influx_client or not self._influx_connected:
            return
            
        try:
            # Convert DataFrame to TickerDataPoint objects
            data_points: list[TickerDataPoint] = []
            
            for _, row in df.iterrows():
                try:
                    # Handle timestamp conversion
                    if "date" in df.columns:
                        timestamp = pd.to_datetime(row["date"])
                    elif "epoch_time" in df.columns:
                        timestamp = pd.to_datetime(row["epoch_time"], unit="s")
                    else:
                        continue
                        
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
            
            if data_points:
                # Write in batches
                loop = asyncio.new_event_loop()
                success_count = 0
                for i in range(0, len(data_points), self._config.batch_write_size):
                    batch = data_points[i:i + self._config.batch_write_size]
                    result = loop.run_until_complete(
                        self._influx_client.write_ticker_batch(batch)
                    )
                    if result:
                        success_count += len(batch)
                loop.close()
                if success_count > 0:
                    logger.info(f"Successfully saved {success_count}/{len(data_points)} records to InfluxDB for {symbol}")
                else:
                    logger.warning(f"Failed to save any records to InfluxDB for {symbol}")
                
        except Exception as e:
            logger.warning(f"Failed to save to InfluxDB for {symbol}: {e}")

    def fetch_full_year_data(self, symbol: str) -> pd.DataFrame:
        """
        Fetches a full year's worth of data for the given symbol.

        Args:
            symbol (str): The trading symbol to fetch data for.

        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        now: float = datetime.now().timestamp()
        initial_time: float = now - self.data_len
        return self.fetch_data(symbol, initial_time, now)

    def update_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Updates the data for a specific symbol by fetching missing data and concatenating it.

        Args:
            symbol (str): The trading symbol to update data for.
            df (pd.DataFrame): Existing DataFrame containing data for the symbol.
        """
        try:
            last_timestamp: float = df['epoch_time'].max() if not df.empty else 0
            now: float = datetime.now().timestamp()
            # TODO: change update_missing_records  condition for updating missing records
            if config.scheduler.update_missing_records:
                if (now - last_timestamp) > self.data_len:
                    df = self.fetch_full_year_data(symbol)
                else:
                    missing_data: pd.DataFrame = self.fetch_data(symbol, last_timestamp, now)
                    df = pd.concat([df, missing_data]).drop_duplicates(
                        subset='epoch_time'
                    ).reset_index(drop=True)

            initial_time: float = now - self.data_len
            self.data[symbol] = df[df['epoch_time'] > initial_time]
        except Exception as e:
            logger.error(f"Error updating data for {symbol}: {e}")
            return None

    def fetch_data(self, symbol: str, start_epoch_time: float, end_epoch_time: float) -> pd.DataFrame:
        """
        Fetches trading data for a given symbol between start and end epoch times.

        Args:
            symbol (str): The trading symbol to fetch data for.
            start_epoch_time (float): The start time in epoch seconds.
            end_epoch_time (float): The end time in epoch seconds.

        Returns:
            pd.DataFrame: DataFrame containing the fetched trading data with columns defined in TICKER_COLS.
        """
        try:
            ONE_DAY_SECONDS: int = 86400
            
            total_data: pd.DataFrame = pd.DataFrame()
            date_col: str = config.columns.common_columns.date
            IST = pytz.timezone(config.scheduler.timezone)

            while start_epoch_time < end_epoch_time:
                attempt: int = 0
                current_time: float = datetime.now(IST).timestamp()
                chunk_end_time: float = min(
                    start_epoch_time + config.scheduler.chunk_size_days * ONE_DAY_SECONDS, end_epoch_time
                )
                inp_payload: Dict[str, str] = {
                    key: value.format(
                        symbol=get_NSE_symbol(symbol),
                        interval=config.scheduler.data_fetch_cron_interval_min,
                        start_epoch_time=int(start_epoch_time),
                        end_epoch_time=int(chunk_end_time)
                    )
                    for key, value in config.base_payload_args.items()
                }

                ## API call to fetch data
                while attempt < config.scheduler.max_api_call_attempts:
                    try:
                        cs_data: Dict = self.fyres.history(inp_payload)
                        df: pd.DataFrame = pd.DataFrame(
                            cs_data['candles'], columns=config.columns.cs_api_data_cols
                        )
                        total_data = pd.concat([total_data, df])
                        # logger.info(
                        #     f"time diff in hours symbol {symbol}: {(current_time - df[config.columns.common_columns.epoch_time].max())//3600}"
                        # )
                        break
                    except Exception as e:
                        if cs_data.get('code') == 429:
                            logger.debug(
                                f"Rate limit exceeded. Waiting {config.scheduler.wait_time_between_api_calls} seconds before retrying..."
                            )
                            time.sleep(config.scheduler.wait_time_between_api_calls)
                            attempt += 1
                        else:
                            logger.error(
                                f"Error fetching data for {symbol}: {e}"
                            )
                            break
                start_epoch_time = chunk_end_time

                total_data[date_col] = pd.to_datetime(
                    total_data[config.columns.common_columns.epoch_time], unit='s'
                )
                total_data[date_col] = total_data[date_col].dt.tz_localize('UTC').dt.tz_convert(config.scheduler.timezone)
                total_data[date_col] = total_data[date_col].dt.tz_localize(None).dt.round('5min')
            return total_data
        except Exception as e:
            return pd.DataFrame({})
        

    def schedule_data_updates(self) -> None:
        """
        Schedule regular data updates during trading hours.
        """
        def delayed_job() -> None:
            """
            Delayed job execution to ensure trading hours alignment.
            """
            # Wait for 5 seconds before executing the actual job
            time.sleep(5)
            self.update_data_regularly()

        # Scheduling the data updates only during the trading hours
        self.scheduler.add_job(
            delayed_job,
            'cron',
            day_of_week=config.scheduler.day_of_week,
            hour=config.scheduler.hour,
            minute=f'*/{config.scheduler.data_fetch_cron_interval_min}',
            timezone=get_timezone(),
            id='update_data_regularly_job'
        )

    def update_data_regularly(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Regularly update the data during trading hours.

        Returns:
            Optional[Dict[str, pd.DataFrame]]: Updated data dictionary if within trading hours, else None.
        """
        try:
            now: datetime = datetime.now(get_timezone())
            logger.debug(f"Attempting data update at {now}")
            if _time(9, 0) <= now.time() <= _time(15, 0):
                for symbol in config.symbols:
                    last_update: float = now.timestamp() - 5 * 60
                    self.update_data(symbol, self.data[symbol])
                return self.data
            else:
                logger.debug("Outside trading hours")
                return None
        except Exception as e:
            logger.error("Error in scheduled data update")
            return None

    def backup_data(self) -> None:
        """
        Back up current trading data to all configured storage backends.
        
        Writes to both CSV (for redundancy) and InfluxDB (for querying).
        """
        now = datetime.now()
        logger.info(f"Starting ticker data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        success_count = 0
        fail_count = 0
        
        for symbol, df in self.data.items():
            try:
                self._persist_data(symbol, df)
                success_count += 1
            except Exception as e:
                logger.error(f"Error backing up data for {symbol}: {e}")
                fail_count += 1
        
        logger.info(
            f"Backup completed: {success_count} success, {fail_count} failed "
            f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    
    async def backup_data_async(self) -> None:
        """
        Async version of backup_data for use with Ray actors.
        
        Writes to InfluxDB asynchronously for better performance.
        """
        now = datetime.now()
        logger.info(f"Starting async ticker data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if not self._influx_client or not self._influx_connected:
            # Fall back to sync backup
            self.backup_data()
            return
        
        tasks = []
        for symbol, df in self.data.items():
            if df.empty:
                continue
                
            # Prepare data points
            data_points = self._df_to_ticker_points(symbol, df)
            if data_points:
                tasks.append(self._influx_client.write_ticker_batch(data_points))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success = sum(1 for r in results if r is True)
            logger.info(f"Async backup completed: {success}/{len(tasks)} batches written")
        
        # Also write CSV backups synchronously
        if self._config.csv_backup_enabled:
            for symbol, df in self.data.items():
                self._save_to_csv(symbol, df)
    
    def _df_to_ticker_points(self, symbol: str, df: pd.DataFrame) -> list:
        """Convert DataFrame to list of TickerDataPoint objects."""
        if not INFLUX_AVAILABLE:
            return []
            
        data_points = []
        for _, row in df.iterrows():
            try:
                if "date" in df.columns:
                    timestamp = pd.to_datetime(row["date"])
                elif "epoch_time" in df.columns:
                    timestamp = pd.to_datetime(row["epoch_time"], unit="s")
                else:
                    continue
                    
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
    
    def close(self) -> None:
        """
        Close connections and clean up resources.
        
        Should be called when shutting down the handler.
        """
        if self._influx_client:
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self._influx_client.close())
                loop.close()
                logger.info("InfluxDB connection closed")
            except Exception as e:
                logger.warning(f"Error closing InfluxDB connection: {e}")
        
        self._influx_connected = False
