# src/utils/influx_client.py
"""
Async InfluxDB client wrapper for high-performance time-series data operations.
Optimized for batch writes and concurrent reads across 100+ symbols.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import AsyncGenerator, Optional

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from loguru import logger
from pydantic import BaseModel, Field, field_validator


class DataBucket(str, Enum):
    """Available InfluxDB buckets for different data types."""
    TICKER_DATA = "ticker_data"
    ORDER_BOOK = "order_book"
    TRADES = "trades"
    METRICS = "system_metrics"


class InfluxDBConfig(BaseModel):
    """Configuration for InfluxDB connection."""
    url: str = Field(default="http://localhost:8086")
    token: str = Field(..., description="InfluxDB authentication token")
    org: str = Field(default="trading")
    batch_size: int = Field(default=1000, ge=1, le=10000)
    flush_interval_ms: int = Field(default=1000, ge=100)
    retry_interval_ms: int = Field(default=5000, ge=1000)
    max_retries: int = Field(default=3, ge=1)
    query_timeout_ms: int = Field(default=30000, ge=5000)
    enabled: bool = Field(default=True)
    fallback_to_csv: bool = Field(default=True)

    @field_validator("token")
    @classmethod
    def token_not_empty(cls, v: str) -> str:
        if not v or v.strip() == "":
            raise ValueError("InfluxDB token cannot be empty")
        return v


class TickerDataPoint(BaseModel):
    """Schema for ticker/candle data points."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    exchange: str = Field(default="NSE")


class OrderBookDataPoint(BaseModel):
    """Schema for order book depth data points."""
    symbol: str
    timestamp: datetime
    total_buy_qty: int
    total_sell_qty: int
    last_traded_price: float
    last_traded_qty: int
    volume: int
    average_traded_price: float
    lower_circuit: float
    upper_circuit: float
    change_percent: float
    # Additional fields from CSV
    bids: Optional[list] = None  # List of bid orders
    asks: Optional[list] = None  # List of ask orders
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    tick_size: Optional[float] = None
    change: Optional[float] = None
    expiry: Optional[str] = None
    open_interest: Optional[int] = None
    open_interest_flag: Optional[bool] = None
    previous_day_open_interest: Optional[int] = None
    open_interest_percent: Optional[float] = None


class InfluxDBClient_Wrapper:
    """
    High-performance async wrapper for InfluxDB operations.
    
    Features:
    - Async batch writes for high throughput
    - Connection pooling and retry logic
    - Type-safe data models with Pydantic
    - Graceful degradation to CSV fallback
    
    Usage:
        async with InfluxDBClient_Wrapper.create(config) as client:
            await client.write_ticker_batch(data_points)
            df = await client.query_ticker_data("RELIANCE", hours=24)
    """
    
    def __init__(self, config: InfluxDBConfig) -> None:
        self._config = config
        self._client: Optional[InfluxDBClient] = None
        self._write_api = None
        self._query_api = None
        self._delete_api: Optional[DeleteApi] = None
        self._is_connected = False
        self._write_semaphore = asyncio.Semaphore(10)  # Limit concurrent writes
        
    @classmethod
    @asynccontextmanager
    async def create(cls, config: InfluxDBConfig) -> AsyncGenerator["InfluxDBClient_Wrapper", None]:
        """Factory method to create and manage client lifecycle."""
        instance = cls(config)
        try:
            await instance.connect()
            yield instance
        finally:
            await instance.close()
    
    async def connect(self) -> bool:
        """Establish connection to InfluxDB with retry logic."""
        if not self._config.enabled:
            logger.warning("InfluxDB is disabled in config")
            return False
            
        for attempt in range(self._config.max_retries):
            try:
                self._client = InfluxDBClient(
                    url=self._config.url,
                    token=self._config.token,
                    org=self._config.org
                )
                
                # Test connection
                health = self._client.health()
                if health.status == "pass":
                    self._write_api = self._client.write_api(write_options=ASYNCHRONOUS)
                    self._query_api = self._client.query_api()
                    self._delete_api = DeleteApi(self._client)
                    self._is_connected = True
                    logger.info(f"Connected to InfluxDB at {self._config.url}")
                    return True
                else:
                    raise InfluxDBError(f"Health check failed: {health.message}")
                    
            except InfluxDBError as e:
                logger.warning(
                    f"InfluxDB connection attempt {attempt + 1}/{self._config.max_retries} failed",
                    extra={"error": str(e), "url": self._config.url}
                )
                if attempt < self._config.max_retries - 1:
                    await asyncio.sleep(self._config.retry_interval_ms / 1000)
                    
        logger.error("Failed to connect to InfluxDB after all retries")
        self._is_connected = False
        return False
    
    async def close(self) -> None:
        """Close connection and flush pending writes."""
        if self._write_api:
            try:
                self._write_api.close()
            except Exception as e:
                logger.warning(f"Error closing write API: {e}")
                
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing InfluxDB client: {e}")
                
        self._is_connected = False
        logger.info("InfluxDB connection closed")
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._is_connected
    
    async def list_buckets(self) -> list[str]:
        """
        List all buckets in InfluxDB.
        
        Returns:
            List of bucket names
        """
        if not self._is_connected or not self._client:
            return []
        
        try:
            buckets_api = self._client.buckets_api()
            buckets = buckets_api.find_buckets()
            return [b.name for b in buckets.buckets]
        except Exception as e:
            logger.warning(f"Error listing buckets: {e}")
            return []
    
    async def ensure_bucket_exists(self, bucket_name: str, retention_days: int = 365) -> bool:
        """
        Ensure a bucket exists, creating it if necessary.
        
        Args:
            bucket_name: Name of the bucket to ensure exists
            retention_days: Retention period in days (default: 365)
            
        Returns:
            True if bucket exists or was created, False otherwise
        """
        if not self._is_connected or not self._client:
            return False
        
        try:
            buckets = await self.list_buckets()
            if bucket_name in buckets:
                return True
            
            # Create bucket
            buckets_api = self._client.buckets_api()
            retention_seconds = retention_days * 24 * 60 * 60
            
            # Get org ID
            org = self._client.organizations_api().find_organizations(org=self._config.org)
            if not org:
                logger.error(f"Organization '{self._config.org}' not found")
                return False
            
            from influxdb_client import BucketRetentionRules
            retention_rules = BucketRetentionRules(type="expire", every_seconds=retention_seconds)
            
            buckets_api.create_bucket(
                bucket_name=bucket_name,
                org_id=org[0].id,
                retention_rules=retention_rules
            )
            logger.info(f"Created bucket '{bucket_name}' with {retention_days} days retention")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to ensure bucket '{bucket_name}' exists: {e}")
            return False
    
    async def write_ticker_batch(
        self, 
        data_points: list[TickerDataPoint],
        bucket: str = DataBucket.TICKER_DATA
    ) -> bool:
        """
        Write batch of ticker data points to InfluxDB.
        
        Args:
            data_points: List of TickerDataPoint objects
            bucket: Target bucket name (can be DataBucket enum or string)
            
        Returns:
            True if write successful, False otherwise
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected, skipping write")
            return False
            
        if not data_points:
            return True
        
        # Convert enum to string value if needed
        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
            
        async with self._write_semaphore:
            try:
                points = [
                    Point("ticker")
                    .tag("symbol", dp.symbol)
                    .tag("exchange", dp.exchange)
                    .field("open", dp.open)
                    .field("high", dp.high)
                    .field("low", dp.low)
                    .field("close", dp.close)
                    .field("volume", dp.volume)
                    .time(dp.timestamp, WritePrecision.S)
                    for dp in data_points
                ]
                
                # Run in executor to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._write_api.write(
                        bucket=bucket_name,
                        org=self._config.org,
                        record=points
                    )
                )
                
                logger.debug(f"Wrote {len(points)} ticker points to InfluxDB bucket '{bucket_name}'")
                return True
                
            except InfluxDBError as e:
                logger.error(
                    "Failed to write ticker batch to InfluxDB",
                    extra={"error": str(e), "point_count": len(data_points)}
                )
                return False
    
    async def write_orderbook_batch(
        self,
        data_points: list[OrderBookDataPoint],
        bucket: str = DataBucket.ORDER_BOOK
    ) -> bool:
        """
        Write batch of order book data points to InfluxDB.
        
        Args:
            data_points: List of OrderBookDataPoint objects
            bucket: Target bucket name
            
        Returns:
            True if write successful, False otherwise
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected, skipping write")
            return False
            
        if not data_points:
            return True
        
        # Convert enum to string value if needed
        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
        
        # Ensure bucket exists before writing
        bucket_exists = await self.ensure_bucket_exists(bucket_name)
        if not bucket_exists:
            logger.warning(f"Could not ensure bucket '{bucket_name}' exists, attempting write anyway")
            
        async with self._write_semaphore:
            try:
                import json
                points = []
                for dp in data_points:
                    point = (
                        Point("orderbook")
                        .tag("symbol", dp.symbol)
                        .field("total_buy_qty", dp.total_buy_qty)
                        .field("total_sell_qty", dp.total_sell_qty)
                        .field("ltp", dp.last_traded_price)
                        .field("ltq", dp.last_traded_qty)
                        .field("volume", dp.volume)
                        .field("atp", dp.average_traded_price)
                        .field("lower_circuit", dp.lower_circuit)
                        .field("upper_circuit", dp.upper_circuit)
                        .field("change_percent", dp.change_percent)
                        .time(dp.timestamp, WritePrecision.S)
                    )
                    
                    # Add optional fields if present
                    if dp.open is not None:
                        point = point.field("open", dp.open)
                    if dp.high is not None:
                        point = point.field("high", dp.high)
                    if dp.low is not None:
                        point = point.field("low", dp.low)
                    if dp.close is not None:
                        point = point.field("close", dp.close)
                    if dp.tick_size is not None:
                        point = point.field("tick_size", dp.tick_size)
                    if dp.change is not None:
                        point = point.field("change", dp.change)
                    if dp.open_interest is not None:
                        point = point.field("open_interest", dp.open_interest)
                    if dp.open_interest_flag is not None:
                        point = point.field("open_interest_flag", int(dp.open_interest_flag))
                    if dp.previous_day_open_interest is not None:
                        point = point.field("previous_day_open_interest", dp.previous_day_open_interest)
                    if dp.open_interest_percent is not None:
                        point = point.field("open_interest_percent", dp.open_interest_percent)
                    if dp.expiry is not None:
                        point = point.field("expiry", dp.expiry)
                    
                    # Serialize bids and asks as JSON strings
                    if dp.bids is not None:
                        point = point.field("bids", json.dumps(dp.bids))
                    if dp.asks is not None:
                        point = point.field("asks", json.dumps(dp.asks))
                    
                    points.append(point)
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._write_api.write(
                        bucket=bucket_name,
                        org=self._config.org,
                        record=points
                    )
                )
                
                logger.debug(f"Wrote {len(points)} orderbook points to InfluxDB bucket '{bucket_name}'")
                return True
                
            except InfluxDBError as e:
                logger.error(
                    "Failed to write orderbook batch to InfluxDB",
                    extra={"error": str(e), "point_count": len(data_points), "bucket": bucket_name}
                )
                return False
    
    async def delete_data(
        self,
        bucket: str = DataBucket.TICKER_DATA,
        start: Optional[datetime] = None,
        stop: Optional[datetime] = None,
        predicate: Optional[str] = None
    ) -> bool:
        """
        Delete data from InfluxDB bucket.
        
        Args:
            bucket: Bucket name (can be DataBucket enum or string)
            start: Start time for deletion (None = all time)
            stop: Stop time for deletion (None = all time)
            predicate: Optional predicate filter (e.g., '_measurement="ticker"')
            
        Returns:
            True if deletion successful, False otherwise
        """
        if not self._is_connected or not self._delete_api:
            logger.warning("InfluxDB not connected, cannot delete data")
            return False
        
        # Convert enum to string value if needed
        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
        
        try:
            # Default to delete all data if no time range specified
            if start is None:
                start = datetime(1970, 1, 1)  # Unix epoch start
            if stop is None:
                stop = datetime.now()
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._delete_api.delete(
                    start=start,
                    stop=stop,
                    predicate=predicate,
                    bucket=bucket_name,
                    org=self._config.org
                )
            )
            
            logger.info(
                f"Deleted data from bucket '{bucket_name}' "
                f"(start: {start}, stop: {stop}, predicate: {predicate or 'none'})"
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete data from InfluxDB: {e}")
            return False
    
    async def query_ticker_data(
        self,
        symbol: str,
        hours: int = 24,
        bucket: str = DataBucket.TICKER_DATA
    ) -> pd.DataFrame:
        """
        Query ticker data for a symbol using query_data_frame for robust Pandas integration.
        
        Args:
            symbol: Stock symbol to query
            hours: Number of hours of historical data
            bucket: Source bucket name (can be DataBucket enum or string)
            
        Returns:
            DataFrame with ticker data (columns: timestamp, symbol, open, high, low, close, volume)
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected")
            return pd.DataFrame()

        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
        
        # Handle ALL_SYMBOLS - skip symbol filter to get all symbols
        if symbol == "ALL_SYMBOLS":
            symbol_filter = ""
        else:
            symbol_filter = f'|> filter(fn: (r) => r["symbol"] == "{symbol}")'
        
        query = f'''
            from(bucket: "{bucket_name}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r["_measurement"] == "ticker")
            {symbol_filter}
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        logger.debug(f"Executing InfluxDB query for {symbol} (hours={hours}, bucket={bucket_name})")

        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._query_api.query_data_frame(query, org=self._config.org)
            )

            if isinstance(df, list):
                if len(df) == 0:
                    logger.debug(f"No ticker data found for {symbol}")
                    return pd.DataFrame()
                df = pd.concat(df, ignore_index=True)

            if df.empty:
                logger.debug(f"No ticker data found for {symbol}")
                return pd.DataFrame()

            column_mapping = {
                "_time": "timestamp",
                "_start": None,
                "_stop": None,
                "_measurement": None,
                "result": None,
                "table": None,
            }

            cols_to_drop = [c for c in df.columns if column_mapping.get(c) is None and c.startswith("_")]
            cols_to_drop.extend(["result", "table"] if "result" in df.columns else [])
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

            if "_time" in df.columns:
                df = df.rename(columns={"_time": "timestamp"})

            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if not df.empty and "timestamp" in df.columns:
                df = df.sort_values("timestamp").reset_index(drop=True)

            logger.debug(f"Queried {len(df)} ticker records for {symbol}")
            return df

        except InfluxDBError as e:
            error_msg = str(e)
            if "Content-Type" in error_msg or "application/json" in error_msg:
                logger.error(
                    f"InfluxDB returned unexpected response format. Check bucket '{bucket_name}' exists and has data.",
                    extra={"error": error_msg, "symbol": symbol}
                )
            else:
                logger.error(
                    f"Failed to query ticker data: {error_msg}",
                    extra={"symbol": symbol, "hours": hours, "bucket": bucket_name}
                )
            return pd.DataFrame()
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            logger.error(
                f"Unexpected error querying ticker data: {error_msg}",
                extra={"symbol": symbol, "hours": hours, "bucket": bucket_name, "query": query[:200], "error_type": error_type}
            )
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()
    
    async def query_training_data(
        self,
        symbols: list[str],
        start_time: datetime,
        end_time: datetime,
        bucket: str = DataBucket.TICKER_DATA
    ) -> pd.DataFrame:
        """
        Query historical ticker data for training purposes using query_data_frame.
        
        Args:
            symbols: List of stock symbols to query
            start_time: Start datetime for query range
            end_time: End datetime for query range
            bucket: Source bucket name (can be DataBucket enum or string)
            
        Returns:
            DataFrame with ticker data for all symbols in the time range
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected")
            return pd.DataFrame()
        
        # Convert enum to string value if needed
        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
        
        # Format timestamps for Flux query
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Build filter for multiple symbols (skip filter if ALL_SYMBOLS to get all data)
        is_all_symbols = len(symbols) == 1 and symbols[0] == "ALL_SYMBOLS"
        if is_all_symbols:
            symbol_filter_line = ""  # No symbol filter - get all symbols
        else:
            symbol_filter = " or ".join([f'r["symbol"] == "{s}"' for s in symbols])
            symbol_filter_line = f'|> filter(fn: (r) => {symbol_filter})'
        
        query = f'''
            from(bucket: "{bucket_name}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r["_measurement"] == "ticker")
            {symbol_filter_line}
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> group()
            |> sort(columns: ["_time", "symbol"])
        '''
        
        logger.debug(f"Executing training data query for {len(symbols)} symbols ({start_str} to {end_str})")
        
        try:
            loop = asyncio.get_event_loop()
            
            # Use query_data_frame for direct Pandas integration
            df = await loop.run_in_executor(
                None,
                lambda: self._query_api.query_data_frame(query, org=self._config.org)
            )
            
            # Handle list of DataFrames
            if isinstance(df, list):
                if len(df) == 0:
                    logger.warning(f"No training data found for {len(symbols)} symbols in range {start_str} to {end_str}")
                    return pd.DataFrame()
                df = pd.concat(df, ignore_index=True)
            
            if df.empty:
                logger.warning(f"No training data found for {len(symbols)} symbols in range {start_str} to {end_str}")
                return pd.DataFrame()
            
            # Drop unwanted InfluxDB metadata columns
            cols_to_drop = ["_start", "_stop", "_measurement", "result", "table"]
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
            
            # Rename _time to timestamp
            if "_time" in df.columns:
                df = df.rename(columns={"_time": "timestamp"})
            
            # Ensure numeric types
            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            if not df.empty:
                df = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
                logger.info(f"Queried {len(df)} training records for {len(symbols)} symbols from {start_str} to {end_str}")
                
            return df
            
        except InfluxDBError as e:
            error_msg = str(e)
            if "Content-Type" in error_msg:
                logger.error(
                    f"InfluxDB returned unexpected response format. Check bucket '{bucket_name}' exists.",
                    extra={"error": error_msg}
                )
            else:
                logger.error(
                    f"Failed to query training data: {error_msg}",
                    extra={"symbols": symbols[:5], "bucket": bucket_name}  # Log first 5 symbols only
                )
            return pd.DataFrame()
        except Exception as e:
            logger.error(
                f"Unexpected error querying training data: {str(e)}",
                extra={"symbols": symbols[:5], "bucket": bucket_name}
            )
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()
    
    async def query_orderbook_data(
        self,
        symbols: list[str],
        start_time: datetime,
        end_time: datetime,
        bucket: str = DataBucket.ORDER_BOOK
    ) -> pd.DataFrame:
        """
        Query historical order book data for training purposes.
        
        Args:
            symbols: List of stock symbols to query (or ["ALL_SYMBOLS"] for all)
            start_time: Start datetime for query range
            end_time: End datetime for query range
            bucket: Source bucket name (can be DataBucket enum or string)
            
        Returns:
            DataFrame with order book data for all symbols in the time range
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected")
            return pd.DataFrame()
        
        # Convert enum to string value if needed
        bucket_name = bucket.value if isinstance(bucket, DataBucket) else str(bucket)
        
        # Format timestamps for Flux query
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Build filter for multiple symbols (skip filter if ALL_SYMBOLS to get all data)
        is_all_symbols = len(symbols) == 1 and symbols[0] == "ALL_SYMBOLS"
        if is_all_symbols:
            symbol_filter_line = ""  # No symbol filter - get all symbols
        else:
            symbol_filter = " or ".join([f'r["symbol"] == "{s}"' for s in symbols])
            symbol_filter_line = f'|> filter(fn: (r) => {symbol_filter})'
        
        query = f'''
            from(bucket: "{bucket_name}")
            |> range(start: {start_str}, stop: {end_str})
            |> filter(fn: (r) => r["_measurement"] == "orderbook")
            {symbol_filter_line}
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> group()
            |> sort(columns: ["_time", "symbol"])
        '''
        
        logger.debug(f"Executing orderbook query for {len(symbols)} symbols ({start_str} to {end_str})")
        
        try:
            loop = asyncio.get_event_loop()
            
            # Use query_data_frame for direct Pandas integration
            df = await loop.run_in_executor(
                None,
                lambda: self._query_api.query_data_frame(query, org=self._config.org)
            )
            
            # Handle list of DataFrames
            if isinstance(df, list):
                if len(df) == 0:
                    logger.warning(f"No orderbook data found for {len(symbols)} symbols in range {start_str} to {end_str}")
                    return pd.DataFrame()
                df = pd.concat(df, ignore_index=True)
            
            if df.empty:
                logger.warning(f"No orderbook data found for {len(symbols)} symbols in range {start_str} to {end_str}")
                return pd.DataFrame()
            
            # Drop unwanted InfluxDB metadata columns
            cols_to_drop = ["_start", "_stop", "_measurement", "result", "table"]
            df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
            
            # Rename _time to last_traded_time (order book timestamp)
            if "_time" in df.columns:
                df = df.rename(columns={"_time": "last_traded_time"})
            
            # Rename abbreviated field names to full names (as expected by DataAggregator)
            field_mapping = {
                "ltp": "last_traded_price",
                "ltq": "last_traded_qty",
                "atp": "average_traded_price",
            }
            df = df.rename(columns={k: v for k, v in field_mapping.items() if k in df.columns})
            
            # Deserialize bids and asks from JSON strings
            import json
            if "bids" in df.columns:
                df["bids"] = df["bids"].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
            if "asks" in df.columns:
                df["asks"] = df["asks"].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
            
            # Ensure numeric types
            numeric_cols = [
                "total_buy_qty", "total_sell_qty", "last_traded_price", "last_traded_qty",
                "volume", "average_traded_price", "lower_circuit", "upper_circuit", "change_percent",
                "open", "high", "low", "close", "tick_size", "change",
                "open_interest", "previous_day_open_interest", "open_interest_percent"
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Handle boolean field
            if "open_interest_flag" in df.columns:
                df["open_interest_flag"] = df["open_interest_flag"].astype(bool)
            
            if not df.empty:
                df = df.sort_values(["symbol", "last_traded_time"]).reset_index(drop=True)
                logger.info(f"Queried {len(df)} orderbook records for {len(symbols)} symbols from {start_str} to {end_str}")
                
            return df
            
        except InfluxDBError as e:
            error_msg = str(e)
            logger.error(
                f"Failed to query orderbook data: {error_msg}",
                extra={"symbols": symbols[:5], "bucket": bucket_name}
            )
            return pd.DataFrame()
        except Exception as e:
            logger.error(
                f"Unexpected error querying orderbook data: {str(e)}",
                extra={"symbols": symbols[:5], "bucket": bucket_name}
            )
            import traceback
            logger.debug(traceback.format_exc())
            return pd.DataFrame()
    
    async def query_latest_ticker(
        self,
        symbols: list[str],
        bucket: str = DataBucket.TICKER_DATA
    ) -> dict[str, TickerDataPoint]:
        """
        Query latest ticker data for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            bucket: Source bucket name
            
        Returns:
            Dictionary mapping symbol to latest TickerDataPoint
        """
        if not self._is_connected:
            logger.warning("InfluxDB not connected")
            return {}
            
        try:
            # Build filter for multiple symbols (skip filter if ALL_SYMBOLS to get all symbols)
            is_all_symbols = len(symbols) == 1 and symbols[0] == "ALL_SYMBOLS"
            if is_all_symbols:
                symbol_filter = ""  # No symbol filter - get latest for all symbols
            else:
                symbol_conditions = " or ".join([f'r["symbol"] == "{s}"' for s in symbols])
                symbol_filter = f'|> filter(fn: (r) => {symbol_conditions})'
            
            query = f'''
                from(bucket: "{bucket}")
                |> range(start: -1h)
                |> filter(fn: (r) => r["_measurement"] == "ticker")
                {symbol_filter}
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> group(columns: ["symbol"])
                |> last()
            '''
            
            loop = asyncio.get_event_loop()
            tables = await loop.run_in_executor(
                None,
                lambda: self._query_api.query(query, org=self._config.org)
            )
            
            result: dict[str, TickerDataPoint] = {}
            for table in tables:
                for record in table.records:
                    symbol = record.values.get("symbol")
                    if symbol:
                        result[symbol] = TickerDataPoint(
                            symbol=symbol,
                            timestamp=record.get_time(),
                            open=record.values.get("open", 0.0),
                            high=record.values.get("high", 0.0),
                            low=record.values.get("low", 0.0),
                            close=record.values.get("close", 0.0),
                            volume=int(record.values.get("volume", 0)),
                            exchange=record.values.get("exchange", "NSE")
                        )
            
            logger.debug(f"Queried latest ticker for {len(result)}/{len(symbols)} symbols")
            return result
            
        except InfluxDBError as e:
            logger.error(
                "Failed to query latest ticker data",
                extra={"error": str(e), "symbol_count": len(symbols)}
            )
            return {}
    
    async def health_check(self) -> bool:
        """Check if InfluxDB is healthy and reachable."""
        if not self._client:
            return False
            
        try:
            health = self._client.health()
            return health.status == "pass"
        except Exception as e:
            logger.warning(f"InfluxDB health check failed: {e}")
            return False


def create_influx_config_from_hydra(hydra_config) -> InfluxDBConfig:
    """
    Create InfluxDBConfig from Hydra configuration.
    
    Args:
        hydra_config: Hydra DictConfig object
        
    Returns:
        InfluxDBConfig instance
    """
    influx_cfg = hydra_config.influxdb
    return InfluxDBConfig(
        url=influx_cfg.url,
        token=influx_cfg.token,
        org=influx_cfg.org,
        batch_size=influx_cfg.write.batch_size,
        flush_interval_ms=influx_cfg.write.flush_interval_ms,
        retry_interval_ms=influx_cfg.write.retry_interval_ms,
        max_retries=influx_cfg.write.max_retries,
        query_timeout_ms=influx_cfg.query.timeout_ms,
        enabled=influx_cfg.enabled,
        fallback_to_csv=influx_cfg.fallback_to_csv
    )


# Debug & Verify
# ==============
# Run: python -c "from src.utils.influx_client import InfluxDBConfig; print(InfluxDBConfig(token='test').model_dump())"
# Verify: Look for "Connected to InfluxDB" in logs
# REPL Test:
#   python -c "
#   import asyncio
#   from src.utils.influx_client import InfluxDBClient_Wrapper, InfluxDBConfig
#   config = InfluxDBConfig(token='your-token')
#   async def test():
#       async with InfluxDBClient_Wrapper.create(config) as client:
#           print(f'Connected: {client.is_connected}')
#   asyncio.run(test())
#   "
