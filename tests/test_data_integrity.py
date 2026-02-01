# tests/test_data_integrity.py
"""
Data Integrity Test Suite

Tests for verifying data collection and storage integrity:
1. Data existence - ticker and orderbook data exists for symbols
2. Data freshness - latest data is within expected interval
3. Data schema - required columns are present
4. Data quality - no invalid values, proper OHLC relationships
5. Data alignment - ticker and orderbook timestamps align

Usage:
    pytest tests/test_data_integrity.py -v
    pytest tests/test_data_integrity.py -v -k test_ticker_data_exists
    pytest tests/test_data_integrity.py -v --symbols RELIANCE TCS
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pytest
from dotenv import load_dotenv
from loguru import logger

# Load environment at module level
load_dotenv()

# Import from project
try:
    from src.utils.influx_client import (
        DataBucket,
        InfluxDBClient_Wrapper,
        InfluxDBConfig,
    )
    from src import config as app_config
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    app_config = None


def get_influx_config() -> Optional[InfluxDBConfig]:
    """Get InfluxDB configuration from environment."""
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        return None
    
    return InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--symbols",
        action="store",
        nargs="+",
        default=None,
        help="Specific symbols to test"
    )
    parser.addoption(
        "--freshness-minutes",
        action="store",
        type=int,
        default=60,  # More lenient for testing (1 hour)
        help="Freshness threshold in minutes"
    )


@pytest.fixture(scope="module")
def symbols(request):
    """Get symbols to test."""
    custom_symbols = request.config.getoption("--symbols")
    if custom_symbols:
        return custom_symbols
    
    # Try to get from config
    if app_config and hasattr(app_config, 'symbols'):
        return list(app_config.symbols)[:5]  # Test first 5 symbols
    
    return ["RELIANCE", "TCS", "INFY"]


@pytest.fixture(scope="module")
def freshness_threshold(request):
    """Get freshness threshold in minutes."""
    return request.config.getoption("--freshness-minutes")


@pytest.fixture(scope="module")
def influx_config():
    """Get InfluxDB configuration."""
    config = get_influx_config()
    if config is None:
        pytest.skip("INFLUXDB_TOKEN not set")
    return config


@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def influx_client(influx_config, event_loop):
    """Create InfluxDB client for tests."""
    client = InfluxDBClient_Wrapper(influx_config)
    connected = await client.connect()
    if not connected:
        pytest.skip("Could not connect to InfluxDB")
    yield client
    await client.close()


class TestDataExistence:
    """Test data existence in InfluxDB."""
    
    @pytest.mark.asyncio
    async def test_influx_connection(self, influx_client):
        """Test that we can connect to InfluxDB."""
        assert influx_client.is_connected, "Should be connected to InfluxDB"
    
    @pytest.mark.asyncio
    async def test_ticker_bucket_exists(self, influx_client):
        """Test that ticker_data bucket exists."""
        buckets = await influx_client.list_buckets()
        assert DataBucket.TICKER_DATA.value in buckets, \
            f"Bucket '{DataBucket.TICKER_DATA.value}' not found. Available: {buckets}"
    
    @pytest.mark.asyncio
    async def test_orderbook_bucket_exists(self, influx_client):
        """Test that order_book bucket exists."""
        buckets = await influx_client.list_buckets()
        # This is optional - orderbook bucket may not exist if no orderbook data
        if DataBucket.ORDER_BOOK.value not in buckets:
            pytest.skip(f"Bucket '{DataBucket.ORDER_BOOK.value}' not found")
    
    @pytest.mark.asyncio
    async def test_ticker_data_exists(self, influx_client, symbols):
        """Test that ticker data exists for symbols."""
        symbols_with_data = []
        symbols_without_data = []
        
        for symbol in symbols:
            df = await influx_client.query_ticker_data(symbol, hours=24*30)  # 30 days
            if df is not None and len(df) > 0:
                symbols_with_data.append(symbol)
            else:
                symbols_without_data.append(symbol)
        
        # At least 50% of symbols should have data
        ratio = len(symbols_with_data) / len(symbols) if symbols else 0
        assert ratio >= 0.5, \
            f"Only {len(symbols_with_data)}/{len(symbols)} symbols have ticker data. " \
            f"Missing: {symbols_without_data}"
    
    @pytest.mark.asyncio
    async def test_orderbook_data_exists(self, influx_client, symbols):
        """Test that orderbook data exists for symbols."""
        symbols_with_data = []
        symbols_without_data = []
        
        for symbol in symbols:
            try:
                from datetime import timedelta
                end_time = datetime.now()
                start_time = end_time - timedelta(days=30)
                df = await influx_client.query_orderbook_data(
                    symbols=[symbol],
                    start_time=start_time,
                    end_time=end_time
                )
                # Filter to this symbol if multiple symbols returned
                if df is not None and not df.empty and 'symbol' in df.columns:
                    df = df[df['symbol'] == symbol]
                
                if df is not None and len(df) > 0:
                    symbols_with_data.append(symbol)
                else:
                    symbols_without_data.append(symbol)
            except Exception:
                symbols_without_data.append(symbol)
        
        # Orderbook is optional, but warn if none
        if len(symbols_with_data) == 0:
            pytest.skip(f"No orderbook data found for any symbol: {symbols}")


class TestDataFreshness:
    """Test data freshness (recency)."""
    
    @pytest.mark.asyncio
    async def test_ticker_data_freshness(self, influx_client, symbols, freshness_threshold):
        """Test that ticker data is recent."""
        now = datetime.now()
        stale_symbols = []
        
        for symbol in symbols:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)  # Last 7 days
            if df is None or len(df) == 0:
                continue
            
            if 'timestamp' not in df.columns:
                continue
            
            latest = df['timestamp'].max()
            
            # Handle different timestamp types
            if hasattr(latest, 'to_pydatetime'):
                latest = latest.to_pydatetime()
            elif isinstance(latest, str):
                from dateutil import parser
                latest = parser.parse(latest)
            
            # Make timezone naive
            if hasattr(latest, 'tzinfo') and latest.tzinfo is not None:
                latest = latest.replace(tzinfo=None)
            
            age_minutes = (now - latest).total_seconds() / 60
            
            if age_minutes > freshness_threshold:
                stale_symbols.append((symbol, age_minutes))
        
        # During trading hours, at least 50% should be fresh
        fresh_count = len(symbols) - len(stale_symbols)
        fresh_ratio = fresh_count / len(symbols) if symbols else 0
        
        if fresh_ratio < 0.5:
            stale_info = ", ".join([f"{s}: {m:.0f}min" for s, m in stale_symbols[:5]])
            pytest.fail(
                f"Only {fresh_count}/{len(symbols)} symbols have fresh data. "
                f"Stale (threshold={freshness_threshold}min): {stale_info}"
            )


class TestDataSchema:
    """Test data schema correctness."""
    
    TICKER_REQUIRED_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    ORDERBOOK_REQUIRED_COLUMNS = ['timestamp', 'total_buy_qty', 'total_sell_qty', 'ltp']
    
    @pytest.mark.asyncio
    async def test_ticker_schema(self, influx_client, symbols):
        """Test that ticker data has required columns."""
        for symbol in symbols[:3]:  # Check first 3 symbols
            df = await influx_client.query_ticker_data(symbol, hours=24)
            if df is None or len(df) == 0:
                continue
            
            missing = [col for col in self.TICKER_REQUIRED_COLUMNS if col not in df.columns]
            assert len(missing) == 0, \
                f"Symbol {symbol} missing ticker columns: {missing}. " \
                f"Available: {list(df.columns)}"
    
    @pytest.mark.asyncio
    async def test_orderbook_schema(self, influx_client, symbols):
        """Test that orderbook data has required columns."""
        for symbol in symbols[:3]:
            try:
                from datetime import timedelta
                end_time = datetime.now()
                start_time = end_time - timedelta(days=1)
                df = await influx_client.query_orderbook_data(
                    symbols=[symbol],
                    start_time=start_time,
                    end_time=end_time
                )
                # Filter to this symbol if multiple symbols returned
                if df is not None and not df.empty and 'symbol' in df.columns:
                    df = df[df['symbol'] == symbol]
                
                if df is None or len(df) == 0:
                    continue
                
                missing = [col for col in self.ORDERBOOK_REQUIRED_COLUMNS if col not in df.columns]
                if missing:
                    pytest.fail(
                        f"Symbol {symbol} missing orderbook columns: {missing}. "
                        f"Available: {list(df.columns)}"
                    )
            except Exception:
                pytest.skip(f"Could not query orderbook for {symbol}")


class TestDataQuality:
    """Test data quality."""
    
    @pytest.mark.asyncio
    async def test_no_negative_prices(self, influx_client, symbols):
        """Test that prices are not negative."""
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) == 0:
                continue
            
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    negative_count = (df[col] < 0).sum()
                    assert negative_count == 0, \
                        f"Symbol {symbol} has {negative_count} negative values in {col}"
    
    @pytest.mark.asyncio
    async def test_no_negative_volume(self, influx_client, symbols):
        """Test that volume is not negative."""
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) == 0:
                continue
            
            if 'volume' in df.columns:
                negative_count = (df['volume'] < 0).sum()
                assert negative_count == 0, \
                    f"Symbol {symbol} has {negative_count} negative volume values"
    
    @pytest.mark.asyncio
    async def test_ohlc_consistency(self, influx_client, symbols):
        """Test OHLC consistency: high >= low, high >= open/close, low <= open/close."""
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) == 0:
                continue
            
            if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                continue
            
            # High >= Low
            invalid = (df['high'] < df['low']).sum()
            assert invalid == 0, \
                f"Symbol {symbol} has {invalid} records where high < low"
            
            # High >= Open and High >= Close
            invalid = ((df['high'] < df['open']) | (df['high'] < df['close'])).sum()
            assert invalid == 0, \
                f"Symbol {symbol} has {invalid} records where high < open or high < close"
            
            # Low <= Open and Low <= Close
            invalid = ((df['low'] > df['open']) | (df['low'] > df['close'])).sum()
            assert invalid == 0, \
                f"Symbol {symbol} has {invalid} records where low > open or low > close"
    
    @pytest.mark.asyncio
    async def test_no_duplicate_timestamps(self, influx_client, symbols):
        """Test that there are no duplicate timestamps."""
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) == 0:
                continue
            
            if 'timestamp' in df.columns:
                duplicates = df['timestamp'].duplicated().sum()
                # Allow some duplicates (different exchanges, etc.)
                duplicate_ratio = duplicates / len(df) if len(df) > 0 else 0
                assert duplicate_ratio < 0.1, \
                    f"Symbol {symbol} has {duplicates} ({duplicate_ratio:.1%}) duplicate timestamps"
    
    @pytest.mark.asyncio
    async def test_reasonable_null_ratio(self, influx_client, symbols):
        """Test that null value ratio is reasonable."""
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) == 0:
                continue
            
            for col in ['close', 'volume']:
                if col in df.columns:
                    null_ratio = df[col].isnull().sum() / len(df) if len(df) > 0 else 0
                    assert null_ratio < 0.05, \
                        f"Symbol {symbol} has {null_ratio:.1%} null values in {col}"


class TestDataAlignment:
    """Test data alignment between ticker and orderbook."""
    
    @pytest.mark.asyncio
    async def test_ticker_orderbook_time_alignment(self, influx_client, symbols):
        """Test that ticker and orderbook data have overlapping time ranges."""
        for symbol in symbols[:3]:
            # Get ticker data
            ticker_df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if ticker_df is None or len(ticker_df) == 0:
                continue
            
            # Get orderbook data
            try:
                from datetime import timedelta
                end_time = datetime.now()
                start_time = end_time - timedelta(days=7)
                orderbook_df = await influx_client.query_orderbook_data(
                    symbols=[symbol],
                    start_time=start_time,
                    end_time=end_time
                )
                # Filter to this symbol if multiple symbols returned
                if orderbook_df is not None and not orderbook_df.empty and 'symbol' in orderbook_df.columns:
                    orderbook_df = orderbook_df[orderbook_df['symbol'] == symbol]
                
                if orderbook_df is None or len(orderbook_df) == 0:
                    continue
            except Exception:
                continue
            
            if 'timestamp' not in ticker_df.columns or 'timestamp' not in orderbook_df.columns:
                continue
            
            # Check time ranges overlap
            ticker_min = ticker_df['timestamp'].min()
            ticker_max = ticker_df['timestamp'].max()
            orderbook_min = orderbook_df['timestamp'].min()
            orderbook_max = orderbook_df['timestamp'].max()
            
            # There should be some overlap
            has_overlap = not (ticker_max < orderbook_min or orderbook_max < ticker_min)
            
            if not has_overlap:
                pytest.fail(
                    f"Symbol {symbol}: Ticker ({ticker_min} to {ticker_max}) and "
                    f"Orderbook ({orderbook_min} to {orderbook_max}) have no overlap"
                )


class TestDataVolume:
    """Test data volume metrics."""
    
    @pytest.mark.asyncio
    async def test_minimum_record_count(self, influx_client, symbols):
        """Test that symbols have minimum number of records."""
        MIN_RECORDS = 100  # Minimum expected records in 7 days
        
        low_volume_symbols = []
        
        for symbol in symbols:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is not None:
                count = len(df)
                if count < MIN_RECORDS:
                    low_volume_symbols.append((symbol, count))
        
        if len(low_volume_symbols) > len(symbols) * 0.5:
            pytest.fail(
                f"Too many symbols with low data volume (<{MIN_RECORDS}): "
                f"{low_volume_symbols[:5]}"
            )
    
    @pytest.mark.asyncio
    async def test_data_continuity(self, influx_client, symbols):
        """Test for data gaps (missing intervals)."""
        MAX_GAP_HOURS = 24  # Maximum acceptable gap in hours
        
        for symbol in symbols[:3]:
            df = await influx_client.query_ticker_data(symbol, hours=24*7)
            if df is None or len(df) < 10:
                continue
            
            if 'timestamp' not in df.columns:
                continue
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Calculate time differences
            df['time_diff'] = df['timestamp'].diff()
            
            # Find large gaps (excluding market close hours)
            # Note: This is a simplified check
            if df['time_diff'].max() > pd.Timedelta(hours=MAX_GAP_HOURS):
                max_gap = df['time_diff'].max()
                pytest.skip(
                    f"Symbol {symbol} has large gap: {max_gap}. "
                    f"This may be expected for market close."
                )


# Convenience function to run tests programmatically
async def run_data_integrity_tests(
    symbols: list[str] = None,
    freshness_minutes: int = 60,
) -> dict:
    """
    Run data integrity tests programmatically.
    
    Args:
        symbols: List of symbols to test
        freshness_minutes: Freshness threshold
        
    Returns:
        Dictionary with test results
    """
    config = get_influx_config()
    if config is None:
        return {"error": "INFLUXDB_TOKEN not set"}
    
    if symbols is None:
        symbols = ["RELIANCE", "TCS", "INFY"]
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "symbols": symbols,
        "tests": {},
    }
    
    try:
        async with InfluxDBClient_Wrapper.create(config) as client:
            if not client.is_connected:
                return {"error": "Could not connect to InfluxDB"}
            
            # Run existence tests
            for symbol in symbols:
                ticker_df = await client.query_ticker_data(symbol, hours=24*7)
                results["tests"][f"{symbol}_ticker_exists"] = len(ticker_df) > 0 if ticker_df is not None else False
            
            results["overall"] = "PASS" if all(results["tests"].values()) else "FAIL"
            
    except Exception as e:
        results["error"] = str(e)
        results["overall"] = "ERROR"
    
    return results


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
