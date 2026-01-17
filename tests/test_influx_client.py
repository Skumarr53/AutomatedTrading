# tests/test_influx_client.py
"""
Unit tests for InfluxDB client wrapper.

Run with: pytest tests/test_influx_client.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig,
    OrderBookDataPoint,
    TickerDataPoint,
)


class TestInfluxDBConfig:
    """Tests for InfluxDBConfig Pydantic model."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        config = InfluxDBConfig(
            url="http://localhost:8086",
            token="test-token",
            org="test-org",
        )
        assert config.url == "http://localhost:8086"
        assert config.token == "test-token"
        assert config.org == "test-org"
        assert config.batch_size == 1000  # default value
        assert config.enabled is True

    def test_empty_token_raises_error(self) -> None:
        """Test that empty token raises validation error."""
        with pytest.raises(ValueError, match="token cannot be empty"):
            InfluxDBConfig(url="http://localhost:8086", token="", org="test-org")

    def test_whitespace_token_raises_error(self) -> None:
        """Test that whitespace-only token raises validation error."""
        with pytest.raises(ValueError, match="token cannot be empty"):
            InfluxDBConfig(url="http://localhost:8086", token="   ", org="test-org")

    def test_batch_size_bounds(self) -> None:
        """Test batch_size validation bounds."""
        # Valid batch size
        config = InfluxDBConfig(token="test", batch_size=5000)
        assert config.batch_size == 5000

        # Too large batch size
        with pytest.raises(ValueError):
            InfluxDBConfig(token="test", batch_size=50000)

        # Zero batch size
        with pytest.raises(ValueError):
            InfluxDBConfig(token="test", batch_size=0)


class TestTickerDataPoint:
    """Tests for TickerDataPoint model."""

    def test_valid_ticker_data(self) -> None:
        """Test creating a valid ticker data point."""
        now = datetime.now()
        point = TickerDataPoint(
            symbol="RELIANCE",
            timestamp=now,
            open=2500.0,
            high=2550.0,
            low=2480.0,
            close=2520.0,
            volume=1000000,
        )
        assert point.symbol == "RELIANCE"
        assert point.exchange == "NSE"  # default
        assert point.close == 2520.0

    def test_custom_exchange(self) -> None:
        """Test ticker with custom exchange."""
        point = TickerDataPoint(
            symbol="INFY",
            timestamp=datetime.now(),
            open=1500.0,
            high=1520.0,
            low=1490.0,
            close=1510.0,
            volume=500000,
            exchange="BSE",
        )
        assert point.exchange == "BSE"


class TestOrderBookDataPoint:
    """Tests for OrderBookDataPoint model."""

    def test_valid_orderbook_data(self) -> None:
        """Test creating a valid order book data point."""
        now = datetime.now()
        point = OrderBookDataPoint(
            symbol="TCS",
            timestamp=now,
            total_buy_qty=100000,
            total_sell_qty=80000,
            last_traded_price=3500.0,
            last_traded_qty=100,
            volume=2000000,
            average_traded_price=3480.0,
            lower_circuit=3150.0,
            upper_circuit=3850.0,
            change_percent=1.5,
        )
        assert point.symbol == "TCS"
        assert point.total_buy_qty == 100000
        assert point.change_percent == 1.5


class TestInfluxDBClientWrapper:
    """Tests for InfluxDBClient_Wrapper."""

    @pytest.fixture
    def config(self) -> InfluxDBConfig:
        """Create test configuration."""
        return InfluxDBConfig(
            url="http://localhost:8086",
            token="test-token",
            org="test-org",
        )

    @pytest.fixture
    def client(self, config: InfluxDBConfig) -> InfluxDBClient_Wrapper:
        """Create client instance without connecting."""
        return InfluxDBClient_Wrapper(config)

    def test_client_initialization(self, client: InfluxDBClient_Wrapper) -> None:
        """Test client initializes correctly."""
        assert client._is_connected is False
        assert client._client is None

    @pytest.mark.asyncio
    async def test_connect_disabled(self) -> None:
        """Test connect returns False when disabled."""
        config = InfluxDBConfig(token="test", enabled=False)
        client = InfluxDBClient_Wrapper(config)
        
        result = await client.connect()
        
        assert result is False
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_write_ticker_batch_not_connected(
        self, client: InfluxDBClient_Wrapper
    ) -> None:
        """Test write returns False when not connected."""
        data_points = [
            TickerDataPoint(
                symbol="TEST",
                timestamp=datetime.now(),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=10000,
            )
        ]
        
        result = await client.write_ticker_batch(data_points)
        
        assert result is False

    @pytest.mark.asyncio
    async def test_write_empty_batch_succeeds(
        self, client: InfluxDBClient_Wrapper
    ) -> None:
        """Test writing empty batch returns True."""
        result = await client.write_ticker_batch([])
        
        assert result is True

    @pytest.mark.asyncio
    async def test_health_check_no_client(
        self, client: InfluxDBClient_Wrapper
    ) -> None:
        """Test health check returns False when no client."""
        result = await client.health_check()
        
        assert result is False

    @pytest.mark.asyncio
    async def test_query_ticker_not_connected(
        self, client: InfluxDBClient_Wrapper
    ) -> None:
        """Test query returns empty DataFrame when not connected."""
        result = await client.query_ticker_data("TEST", hours=24)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """Test async context manager properly closes connection."""
        config = InfluxDBConfig(token="test", enabled=False)
        
        async with InfluxDBClient_Wrapper.create(config) as client:
            assert client is not None
        
        # After exiting context, client should be closed
        assert client._is_connected is False


class TestDataBucket:
    """Tests for DataBucket enum."""

    def test_bucket_values(self) -> None:
        """Test bucket enum values."""
        assert DataBucket.TICKER_DATA == "ticker_data"
        assert DataBucket.ORDER_BOOK == "order_book"
        assert DataBucket.TRADES == "trades"
        assert DataBucket.METRICS == "system_metrics"


# Integration tests (require running InfluxDB)
@pytest.mark.integration
class TestInfluxDBIntegration:
    """Integration tests requiring InfluxDB instance."""

    @pytest.fixture
    def integration_config(self) -> InfluxDBConfig:
        """Create integration test configuration."""
        import os
        
        return InfluxDBConfig(
            url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
            token=os.getenv("INFLUXDB_TOKEN", "test-token"),
            org=os.getenv("INFLUXDB_ORG", "trading"),
        )

    @pytest.mark.asyncio
    async def test_connection(self, integration_config: InfluxDBConfig) -> None:
        """Test actual connection to InfluxDB."""
        async with InfluxDBClient_Wrapper.create(integration_config) as client:
            if client.is_connected:
                health = await client.health_check()
                assert health is True

    @pytest.mark.asyncio
    async def test_write_and_query_ticker(
        self, integration_config: InfluxDBConfig
    ) -> None:
        """Test writing and querying ticker data."""
        async with InfluxDBClient_Wrapper.create(integration_config) as client:
            if not client.is_connected:
                pytest.skip("InfluxDB not available")

            # Write test data
            now = datetime.now()
            data_points = [
                TickerDataPoint(
                    symbol="TEST_INTEGRATION",
                    timestamp=now - timedelta(minutes=i),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=98.0 + i,
                    close=102.0 + i,
                    volume=10000 + i * 100,
                )
                for i in range(5)
            ]

            success = await client.write_ticker_batch(data_points)
            assert success is True

            # Query data back
            df = await client.query_ticker_data("TEST_INTEGRATION", hours=1)
            assert len(df) >= 5


# Debug & Verify
# ==============
# Run unit tests: pytest tests/test_influx_client.py -v
# Run integration tests: pytest tests/test_influx_client.py -v -m integration
# Skip integration tests: pytest tests/test_influx_client.py -v -m "not integration"
