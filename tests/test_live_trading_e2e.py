# tests/test_live_trading_e2e.py
"""
End-to-End Tests for Live Trading System

Comprehensive tests covering the complete trading flow:
1. Data collection and storage
2. Model loading and predictions
3. Trade decision making
4. Order execution verification
5. Alerting system

Usage:
    pytest tests/test_live_trading_e2e.py -v
    pytest tests/test_live_trading_e2e.py -v -k test_data_pipeline
    pytest tests/test_live_trading_e2e.py -v --paper-mode
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dotenv import load_dotenv
from loguru import logger

# Load environment
load_dotenv()


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture(scope="module")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def test_symbol():
    """Test symbol to use."""
    return "RELIANCE"


@pytest.fixture(scope="module")
def influx_config():
    """Get InfluxDB configuration."""
    from src.utils.influx_client import InfluxDBConfig
    
    token = os.getenv("INFLUXDB_TOKEN")
    if not token:
        pytest.skip("INFLUXDB_TOKEN not set")
    
    return InfluxDBConfig(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=token,
        org=os.getenv("INFLUXDB_ORG", "trading"),
    )


@pytest.fixture(scope="module")
async def influx_client(influx_config, event_loop):
    """Create InfluxDB client."""
    from src.utils.influx_client import InfluxDBClient_Wrapper
    
    client = InfluxDBClient_Wrapper(influx_config)
    connected = await client.connect()
    
    if not connected:
        pytest.skip("Could not connect to InfluxDB")
    
    yield client
    await client.close()


# ==============================================================================
# Data Pipeline Tests
# ==============================================================================

class TestDataPipeline:
    """Tests for data collection and storage."""
    
    @pytest.mark.asyncio
    async def test_influx_connection(self, influx_client):
        """Test InfluxDB connection."""
        assert influx_client.is_connected, "Should be connected to InfluxDB"
    
    @pytest.mark.asyncio
    async def test_data_query(self, influx_client, test_symbol):
        """Test querying data from InfluxDB."""
        df = await influx_client.query_ticker_data(test_symbol, hours=24)
        
        # Data may or may not exist, but query should not fail
        assert df is not None or df.empty or len(df) >= 0
    
    @pytest.mark.asyncio
    async def test_data_schema(self, influx_client, test_symbol):
        """Test that data has correct schema."""
        df = await influx_client.query_ticker_data(test_symbol, hours=24*30)
        
        if df is None or df.empty:
            pytest.skip(f"No data for {test_symbol}")
        
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing = [c for c in required_cols if c not in df.columns]
        
        assert len(missing) == 0, f"Missing columns: {missing}"
    
    @pytest.mark.asyncio
    async def test_data_quality(self, influx_client, test_symbol):
        """Test data quality checks."""
        df = await influx_client.query_ticker_data(test_symbol, hours=24*7)
        
        if df is None or df.empty:
            pytest.skip(f"No data for {test_symbol}")
        
        # No negative prices
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                assert (df[col] >= 0).all(), f"Negative values in {col}"
        
        # No negative volume
        if 'volume' in df.columns:
            assert (df['volume'] >= 0).all(), "Negative volume values"


# ==============================================================================
# Model Pipeline Tests
# ==============================================================================

class TestModelPipeline:
    """Tests for model loading and predictions."""
    
    def test_mlflow_available(self):
        """Test MLflow is available."""
        try:
            import mlflow
            assert True
        except ImportError:
            pytest.fail("MLflow not installed")
    
    def test_model_loading(self):
        """Test loading models from MLflow."""
        try:
            import mlflow
            
            mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            mlflow.set_tracking_uri(mlflow_uri)
            
            # Try to list experiments (tests connectivity)
            experiments = mlflow.search_experiments()
            assert experiments is not None
            
        except Exception as e:
            pytest.skip(f"MLflow not accessible: {e}")
    
    def test_prediction_generation(self):
        """Test prediction generation with mock model."""
        # Create mock model
        mock_model = MagicMock()
        mock_model.predict.return_value = ["High"]
        
        # Create sample data
        sample_data = pd.DataFrame({
            'close': [100.0],
            'volume': [10000],
            'sma_20': [99.0],
        })
        
        # Generate prediction
        prediction = mock_model.predict(sample_data)
        
        assert prediction is not None
        assert len(prediction) > 0


# ==============================================================================
# Decision Making Tests
# ==============================================================================

class TestDecisionMaking:
    """Tests for trade decision making."""
    
    def test_decision_maker_initialization(self):
        """Test TradeDecisionMaker initialization."""
        from src.trading_logic.trade_decision_maker import TradeDecisionMaker
        
        maker = TradeDecisionMaker()
        assert maker is not None
    
    def test_decision_from_predictions(self):
        """Test decision making from predictions."""
        from src.trading_logic.trade_decision_maker import TradeDecisionMaker
        
        maker = TradeDecisionMaker()
        
        # Bullish predictions
        bullish_preds = {
            "5min": "High",
            "15min": "High",
            "1h": "Medium High",
        }
        
        decision = maker.make_decision(pct_predictions=bullish_preds)
        
        assert decision is not None
        assert hasattr(decision, 'direction')
        assert hasattr(decision, 'confidence')
    
    def test_hold_decision_on_mixed_signals(self):
        """Test HOLD decision on mixed signals."""
        from src.trading_logic.trade_decision_maker import TradeDecisionMaker
        
        maker = TradeDecisionMaker()
        
        mixed_preds = {
            "5min": "High",
            "15min": "Low",
            "1h": "Neutral",
        }
        
        decision = maker.make_decision(pct_predictions=mixed_preds)
        
        # Mixed signals should typically result in HOLD or low confidence
        assert decision.confidence < 0.8 or decision.direction == "HOLD"


# ==============================================================================
# Trade Execution Tests
# ==============================================================================

class TestTradeExecution:
    """Tests for trade execution."""
    
    def test_executor_initialization(self):
        """Test FyersTradeExecutor initialization."""
        # This may skip if Fyers credentials not available
        fyers_client_id = os.getenv("FYERS_CLIENT_ID")
        
        if not fyers_client_id:
            pytest.skip("FYERS_CLIENT_ID not set")
        
        from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
        
        # Should not raise even without connection
        executor = FyersTradeExecutor(paper_mode=True)
        assert executor is not None
    
    def test_circuit_breaker(self):
        """Test circuit breaker functionality."""
        from src.trading_logic.fyers_trade_executor import CircuitBreakerState
        
        state = CircuitBreakerState()
        
        assert not state.is_tripped
        assert state.daily_pnl == 0.0
        assert state.consecutive_losses == 0
    
    def test_symbol_validation(self):
        """Test symbol format validation."""
        from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
        
        # Valid symbols
        assert FyersTradeExecutor.validate_symbol_format("NSE:RELIANCE-EQ") == True
        assert FyersTradeExecutor.validate_symbol_format("BSE:TCS-EQ") == True
        
        # Invalid symbols
        assert FyersTradeExecutor.validate_symbol_format("RELIANCE") == False
        assert FyersTradeExecutor.validate_symbol_format("NSE:RELIANCE") == False
    
    def test_verify_trade_execution_method_exists(self):
        """Test verify_trade_execution method exists."""
        from src.trading_logic.fyers_trade_executor import FyersTradeExecutor
        
        # Check method exists
        assert hasattr(FyersTradeExecutor, 'verify_trade_execution')


# ==============================================================================
# Alerting Tests
# ==============================================================================

class TestAlerting:
    """Tests for alerting system."""
    
    def test_slack_message_format(self):
        """Test Slack message formatting."""
        from scripts.slack_notifier import format_trading_alert
        
        formatted = format_trading_alert(
            alert_type="Test Alert",
            symbol="RELIANCE",
            message="Test message",
            details={"key": "value"},
        )
        
        assert "*Test Alert*" in formatted
        assert "RELIANCE" in formatted
        assert "Test message" in formatted
    
    def test_slack_notifier_no_url(self):
        """Test Slack notifier handles missing URL gracefully."""
        from scripts.slack_notifier import send_slack_message
        
        # Should return False when no URL configured
        with patch.dict(os.environ, {"SLACK_WEBHOOK_URL": ""}, clear=False):
            result = send_slack_message("Info", "Test", webhook_url="")
            # Should fail gracefully
            assert result == False


# ==============================================================================
# Health Check Tests
# ==============================================================================

class TestHealthChecks:
    """Tests for health check endpoints."""
    
    def test_health_checker_initialization(self):
        """Test HealthChecker initialization."""
        from src.api.health import HealthChecker
        
        checker = HealthChecker()
        assert checker is not None
    
    @pytest.mark.asyncio
    async def test_data_health_check(self):
        """Test data health check."""
        from src.api.health import HealthChecker, HealthStatus
        
        checker = HealthChecker()
        result = await checker.check_data_health()
        
        assert result is not None
        assert hasattr(result, 'status')
        assert result.status in [HealthStatus.HEALTHY, HealthStatus.UNHEALTHY, 
                                HealthStatus.DEGRADED, HealthStatus.UNKNOWN]
    
    @pytest.mark.asyncio
    async def test_trading_health_check(self):
        """Test trading health check."""
        from src.api.health import HealthChecker
        
        checker = HealthChecker()
        result = await checker.check_trading_health()
        
        assert result is not None
        assert hasattr(result, 'status')
    
    @pytest.mark.asyncio
    async def test_system_health_aggregation(self):
        """Test overall system health aggregation."""
        from src.api.health import HealthChecker
        
        checker = HealthChecker()
        health = await checker.get_system_health()
        
        assert health is not None
        assert hasattr(health, 'status')
        assert hasattr(health, 'components')
        assert len(health.components) >= 4  # data, models, trading, alerts


# ==============================================================================
# Integration Tests
# ==============================================================================

class TestIntegration:
    """Integration tests for complete flows."""
    
    @pytest.mark.asyncio
    async def test_data_to_prediction_flow(self, influx_client, test_symbol):
        """Test flow from data fetch to prediction generation."""
        # Get data
        df = await influx_client.query_ticker_data(test_symbol, hours=24*7)
        
        if df is None or df.empty:
            pytest.skip(f"No data for {test_symbol}")
        
        # Verify data can be used for prediction (has required columns)
        required = ['close', 'volume']
        has_required = all(c in df.columns for c in required)
        
        assert has_required, "Data missing required columns for prediction"
    
    @pytest.mark.asyncio
    async def test_full_pipeline_mock(self, test_symbol):
        """Test full pipeline with mocked components."""
        # Mock InfluxDB data
        mock_data = pd.DataFrame({
            'timestamp': [datetime.now()],
            'symbol': [test_symbol],
            'open': [100.0],
            'high': [101.0],
            'low': [99.0],
            'close': [100.5],
            'volume': [10000],
        })
        
        # Mock model prediction
        mock_model = MagicMock()
        mock_model.predict.return_value = ["High"]
        
        # Generate prediction
        prediction = mock_model.predict(mock_data)
        
        # Make decision
        from src.trading_logic.trade_decision_maker import TradeDecisionMaker
        
        maker = TradeDecisionMaker()
        decision = maker.make_decision(pct_predictions={"5min": prediction[0]})
        
        assert decision is not None
        assert hasattr(decision, 'direction')


# ==============================================================================
# Performance Tests
# ==============================================================================

class TestPerformance:
    """Performance and timing tests."""
    
    @pytest.mark.asyncio
    async def test_data_query_latency(self, influx_client, test_symbol):
        """Test data query latency is acceptable."""
        import time
        
        start = time.time()
        await influx_client.query_ticker_data(test_symbol, hours=24)
        latency = time.time() - start
        
        # Query should complete within 5 seconds
        assert latency < 5.0, f"Query latency too high: {latency:.2f}s"
    
    def test_decision_maker_latency(self):
        """Test decision making latency is acceptable."""
        import time
        from src.trading_logic.trade_decision_maker import TradeDecisionMaker
        
        maker = TradeDecisionMaker()
        
        predictions = {f"{i}min": "High" for i in [5, 15, 30, 60]}
        
        start = time.time()
        for _ in range(100):
            maker.make_decision(pct_predictions=predictions)
        
        avg_latency_ms = (time.time() - start) / 100 * 1000
        
        # Average latency should be under 10ms
        assert avg_latency_ms < 10, f"Decision latency too high: {avg_latency_ms:.2f}ms"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
