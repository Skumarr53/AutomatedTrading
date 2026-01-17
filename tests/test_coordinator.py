# tests/test_coordinator.py
"""
Tests for TradingCoordinator and distributed trading workflow.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.distributed.coordinator import (
    CoordinatorConfig,
    CycleMetrics,
    TradingCoordinator,
)


class TestCoordinatorConfig:
    """Tests for CoordinatorConfig."""
    
    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = CoordinatorConfig()
        
        assert config.cycle_timeout_seconds == 180.0
        assert config.target_cycle_duration_seconds == 180.0
        assert config.max_concurrent_fetches == 20
        assert config.max_concurrent_signals == 10
        assert config.enable_trade_execution is True
        assert config.dry_run_mode is False
    
    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = CoordinatorConfig(
            cycle_timeout_seconds=120.0,
            dry_run_mode=True,
            max_retries=5
        )
        
        assert config.cycle_timeout_seconds == 120.0
        assert config.dry_run_mode is True
        assert config.max_retries == 5
    
    def test_validation_bounds(self) -> None:
        """Test configuration validation."""
        # Should raise for invalid timeout
        with pytest.raises(ValueError):
            CoordinatorConfig(cycle_timeout_seconds=10.0)  # Below minimum
        
        with pytest.raises(ValueError):
            CoordinatorConfig(max_concurrent_fetches=200)  # Above maximum


class TestCycleMetrics:
    """Tests for CycleMetrics dataclass."""
    
    def test_metrics_creation(self) -> None:
        """Test metrics creation and defaults."""
        metrics = CycleMetrics(
            cycle_id="test_cycle_1",
            start_time=datetime.now()
        )
        
        assert metrics.cycle_id == "test_cycle_1"
        assert metrics.symbols_processed == 0
        assert metrics.symbols_failed == 0
        assert metrics.signals_generated == 0
        assert metrics.trades_executed == 0
        assert metrics.errors == []
    
    def test_metrics_to_dict(self) -> None:
        """Test metrics serialization."""
        metrics = CycleMetrics(
            cycle_id="test_cycle_2",
            start_time=datetime.now(),
            symbols_processed=10,
            signals_generated=5
        )
        metrics.end_time = datetime.now()
        metrics.total_duration_ms = 1500.0
        
        result = metrics.to_dict()
        
        assert result["cycle_id"] == "test_cycle_2"
        assert result["symbols_processed"] == 10
        assert result["signals_generated"] == 5
        assert result["total_duration_ms"] == 1500.0
        assert result["end_time"] is not None


class TestTradingCoordinator:
    """Tests for TradingCoordinator."""
    
    @pytest.fixture
    def config(self) -> CoordinatorConfig:
        """Create test configuration."""
        return CoordinatorConfig(
            cycle_timeout_seconds=60.0,
            dry_run_mode=True
        )
    
    @pytest.fixture
    def coordinator(self, config: CoordinatorConfig) -> TradingCoordinator:
        """Create test coordinator."""
        return TradingCoordinator(
            coord_config=config,
            data_ingestor_actors=[],
            signal_generator_actors=[],
            trade_executor_actor=None,
            fyers_instance=None
        )
    
    def test_initialization(self, coordinator: TradingCoordinator) -> None:
        """Test coordinator initialization."""
        assert coordinator.is_running is False
        assert coordinator.last_metrics is None
        assert coordinator._cycle_count == 0
    
    def test_get_status(self, coordinator: TradingCoordinator) -> None:
        """Test status reporting."""
        status = coordinator.get_status()
        
        assert status["is_running"] is False
        assert status["cycle_count"] == 0
        assert status["data_actors"] == 0
        assert status["signal_actors"] == 0
        assert status["has_executor"] is False
    
    @pytest.mark.asyncio
    async def test_run_cycle_no_actors(self, coordinator: TradingCoordinator) -> None:
        """Test running cycle with no actors."""
        metrics = await coordinator.run_cycle()
        
        assert metrics.cycle_id.startswith("cycle_1_")
        assert metrics.end_time is not None
        assert metrics.total_duration_ms > 0
    
    @pytest.mark.asyncio
    async def test_run_cycle_increments_count(self, coordinator: TradingCoordinator) -> None:
        """Test cycle count incrementing."""
        assert coordinator._cycle_count == 0
        
        await coordinator.run_cycle()
        assert coordinator._cycle_count == 1
        
        await coordinator.run_cycle()
        assert coordinator._cycle_count == 2
    
    def test_request_shutdown(self, coordinator: TradingCoordinator) -> None:
        """Test shutdown request."""
        assert coordinator._shutdown_requested is False
        
        coordinator.request_shutdown()
        
        assert coordinator._shutdown_requested is True


class TestCoordinatorWithMocks:
    """Tests for TradingCoordinator with mocked actors."""
    
    @pytest.fixture
    def mock_data_actor(self) -> MagicMock:
        """Create mock data ingestor actor."""
        actor = MagicMock()
        actor.fetch_all_symbols.remote.return_value = MagicMock()
        actor.get_all_cached_data.remote.return_value = MagicMock()
        return actor
    
    @pytest.fixture
    def mock_signal_actor(self) -> MagicMock:
        """Create mock signal generator actor."""
        actor = MagicMock()
        actor.process.remote.return_value = MagicMock()
        return actor
    
    @pytest.mark.asyncio
    @patch('src.distributed.coordinator.RAY_AVAILABLE', False)
    async def test_fetch_without_ray(self) -> None:
        """Test data fetching without Ray."""
        config = CoordinatorConfig(dry_run_mode=True)
        coordinator = TradingCoordinator(
            coord_config=config,
            data_ingestor_actors=[],
            signal_generator_actors=[],
        )
        
        results = await coordinator._fetch_all_data()
        assert results == []
    
    @pytest.mark.asyncio
    @patch('src.distributed.coordinator.RAY_AVAILABLE', False)
    async def test_generate_signals_without_data(self) -> None:
        """Test signal generation without data."""
        config = CoordinatorConfig(dry_run_mode=True)
        coordinator = TradingCoordinator(
            coord_config=config,
            data_ingestor_actors=[],
            signal_generator_actors=[],
        )
        
        signals = await coordinator._generate_all_signals()
        assert signals == []


# Debug & Verify
# ==============
# Run: pytest tests/test_coordinator.py -v
# Expected: All tests pass
