# tests/test_trade_executor.py
"""
Tests for TradeExecutorActor, RateLimiter, and CircuitBreaker.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.distributed.actors.trade_executor import (
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    TradeExecutorConfig,
)


class TestTradeExecutorConfig:
    """Tests for TradeExecutorConfig."""
    
    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = TradeExecutorConfig(actor_id="test")
        
        assert config.max_requests_per_second == 8.0
        assert config.circuit_failure_threshold == 5
        assert config.max_concurrent_positions == 10
        assert config.dry_run_mode is False
    
    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = TradeExecutorConfig(
            actor_id="test",
            max_requests_per_second=5.0,
            dry_run_mode=True,
            max_concurrent_positions=20
        )
        
        assert config.max_requests_per_second == 5.0
        assert config.dry_run_mode is True
        assert config.max_concurrent_positions == 20
    
    def test_validation_bounds(self) -> None:
        """Test configuration validation."""
        # Rate limit too low
        with pytest.raises(ValueError):
            TradeExecutorConfig(actor_id="test", max_requests_per_second=0.5)
        
        # Circuit threshold too low
        with pytest.raises(ValueError):
            TradeExecutorConfig(actor_id="test", circuit_failure_threshold=0)


class TestRateLimiter:
    """Tests for RateLimiter (token bucket)."""
    
    def test_initial_tokens(self) -> None:
        """Test rate limiter starts with full tokens."""
        limiter = RateLimiter(max_requests_per_second=10.0)
        
        # Should be able to acquire tokens immediately
        assert limiter.acquire() is True
        assert limiter.acquire() is True
    
    def test_token_exhaustion(self) -> None:
        """Test tokens get exhausted."""
        limiter = RateLimiter(max_requests_per_second=3.0)
        
        # Exhaust all tokens
        assert limiter.acquire() is True
        assert limiter.acquire() is True
        assert limiter.acquire() is True
        
        # Should be rate limited now
        assert limiter.acquire() is False
    
    def test_token_refill(self) -> None:
        """Test tokens refill over time."""
        limiter = RateLimiter(max_requests_per_second=10.0)
        
        # Exhaust all tokens
        for _ in range(10):
            limiter.acquire()
        
        # Should be empty
        assert limiter.acquire() is False
        
        # Wait for refill
        time.sleep(0.2)  # Wait 200ms for ~2 tokens
        
        # Should have some tokens now
        assert limiter.acquire() is True
    
    def test_get_wait_time(self) -> None:
        """Test wait time calculation."""
        limiter = RateLimiter(max_requests_per_second=10.0)
        
        # Initially no wait
        assert limiter.get_wait_time() == 0.0
        
        # Exhaust tokens
        for _ in range(15):  # More than available
            limiter.acquire()
        
        # Should have wait time now
        wait = limiter.get_wait_time()
        assert wait > 0
    
    @pytest.mark.asyncio
    async def test_acquire_async(self) -> None:
        """Test async token acquisition."""
        limiter = RateLimiter(max_requests_per_second=20.0)
        
        # Should acquire quickly
        start = time.perf_counter()
        await limiter.acquire_async()
        elapsed = time.perf_counter() - start
        
        assert elapsed < 0.1  # Should be nearly instant


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""
    
    @pytest.fixture
    def circuit(self) -> CircuitBreaker:
        """Create test circuit breaker."""
        return CircuitBreaker(
            failure_threshold=3,
            recovery_timeout_seconds=1.0,
            half_open_max_requests=2
        )
    
    def test_initial_state(self, circuit: CircuitBreaker) -> None:
        """Test circuit starts closed."""
        assert circuit.state == CircuitState.CLOSED
        assert circuit.can_execute() is True
    
    def test_success_recording(self, circuit: CircuitBreaker) -> None:
        """Test successful request recording."""
        circuit.record_success()
        
        assert circuit.stats.success_count == 1
        assert circuit.state == CircuitState.CLOSED
    
    def test_failure_threshold(self, circuit: CircuitBreaker) -> None:
        """Test circuit opens after threshold failures."""
        # Record failures up to threshold
        circuit.record_failure()
        assert circuit.state == CircuitState.CLOSED
        
        circuit.record_failure()
        assert circuit.state == CircuitState.CLOSED
        
        circuit.record_failure()  # Threshold reached
        assert circuit.state == CircuitState.OPEN
    
    def test_open_blocks_requests(self, circuit: CircuitBreaker) -> None:
        """Test open circuit blocks requests."""
        # Open the circuit
        for _ in range(3):
            circuit.record_failure()
        
        assert circuit.state == CircuitState.OPEN
        assert circuit.can_execute() is False
        assert circuit.stats.total_blocked_requests == 1
    
    def test_recovery_to_half_open(self, circuit: CircuitBreaker) -> None:
        """Test circuit transitions to half-open after timeout."""
        # Open the circuit
        for _ in range(3):
            circuit.record_failure()
        
        assert circuit.state == CircuitState.OPEN
        
        # Manipulate last failure time to simulate timeout
        circuit._stats.last_failure_time = datetime.now() - timedelta(seconds=2)
        
        # Check should trigger transition
        assert circuit.can_execute() is True
        assert circuit.state == CircuitState.HALF_OPEN
    
    def test_half_open_success_closes(self, circuit: CircuitBreaker) -> None:
        """Test success in half-open state closes circuit."""
        # Open the circuit
        for _ in range(3):
            circuit.record_failure()
        
        # Transition to half-open
        circuit._stats.last_failure_time = datetime.now() - timedelta(seconds=2)
        circuit.can_execute()
        
        assert circuit.state == CircuitState.HALF_OPEN
        
        # Record success
        circuit.record_success()
        
        assert circuit.state == CircuitState.CLOSED
        assert circuit.stats.failure_count == 0
    
    def test_half_open_failure_reopens(self, circuit: CircuitBreaker) -> None:
        """Test failure in half-open state reopens circuit."""
        # Open the circuit
        for _ in range(3):
            circuit.record_failure()
        
        # Transition to half-open
        circuit._stats.last_failure_time = datetime.now() - timedelta(seconds=2)
        circuit.can_execute()
        
        assert circuit.state == CircuitState.HALF_OPEN
        
        # Record failure
        circuit.record_failure()
        
        assert circuit.state == CircuitState.OPEN
    
    def test_half_open_request_limit(self, circuit: CircuitBreaker) -> None:
        """Test half-open limits concurrent requests."""
        # Open and transition to half-open
        for _ in range(3):
            circuit.record_failure()
        circuit._stats.last_failure_time = datetime.now() - timedelta(seconds=2)
        
        # First requests allowed
        assert circuit.can_execute() is True
        assert circuit.can_execute() is True
        
        # Third request blocked (max is 2)
        assert circuit.can_execute() is False


class TestTradeExecutorIntegration:
    """Integration tests for TradeExecutorActor."""
    
    @pytest.fixture
    def mock_signal(self) -> MagicMock:
        """Create mock trading signal."""
        signal = MagicMock()
        signal.symbol = "RELIANCE"
        signal.signal = "BUY"
        signal.score = 0.75
        signal.confidence = 0.85
        signal.price = 2500.0
        return signal
    
    @pytest.mark.asyncio
    @patch('src.distributed.actors.trade_executor.RAY_AVAILABLE', False)
    async def test_dry_run_execution(self, mock_signal: MagicMock) -> None:
        """Test trade execution in dry run mode."""
        # Import after patching
        from src.distributed.actors.trade_executor import TradeExecutorActor
        
        config = TradeExecutorConfig(
            actor_id="test",
            dry_run_mode=True
        )
        
        # Create actor directly (not as Ray actor)
        executor = TradeExecutorActor.__wrapped__(config)
        await executor.initialize()
        
        # Execute signal
        count = await executor.execute_signals([mock_signal])
        
        assert count == 1
        stats = executor.get_execution_stats()
        assert stats["orders_executed"] == 1
    
    @pytest.mark.asyncio
    @patch('src.distributed.actors.trade_executor.RAY_AVAILABLE', False)
    async def test_cooldown_blocking(self, mock_signal: MagicMock) -> None:
        """Test cooldown prevents rapid trades."""
        from src.distributed.actors.trade_executor import TradeExecutorActor
        
        config = TradeExecutorConfig(
            actor_id="test",
            dry_run_mode=True,
            cooldown_between_trades_seconds=60.0
        )
        
        executor = TradeExecutorActor.__wrapped__(config)
        await executor.initialize()
        
        # First trade should work
        count1 = await executor.execute_signals([mock_signal])
        assert count1 == 1
        
        # Second immediate trade should be blocked by cooldown
        count2 = await executor.execute_signals([mock_signal])
        assert count2 == 0


# Debug & Verify
# ==============
# Run: pytest tests/test_trade_executor.py -v
# Expected: All tests pass
