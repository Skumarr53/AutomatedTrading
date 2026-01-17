# src/distributed/actors/trade_executor.py
"""
Ray Actor for centralized trade execution with rate limiting and circuit breakers.

Single instance to prevent race conditions and ensure consistent position management.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

from loguru import logger
from pydantic import Field

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None

from src.distributed.actors.base import BaseActor, BaseActorConfig


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Blocking requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_state_change: datetime = field(default_factory=datetime.now)
    total_blocked_requests: int = 0


class TradeExecutorConfig(BaseActorConfig):
    """Configuration for TradeExecutorActor."""
    
    # Rate limiting (Fyers API limit: ~10 requests/second)
    max_requests_per_second: float = Field(default=8.0, ge=1.0, le=20.0)
    rate_limit_window_seconds: float = Field(default=1.0, ge=0.1)
    
    # Circuit breaker settings
    circuit_failure_threshold: int = Field(default=5, ge=1)
    circuit_recovery_timeout_seconds: float = Field(default=30.0, ge=5.0)
    circuit_half_open_max_requests: int = Field(default=3, ge=1)
    
    # Position management
    max_concurrent_positions: int = Field(default=10, ge=1, le=50)
    cooldown_between_trades_seconds: float = Field(default=60.0, ge=0.0)
    
    # Risk management
    max_position_value: float = Field(default=100000.0, ge=1000.0)
    risk_per_trade_percent: float = Field(default=2.0, ge=0.1, le=10.0)
    
    # Feature flags
    dry_run_mode: bool = Field(default=False)
    enable_short_selling: bool = Field(default=False)


def create_trade_executor_actor(config: TradeExecutorConfig):
    """
    Factory function to create a TradeExecutorActor.
    
    Args:
        config: Actor configuration
        
    Returns:
        TradeExecutorActor instance (Ray actor handle or regular object)
    """
    if RAY_AVAILABLE and ray.is_initialized():
        # Wrap class with ray.remote() at call time, then configure options
        return ray.remote(TradeExecutorActor).options(
            name="trade_executor",
            lifetime="detached",
            max_restarts=3,
            max_concurrency=1,  # Single-threaded to prevent race conditions
        ).remote(config)
    else:
        return TradeExecutorActor(config)


class RateLimiter:
    """Token bucket rate limiter for API calls."""
    
    def __init__(self, max_requests_per_second: float, window_seconds: float = 1.0) -> None:
        self._max_rate = max_requests_per_second
        self._window = window_seconds
        self._tokens = max_requests_per_second
        self._last_update = time.monotonic()
        self._request_times: deque = deque(maxlen=int(max_requests_per_second * 10))
    
    def acquire(self) -> bool:
        """
        Try to acquire a token for making a request.
        
        Returns:
            True if request allowed, False if rate limited
        """
        now = time.monotonic()
        
        # Refill tokens based on elapsed time
        elapsed = now - self._last_update
        self._tokens = min(self._max_rate, self._tokens + elapsed * self._max_rate)
        self._last_update = now
        
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            self._request_times.append(now)
            return True
        
        return False
    
    async def acquire_async(self) -> None:
        """Async version that waits until token available."""
        while not self.acquire():
            await asyncio.sleep(0.05)
    
    def get_wait_time(self) -> float:
        """Get estimated wait time until next token available."""
        if self._tokens >= 1.0:
            return 0.0
        return (1.0 - self._tokens) / self._max_rate


class CircuitBreaker:
    """Circuit breaker for API fault tolerance."""
    
    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout_seconds: float,
        half_open_max_requests: int,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = timedelta(seconds=recovery_timeout_seconds)
        self._half_open_max = half_open_max_requests
        self._stats = CircuitBreakerStats()
        self._half_open_requests = 0
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._stats.state
    
    @property
    def stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics."""
        return self._stats
    
    def can_execute(self) -> bool:
        """Check if request can proceed through circuit."""
        self._check_recovery()
        
        if self._stats.state == CircuitState.CLOSED:
            return True
        
        if self._stats.state == CircuitState.HALF_OPEN:
            if self._half_open_requests < self._half_open_max:
                self._half_open_requests += 1
                return True
            return False
        
        # OPEN state
        self._stats.total_blocked_requests += 1
        return False
    
    def record_success(self) -> None:
        """Record successful request."""
        self._stats.success_count += 1
        
        if self._stats.state == CircuitState.HALF_OPEN:
            # Successful request in half-open state -> close circuit
            self._transition_to(CircuitState.CLOSED)
            self._stats.failure_count = 0
            self._half_open_requests = 0
    
    def record_failure(self) -> None:
        """Record failed request."""
        self._stats.failure_count += 1
        self._stats.last_failure_time = datetime.now()
        
        if self._stats.state == CircuitState.HALF_OPEN:
            # Failure in half-open state -> reopen circuit
            self._transition_to(CircuitState.OPEN)
            self._half_open_requests = 0
        elif self._stats.failure_count >= self._failure_threshold:
            # Threshold exceeded -> open circuit
            self._transition_to(CircuitState.OPEN)
    
    def _check_recovery(self) -> None:
        """Check if circuit should transition to half-open."""
        if self._stats.state != CircuitState.OPEN:
            return
        
        if self._stats.last_failure_time:
            elapsed = datetime.now() - self._stats.last_failure_time
            if elapsed >= self._recovery_timeout:
                self._transition_to(CircuitState.HALF_OPEN)
                self._half_open_requests = 0
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to new state."""
        old_state = self._stats.state
        self._stats.state = new_state
        self._stats.last_state_change = datetime.now()
        logger.info(f"Circuit breaker: {old_state.value} -> {new_state.value}")


class TradeExecutorActor(BaseActor):
    """
    Ray Actor for centralized trade execution.
    
    Features:
    - Rate limiting (token bucket)
    - Circuit breaker for API fault tolerance
    - Position tracking and management
    - Order queue for batching
    
    Usage:
        config = TradeExecutorConfig(actor_id="executor")
        actor = create_trade_executor_actor(config)
        
        # Execute signals
        result = ray.get(actor.execute_signals.remote(signals))
    """
    
    def __init__(self, config: TradeExecutorConfig) -> None:
        """Initialize TradeExecutorActor."""
        super().__init__(config)
        self._exec_config = config
        
        # Rate limiter
        self._rate_limiter = RateLimiter(
            max_requests_per_second=config.max_requests_per_second,
            window_seconds=config.rate_limit_window_seconds,
        )
        
        # Circuit breaker
        self._circuit = CircuitBreaker(
            failure_threshold=config.circuit_failure_threshold,
            recovery_timeout_seconds=config.circuit_recovery_timeout_seconds,
            half_open_max_requests=config.circuit_half_open_max_requests,
        )
        
        # Position tracking
        self._positions: dict[str, dict] = {}
        self._last_trade_time: dict[str, datetime] = {}
        
        # Trade executor (Fyers)
        self._fyers_executor = None
        
        # Order queue
        self._pending_orders: deque = deque(maxlen=100)
        
        # Statistics
        self._orders_executed = 0
        self._orders_failed = 0
        self._orders_blocked = 0
    
    async def _initialize(self) -> None:
        """Initialize trade executor resources."""
        logger.info(f"TradeExecutorActor {self._actor_id} initialized")
        logger.info(f"  Rate limit: {self._exec_config.max_requests_per_second}/s")
        logger.info(f"  Dry run mode: {self._exec_config.dry_run_mode}")
    
    async def _process(self, *args: Any, **kwargs: Any) -> int:
        """
        Main processing - execute pending orders.
        
        Returns:
            Number of orders executed
        """
        return await self.execute_pending_orders()
    
    async def _cleanup(self) -> None:
        """Cleanup resources on shutdown."""
        # Process any remaining orders
        remaining = len(self._pending_orders)
        if remaining > 0:
            logger.warning(f"Shutdown with {remaining} pending orders")
        
        logger.info(
            f"TradeExecutorActor {self._actor_id} shutdown. "
            f"Executed: {self._orders_executed}, Failed: {self._orders_failed}, "
            f"Blocked: {self._orders_blocked}"
        )
    
    def set_fyers_executor(self, fyers_executor: Any) -> None:
        """
        Set the Fyers trade executor.
        
        Args:
            fyers_executor: FyersTradeExecutor instance
        """
        self._fyers_executor = fyers_executor
        logger.info(f"TradeExecutorActor: Fyers executor set")
    
    async def execute_signals(self, signals: list) -> int:
        """
        Execute trading signals.
        
        Args:
            signals: List of TradingSignal objects
            
        Returns:
            Number of trades executed
        """
        if not signals:
            return 0
        
        executed = 0
        
        for signal in signals:
            try:
                # Check cooldown for symbol
                if not self._check_cooldown(signal.symbol):
                    logger.debug(f"Cooldown active for {signal.symbol}")
                    continue
                
                # Check position limits
                if not self._check_position_limits(signal):
                    logger.debug(f"Position limit reached for {signal.symbol}")
                    continue
                
                # Execute trade
                success = await self._execute_single_trade(signal)
                if success:
                    executed += 1
                    self._last_trade_time[signal.symbol] = datetime.now()
                    
            except Exception as e:
                logger.error(f"Error executing signal for {signal.symbol}: {e}")
        
        return executed
    
    async def _execute_single_trade(self, signal: Any) -> bool:
        """
        Execute a single trade with rate limiting and circuit breaker.
        
        Args:
            signal: TradingSignal object
            
        Returns:
            True if trade executed successfully
        """
        # Check circuit breaker
        if not self._circuit.can_execute():
            logger.warning(f"Circuit open, blocking trade for {signal.symbol}")
            self._orders_blocked += 1
            return False
        
        # Acquire rate limit token
        await self._rate_limiter.acquire_async()
        
        try:
            if self._exec_config.dry_run_mode:
                logger.info(
                    f"[DRY RUN] Would execute {signal.signal} for {signal.symbol} "
                    f"(score: {signal.score:.2f}, confidence: {signal.confidence:.2f})"
                )
                self._orders_executed += 1
                self._circuit.record_success()
                return True
            
            if not self._fyers_executor:
                logger.warning("No Fyers executor available")
                return False
            
            # Execute through Fyers API
            success = await self._execute_via_fyers(signal)
            
            if success:
                self._orders_executed += 1
                self._circuit.record_success()
                self._update_position(signal)
            else:
                self._orders_failed += 1
                self._circuit.record_failure()
            
            return success
            
        except Exception as e:
            logger.error(f"Trade execution error for {signal.symbol}: {e}")
            self._orders_failed += 1
            self._circuit.record_failure()
            return False
    
    async def _execute_via_fyers(self, signal: Any) -> bool:
        """Execute trade via Fyers API."""
        from src.utils.utils import get_NSE_symbol
        
        fyers_symbol = get_NSE_symbol(signal.symbol)
        
        if signal.signal == "BUY":
            # Calculate position size
            qty = self._fyers_executor.calculate_position_size(
                symbol=fyers_symbol,
                price=getattr(signal, 'price', 0) or 100,  # Fallback price
                risk_percent=self._exec_config.risk_per_trade_percent,
                max_position_size=int(self._exec_config.max_position_value / 100),
            )
            
            if qty > 0:
                result = self._fyers_executor.place_market_order(
                    symbol=fyers_symbol,
                    qty=qty,
                    side="BUY",
                )
                return result is not None
            
        elif signal.signal == "SELL":
            # Check if we have a position to sell
            position = self._fyers_executor.get_position_for_symbol(fyers_symbol)
            if position and position.get('netQty', 0) > 0:
                return self._fyers_executor.exit_position(fyers_symbol)
        
        return False
    
    def _check_cooldown(self, symbol: str) -> bool:
        """Check if cooldown period has passed for symbol."""
        if symbol not in self._last_trade_time:
            return True
        
        elapsed = (datetime.now() - self._last_trade_time[symbol]).total_seconds()
        return elapsed >= self._exec_config.cooldown_between_trades_seconds
    
    def _check_position_limits(self, signal: Any) -> bool:
        """Check if position limits allow this trade."""
        if signal.signal == "SELL":
            return True  # Always allow closing positions
        
        # Check concurrent position limit
        current_positions = len([p for p in self._positions.values() if p.get('qty', 0) > 0])
        return current_positions < self._exec_config.max_concurrent_positions
    
    def _update_position(self, signal: Any) -> None:
        """Update local position tracking."""
        if signal.signal == "BUY":
            self._positions[signal.symbol] = {
                "symbol": signal.symbol,
                "qty": 1,  # Simplified
                "entry_time": datetime.now(),
            }
        elif signal.signal == "SELL":
            self._positions.pop(signal.symbol, None)
    
    async def execute_pending_orders(self) -> int:
        """Execute all pending orders in queue."""
        executed = 0
        
        while self._pending_orders:
            order = self._pending_orders.popleft()
            success = await self._execute_single_trade(order)
            if success:
                executed += 1
        
        return executed
    
    def queue_order(self, signal: Any) -> bool:
        """
        Add order to pending queue.
        
        Args:
            signal: TradingSignal object
            
        Returns:
            True if queued successfully
        """
        if len(self._pending_orders) >= self._pending_orders.maxlen:
            logger.warning("Order queue full, dropping order")
            return False
        
        self._pending_orders.append(signal)
        return True
    
    def get_execution_stats(self) -> dict[str, Any]:
        """Get execution statistics."""
        return {
            "orders_executed": self._orders_executed,
            "orders_failed": self._orders_failed,
            "orders_blocked": self._orders_blocked,
            "pending_orders": len(self._pending_orders),
            "positions": len(self._positions),
            "circuit_state": self._circuit.state.value,
            "circuit_failures": self._circuit.stats.failure_count,
            "rate_limit_wait": self._rate_limiter.get_wait_time(),
        }


# Keep original class for non-Ray usage
# The factory function create_trade_executor_actor handles Ray wrapping
# DO NOT apply ray.remote() here - it causes double-wrapping errors

# Debug & Verify
# ==============
# Run: python -c "from src.distributed.actors.trade_executor import TradeExecutorConfig; print(TradeExecutorConfig(actor_id='test').model_dump())"
# Verify: No import errors
