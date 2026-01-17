# src/distributed/actors/base.py
"""
Base actor class with common functionality for all trading actors.

Provides:
- Health monitoring and metrics collection
- Graceful shutdown handling
- Error recovery and retry logic
- Logging integration
"""
from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from loguru import logger
from pydantic import BaseModel, Field

# Type variable for actor state
T = TypeVar("T")


class ActorState(str, Enum):
    """Possible states for an actor."""
    INITIALIZING = "initializing"
    READY = "ready"
    PROCESSING = "processing"
    ERROR = "error"
    SHUTTING_DOWN = "shutting_down"
    STOPPED = "stopped"


@dataclass
class ActorMetrics:
    """Metrics collected by an actor."""
    # Counters
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_processing_time_ms: float = 0.0
    
    # Timing
    last_task_time: Optional[datetime] = None
    last_error_time: Optional[datetime] = None
    
    # Performance
    avg_task_duration_ms: float = 0.0
    max_task_duration_ms: float = 0.0
    min_task_duration_ms: float = float('inf')
    
    # Errors
    consecutive_errors: int = 0
    error_messages: list[str] = field(default_factory=list)
    
    def record_task(self, duration_ms: float, success: bool, error_msg: Optional[str] = None) -> None:
        """Record a task completion."""
        if success:
            self.tasks_completed += 1
            self.consecutive_errors = 0
            self.total_processing_time_ms += duration_ms
            self.avg_task_duration_ms = self.total_processing_time_ms / self.tasks_completed
            self.max_task_duration_ms = max(self.max_task_duration_ms, duration_ms)
            self.min_task_duration_ms = min(self.min_task_duration_ms, duration_ms)
            self.last_task_time = datetime.now()
        else:
            self.tasks_failed += 1
            self.consecutive_errors += 1
            self.last_error_time = datetime.now()
            if error_msg:
                self.error_messages.append(f"{datetime.now()}: {error_msg}")
                # Keep only last 10 errors
                self.error_messages = self.error_messages[-10:]


@dataclass
class ActorHealth:
    """Health status of an actor."""
    actor_id: str
    actor_type: str
    state: ActorState
    is_healthy: bool
    uptime_seconds: float
    metrics: ActorMetrics
    assigned_symbols: list[str] = field(default_factory=list)
    last_heartbeat: Optional[datetime] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "actor_id": self.actor_id,
            "actor_type": self.actor_type,
            "state": self.state.value,
            "is_healthy": self.is_healthy,
            "uptime_seconds": self.uptime_seconds,
            "tasks_completed": self.metrics.tasks_completed,
            "tasks_failed": self.metrics.tasks_failed,
            "avg_task_duration_ms": self.metrics.avg_task_duration_ms,
            "consecutive_errors": self.metrics.consecutive_errors,
            "assigned_symbols": self.assigned_symbols,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "error_message": self.error_message,
        }


class BaseActorConfig(BaseModel):
    """Base configuration for actors."""
    actor_id: str = Field(..., description="Unique identifier for this actor")
    max_retries: int = Field(default=3, ge=1, le=10)
    retry_delay_seconds: float = Field(default=1.0, ge=0.1, le=60.0)
    health_check_interval_seconds: float = Field(default=30.0, ge=5.0)
    max_consecutive_errors: int = Field(default=5, ge=1)


class BaseActor(ABC):
    """
    Abstract base class for all Ray actors in the trading system.
    
    Provides common functionality:
    - Health monitoring with heartbeat
    - Metrics collection
    - Error handling with retry logic
    - Graceful shutdown
    
    Subclasses must implement:
    - _initialize(): Setup actor-specific resources
    - _process(): Main processing logic
    - _cleanup(): Cleanup resources on shutdown
    """
    
    def __init__(self, config: BaseActorConfig) -> None:
        """
        Initialize the base actor.
        
        Args:
            config: Actor configuration
        """
        self._config = config
        self._actor_id = config.actor_id
        self._state = ActorState.INITIALIZING
        self._metrics = ActorMetrics()
        self._start_time = datetime.now()
        self._last_heartbeat = datetime.now()
        self._assigned_symbols: list[str] = []
        self._shutdown_requested = False
        
        logger.info(f"Actor {self._actor_id} initializing...")
    
    @property
    def actor_id(self) -> str:
        """Get actor ID."""
        return self._actor_id
    
    @property
    def state(self) -> ActorState:
        """Get current actor state."""
        return self._state
    
    @property
    def is_healthy(self) -> bool:
        """Check if actor is healthy."""
        return (
            self._state in (ActorState.READY, ActorState.PROCESSING)
            and self._metrics.consecutive_errors < self._config.max_consecutive_errors
        )
    
    def get_health(self) -> ActorHealth:
        """Get actor health status."""
        uptime = (datetime.now() - self._start_time).total_seconds()
        
        error_msg = None
        if self._metrics.error_messages:
            error_msg = self._metrics.error_messages[-1]
        
        return ActorHealth(
            actor_id=self._actor_id,
            actor_type=self.__class__.__name__,
            state=self._state,
            is_healthy=self.is_healthy,
            uptime_seconds=uptime,
            metrics=self._metrics,
            assigned_symbols=self._assigned_symbols.copy(),
            last_heartbeat=self._last_heartbeat,
            error_message=error_msg,
        )
    
    def get_metrics(self) -> ActorMetrics:
        """Get actor metrics."""
        return self._metrics
    
    def assign_symbols(self, symbols: list[str]) -> None:
        """
        Assign symbols to this actor for processing.
        
        Args:
            symbols: List of stock symbols to process
        """
        self._assigned_symbols = symbols.copy()
        logger.info(f"Actor {self._actor_id} assigned {len(symbols)} symbols: {symbols[:5]}...")
    
    def heartbeat(self) -> datetime:
        """Update heartbeat timestamp and return it."""
        self._last_heartbeat = datetime.now()
        return self._last_heartbeat
    
    async def initialize(self) -> bool:
        """
        Initialize actor resources.
        
        Returns:
            True if initialization successful
        """
        try:
            self._state = ActorState.INITIALIZING
            await self._initialize()
            self._state = ActorState.READY
            logger.info(f"Actor {self._actor_id} initialized successfully")
            return True
        except Exception as e:
            self._state = ActorState.ERROR
            self._metrics.record_task(0, False, f"Init failed: {e}")
            logger.error(f"Actor {self._actor_id} initialization failed: {e}")
            return False
    
    async def process(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute main processing with timing and error handling.
        
        Wraps _process() with metrics collection and retry logic.
        """
        if self._shutdown_requested:
            logger.warning(f"Actor {self._actor_id} received task during shutdown")
            return None
        
        start_time = time.perf_counter()
        self._state = ActorState.PROCESSING
        
        last_error: Optional[Exception] = None
        
        for attempt in range(self._config.max_retries):
            try:
                result = await self._process(*args, **kwargs)
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.record_task(duration_ms, True)
                self._state = ActorState.READY
                return result
                
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Actor {self._actor_id} attempt {attempt + 1}/{self._config.max_retries} "
                    f"failed: {e}"
                )
                if attempt < self._config.max_retries - 1:
                    await asyncio.sleep(self._config.retry_delay_seconds * (attempt + 1))
        
        # All retries exhausted
        duration_ms = (time.perf_counter() - start_time) * 1000
        error_msg = str(last_error) if last_error else "Unknown error"
        self._metrics.record_task(duration_ms, False, error_msg)
        
        if self._metrics.consecutive_errors >= self._config.max_consecutive_errors:
            self._state = ActorState.ERROR
            logger.error(f"Actor {self._actor_id} entered ERROR state after {self._metrics.consecutive_errors} consecutive errors")
        else:
            self._state = ActorState.READY
            
        raise last_error if last_error else RuntimeError("Processing failed")
    
    async def shutdown(self) -> None:
        """
        Gracefully shutdown the actor.
        
        Waits for current task to complete and cleans up resources.
        """
        logger.info(f"Actor {self._actor_id} shutting down...")
        self._shutdown_requested = True
        self._state = ActorState.SHUTTING_DOWN
        
        try:
            await self._cleanup()
        except Exception as e:
            logger.error(f"Actor {self._actor_id} cleanup error: {e}")
        
        self._state = ActorState.STOPPED
        logger.info(f"Actor {self._actor_id} shutdown complete")
    
    @abstractmethod
    async def _initialize(self) -> None:
        """
        Initialize actor-specific resources.
        
        Override in subclass to setup connections, load models, etc.
        """
        pass
    
    @abstractmethod
    async def _process(self, *args: Any, **kwargs: Any) -> Any:
        """
        Main processing logic.
        
        Override in subclass to implement actor-specific processing.
        """
        pass
    
    @abstractmethod
    async def _cleanup(self) -> None:
        """
        Cleanup actor resources.
        
        Override in subclass to close connections, save state, etc.
        """
        pass


# Debug & Verify
# ==============
# Run: python -c "from src.distributed.actors.base import BaseActor, ActorHealth; print('Base actor loaded')"
# Verify: No import errors
