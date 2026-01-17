# src/distributed/actors/__init__.py
"""
Ray Actor classes for distributed trading operations.

Provides:
- DataIngestorActor: Parallel data fetching for multiple symbols
- SignalGeneratorActor: Parallel ML inference and signal generation
- TradeExecutorActor: Centralized order execution with rate limiting

Usage:
    # Use factory functions to create actors (handles Ray availability)
    from src.distributed.actors import create_data_ingestor_actor, DataIngestorConfig
    
    config = DataIngestorConfig(actor_id="ingestor_1")
    actor = create_data_ingestor_actor(config)  # Returns Ray handle or local instance
"""
from src.distributed.actors.base import BaseActor, ActorHealth, ActorMetrics
from src.distributed.actors.data_ingestor import (
    DataIngestorActor,
    DataIngestorConfig,
    create_data_ingestor_actor,
)
from src.distributed.actors.signal_generator import (
    SignalGeneratorActor,
    SignalGeneratorConfig,
    create_signal_generator_actor,
)
from src.distributed.actors.trade_executor import (
    TradeExecutorActor,
    TradeExecutorConfig,
    create_trade_executor_actor,
)

__all__ = [
    # Base classes
    "BaseActor",
    "ActorHealth",
    "ActorMetrics",
    # Data ingestion
    "DataIngestorActor",
    "DataIngestorConfig",
    "create_data_ingestor_actor",
    # Signal generation
    "SignalGeneratorActor",
    "SignalGeneratorConfig",
    "create_signal_generator_actor",
    # Trade execution
    "TradeExecutorActor",
    "TradeExecutorConfig",
    "create_trade_executor_actor",
]
