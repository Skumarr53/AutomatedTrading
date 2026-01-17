# src/distributed/__init__.py
"""
Distributed computing module using Ray for parallel processing.

This module provides:
- Ray cluster initialization and management
- Base actor classes for data ingestion, signal generation, and trade execution
- Trading coordinator for orchestrating parallel workflows
- Utilities for distributed data sharing and coordination
"""
from src.distributed.ray_init import (
    RayClusterConfig,
    init_ray_cluster,
    shutdown_ray_cluster,
    get_ray_status,
    is_ray_initialized,
)

from src.distributed.coordinator import (
    TradingCoordinator,
    CoordinatorConfig,
    CycleMetrics,
)

__all__ = [
    # Ray management
    "RayClusterConfig",
    "init_ray_cluster",
    "shutdown_ray_cluster", 
    "get_ray_status",
    "is_ray_initialized",
    # Coordinator
    "TradingCoordinator",
    "CoordinatorConfig",
    "CycleMetrics",
]
