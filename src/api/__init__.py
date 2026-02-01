# src/api/__init__.py
"""
API module for the AutomatedTrading system.

Provides REST API endpoints for:
- Health checks
- System status
- Trading operations
"""

from src.api.health import health_router, HealthChecker, HealthStatus

__all__ = [
    "health_router",
    "HealthChecker",
    "HealthStatus",
]
