# src/api/server.py
"""
FastAPI server for health checks and metrics.

Runs alongside the main trading application to expose:
- /health - Full health status
- /healthz - Kubernetes liveness probe
- /ready - Kubernetes readiness probe
- /metrics - Prometheus metrics (future)
"""
from __future__ import annotations

import asyncio
import os
import signal
import threading
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from loguru import logger

from src.api.health import (
    HealthChecker,
    HealthStatus,
    SystemHealth,
)


# Global references for health checks
_app_instance: Optional[Any] = None
_shutdown_callback: Optional[Callable] = None


def set_app_instance(app: Any) -> None:
    """Set the main application instance for health checks."""
    global _app_instance
    _app_instance = app


def set_shutdown_callback(callback: Callable) -> None:
    """Set callback to invoke on shutdown signal."""
    global _shutdown_callback
    _shutdown_callback = callback


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Health API server starting...")
    yield
    logger.info("Health API server shutting down...")


# Create FastAPI app
api = FastAPI(
    title="Trading App Health API",
    description="Health and metrics endpoints for the automated trading application",
    version="1.0.0",
    lifespan=lifespan,
)


@api.get("/health", tags=["Health"])
async def health_check():
    """
    Full health check endpoint.
    
    Returns detailed status of all components:
    - InfluxDB connection
    - MLflow server
    - Ray cluster
    - Fyers API
    - Scheduler
    """
    checker = HealthChecker()
    
    health = await checker.get_system_health()
    
    # Set appropriate HTTP status
    status_code = 200 if health.status == HealthStatus.HEALTHY else 503
    
    return JSONResponse(
        content=health.to_dict(),
        status_code=status_code
    )


@api.get("/healthz", tags=["Health"])
async def liveness_probe():
    """
    Kubernetes liveness probe.
    
    Returns 200 if the process is alive.
    Used by K8s to determine if the container needs to be restarted.
    """
    return {"status": "alive"}


@api.get("/ready", tags=["Health"])
async def readiness_probe():
    """
    Kubernetes readiness probe.
    
    Returns 200 if the app is ready to serve traffic.
    Used by K8s to determine if traffic should be routed to this pod.
    """
    checker = HealthChecker()
    health = await checker.get_system_health()
    
    # Ready if not unhealthy
    ready = health.status != HealthStatus.UNHEALTHY
    status_code = 200 if ready else 503
    return JSONResponse(
        content={"ready": ready, "status": health.status.value},
        status_code=status_code
    )


@api.get("/metrics", tags=["Metrics"])
async def metrics():
    """
    Prometheus metrics endpoint.
    
    Returns metrics in Prometheus exposition format.
    """
    # Placeholder for future Prometheus integration
    metrics_data = []
    
    # Add basic metrics
    checker = HealthChecker()
    health = await checker.get_system_health()
    metrics_data.append("# HELP trading_app_uptime_seconds Application uptime in seconds")
    metrics_data.append("# TYPE trading_app_uptime_seconds gauge")
    metrics_data.append(f"trading_app_uptime_seconds {health.uptime_seconds:.2f}")
    
    # Add Ray metrics if available
    if _app_instance and hasattr(_app_instance, 'distributed_mode'):
        mode = 1 if _app_instance.distributed_mode else 0
        metrics_data.append(f"# HELP trading_app_distributed_mode Whether app is running in distributed mode")
        metrics_data.append(f"# TYPE trading_app_distributed_mode gauge")
        metrics_data.append(f"trading_app_distributed_mode {mode}")
    
    # Add cycle metrics if coordinator available
    if _app_instance and hasattr(_app_instance, '_trading_coordinator'):
        coord = _app_instance._trading_coordinator
        if coord and coord.last_metrics:
            m = coord.last_metrics
            metrics_data.append(f"# HELP trading_cycle_duration_ms Last cycle duration in milliseconds")
            metrics_data.append(f"# TYPE trading_cycle_duration_ms gauge")
            metrics_data.append(f"trading_cycle_duration_ms {m.total_duration_ms:.2f}")
            
            metrics_data.append(f"# HELP trading_symbols_processed Symbols processed in last cycle")
            metrics_data.append(f"# TYPE trading_symbols_processed gauge")
            metrics_data.append(f"trading_symbols_processed {m.symbols_processed}")
            
            metrics_data.append(f"# HELP trading_signals_generated Signals generated in last cycle")
            metrics_data.append(f"# TYPE trading_signals_generated gauge")
            metrics_data.append(f"trading_signals_generated {m.signals_generated}")
            
            metrics_data.append(f"# HELP trading_trades_executed Trades executed in last cycle")
            metrics_data.append(f"# TYPE trading_trades_executed gauge")
            metrics_data.append(f"trading_trades_executed {m.trades_executed}")
    
    # Add health check metrics
    health_status = checker.get_full_health()
    components_healthy = sum(1 for c in health_status.components.values() if c.status == "healthy")
    components_total = len(health_status.components)
    
    metrics_data.append(f"# HELP trading_components_healthy Number of healthy components")
    metrics_data.append(f"# TYPE trading_components_healthy gauge")
    metrics_data.append(f"trading_components_healthy {components_healthy}")
    
    metrics_data.append(f"# HELP trading_components_total Total number of components")
    metrics_data.append(f"# TYPE trading_components_total gauge")
    metrics_data.append(f"trading_components_total {components_total}")
    
    # Add InfluxDB write metrics (if available from health checker)
    for component_name, component in health_status.components.items():
        if "influxdb" in component_name.lower():
            metrics_data.append(f"# HELP trading_influxdb_healthy InfluxDB health status (1=healthy, 0=unhealthy)")
            metrics_data.append(f"# TYPE trading_influxdb_healthy gauge")
            metrics_data.append(f"trading_influxdb_healthy {1 if component.status == 'healthy' else 0}")
    
    return Response(
        content="\n".join(metrics_data),
        media_type="text/plain; version=0.0.4"
    )


@api.post("/shutdown", tags=["Admin"])
async def shutdown():
    """
    Graceful shutdown endpoint.
    
    Triggers application shutdown with cleanup.
    Should be protected in production.
    """
    if _shutdown_callback:
        logger.info("Shutdown requested via API")
        # Run shutdown in background
        asyncio.create_task(_async_shutdown())
        return {"status": "shutdown initiated"}
    else:
        return JSONResponse(
            content={"error": "Shutdown callback not configured"},
            status_code=500
        )


async def _async_shutdown():
    """Execute shutdown callback asynchronously."""
    await asyncio.sleep(0.5)  # Brief delay to send response
    if _shutdown_callback:
        _shutdown_callback()


def run_health_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    app_instance: Optional[Any] = None,
    shutdown_callback: Optional[Callable] = None,
) -> threading.Thread:
    """
    Run health server in a background thread.
    
    Args:
        host: Host to bind to
        port: Port to listen on
        app_instance: Main application instance for health checks
        shutdown_callback: Callback to invoke on shutdown
        
    Returns:
        Thread running the server
    """
    if app_instance:
        set_app_instance(app_instance)
    if shutdown_callback:
        set_shutdown_callback(shutdown_callback)
    
    config = uvicorn.Config(
        app=api,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    
    thread = threading.Thread(
        target=server.run,
        daemon=True,
        name="health-server"
    )
    thread.start()
    
    logger.info(f"Health API server running at http://{host}:{port}")
    return thread


# Debug & Verify
# ==============
# Run: uvicorn src.api.server:api --host 0.0.0.0 --port 8080 --reload
# Test: curl http://localhost:8080/healthz
