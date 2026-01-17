# src/api/health.py
"""
Health Check API for the Trading Application.

Provides endpoints for:
- Liveness probe (is the app running?)
- Readiness probe (is the app ready to accept traffic?)
- Detailed health status (database, Ray, Fyers API connections)
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from loguru import logger
from pydantic import BaseModel, Field


class HealthStatus(str, Enum):
    """Health status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class ComponentHealth(BaseModel):
    """Health status for a single component."""
    name: str
    status: HealthStatus
    latency_ms: Optional[float] = None
    message: Optional[str] = None
    last_check: datetime = Field(default_factory=datetime.now)


class HealthResponse(BaseModel):
    """Complete health check response."""
    status: HealthStatus
    version: str = Field(default="1.0.0")
    uptime_seconds: float
    timestamp: datetime = Field(default_factory=datetime.now)
    components: list[ComponentHealth] = Field(default_factory=list)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class HealthChecker:
    """
    Health checker for trading application components.
    
    Checks:
    - InfluxDB connection
    - MLflow server availability
    - Ray cluster status
    - Fyers API connectivity
    - Scheduler status
    """
    
    def __init__(self) -> None:
        """Initialize health checker."""
        self._start_time = time.time()
        self._version = os.getenv("APP_VERSION", "1.0.0")
    
    @property
    def uptime_seconds(self) -> float:
        """Get application uptime in seconds."""
        return time.time() - self._start_time
    
    async def check_influxdb(self) -> ComponentHealth:
        """Check InfluxDB connectivity."""
        start = time.perf_counter()
        
        try:
            from influxdb_client import InfluxDBClient
            
            url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
            token = os.getenv("INFLUXDB_TOKEN", "")
            org = os.getenv("INFLUXDB_ORG", "trading")
            
            client = InfluxDBClient(url=url, token=token, org=org)
            health = client.health()
            client.close()
            
            latency = (time.perf_counter() - start) * 1000
            
            if health.status == "pass":
                return ComponentHealth(
                    name="influxdb",
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency,
                    message=f"Connected to {url}"
                )
            else:
                return ComponentHealth(
                    name="influxdb",
                    status=HealthStatus.DEGRADED,
                    latency_ms=latency,
                    message=f"InfluxDB status: {health.status}"
                )
                
        except Exception as e:
            return ComponentHealth(
                name="influxdb",
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    async def check_mlflow(self) -> ComponentHealth:
        """Check MLflow server connectivity."""
        start = time.perf_counter()
        
        try:
            import httpx
            
            url = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{url}/health")
                
            latency = (time.perf_counter() - start) * 1000
            
            if response.status_code == 200:
                return ComponentHealth(
                    name="mlflow",
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency,
                    message=f"Connected to {url}"
                )
            else:
                return ComponentHealth(
                    name="mlflow",
                    status=HealthStatus.DEGRADED,
                    latency_ms=latency,
                    message=f"HTTP {response.status_code}"
                )
                
        except Exception as e:
            return ComponentHealth(
                name="mlflow",
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                message=f"Connection failed: {str(e)}"
            )
    
    async def check_ray(self) -> ComponentHealth:
        """Check Ray cluster status."""
        start = time.perf_counter()
        
        try:
            import ray
            
            if not ray.is_initialized():
                return ComponentHealth(
                    name="ray",
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=(time.perf_counter() - start) * 1000,
                    message="Ray not initialized"
                )
            
            # Get cluster resources
            resources = ray.cluster_resources()
            latency = (time.perf_counter() - start) * 1000
            
            cpus = resources.get("CPU", 0)
            
            return ComponentHealth(
                name="ray",
                status=HealthStatus.HEALTHY,
                latency_ms=latency,
                message=f"Cluster active: {cpus} CPUs available"
            )
            
        except ImportError:
            return ComponentHealth(
                name="ray",
                status=HealthStatus.DEGRADED,
                latency_ms=(time.perf_counter() - start) * 1000,
                message="Ray not installed (running in sequential mode)"
            )
        except Exception as e:
            return ComponentHealth(
                name="ray",
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                message=f"Check failed: {str(e)}"
            )
    
    async def check_fyers(self, fyers_instance: Optional[Any] = None) -> ComponentHealth:
        """Check Fyers API connectivity."""
        start = time.perf_counter()
        
        try:
            if fyers_instance is None:
                return ComponentHealth(
                    name="fyers",
                    status=HealthStatus.DEGRADED,
                    latency_ms=(time.perf_counter() - start) * 1000,
                    message="Fyers instance not provided"
                )
            
            # Try to get profile (lightweight API call)
            response = fyers_instance.get_profile()
            latency = (time.perf_counter() - start) * 1000
            
            if response.get("s") == "ok":
                return ComponentHealth(
                    name="fyers",
                    status=HealthStatus.HEALTHY,
                    latency_ms=latency,
                    message="API connected"
                )
            else:
                return ComponentHealth(
                    name="fyers",
                    status=HealthStatus.DEGRADED,
                    latency_ms=latency,
                    message=f"API response: {response.get('message', 'unknown')}"
                )
                
        except Exception as e:
            return ComponentHealth(
                name="fyers",
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                message=f"API check failed: {str(e)}"
            )
    
    def check_scheduler(self, scheduler: Optional[Any] = None) -> ComponentHealth:
        """Check APScheduler status."""
        start = time.perf_counter()
        
        try:
            if scheduler is None:
                return ComponentHealth(
                    name="scheduler",
                    status=HealthStatus.DEGRADED,
                    latency_ms=(time.perf_counter() - start) * 1000,
                    message="Scheduler not provided"
                )
            
            if scheduler.running:
                jobs = scheduler.get_jobs()
                return ComponentHealth(
                    name="scheduler",
                    status=HealthStatus.HEALTHY,
                    latency_ms=(time.perf_counter() - start) * 1000,
                    message=f"Running with {len(jobs)} jobs"
                )
            else:
                return ComponentHealth(
                    name="scheduler",
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=(time.perf_counter() - start) * 1000,
                    message="Scheduler not running"
                )
                
        except Exception as e:
            return ComponentHealth(
                name="scheduler",
                status=HealthStatus.UNHEALTHY,
                latency_ms=(time.perf_counter() - start) * 1000,
                message=f"Check failed: {str(e)}"
            )
    
    async def get_health(
        self,
        fyers_instance: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        check_all: bool = True
    ) -> HealthResponse:
        """
        Get complete health status.
        
        Args:
            fyers_instance: Fyers API instance
            scheduler: APScheduler instance
            check_all: If True, checks all components; if False, minimal check
            
        Returns:
            HealthResponse with overall status and component details
        """
        components: list[ComponentHealth] = []
        
        if check_all:
            # Check all components in parallel
            import asyncio
            
            checks = await asyncio.gather(
                self.check_influxdb(),
                self.check_mlflow(),
                self.check_ray(),
                self.check_fyers(fyers_instance),
                return_exceptions=True
            )
            
            for check in checks:
                if isinstance(check, ComponentHealth):
                    components.append(check)
                elif isinstance(check, Exception):
                    logger.warning(f"Health check failed: {check}")
            
            # Add scheduler (sync check)
            components.append(self.check_scheduler(scheduler))
        
        # Determine overall status
        statuses = [c.status for c in components]
        
        if HealthStatus.UNHEALTHY in statuses:
            overall = HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY
        
        return HealthResponse(
            status=overall,
            version=self._version,
            uptime_seconds=self.uptime_seconds,
            components=components
        )
    
    def get_liveness(self) -> dict[str, Any]:
        """Simple liveness check (is the process alive?)."""
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat()
        }
    
    async def get_readiness(
        self,
        scheduler: Optional[Any] = None
    ) -> dict[str, Any]:
        """
        Readiness check (is the app ready to serve traffic?).
        
        Checks:
        - Scheduler is running
        - At least one critical component is healthy
        """
        # Check scheduler
        sched_health = self.check_scheduler(scheduler)
        
        # Quick InfluxDB check
        influx_health = await self.check_influxdb()
        
        is_ready = (
            sched_health.status != HealthStatus.UNHEALTHY and
            influx_health.status != HealthStatus.UNHEALTHY
        )
        
        return {
            "ready": is_ready,
            "timestamp": datetime.now().isoformat(),
            "scheduler": sched_health.status.value,
            "database": influx_health.status.value
        }


# Global health checker instance
_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """Get or create global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


# Debug & Verify
# ==============
# Run: python -c "from src.api.health import HealthChecker; print(HealthChecker().get_liveness())"
# Verify: Returns status ok with timestamp
