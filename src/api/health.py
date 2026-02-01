# src/api/health.py
"""
Health Check API Endpoints

Provides component-specific health endpoints for monitoring:
- /health - Overall system health
- /health/data - Data collection/InfluxDB health
- /health/models - MLflow model availability
- /health/trading - Trading execution health
- /health/alerts - Alerting system health

Usage:
    # Run standalone for testing
    python -m src.api.health
    
    # Or import and mount in FastAPI app
    from src.api.health import health_router
    app.include_router(health_router)
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from loguru import logger

try:
    from fastapi import APIRouter, FastAPI
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    logger.warning("FastAPI not available, health endpoints will be disabled")


class HealthStatus(str, Enum):
    """Health status enum."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """Health status of a single component."""
    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    last_checked: str = ""
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}
        if not self.last_checked:
            self.last_checked = datetime.now().isoformat()


@dataclass
class SystemHealth:
    """Overall system health."""
    status: HealthStatus
    timestamp: str
    version: str
    uptime_seconds: float
    components: List[ComponentHealth]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'status': self.status.value,
            'timestamp': self.timestamp,
            'version': self.version,
            'uptime_seconds': self.uptime_seconds,
            'components': [
                {
                    'name': c.name,
                    'status': c.status.value,
                    'message': c.message,
                    'latency_ms': c.latency_ms,
                    'last_checked': c.last_checked,
                    'details': c.details,
                }
                for c in self.components
            ],
        }


class HealthChecker:
    """
    Health checker for all system components.
    
    Provides methods to check individual components and aggregate health.
    """
    
    _start_time: datetime = None
    
    def __init__(self):
        """Initialize health checker."""
        if HealthChecker._start_time is None:
            HealthChecker._start_time = datetime.now()
        self._cache: Dict[str, ComponentHealth] = {}
        self._cache_ttl_seconds = 30
    
    async def check_data_health(self) -> ComponentHealth:
        """Check data collection and InfluxDB health."""
        result = ComponentHealth(
            name="data",
            status=HealthStatus.HEALTHY,
        )
        
        try:
            import time
            start = time.time()
            
            # Check InfluxDB connection
            from dotenv import load_dotenv
            load_dotenv()
            
            influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
            influx_token = os.getenv("INFLUXDB_TOKEN")
            
            if not influx_token:
                result.status = HealthStatus.DEGRADED
                result.message = "INFLUXDB_TOKEN not configured"
                return result
            
            import requests
            response = requests.get(f"{influx_url}/health", timeout=5)
            result.latency_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "pass":
                    result.message = "InfluxDB healthy"
                    result.details["influx_version"] = data.get("version")
                else:
                    result.status = HealthStatus.DEGRADED
                    result.message = f"InfluxDB status: {data.get('status')}"
            else:
                result.status = HealthStatus.UNHEALTHY
                result.message = f"InfluxDB returned HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = HealthStatus.UNHEALTHY
            result.message = "Cannot connect to InfluxDB"
        except Exception as e:
            result.status = HealthStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    async def check_models_health(self) -> ComponentHealth:
        """Check MLflow model availability."""
        result = ComponentHealth(
            name="models",
            status=HealthStatus.HEALTHY,
        )
        
        try:
            import time
            start = time.time()
            
            # Check MLflow tracking server
            mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            
            import requests
            response = requests.get(f"{mlflow_uri}/health", timeout=5)
            result.latency_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                result.message = "MLflow server healthy"
                result.details["mlflow_uri"] = mlflow_uri
            elif response.status_code == 404:
                # MLflow might not have /health endpoint
                response = requests.get(f"{mlflow_uri}/api/2.0/mlflow/experiments/list", timeout=5)
                if response.status_code in [200, 401]:  # 401 = auth required but server is up
                    result.message = "MLflow server accessible"
                else:
                    result.status = HealthStatus.DEGRADED
                    result.message = "MLflow server issues"
            else:
                result.status = HealthStatus.UNHEALTHY
                result.message = f"MLflow returned HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = HealthStatus.UNHEALTHY
            result.message = "Cannot connect to MLflow"
        except Exception as e:
            result.status = HealthStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    async def check_trading_health(self) -> ComponentHealth:
        """Check trading execution health."""
        result = ComponentHealth(
            name="trading",
            status=HealthStatus.HEALTHY,
        )
        
        try:
            import time
            start = time.time()
            
            # Check Fyers credentials
            fyers_client_id = os.getenv("FYERS_CLIENT_ID")
            fyers_token = os.getenv("FYERS_ACCESS_TOKEN")
            
            if not fyers_client_id:
                result.status = HealthStatus.DEGRADED
                result.message = "FYERS_CLIENT_ID not configured"
                return result
            
            result.latency_ms = (time.time() - start) * 1000
            
            # Check trading hours (IST)
            now = datetime.now()
            market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
            market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
            
            is_market_hours = market_open <= now <= market_close
            is_weekday = now.weekday() < 5
            
            result.details["market_hours"] = is_market_hours and is_weekday
            result.details["fyers_configured"] = bool(fyers_client_id)
            result.details["token_available"] = bool(fyers_token)
            
            if fyers_token:
                result.message = "Trading system ready"
            else:
                result.status = HealthStatus.DEGRADED
                result.message = "Fyers token not available (need to authenticate)"
        
        except Exception as e:
            result.status = HealthStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    async def check_alerts_health(self) -> ComponentHealth:
        """Check alerting system health."""
        result = ComponentHealth(
            name="alerts",
            status=HealthStatus.HEALTHY,
        )
        
        try:
            import time
            start = time.time()
            
            # Check Slack webhook
            slack_url = os.getenv("SLACK_WEBHOOK_URL")
            telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
            
            channels_configured = []
            
            if slack_url:
                channels_configured.append("slack")
            
            if telegram_token:
                channels_configured.append("telegram")
            
            result.latency_ms = (time.time() - start) * 1000
            result.details["channels_configured"] = channels_configured
            
            if not channels_configured:
                result.status = HealthStatus.DEGRADED
                result.message = "No alert channels configured"
            else:
                result.message = f"Alert channels: {', '.join(channels_configured)}"
        
        except Exception as e:
            result.status = HealthStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    async def get_system_health(self) -> SystemHealth:
        """Get overall system health."""
        # Run all checks in parallel
        data, models, trading, alerts = await asyncio.gather(
            self.check_data_health(),
            self.check_models_health(),
            self.check_trading_health(),
            self.check_alerts_health(),
        )
        
        components = [data, models, trading, alerts]
        
        # Determine overall status
        unhealthy_count = sum(1 for c in components if c.status == HealthStatus.UNHEALTHY)
        degraded_count = sum(1 for c in components if c.status == HealthStatus.DEGRADED)
        
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        uptime = (datetime.now() - self._start_time).total_seconds()
        
        return SystemHealth(
            status=overall_status,
            timestamp=datetime.now().isoformat(),
            version=os.getenv("APP_VERSION", "1.0.0"),
            uptime_seconds=uptime,
            components=components,
        )


# Create router if FastAPI is available
health_router = APIRouter(prefix="/health", tags=["health"]) if FASTAPI_AVAILABLE else None
_health_checker = HealthChecker() if FASTAPI_AVAILABLE else None


if FASTAPI_AVAILABLE and health_router:
    
    @health_router.get("")
    async def get_health():
        """Get overall system health."""
        health = await _health_checker.get_system_health()
        status_code = 200 if health.status == HealthStatus.HEALTHY else 503
        return JSONResponse(content=health.to_dict(), status_code=status_code)
    
    @health_router.get("/data")
    async def get_data_health():
        """Get data collection health."""
        health = await _health_checker.check_data_health()
        status_code = 200 if health.status == HealthStatus.HEALTHY else 503
        return JSONResponse(
            content=asdict(health),
            status_code=status_code,
        )
    
    @health_router.get("/models")
    async def get_models_health():
        """Get MLflow models health."""
        health = await _health_checker.check_models_health()
        status_code = 200 if health.status == HealthStatus.HEALTHY else 503
        return JSONResponse(
            content=asdict(health),
            status_code=status_code,
        )
    
    @health_router.get("/trading")
    async def get_trading_health():
        """Get trading system health."""
        health = await _health_checker.check_trading_health()
        status_code = 200 if health.status == HealthStatus.HEALTHY else 503
        return JSONResponse(
            content=asdict(health),
            status_code=status_code,
        )
    
    @health_router.get("/alerts")
    async def get_alerts_health():
        """Get alerting system health."""
        health = await _health_checker.check_alerts_health()
        status_code = 200 if health.status == HealthStatus.HEALTHY else 503
        return JSONResponse(
            content=asdict(health),
            status_code=status_code,
        )
    
    @health_router.get("/live")
    async def liveness_probe():
        """Kubernetes liveness probe - always returns 200 if app is running."""
        return {"status": "alive"}
    
    @health_router.get("/ready")
    async def readiness_probe():
        """Kubernetes readiness probe - checks if app is ready to serve."""
        health = await _health_checker.get_system_health()
        if health.status == HealthStatus.UNHEALTHY:
            return JSONResponse(
                content={"status": "not_ready", "reason": "critical components unhealthy"},
                status_code=503,
            )
        return {"status": "ready"}


def create_health_app() -> FastAPI:
    """Create standalone health check FastAPI app."""
    if not FASTAPI_AVAILABLE:
        raise ImportError("FastAPI is required for health endpoints")
    
    app = FastAPI(
        title="Trading System Health API",
        description="Health check endpoints for the automated trading system",
        version="1.0.0",
    )
    
    app.include_router(health_router)
    
    return app


if __name__ == "__main__":
    # Run standalone health server
    import uvicorn
    
    app = create_health_app()
    uvicorn.run(app, host="0.0.0.0", port=8080)
