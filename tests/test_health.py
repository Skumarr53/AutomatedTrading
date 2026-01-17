# tests/test_health.py
"""
Tests for Health Check API.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.health import (
    ComponentHealth,
    HealthChecker,
    HealthResponse,
    HealthStatus,
    get_health_checker,
)


class TestHealthStatus:
    """Tests for HealthStatus enum."""
    
    def test_status_values(self) -> None:
        """Test health status values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"


class TestComponentHealth:
    """Tests for ComponentHealth model."""
    
    def test_component_creation(self) -> None:
        """Test component health creation."""
        component = ComponentHealth(
            name="test_component",
            status=HealthStatus.HEALTHY,
            latency_ms=50.0,
            message="All good"
        )
        
        assert component.name == "test_component"
        assert component.status == HealthStatus.HEALTHY
        assert component.latency_ms == 50.0
        assert component.message == "All good"
        assert component.last_check is not None
    
    def test_component_defaults(self) -> None:
        """Test component health defaults."""
        component = ComponentHealth(
            name="minimal",
            status=HealthStatus.DEGRADED
        )
        
        assert component.latency_ms is None
        assert component.message is None


class TestHealthResponse:
    """Tests for HealthResponse model."""
    
    def test_response_creation(self) -> None:
        """Test health response creation."""
        response = HealthResponse(
            status=HealthStatus.HEALTHY,
            uptime_seconds=3600.0
        )
        
        assert response.status == HealthStatus.HEALTHY
        assert response.uptime_seconds == 3600.0
        assert response.version == "1.0.0"
        assert response.components == []
    
    def test_response_with_components(self) -> None:
        """Test health response with components."""
        components = [
            ComponentHealth(name="db", status=HealthStatus.HEALTHY),
            ComponentHealth(name="cache", status=HealthStatus.DEGRADED),
        ]
        
        response = HealthResponse(
            status=HealthStatus.DEGRADED,
            uptime_seconds=100.0,
            components=components
        )
        
        assert len(response.components) == 2


class TestHealthChecker:
    """Tests for HealthChecker."""
    
    @pytest.fixture
    def checker(self) -> HealthChecker:
        """Create test health checker."""
        return HealthChecker()
    
    def test_uptime(self, checker: HealthChecker) -> None:
        """Test uptime calculation."""
        import time
        time.sleep(0.1)
        
        uptime = checker.uptime_seconds
        assert uptime >= 0.1
    
    def test_liveness(self, checker: HealthChecker) -> None:
        """Test liveness probe."""
        result = checker.get_liveness()
        
        assert result["status"] == "ok"
        assert "timestamp" in result
    
    @pytest.mark.asyncio
    async def test_check_influxdb_connection_error(self, checker: HealthChecker) -> None:
        """Test InfluxDB check with connection error."""
        with patch.dict('os.environ', {'INFLUXDB_URL': 'http://nonexistent:8086'}):
            result = await checker.check_influxdb()
        
        assert result.name == "influxdb"
        assert result.status == HealthStatus.UNHEALTHY
        assert "Connection failed" in (result.message or "")
    
    @pytest.mark.asyncio
    async def test_check_mlflow_connection_error(self, checker: HealthChecker) -> None:
        """Test MLflow check with connection error."""
        with patch.dict('os.environ', {'MLFLOW_TRACKING_URI': 'http://nonexistent:5000'}):
            result = await checker.check_mlflow()
        
        assert result.name == "mlflow"
        assert result.status == HealthStatus.UNHEALTHY
    
    @pytest.mark.asyncio
    async def test_check_ray_not_initialized(self, checker: HealthChecker) -> None:
        """Test Ray check when not initialized."""
        with patch('ray.is_initialized', return_value=False):
            result = await checker.check_ray()
        
        assert result.name == "ray"
        assert result.status == HealthStatus.UNHEALTHY
        assert "not initialized" in (result.message or "").lower()
    
    @pytest.mark.asyncio
    async def test_check_fyers_no_instance(self, checker: HealthChecker) -> None:
        """Test Fyers check without instance."""
        result = await checker.check_fyers(fyers_instance=None)
        
        assert result.name == "fyers"
        assert result.status == HealthStatus.DEGRADED
    
    def test_check_scheduler_not_running(self, checker: HealthChecker) -> None:
        """Test scheduler check when not running."""
        mock_scheduler = MagicMock()
        mock_scheduler.running = False
        
        result = checker.check_scheduler(scheduler=mock_scheduler)
        
        assert result.name == "scheduler"
        assert result.status == HealthStatus.UNHEALTHY
    
    def test_check_scheduler_running(self, checker: HealthChecker) -> None:
        """Test scheduler check when running."""
        mock_scheduler = MagicMock()
        mock_scheduler.running = True
        mock_scheduler.get_jobs.return_value = [MagicMock(), MagicMock()]
        
        result = checker.check_scheduler(scheduler=mock_scheduler)
        
        assert result.name == "scheduler"
        assert result.status == HealthStatus.HEALTHY
        assert "2 jobs" in (result.message or "")
    
    @pytest.mark.asyncio
    async def test_get_health_overall_healthy(self, checker: HealthChecker) -> None:
        """Test overall health when all components healthy."""
        # Mock all checks to return healthy
        with patch.object(checker, 'check_influxdb', return_value=ComponentHealth(
            name="influxdb", status=HealthStatus.HEALTHY
        )):
            with patch.object(checker, 'check_mlflow', return_value=ComponentHealth(
                name="mlflow", status=HealthStatus.HEALTHY
            )):
                with patch.object(checker, 'check_ray', return_value=ComponentHealth(
                    name="ray", status=HealthStatus.HEALTHY
                )):
                    with patch.object(checker, 'check_fyers', return_value=ComponentHealth(
                        name="fyers", status=HealthStatus.HEALTHY
                    )):
                        with patch.object(checker, 'check_scheduler', return_value=ComponentHealth(
                            name="scheduler", status=HealthStatus.HEALTHY
                        )):
                            response = await checker.get_health(check_all=True)
        
        assert response.status == HealthStatus.HEALTHY
        assert len(response.components) == 5
    
    @pytest.mark.asyncio
    async def test_get_health_overall_degraded(self, checker: HealthChecker) -> None:
        """Test overall health when one component degraded."""
        with patch.object(checker, 'check_influxdb', return_value=ComponentHealth(
            name="influxdb", status=HealthStatus.HEALTHY
        )):
            with patch.object(checker, 'check_mlflow', return_value=ComponentHealth(
                name="mlflow", status=HealthStatus.DEGRADED
            )):
                with patch.object(checker, 'check_ray', return_value=ComponentHealth(
                    name="ray", status=HealthStatus.HEALTHY
                )):
                    with patch.object(checker, 'check_fyers', return_value=ComponentHealth(
                        name="fyers", status=HealthStatus.HEALTHY
                    )):
                        with patch.object(checker, 'check_scheduler', return_value=ComponentHealth(
                            name="scheduler", status=HealthStatus.HEALTHY
                        )):
                            response = await checker.get_health(check_all=True)
        
        assert response.status == HealthStatus.DEGRADED
    
    @pytest.mark.asyncio
    async def test_get_health_overall_unhealthy(self, checker: HealthChecker) -> None:
        """Test overall health when one component unhealthy."""
        with patch.object(checker, 'check_influxdb', return_value=ComponentHealth(
            name="influxdb", status=HealthStatus.UNHEALTHY
        )):
            with patch.object(checker, 'check_mlflow', return_value=ComponentHealth(
                name="mlflow", status=HealthStatus.HEALTHY
            )):
                with patch.object(checker, 'check_ray', return_value=ComponentHealth(
                    name="ray", status=HealthStatus.HEALTHY
                )):
                    with patch.object(checker, 'check_fyers', return_value=ComponentHealth(
                        name="fyers", status=HealthStatus.HEALTHY
                    )):
                        with patch.object(checker, 'check_scheduler', return_value=ComponentHealth(
                            name="scheduler", status=HealthStatus.HEALTHY
                        )):
                            response = await checker.get_health(check_all=True)
        
        assert response.status == HealthStatus.UNHEALTHY
    
    @pytest.mark.asyncio
    async def test_get_readiness(self, checker: HealthChecker) -> None:
        """Test readiness probe."""
        mock_scheduler = MagicMock()
        mock_scheduler.running = True
        mock_scheduler.get_jobs.return_value = []
        
        with patch.object(checker, 'check_influxdb', return_value=ComponentHealth(
            name="influxdb", status=HealthStatus.HEALTHY
        )):
            result = await checker.get_readiness(scheduler=mock_scheduler)
        
        assert result["ready"] is True


class TestHealthCheckerSingleton:
    """Tests for global health checker instance."""
    
    def test_get_health_checker_singleton(self) -> None:
        """Test that get_health_checker returns same instance."""
        checker1 = get_health_checker()
        checker2 = get_health_checker()
        
        assert checker1 is checker2


# Debug & Verify
# ==============
# Run: pytest tests/test_health.py -v
# Expected: All tests pass
