#!/usr/bin/env python3
"""
Monitoring Infrastructure Verification Script

Verifies that monitoring components are accessible and functioning:
1. Prometheus server connectivity
2. Grafana accessibility
3. InfluxDB health
4. Application metrics endpoints
5. Dashboard configuration

Usage:
    python scripts/verify_monitoring.py
    python scripts/verify_monitoring.py --prometheus-only
    python scripts/verify_monitoring.py --check-dashboards
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class ComponentStatus(str, Enum):
    """Status of a monitoring component."""
    UP = "UP"
    DOWN = "DOWN"
    DEGRADED = "DEGRADED"
    UNKNOWN = "UNKNOWN"


@dataclass
class ComponentCheck:
    """Result of checking a monitoring component."""
    component: str
    status: ComponentStatus
    message: str = ""
    url: str = ""
    response_time_ms: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)


class MonitoringVerifier:
    """
    Verify monitoring infrastructure components.
    
    Checks Prometheus, Grafana, InfluxDB, and application metrics.
    """
    
    def __init__(
        self,
        prometheus_url: str = "http://localhost:9090",
        grafana_url: str = "http://localhost:3000",
        influxdb_url: str = "http://localhost:8086",
        app_metrics_url: str = "http://localhost:8000",
    ):
        """
        Initialize verifier.
        
        Args:
            prometheus_url: Prometheus server URL
            grafana_url: Grafana server URL
            influxdb_url: InfluxDB server URL
            app_metrics_url: Application metrics endpoint URL
        """
        self.prometheus_url = prometheus_url
        self.grafana_url = grafana_url
        self.influxdb_url = influxdb_url
        self.app_metrics_url = app_metrics_url
    
    def check_prometheus(self) -> ComponentCheck:
        """Check Prometheus server health."""
        result = ComponentCheck(
            component="Prometheus",
            status=ComponentStatus.UP,
            url=self.prometheus_url,
        )
        
        try:
            import time
            start = time.time()
            
            # Check Prometheus health endpoint
            health_url = f"{self.prometheus_url}/-/healthy"
            response = requests.get(health_url, timeout=5)
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                result.message = "Prometheus is healthy"
                
                # Get some stats
                try:
                    status_url = f"{self.prometheus_url}/api/v1/status/runtimeinfo"
                    status_resp = requests.get(status_url, timeout=5)
                    if status_resp.status_code == 200:
                        data = status_resp.json()
                        if data.get("status") == "success":
                            info = data.get("data", {})
                            result.details["start_time"] = info.get("startTime")
                            result.details["goroutines"] = info.get("goroutines")
                except Exception:
                    pass
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = ComponentStatus.DOWN
            result.message = "Connection refused - is Prometheus running?"
        except requests.exceptions.Timeout:
            result.status = ComponentStatus.DEGRADED
            result.message = "Request timed out"
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def check_prometheus_targets(self) -> ComponentCheck:
        """Check Prometheus scrape targets status."""
        result = ComponentCheck(
            component="Prometheus Targets",
            status=ComponentStatus.UP,
            url=f"{self.prometheus_url}/api/v1/targets",
        )
        
        try:
            import time
            start = time.time()
            
            response = requests.get(result.url, timeout=5)
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    targets = data.get("data", {}).get("activeTargets", [])
                    
                    up_count = sum(1 for t in targets if t.get("health") == "up")
                    down_count = sum(1 for t in targets if t.get("health") == "down")
                    
                    result.details["total_targets"] = len(targets)
                    result.details["up"] = up_count
                    result.details["down"] = down_count
                    
                    if down_count > 0:
                        result.status = ComponentStatus.DEGRADED
                        result.message = f"{down_count}/{len(targets)} targets down"
                    else:
                        result.message = f"All {len(targets)} targets healthy"
                else:
                    result.status = ComponentStatus.UNKNOWN
                    result.message = "Unexpected response format"
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def check_grafana(self) -> ComponentCheck:
        """Check Grafana server health."""
        result = ComponentCheck(
            component="Grafana",
            status=ComponentStatus.UP,
            url=self.grafana_url,
        )
        
        try:
            import time
            start = time.time()
            
            # Check Grafana health endpoint
            health_url = f"{self.grafana_url}/api/health"
            response = requests.get(health_url, timeout=5)
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                db_status = data.get("database", "unknown")
                
                if db_status == "ok":
                    result.message = "Grafana is healthy"
                    result.details["version"] = data.get("version")
                else:
                    result.status = ComponentStatus.DEGRADED
                    result.message = f"Database status: {db_status}"
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = ComponentStatus.DOWN
            result.message = "Connection refused - is Grafana running?"
        except requests.exceptions.Timeout:
            result.status = ComponentStatus.DEGRADED
            result.message = "Request timed out"
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def check_grafana_datasources(self, api_key: Optional[str] = None) -> ComponentCheck:
        """Check Grafana data sources configuration."""
        result = ComponentCheck(
            component="Grafana Datasources",
            status=ComponentStatus.UP,
            url=f"{self.grafana_url}/api/datasources",
        )
        
        api_key = api_key or os.getenv("GRAFANA_API_KEY")
        
        if not api_key:
            result.status = ComponentStatus.UNKNOWN
            result.message = "GRAFANA_API_KEY not set - cannot check datasources"
            return result
        
        try:
            import time
            start = time.time()
            
            headers = {"Authorization": f"Bearer {api_key}"}
            response = requests.get(result.url, headers=headers, timeout=5)
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                datasources = response.json()
                result.details["count"] = len(datasources)
                result.details["datasources"] = [ds.get("name") for ds in datasources]
                result.message = f"Found {len(datasources)} datasource(s)"
            elif response.status_code == 401:
                result.status = ComponentStatus.UNKNOWN
                result.message = "Invalid API key"
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def check_influxdb(self) -> ComponentCheck:
        """Check InfluxDB health."""
        result = ComponentCheck(
            component="InfluxDB",
            status=ComponentStatus.UP,
            url=self.influxdb_url,
        )
        
        try:
            import time
            start = time.time()
            
            # Check InfluxDB health endpoint
            health_url = f"{self.influxdb_url}/health"
            response = requests.get(health_url, timeout=5)
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                data = response.json()
                status = data.get("status")
                
                if status == "pass":
                    result.message = "InfluxDB is healthy"
                    result.details["name"] = data.get("name")
                    result.details["version"] = data.get("version")
                else:
                    result.status = ComponentStatus.DEGRADED
                    result.message = f"Status: {status}"
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = ComponentStatus.DOWN
            result.message = "Connection refused - is InfluxDB running?"
        except requests.exceptions.Timeout:
            result.status = ComponentStatus.DEGRADED
            result.message = "Request timed out"
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def check_app_metrics(self) -> ComponentCheck:
        """Check application Prometheus metrics endpoint."""
        result = ComponentCheck(
            component="App Metrics",
            status=ComponentStatus.UP,
            url=f"{self.app_metrics_url}/metrics",
        )
        
        try:
            import time
            start = time.time()
            
            response = requests.get(result.url, timeout=5)
            
            result.response_time_ms = (time.time() - start) * 1000
            
            if response.status_code == 200:
                # Count metrics (lines starting without #)
                lines = response.text.strip().split('\n')
                metric_lines = [l for l in lines if l and not l.startswith('#')]
                
                result.message = f"Serving {len(metric_lines)} metrics"
                result.details["metric_count"] = len(metric_lines)
            elif response.status_code == 404:
                result.status = ComponentStatus.DOWN
                result.message = "Metrics endpoint not found"
            else:
                result.status = ComponentStatus.DOWN
                result.message = f"HTTP {response.status_code}"
        
        except requests.exceptions.ConnectionError:
            result.status = ComponentStatus.DOWN
            result.message = "Connection refused - is the app running?"
        except requests.exceptions.Timeout:
            result.status = ComponentStatus.DEGRADED
            result.message = "Request timed out"
        except Exception as e:
            result.status = ComponentStatus.UNKNOWN
            result.message = str(e)
        
        return result
    
    def run_all_checks(self) -> List[ComponentCheck]:
        """Run all monitoring checks."""
        checks = [
            self.check_prometheus,
            self.check_prometheus_targets,
            self.check_grafana,
            self.check_influxdb,
            self.check_app_metrics,
        ]
        
        results = []
        for check_func in checks:
            try:
                result = check_func()
            except Exception as e:
                result = ComponentCheck(
                    component=check_func.__name__,
                    status=ComponentStatus.UNKNOWN,
                    message=str(e),
                )
            results.append(result)
        
        return results


def print_results(results: List[ComponentCheck]) -> None:
    """Print check results to console."""
    status_emoji = {
        ComponentStatus.UP: "✅",
        ComponentStatus.DOWN: "❌",
        ComponentStatus.DEGRADED: "⚠️",
        ComponentStatus.UNKNOWN: "❓",
    }
    
    print("\n" + "=" * 70)
    print("MONITORING INFRASTRUCTURE STATUS")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 70)
    
    for result in results:
        emoji = status_emoji[result.status]
        time_str = f"{result.response_time_ms:.0f}ms" if result.response_time_ms > 0 else "-"
        
        print(f"{emoji} {result.component:<25} {result.status.value:<10} {time_str:>10}")
        if result.message:
            print(f"   └─ {result.message}")
        if result.details:
            for key, value in result.details.items():
                print(f"      • {key}: {value}")
    
    print("-" * 70)
    
    # Summary
    up_count = sum(1 for r in results if r.status == ComponentStatus.UP)
    down_count = sum(1 for r in results if r.status == ComponentStatus.DOWN)
    degraded_count = sum(1 for r in results if r.status == ComponentStatus.DEGRADED)
    
    print(f"Summary: {up_count} up, {down_count} down, {degraded_count} degraded")
    
    if down_count > 0:
        print("\n⚠️  Some components are DOWN. Please check:")
        for r in results:
            if r.status == ComponentStatus.DOWN:
                print(f"   • {r.component}: {r.message}")
    
    print("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Verify monitoring infrastructure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("--prometheus-url", type=str, default="http://localhost:9090",
                        help="Prometheus server URL")
    parser.add_argument("--grafana-url", type=str, default="http://localhost:3000",
                        help="Grafana server URL")
    parser.add_argument("--influxdb-url", type=str, default="http://localhost:8086",
                        help="InfluxDB server URL")
    parser.add_argument("--app-metrics-url", type=str, default="http://localhost:8000",
                        help="Application metrics URL")
    parser.add_argument("--prometheus-only", action="store_true",
                        help="Check only Prometheus")
    parser.add_argument("--check-dashboards", action="store_true",
                        help="Check Grafana dashboards (requires API key)")
    
    args = parser.parse_args()
    
    load_dotenv()
    
    verifier = MonitoringVerifier(
        prometheus_url=args.prometheus_url,
        grafana_url=args.grafana_url,
        influxdb_url=args.influxdb_url,
        app_metrics_url=args.app_metrics_url,
    )
    
    if args.prometheus_only:
        results = [
            verifier.check_prometheus(),
            verifier.check_prometheus_targets(),
        ]
    elif args.check_dashboards:
        results = verifier.run_all_checks()
        results.append(verifier.check_grafana_datasources())
    else:
        results = verifier.run_all_checks()
    
    print_results(results)
    
    # Exit with failure code if any components are down
    has_failures = any(r.status == ComponentStatus.DOWN for r in results)
    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
