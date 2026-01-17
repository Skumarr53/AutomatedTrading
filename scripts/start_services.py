#!/usr/bin/env python3
"""
scripts/start_services.py
Check and start required container services if not running.

This script ensures all required services (InfluxDB, MLflow) are running
before the application starts.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from loguru import logger

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

REQUIRED_SERVICES = ["trading-influxdb", "trading-mlflow"]
COMPOSE_FILE = PROJECT_ROOT / "compose.yml"
MAX_WAIT_SECONDS = 120  # Maximum time to wait for services to become healthy


def check_podman_available() -> bool:
    """Check if podman is available."""
    try:
        subprocess.run(
            ["podman", "--version"],
            capture_output=True,
            check=True,
            timeout=5
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def check_podman_compose_available() -> bool:
    """Check if podman-compose is available."""
    # Try podman-compose first
    try:
        subprocess.run(
            ["podman-compose", "--version"],
            capture_output=True,
            check=True,
            timeout=5
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    
    # Try podman compose
    try:
        subprocess.run(
            ["podman", "compose", "version"],
            capture_output=True,
            check=True,
            timeout=5
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def get_running_containers() -> List[str]:
    """Get list of running container names."""
    try:
        result = subprocess.run(
            ["podman", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )
        return [name.strip() for name in result.stdout.strip().split("\n") if name.strip()]
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Failed to get running containers: {e}")
        return []


def is_container_healthy(container_name: str) -> bool:
    """Check if a container is healthy."""
    try:
        result = subprocess.run(
            ["podman", "inspect", "--format", "{{.State.Health.Status}}", container_name],
            capture_output=True,
            text=True,
            check=True,
            timeout=5
        )
        health_status = result.stdout.strip()
        # "healthy" or "none" (no healthcheck) are both acceptable
        return health_status in ("healthy", "none")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def start_services() -> bool:
    """Start services using podman-compose."""
    if not COMPOSE_FILE.exists():
        logger.error(f"Compose file not found: {COMPOSE_FILE}")
        return False
    
    # Determine which command to use
    if check_podman_compose_available():
        # Try podman-compose first
        try:
            subprocess.run(["podman-compose", "--version"], capture_output=True, check=True)
            compose_cmd = "podman-compose"
        except:
            compose_cmd = "podman compose"
    else:
        logger.error("podman-compose not available")
        return False
    
    try:
        logger.info(f"Starting services using {compose_cmd}...")
        
        # Ensure data directories exist
        ensure_script = PROJECT_ROOT / "scripts" / "ensure_data_directories.sh"
        if ensure_script.exists():
            subprocess.run(["bash", str(ensure_script)], check=True, timeout=30)
        
        # Start services
        cmd = [compose_cmd.split()[0]] + compose_cmd.split()[1:] + ["-f", str(COMPOSE_FILE), "up", "-d"]
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            logger.error(f"Failed to start services: {result.stderr}")
            return False
        
        logger.info("Services started, waiting for health checks...")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Timeout starting services")
        return False
    except Exception as e:
        logger.error(f"Error starting services: {e}")
        return False


def wait_for_health(container_name: str, max_wait: int = MAX_WAIT_SECONDS) -> bool:
    """Wait for container to become healthy."""
    start_time = time.time()
    check_interval = 5
    
    while time.time() - start_time < max_wait:
        if is_container_healthy(container_name):
            return True
        time.sleep(check_interval)
        logger.debug(f"Waiting for {container_name} to become healthy...")
    
    return False


def ensure_containers_running() -> bool:
    """
    Check if all required containers are running and healthy.
    Start them if missing.
    
    Returns:
        True if all containers are healthy, False otherwise
    """
    if not check_podman_available():
        logger.error("podman not available. Please install podman.")
        return False
    
    running_containers = get_running_containers()
    missing_containers = []
    unhealthy_containers = []
    
    # Check each required service
    for service in REQUIRED_SERVICES:
        if service not in running_containers:
            missing_containers.append(service)
        elif not is_container_healthy(service):
            unhealthy_containers.append(service)
    
    # If all healthy, return success
    if not missing_containers and not unhealthy_containers:
        logger.info("All required containers are running and healthy ✓")
        return True
    
    # Log status
    if missing_containers:
        logger.warning(f"Missing containers: {', '.join(missing_containers)}")
    if unhealthy_containers:
        logger.warning(f"Unhealthy containers: {', '.join(unhealthy_containers)}")
    
    # Start missing services
    if missing_containers:
        logger.info("Starting missing services...")
        if not start_services():
            logger.error("Failed to start services")
            return False
        
        # Wait for services to become healthy
        all_healthy = True
        for service in missing_containers:
            logger.info(f"Waiting for {service} to become healthy...")
            if not wait_for_health(service):
                logger.error(f"{service} did not become healthy within timeout")
                all_healthy = False
        
        if not all_healthy:
            return False
    
    # Re-check unhealthy containers
    if unhealthy_containers:
        logger.warning("Some containers are unhealthy. They may recover automatically.")
        # Give them some time
        time.sleep(10)
        for service in unhealthy_containers:
            if not is_container_healthy(service):
                logger.error(f"{service} is still unhealthy")
                return False
    
    logger.success("All containers are running and healthy ✓")
    return True


if __name__ == "__main__":
    success = ensure_containers_running()
    sys.exit(0 if success else 1)
