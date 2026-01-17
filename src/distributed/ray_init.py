# src/distributed/ray_init.py
"""
Ray cluster initialization and management.

Provides configuration and lifecycle management for Ray distributed computing.
Optimized for processing 100+ symbols with 5-minute intervals.
"""
from __future__ import annotations

import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

from loguru import logger
from pydantic import BaseModel, Field

# Lazy import Ray to allow graceful fallback
try:
    import ray
    from ray.util.state import list_actors
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    ray = None
    logger.warning("Ray not installed. Distributed processing unavailable.")


class RayClusterConfig(BaseModel):
    """Configuration for Ray cluster initialization."""
    
    # Cluster settings
    num_cpus: Optional[int] = Field(
        default=None, 
        description="Number of CPUs to use. None = all available"
    )
    num_gpus: Optional[int] = Field(
        default=None,
        description="Number of GPUs to use. None = all available"
    )
    memory: Optional[int] = Field(
        default=None,
        description="Memory limit in bytes. None = no limit"
    )
    object_store_memory: Optional[int] = Field(
        default=None,
        description="Object store memory limit. None = auto"
    )
    
    # Dashboard settings
    dashboard_host: str = Field(default="0.0.0.0")
    dashboard_port: int = Field(default=8265, ge=1024, le=65535)
    include_dashboard: bool = Field(default=True)
    
    # Prometheus metrics settings
    metrics_export_port: int = Field(
        default=8080, 
        ge=1024, 
        le=65535,
        description="Port for Prometheus metrics export"
    )
    
    # Actor settings for trading workload
    data_ingestor_actors: int = Field(
        default=5, 
        ge=1, 
        le=20,
        description="Number of data ingestor actors (each handles ~20 symbols)"
    )
    symbols_per_actor: int = Field(
        default=20,
        ge=1,
        le=50,
        description="Max symbols assigned to each ingestor actor"
    )
    signal_generator_actors: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of signal generator actors"
    )
    
    # Connection settings
    address: Optional[str] = Field(
        default=None,
        description="Ray cluster address. None = start new local cluster"
    )
    namespace: str = Field(default="trading")
    
    # Logging
    log_to_driver: bool = Field(default=True)
    logging_level: str = Field(default="INFO")


@dataclass
class RayClusterStatus:
    """Status information about Ray cluster."""
    is_initialized: bool = False
    num_nodes: int = 0
    num_cpus: float = 0.0
    num_gpus: float = 0.0
    object_store_memory: int = 0
    dashboard_url: Optional[str] = None
    actors: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def init_ray_cluster(config: Optional[RayClusterConfig] = None) -> bool:
    """
    Initialize Ray cluster for distributed trading operations.
    
    Args:
        config: Optional cluster configuration. Uses defaults if not provided.
        
    Returns:
        True if initialization successful, False otherwise.
        
    Example:
        >>> config = RayClusterConfig(num_cpus=8, data_ingestor_actors=4)
        >>> success = init_ray_cluster(config)
        >>> if success:
        ...     print("Ray cluster ready for trading")
    """
    if not RAY_AVAILABLE:
        logger.error("Ray is not installed. Install with: uv add 'ray[default]>=2.9.0'")
        return False
    
    if ray.is_initialized():
        # Verify Ray is actually connected, not just initialized
        try:
            # Check if Ray worker is actually connected
            worker = ray._private.worker.global_worker
            if not worker.connected:
                logger.warning("Ray is initialized but not connected, reinitializing...")
                # Ray is initialized but disconnected, shutdown and reinitialize
                try:
                    ray.shutdown()
                except Exception:
                    pass
                # Fall through to initialization below
            else:
                # Also verify cluster has alive nodes
                nodes = ray.nodes()
                alive_nodes = [n for n in nodes if n.get("Alive", False)]
                
                if not alive_nodes:
                    logger.warning("Ray is connected but no alive nodes found, reinitializing...")
                    try:
                        ray.shutdown()
                    except Exception:
                        pass
                    # Fall through to initialization below
                else:
                    logger.info(f"Ray already initialized and connected with {len(alive_nodes)} alive node(s), skipping initialization")
                    return True
        except Exception as e:
            logger.warning(f"Ray is initialized but verification failed: {e}. Reinitializing...")
            # Ray is initialized but verification failed, shutdown and reinitialize
            try:
                ray.shutdown()
            except Exception:
                pass
            # Fall through to initialization below
    
    config = config or RayClusterConfig()
    
    # Check RAY_ADDRESS environment variable - Ray uses this automatically
    # If it's set and we want to start local, we need to handle it
    ray_address_env = os.getenv("RAY_ADDRESS")
    if ray_address_env and not config.address:
        # RAY_ADDRESS is set in environment but config doesn't specify address
        # If RAY_ADDRESS is "auto", Ray will try to auto-detect, which can fail
        # We'll override it by explicitly starting local (address=None)
        logger.debug(f"RAY_ADDRESS env var is set to '{ray_address_env}', but config.address is None")
        logger.debug("Will start local cluster (RAY_ADDRESS will be ignored when address=None)")
    
    try:
        # Build initialization arguments
        init_kwargs: dict[str, Any] = {
            "namespace": config.namespace,
            "log_to_driver": config.log_to_driver,
            "logging_level": config.logging_level,
        }
        
        # Explicitly set address=None to start local cluster (overrides RAY_ADDRESS env var)
        # Only set address if config explicitly specifies one
        if config.address:
            init_kwargs["address"] = config.address
        else:
            # Explicitly start local cluster by setting address=None
            # This prevents Ray from using RAY_ADDRESS environment variable
            init_kwargs["address"] = None
        
        # Add optional resource limits
        if config.num_cpus is not None:
            init_kwargs["num_cpus"] = config.num_cpus
        if config.num_gpus is not None:
            init_kwargs["num_gpus"] = config.num_gpus
        if config.memory is not None:
            init_kwargs["_memory"] = config.memory
        if config.object_store_memory is not None:
            init_kwargs["object_store_memory"] = config.object_store_memory
        
        # Dashboard configuration
        if config.include_dashboard:
            init_kwargs["include_dashboard"] = True
            init_kwargs["dashboard_host"] = config.dashboard_host
            init_kwargs["dashboard_port"] = config.dashboard_port
            # Enable Prometheus metrics export
            init_kwargs["_metrics_export_port"] = config.metrics_export_port
        
        # Connect to existing cluster or start new one
        context = None
        
        if config.address:
            # Try to connect to remote cluster first
            try:
                logger.info(f"Attempting to connect to Ray cluster at {config.address}")
                context = ray.init(**init_kwargs)
                logger.info(f"Successfully connected to remote Ray cluster at {config.address}")
            except (ConnectionError, RuntimeError, Exception) as e:
                logger.warning(
                    f"Could not connect to Ray cluster at {config.address}: {e}. "
                    f"Starting local cluster instead."
                )
                # Remove address from kwargs and explicitly set to None to start local
                init_kwargs["address"] = None
                try:
                    context = ray.init(**init_kwargs)
                    logger.info("Started local Ray cluster as fallback")
                except Exception as fallback_error:
                    logger.error(f"Failed to start local Ray cluster as fallback: {fallback_error}")
                    raise
        else:
            # Start local cluster - explicitly set address=None to override RAY_ADDRESS env var
            logger.info("Starting local Ray cluster")
            
            # Temporarily unset RAY_ADDRESS if it's set, to force local cluster start
            ray_address_backup = None
            if "RAY_ADDRESS" in os.environ:
                ray_address_backup = os.environ.pop("RAY_ADDRESS")
                logger.debug(f"Temporarily unset RAY_ADDRESS={ray_address_backup} to start local cluster")
            
            try:
                # Ensure address is None (don't rely on Ray's env var detection)
                init_kwargs["address"] = None
                logger.debug(f"Calling ray.init() with address=None to start local cluster")
                context = ray.init(**init_kwargs)
                logger.debug("ray.init() completed successfully")
            except ConnectionError as conn_error:
                # If still getting connection error, RAY_ADDRESS might still be interfering
                error_msg = str(conn_error)
                if "RAY_ADDRESS" in error_msg or "Could not find any running Ray instance" in error_msg:
                    logger.error(
                        f"Ray failed to start local cluster. This may be due to RAY_ADDRESS environment variable. "
                        f"Error: {conn_error}"
                    )
                    logger.error(
                        "To fix: Unset RAY_ADDRESS environment variable or set it to empty string: "
                        "export RAY_ADDRESS='' or unset RAY_ADDRESS"
                    )
                raise
            finally:
                # Restore RAY_ADDRESS if it was set
                if ray_address_backup is not None:
                    os.environ["RAY_ADDRESS"] = ray_address_backup
                    logger.debug(f"Restored RAY_ADDRESS={ray_address_backup}")
        
        # Verify Ray is actually connected after initialization
        if not ray.is_initialized():
            raise RuntimeError("Ray initialization completed but ray.is_initialized() returns False")
        
        worker = ray._private.worker.global_worker
        if not worker.connected:
            raise RuntimeError("Ray initialization completed but worker.connected is False")
        
        # Verify cluster has resources
        try:
            resources = ray.available_resources()
            nodes = ray.nodes()
            alive_nodes = [n for n in nodes if n.get("Alive", False)]
            
            if not alive_nodes:
                raise RuntimeError("Ray initialized but no alive nodes found in cluster")
            
            logger.info(f"Ray cluster verified: {len(alive_nodes)} alive node(s)")
        except Exception as verify_error:
            logger.error(f"Ray cluster verification failed: {verify_error}")
            # Cleanup and re-raise
            try:
                ray.shutdown()
            except Exception:
                pass
            raise RuntimeError(f"Ray cluster verification failed: {verify_error}") from verify_error
        
        # Log cluster info
        dashboard_url = context.dashboard_url if hasattr(context, 'dashboard_url') else None
        logger.info(f"Ray cluster initialized successfully")
        logger.info(f"  Dashboard: {dashboard_url or 'disabled'}")
        logger.info(f"  Namespace: {config.namespace}")
        logger.info(f"  CPUs available: {ray.available_resources().get('CPU', 0)}")
        logger.info(f"  GPUs available: {ray.available_resources().get('GPU', 0)}")
        
        # Setup graceful shutdown handler
        _setup_shutdown_handlers()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize Ray cluster: {e}")
        return False


def shutdown_ray_cluster(graceful: bool = True) -> None:
    """
    Shutdown Ray cluster and cleanup resources.
    
    Args:
        graceful: If True, wait for actors to complete current tasks.
    """
    if not RAY_AVAILABLE or not ray.is_initialized():
        logger.debug("Ray not initialized, nothing to shutdown")
        return
    
    try:
        if graceful:
            logger.info("Initiating graceful Ray shutdown...")
            # TODO: Signal actors to complete current work
            
        ray.shutdown()
        logger.info("Ray cluster shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during Ray shutdown: {e}")


def get_ray_status() -> RayClusterStatus:
    """
    Get current status of Ray cluster.
    
    Returns:
        RayClusterStatus with cluster information.
    """
    status = RayClusterStatus()
    
    if not RAY_AVAILABLE:
        status.errors.append("Ray not installed")
        return status
    
    if not ray.is_initialized():
        status.errors.append("Ray not initialized")
        return status
    
    try:
        status.is_initialized = True
        
        # Get cluster resources
        resources = ray.available_resources()
        status.num_cpus = resources.get("CPU", 0)
        status.num_gpus = resources.get("GPU", 0)
        status.object_store_memory = int(resources.get("object_store_memory", 0))
        
        # Get node count
        nodes = ray.nodes()
        status.num_nodes = len([n for n in nodes if n.get("Alive", False)])
        
        # Get dashboard URL
        try:
            context = ray.get_runtime_context()
            if hasattr(context, 'gcs_address'):
                # Construct dashboard URL from GCS address
                gcs_host = context.gcs_address.split(":")[0]
                status.dashboard_url = f"http://{gcs_host}:8265"
        except Exception:
            pass
        
        # Count actors by type
        try:
            actors = list_actors(filters=[("state", "=", "ALIVE")])
            for actor in actors:
                actor_class = actor.get("class_name", "Unknown")
                status.actors[actor_class] = status.actors.get(actor_class, 0) + 1
        except Exception:
            # list_actors may not be available in all Ray versions
            pass
            
    except Exception as e:
        status.errors.append(f"Error getting status: {e}")
    
    return status


def is_ray_initialized() -> bool:
    """Check if Ray is initialized."""
    if not RAY_AVAILABLE:
        return False
    return ray.is_initialized()


def _setup_shutdown_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""
    def shutdown_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, initiating shutdown...")
        shutdown_ray_cluster(graceful=True)
        sys.exit(0)
    
    # Register handlers for common termination signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)


def create_config_from_hydra(hydra_config) -> RayClusterConfig:
    """
    Create RayClusterConfig from Hydra configuration.
    
    Args:
        hydra_config: Hydra DictConfig object with ray settings
        
    Returns:
        RayClusterConfig instance
    """
    ray_cfg = getattr(hydra_config, 'ray', {})
    actors_cfg = ray_cfg.get('actors', {})
    
    return RayClusterConfig(
        num_cpus=ray_cfg.get('num_cpus'),
        dashboard_host=ray_cfg.get('dashboard_host', '0.0.0.0'),
        dashboard_port=ray_cfg.get('dashboard_port', 8265),
        include_dashboard=ray_cfg.get('enabled', True),
        metrics_export_port=ray_cfg.get('metrics_export_port', 8080),
        data_ingestor_actors=actors_cfg.get('data_ingestor', {}).get('num_actors', 5),
        symbols_per_actor=actors_cfg.get('data_ingestor', {}).get('symbols_per_actor', 20),
        signal_generator_actors=actors_cfg.get('signal_generator', {}).get('num_actors', 4),
    )


# Debug & Verify
# ==============
# Run: python -c "from src.distributed.ray_init import init_ray_cluster; init_ray_cluster()"
# Verify: Look for "Ray cluster initialized" in logs
# Dashboard: http://localhost:8265
# REPL Test:
#   python -c "
#   from src.distributed import init_ray_cluster, get_ray_status
#   init_ray_cluster()
#   status = get_ray_status()
#   print(f'Initialized: {status.is_initialized}, CPUs: {status.num_cpus}')
#   "
