import hydra
from omegaconf import DictConfig
from loguru import logger


def get_config() -> DictConfig:
    """Load the application configuration using Hydra."""
    try:
        with hydra.initialize(config_path="."):
            return hydra.compose(config_name="config.yaml")
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise
