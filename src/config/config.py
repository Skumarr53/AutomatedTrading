import hydra
from omegaconf import DictConfig, OmegaConf
from pathlib import Path
from loguru import logger
import sys,os
from typing import Optional

def setup_logging(log_file_path: str = "logs/log_file.log"):
    """
    Set up the Loguru logger with console and file handlers.
    
    Args:
        log_file_path (str): Path to the log file. Defaults to "logs/log_file.log".
        log_level (str): Logging level. Defaults to "INFO".
    """

    if config.environment.app_settings.env == "prod":
        log_level = "ERROR"
        logger.debug(f"Setting log level for 'apscheduler' to {log_level}")
        logger.bind(name='apscheduler').level(log_level)
    else:
        logger.debug(f"Setting log level for 'apscheduler' to {log_level}")
        logger.bind(name='apscheduler').level(log_level)

    # Ensure log directory exists
    log_directory = Path(log_file_path).parent
    os.makedirs(log_directory, exist_ok=True)

    # Remove any existing handlers (useful when setting up logging multiple times in tests or notebooks)
    logger.remove()
    
    # Console Handler
    logger.add(sys.stdout, level=log_level, format="{time} | {level:10} | {message}")
    
    # File Handler with Rotation and Retention
    logger.add(log_file_path, level=log_level, format="{time} | {level:10} | {message}", rotation="10 MB", retention="7 days", compression="zip")

    logger.info("Logging setup completed.")


# If needed, initialize the logger during module import
setup_logging()

_config: Optional[DictConfig] = None

def get_config() -> DictConfig:
    global _config
    # If the configuration is not already loaded, initialize and compose it
    if _config is None:
        try:
            with hydra.initialize(config_path="."):
                _config = hydra.compose(config_name="config.yaml")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    return _config

config = get_config()
