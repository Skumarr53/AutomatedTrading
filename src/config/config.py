import hydra
import logging
from omegaconf import DictConfig, OmegaConf

import os
from typing import Optional

def setup_logging():
    log_format = "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')

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
            logging.error(f"Error loading configuration: {e}")
            raise
    return _config

config = get_config()
