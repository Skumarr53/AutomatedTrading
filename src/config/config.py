import hydra
from omegaconf import DictConfig, OmegaConf
from pathlib import Path
from loguru import logger
import sys,os
from typing import Optional

# Load environment variables BEFORE Hydra initialization
# This ensures oc.env resolvers can access .env file variables
from dotenv import load_dotenv
load_dotenv()


def get_config() -> DictConfig:
    # global _config
    # If the configuration is not already loaded, initialize and compose it
    # if _config is None:
    try:
        with hydra.initialize(config_path="."):
            _config = hydra.compose(config_name="config.yaml") #version_base="1.1"
        return _config
    except Exception as e:
        raise f"Error loading configuration: {e}"
    

