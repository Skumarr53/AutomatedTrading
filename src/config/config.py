import hydra
from omegaconf import DictConfig
from functools import lru_cache

"""Configuration loading utilities."""



@lru_cache(maxsize=1)
def get_config() -> DictConfig:
    """Load and cache the Hydra configuration once."""
    try:
        with hydra.initialize(config_path="."):
            _config = hydra.compose(config_name="config.yaml")
        return _config
    except Exception as e:  # pragma: no cover - configuration errors are fatal
        raise RuntimeError(f"Error loading configuration: {e}")


