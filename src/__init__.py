from src.config.config import get_config
from src.config.log_config import setup_logging
import pprint

# Load configuration and set up logging at package import time
config = get_config()
setup_logging()

pp = pprint.PrettyPrinter(indent=4)

__all__ = ["config", "pp"]
