from src.config.config import get_config

# Initialize configuration first
config = get_config()

# Now set up logging using the initialized config
from src.config.log_config import setup_logging
setup_logging()