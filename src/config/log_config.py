from loguru import logger
import sys
from pathlib import Path
import os

def setup_logging(log_file_path: str = "logs/log_file.log", log_level: str = "INFO"):
    """
    Set up the Loguru logger with console and file handlers.
    
    Args:
        log_file_path (str): Path to the log file. Defaults to "logs/log_file.log".
        log_level (str): Logging level. Defaults to "INFO".
    """
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