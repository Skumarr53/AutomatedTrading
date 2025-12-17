"""
Centralized Warning Configuration Module

This module provides utilities for managing Python warnings across the codebase.
Warnings can be enabled/disabled via environment variables for debugging.

Usage:
    # At the top of any module
    from src.utils.warning_config import suppress_warnings
    suppress_warnings()

    # Or in main.py
    from src.utils.warning_config import configure_all_warnings
    configure_all_warnings()

Environment Variables:
    SUPPRESS_WARNINGS: Set to '0' to enable all warnings for debugging
    VERBOSE_LOGGING: Set to '1' to enable verbose logging for all libraries
"""

import os
import warnings
import logging
from typing import List, Optional


def suppress_warnings(
    categories: Optional[List[type]] = None,
    messages: Optional[List[str]] = None
) -> None:
    """
    Suppress specific warning categories and messages.
    
    Args:
        categories: List of warning categories to suppress (e.g., [FutureWarning, DeprecationWarning])
        messages: List of message patterns to suppress (regex patterns)
    """
    # Check if warnings should be suppressed
    if os.getenv('SUPPRESS_WARNINGS', '1') == '0':
        return  # Don't suppress if explicitly disabled
    
    # Default categories to suppress
    if categories is None:
        categories = [
            FutureWarning,
            DeprecationWarning,
            UserWarning,
        ]
    
    # Default message patterns to suppress
    if messages is None:
        messages = [
            '.*T.*is deprecated.*',
            '.*fill_method.*is deprecated.*',
            '.*resource_tracker.*',
            '.*chained_assignment.*',
            '.*copy_on_write.*',
        ]
    
    # Suppress by category
    for category in categories:
        warnings.filterwarnings('ignore', category=category)
    
    # Suppress by message pattern
    for pattern in messages:
        warnings.filterwarnings('ignore', message=pattern)


def configure_library_logging(verbose: bool = False) -> None:
    """
    Configure logging levels for noisy third-party libraries.
    
    Args:
        verbose: If True, use INFO level; if False, use WARNING/ERROR level
    """
    # Check environment variable
    verbose = verbose or os.getenv('VERBOSE_LOGGING', '0') == '1'
    
    libraries = {
        'joblib': logging.ERROR,
        'mlflow': logging.WARNING,
        'urllib3': logging.WARNING,
        'matplotlib': logging.WARNING,
        'sklearn': logging.WARNING,
        'imblearn': logging.WARNING,
        'numba': logging.WARNING,
        'numexpr': logging.WARNING,
    }
    
    if verbose:
        # Set all to INFO for debugging
        for lib in libraries:
            logging.getLogger(lib).setLevel(logging.INFO)
    else:
        # Set to quiet levels
        for lib, level in libraries.items():
            logging.getLogger(lib).setLevel(level)


def configure_pandas_options() -> None:
    """Configure pandas options to suppress performance warnings."""
    import pandas as pd
    
    if os.getenv('SUPPRESS_WARNINGS', '1') == '0':
        return  # Don't modify if warnings are enabled
    
    options = {
        'mode.chained_assignment': None,  # Suppress SettingWithCopyWarning
        'mode.copy_on_write': False,      # Suppress copy-on-write warning
        'display.max_rows': 100,          # Limit row display
        'display.max_columns': 50,        # Limit column display
    }
    
    for option, value in options.items():
        try:
            pd.set_option(option, value)
        except Exception:
            # Option might not exist in this pandas version
            pass


def configure_numpy_warnings() -> None:
    """Configure numpy to suppress specific warnings."""
    import numpy as np
    
    if os.getenv('SUPPRESS_WARNINGS', '1') == '0':
        return
    
    # Suppress numpy runtime warnings (divide by zero, etc.)
    np.seterr(divide='ignore', invalid='ignore')


def configure_all_warnings() -> None:
    """
    Configure all warning and logging settings at once.
    This should be called at the start of the application.
    """
    suppress_warnings()
    configure_library_logging()
    configure_pandas_options()
    configure_numpy_warnings()
    
    # Set joblib to not show progress bars and minimize output
    os.environ.setdefault('JOBLIB_TEMP_FOLDER', '/tmp')
    
    # Log configuration status
    if os.getenv('SUPPRESS_WARNINGS', '1') == '1':
        print("✓ Warning suppression enabled (set SUPPRESS_WARNINGS=0 to disable)")
    else:
        print("⚠ Warning suppression disabled (debugging mode)")


# Convenience function for module-level import
def setup_module_warnings() -> None:
    """
    Quick setup for warnings in any module.
    Import and call at the top of any Python module.
    """
    suppress_warnings()


# Auto-configure if imported directly
if __name__ != '__main__':
    # Only suppress if not in debugging mode
    if os.getenv('SUPPRESS_WARNINGS', '1') == '1':
        suppress_warnings()

