from src import config
import re
import numpy as np
import pandas as pd
import time
import pytz
from selenium.webdriver.chrome.options import Options
from functools import partial, lru_cache
from typing import List
import datetime
import yaml


@lru_cache(maxsize=None)
def load_config(filename: str):
    """Load YAML configuration from ``filename`` and cache the result."""
    with open(filename, 'r') as file:
        return yaml.safe_load(file)


def get_NSE_symbol(symbol: str) -> str:
    """
    Constructs the NSE symbol format for a given stock symbol.

    Args:
        symbol (str): The stock symbol to be converted to NSE format.

    Returns:
        str: The NSE symbol in the format "NSE:{symbol}-INDEX" for NIFTY symbols, 
        otherwise "NSE:{symbol}-EQ".
    
    Example:
        >>> get_NSE_symbol('NIFTY50')
        'NSE:NIFTY50-INDEX'
        >>> get_NSE_symbol('RELIANCE')
        'NSE:RELIANCE-EQ'
    """
    return f"NSE:{symbol}-{'INDEX' if 'NIFTY' in symbol else 'EQ'}"


def get_chrome_options() -> Options:
    """
    Configures Chrome browser options for Selenium WebDriver.

    Returns:
        Options: Configured Chrome options for Selenium WebDriver.

    Example:
        >>> options = get_chrome_options()
    """
    options = Options()
    # Uncomment or add additional options as needed
    # options.add_argument("--headless")
    return options

 
@lru_cache(maxsize=None)
def load_symbols(symbols_file: str) -> List[str]:
    """
    Loads stock symbols from a specified file, one per line.

    Args:
        symbols_file (str): Path to the file containing stock symbols.

    Returns:
        List[str]: A list of stock symbols from the file. 
        Returns an empty list if the file is not found.

    Example:
        >>> load_symbols('symbols.txt')
        ['RELIANCE', 'TCS', 'INFY']

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    try:
        with open(symbols_file, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Symbols file not found: {symbols_file}")
        return []


def determine_mode() -> str:
    """
    Determines the market mode based on the current time in the 'Asia/Kolkata' timezone.
    
    The function checks if the current time falls within the live market hours (Monday to Friday, 9 AM to 3 PM).
    
    Returns:
        str: "LIVE" if the market is open, otherwise "BACKTEST".

    Example:
        >>> determine_mode()
        'LIVE'  # If current time is during live market hours
    """
    current_utc = datetime.datetime.now()
    market_tz = pytz.timezone(config.scheduler.timezone)
    current_market_time = current_utc.astimezone(market_tz)

    if current_market_time.weekday() < 5 and 9 <= current_market_time.hour < 15:
        return "LIVE"
    else:
        return "BACKTEST"
    


def epoch_to_ist(epoch_time: float) -> datetime.datetime:
    """
    Converts a given epoch timestamp to IST (Indian Standard Time).

    Args:
        epoch_time (float): The epoch timestamp to convert.

    Returns:
        datetime.datetime: The corresponding IST datetime.

    Example:
        >>> epoch_to_ist(1634832335)
        datetime.datetime(2021, 10, 21, 18, 28, 55, tzinfo=datetime.timezone(datetime.timedelta(seconds=19800)))

    """
    ist_timezone = datetime.timezone(datetime.timedelta(hours=5, minutes=30))  # IST timezone offset
    ist_datetime = datetime.datetime.fromtimestamp(epoch_time, tz=ist_timezone)
    return ist_datetime

def get_timezone():
   return pytz.timezone(config.scheduler.timezone)


def get_trunc_output(output):
    return output if not config.trading_config.trade_mode == 'LIVE' else output.iloc[-1:]
