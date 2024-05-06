import numpy as np
import pandas as pd
import time
import pytz
from selenium.webdriver.chrome.options import Options
from typing import List
import datetime
import yaml


def load_config(filename):
    with open(filename, 'r') as file:
        return yaml.safe_load(file)


def get_NSE_symbol(symbol):
    return f"NSE:{symbol}-{'INDEX' if 'NIFTY' in symbol else 'EQ'}"

def get_chrome_options():
    options = Options()
    options.add_argument("--headless")
    # Add any other options you need here
    return options


def load_symbols(symbols_file: str) -> List[str]:
    """
    Load stock symbols from a file.
    :param symbols_file: Path to the file containing stock symbols.
    :return: List of stock symbols.
    """
    try:
        with open(symbols_file, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Symbols file not found: {symbols_file}")
        return []


def determine_mode():
    current_utc = datetime.datetime.now()
    market_tz = pytz.timezone('Asia/Kolkata')
    current_market_time = current_utc.astimezone(market_tz)

    if current_market_time.weekday() < 5 and 9 <= current_market_time.hour < 15:
        return "LIVE"
    else:
        return "BACKTEST"

def epoch_to_ist(epoch_time):
    ist_timezone = datetime.timezone(datetime.timedelta(
        hours=5, minutes=30))  # IST timezone offset
    ist_datetime = datetime.datetime.fromtimestamp(
        epoch_time, tz=ist_timezone)
    return ist_datetime


def categorize_percent_change(series: pd.Series, window_size: int) -> pd.Series:
    """
    Calculates the percent change of a series over a specified forward window size
    and categorizes the changes into buckets based on standard deviations from the mean.

    Args:
    series (pd.Series): Series of close prices captured at 5 min intervals.
    window_size (int): Window size in minutes.

    Returns:
    pd.Series: A series containing the categories of percent change for each window.
    """

    # Calculate forward percent change
    pct_change = series.pct_change(
        periods=window_size // 5).shift(-window_size // 5) * 100

    # Compute mean and standard deviation
    mu = pct_change.mean()
    sigma = pct_change.std()

    # Define buckets
    def categorize(value):
        if pd.isna(value):
            return np.nan
        elif value > mu + 1.5 * sigma:
            return 'High'
        elif mu + 0.5 * sigma < value <= mu + 1.5 * sigma:
            return 'Medium High'
        elif mu - 0.5 * sigma <= value <= mu + 0.5 * sigma:
            return 'Neutral'
        elif mu - 1.5 * sigma < value <= mu - 0.5 * sigma:
            return 'Medium Low'
        else:
            return 'Low'

    # Apply categorization
    categories = pct_change.apply(categorize)

    return categories