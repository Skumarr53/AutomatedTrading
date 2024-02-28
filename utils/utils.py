import time
import pytz
from selenium.webdriver.chrome.options import Options
from typing import List
import datetime


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
