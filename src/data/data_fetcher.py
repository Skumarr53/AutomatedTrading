# src/data/data_fetcher.py
import time
import pytz
import json
import requests
import logging
from fyers_apiv3 import fyersModel  # accessToken
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, time as _time
import os
import pandas as pd
from typing import List, Dict, Callable, Optional
from src.utils.utils import load_symbols, get_NSE_symbol
from src.config.config import config, setup_logging

setup_logging()


class DataHandler:
    """
    Handles fetching, loading, updating, and backing up trading data for automated trading.

    Attributes:
        trading_mode (str): Mode of trading, either 'BACKTEST' or 'LIVE'.
        fyres (FyresInstance): Instance of the FYRES API client.
        file_path (str): Path to the directory where ticker data is stored.
        symbols (List[str]): List of trading symbols.
        data (Dict[str, pd.DataFrame]): Dictionary storing data for each symbol.
        data_len (int): Duration of data to maintain, in seconds.
        callback (Optional[Callable[[Dict[str, pd.DataFrame]], None]]): 
            Callback function to execute after loading data.
        scheduler (Scheduler): Scheduler instance for managing jobs.
    """
    def __init__(self, fyres_instance: 'fyersModel', scheduler: 'BackgroundScheduler') -> None:
        """
        Initializes the DataHandler with the given FYRES instance and scheduler.

        Args:
            fyres_instance (FyresInstance): An instance of the FYRES API client.
            scheduler (Scheduler): Scheduler instance for managing jobs.
        """
        self.trading_mode: str = config.trading_config.trade_mode

        self.fyres = fyres_instance
        self.file_path: str = config.paths.ticker_filename
        self.symbols: List[str] = load_symbols(config.paths.symbols_path)
        self.data: Dict[str, pd.DataFrame] = {symbol: pd.DataFrame() for symbol in self.symbols}
        ## TODO:
        self.data_len: int = config.backtest_data_load.backtest_data_length_years * 12 * 30 * 24 * 60 * 60
        self.callback: Optional[Callable[[Dict[str, pd.DataFrame]], None]] = None
        self.scheduler = scheduler

        for symbol in self.symbols:
            self.load_or_initialize_data(symbol)

        if self.trading_mode == "BACKTEST":
            self.load_historical_data()
        elif self.trading_mode == "LIVE":
            self.configure_scheduler()

    def register_callback(self, callback: Callable[[Dict[str, pd.DataFrame]], None]) -> None:
        """
        Registers a callback function to be called after loading historical data.

        Args:
            callback (Callable[[Dict[str, pd.DataFrame]], None]): 
                A function that takes the data dictionary as input.
        """
        self.callback = callback

    def load_historical_data(self) -> None:
        """
        Loads historical data and invokes the registered callback if available.
        """
        if self.callback:
            self.callback(self.data)

    def get_scheduler(self) -> 'BackgroundScheduler':
        """
        Retrieves the scheduler instance.

        Returns:
            Scheduler: The scheduler instance used for managing jobs.
        """
        return self.scheduler

    def configure_scheduler(self) -> None:
        """
        Configures the scheduler by adding necessary jobs, such as data backup.
        """
        # Adding jobs to the scheduler
        self.scheduler.add_job(
            self.backup_data,
            'interval',
            hours=config.scheduler.backup_interval_hours,
            id='backup_data_job'
        )
        # self.schedule_data_updates()

    def load_or_initialize_data(self, symbol: str) -> pd.DataFrame:
        """
        Loads existing data for a symbol from a CSV file or initializes it by fetching full-year data.

        Args:
            symbol (str): The trading symbol to load data for.

        Returns:
            pd.DataFrame: The loaded or initialized data for the symbol.
        """
        #TODO:
        symbol_file: str = os.path.join(
            self.file_path, f"{symbol}_{config.backtest_data_load.ticker_file_suffix}.csv"
        )
        if os.path.exists(symbol_file):
            try:
                df: pd.DataFrame = pd.read_csv(
                        symbol_file,
                        on_bad_lines="skip",
                        engine="python",
                    )
            except Exception as e:
                logging.exception(f"Error loading data for {symbol}: {e}")
                return pd.DataFrame()
        else:
            df: pd.DataFrame = self.fetch_full_year_data(symbol)
        self.update_data(symbol, df)
        self.data[symbol].to_csv(symbol_file, index=False)
        return self.data[symbol]

    def fetch_full_year_data(self, symbol: str) -> pd.DataFrame:
        """
        Fetches a full year's worth of data for the given symbol.

        Args:
            symbol (str): The trading symbol to fetch data for.

        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        now: float = datetime.now().timestamp()
        initial_time: float = now - self.data_len
        return self.fetch_data(symbol, initial_time, now)

    def update_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Updates the data for a specific symbol by fetching missing data and concatenating it.

        Args:
            symbol (str): The trading symbol to update data for.
            df (pd.DataFrame): Existing DataFrame containing data for the symbol.
        """
        last_timestamp: float = df['epoch_time'].max() if not df.empty else 0
        now: float = datetime.now().timestamp()
        # TODO: uncomment below condition for updating 
        if (now - last_timestamp) > self.data_len:
            df = self.fetch_full_year_data(symbol)
        else:
            missing_data: pd.DataFrame = self.fetch_data(symbol, last_timestamp, now)
            df = pd.concat([df, missing_data]).drop_duplicates(
                subset='epoch_time'
            ).reset_index(drop=True)

        initial_time: float = now - self.data_len
        self.data[symbol] = df[df['epoch_time'] > initial_time]

    def fetch_data(self, symbol: str, start_epoch_time: float, end_epoch_time: float) -> pd.DataFrame:
        """
        Fetches trading data for a given symbol between start and end epoch times.

        Args:
            symbol (str): The trading symbol to fetch data for.
            start_epoch_time (float): The start time in epoch seconds.
            end_epoch_time (float): The end time in epoch seconds.

        Returns:
            pd.DataFrame: DataFrame containing the fetched trading data with columns defined in TICKER_COLS.
        """
        ONE_DAY_SECONDS: int = 86400
        ticker_cols = config.columns.ticker_cols
        
        total_data: pd.DataFrame = pd.DataFrame()
        date_col: str = ticker_cols[-1]
        IST = pytz.timezone(config.scheduler.timezone)

        while start_epoch_time < end_epoch_time:
            attempt: int = 0
            current_time: float = datetime.now(IST).timestamp()
            chunk_end_time: float = min(
                start_epoch_time + config.scheduler.chunk_size_days * ONE_DAY_SECONDS, end_epoch_time
            )
            inp_payload: Dict[str, str] = {
                key: value.format(
                    symbol=get_NSE_symbol(symbol),
                    interval=config.scheduler.data_fetch_cron_interval_min,
                    start_epoch_time=int(start_epoch_time),
                    end_epoch_time=int(chunk_end_time)
                )
                for key, value in config.base_payload_args.items()
            }

            ## API call to fetch data
            while attempt < config.scheduler.max_api_call_attempts:
                try:
                    cs_data: Dict = self.fyres.history(inp_payload)
                    df: pd.DataFrame = pd.DataFrame(
                        cs_data['candles'], columns=ticker_cols[:6]
                    )
                    total_data = pd.concat([total_data, df])
                    logging.info(
                        f"time diff in seconds symbol {symbol}: {current_time - df[ticker_cols[0]].max()}"
                    )
                    break
                except Exception as e:
                    if cs_data.get('code') == 429:
                        logging.info(
                            f"Rate limit exceeded. Waiting {config.scheduler.wait_time_between_api_calls} seconds before retrying..."
                        )
                        time.sleep(config.scheduler.wait_time_between_api_calls)
                        attempt += 1
                    else:
                        logging.exception(
                            f"Error fetching data for {symbol}: {e}"
                        )
                        break
            start_epoch_time = chunk_end_time

            total_data[date_col] = pd.to_datetime(
                total_data[ticker_cols[0]], unit='s'
            )
            total_data[date_col] = total_data[date_col].dt.tz_localize('UTC').dt.tz_convert(config.scheduler.timezone)
            total_data[date_col] = total_data[date_col].dt.tz_localize(None).dt.round('5min')
        return total_data

    def schedule_data_updates(self) -> None:
        """
        Schedule regular data updates during trading hours.
        """
        IST = pytz.timezone(config.scheduler.timezone)

        def delayed_job() -> None:
            """
            Delayed job execution to ensure trading hours alignment.
            """
            # Wait for 5 seconds before executing the actual job
            time.sleep(5)
            self.update_data_regularly()

        # Scheduling the data updates only during the trading hours
        self.scheduler.add_job(
            delayed_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute=f'*/{config.scheduler.data_fetch_cron_interval_min}',
            timezone=IST,
            id='update_data_regularly_job'
        )

    def update_data_regularly(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Regularly update the data during trading hours.

        Returns:
            Optional[Dict[str, pd.DataFrame]]: Updated data dictionary if within trading hours, else None.
        """
        try:
            IST = pytz.timezone(config.scheduler.timezone)
            now: datetime = datetime.now(IST)
            logging.debug(f"Attempting data update at {now}")
            if _time(9, 0) <= now.time() <= _time(15, 0):
                for symbol in self.symbols:
                    last_update: float = now.timestamp() - 5 * 60
                    self.update_data(symbol, self.data[symbol])
                return self.data
            else:
                logging.debug("Outside trading hours")
                return None
        except Exception as e:
            logging.exception("Error in scheduled data update")
            return None

    def backup_data(self) -> None:
        """
        Backs up the current trading data by saving each symbol's data to a CSV file.
        """
        now: datetime = datetime.now()
        logging.info(
            f"Starting ticker data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        for symbol, df in self.data.items():
            try:
                df.to_csv(
                    os.path.join(
                        # TODO:
                        self.file_path, f"{symbol}_{config.TICKER_FILE_SUFF}.csv"
                    ),
                    index=False
                )
                logging.info(
                    f"Data backup completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except Exception as e:
                logging.exception(f"Error backing up data for {symbol}: {e}")
