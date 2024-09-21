# Reviewing and optimizing the data/data_fetcher.py script

# data/data_fetcher.py
import time
import pytz
import json
import requests
import logging
from datetime import datetime, time as _time
import os
import pandas as pd
from utils.utils import load_symbols, get_NSE_symbol
from src.config.config import MODEL_CONFIG
from src.config import config, log_config, columns_def
log_config.setup_logging()


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
        callback (Optional[Callable]): Callback function to execute after loading data.
        scheduler (Scheduler): Scheduler instance for managing jobs.
    """
    def __init__(self, fyres_instance: FyresInstance, scheduler: Scheduler) -> None:
        """
        Initializes the DataHandler with the given FYRES instance and scheduler.

        Args:
            fyres_instance (FyresInstance): An instance of the FYRES API client.
            scheduler (Scheduler): Scheduler instance for managing jobs.
        """
        self.trading_mode = config.TRADE_MODE

        self.fyres = fyres_instance
        self.file_path = config.TICKER_FILENAME
        self.symbols = load_symbols(config.SYMBOLS_PATH)
        self.data = {symbol: [] for symbol in self.symbols}
        self.data_len = config.BACKTEST_DATA_LENGTH_YEARS * 12 * 30 * 24 * 60 * 60
        self.callback = None
        self.scheduler = scheduler

        for symbol in self.symbols: self.load_or_initialize_data(symbol)

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


    def get_scheduler(self) -> Scheduler:
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
        self.scheduler.add_job(self.backup_data, 'interval',
                               hours=config.BACKUP_INTERVAL_HOURS, id='backup_data_job')
        # self.schedule_data_updates()

    def load_or_initialize_data(self, symbol: str) -> pd.DataFrame:
        """
        Loads existing data for a symbol from a CSV file or initializes it by fetching full-year data.

        Args:
            symbol (str): The trading symbol to load data for.

        Returns:
            pd.DataFrame: The loaded or initialized data for the symbol.
        """
        symbol_file = os.path.join(
            self.file_path, f"{symbol}_{config.TICKER_FILE_SUFF}.csv")
        if os.path.exists(symbol_file):
            try:
                df = pd.read_csv(symbol_file)
            except Exception as e:
                logging.exception(f"Error loading data for {symbol}: {e}")
                return pd.DataFrame()
        else:
            df = self.fetch_full_year_data(symbol)
        self.update_data(symbol, df)
        self.data[symbol].to_csv(symbol_file, index=False)

    def fetch_full_year_data(self, symbol: str) -> pd.DataFrame:
        """
        Fetches a full year's worth of data for the given symbol.

        Args:
            symbol (str): The trading symbol to fetch data for.

        Returns:
            pd.DataFrame: DataFrame containing the fetched data.
        """
        now = datetime.now().timestamp()
        intial_time = now - self.data_len
        return self.fetch_data(symbol, intial_time, now)

    def update_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Updates the data for a specific symbol by fetching missing data and concatenating it.

        Args:
            symbol (str): The trading symbol to update data for.
            df (pd.DataFrame): Existing DataFrame containing data for the symbol.
        """
        last_timestamp = df['epoch_time'].max() if not df.empty else 0
        now = datetime.now().timestamp()
        # TODO: uncomment below condition for upadtion 
        if (now - last_timestamp) > (self.data_len):
            df = self.fetch_full_year_data(symbol)
        else:
            missing_data = self.fetch_data(symbol, last_timestamp, now)
            df = pd.concat([df, missing_data]).drop_duplicates(
                subset='epoch_time').reset_index(drop=True)

        intial_time = now - self.data_len
        self.data[symbol] = df[df['epoch_time'] > intial_time]
    

    # Function to convert epoch to datetime
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
        ONE_DAY_SECONDS = 86400
        total_data = pd.DataFrame()
        date_col = columns_def.TICKER_COLS[-1]
        IST = pytz.timezone(config.TIMEZONE)
        
        
        
        while start_epoch_time < end_epoch_time:
            attempt = 0
            now = datetime.now(IST).timestamp()
            chunk_end_time = min(
                start_epoch_time + config.CHUNK_SIZE_DAYS * ONE_DAY_SECONDS, end_epoch_time)
            inp_payload = {key: value.format(symbol=get_NSE_symbol(symbol),
                                            interval=config.DATA_FETCH_CRON_INTERVAL_MIN,
                                            start_epoch_time=int(
                                                start_epoch_time),
                                            end_epoch_time=int(chunk_end_time))
                           for key, value in MODEL_CONFIG['BASE_PAYLOAD'].items()}
            
            ## API call to fetch data
            while attempt < config.MAX_API_CALL_ATTEMPT:
                try:
                    cs_data = self.fyres.history(inp_payload)
                    df = pd.DataFrame(
                        cs_data['candles'], columns=columns_def.TICKER_COLS[:6])
                    total_data = pd.concat([total_data, df])
                    logging.info(
                        f"time diff in  seconds symbol {symbol}: {now - df[columns_def.TICKER_COLS[0]].max()}")
                    break
                except Exception as e:
                    if cs_data['code'] == 429:
                        logging.info(
                            f"Rate limit exceeded. Waiting {config.WAIT_TIME_BETWEEN_API_CALLS} seconds before retrying...")
                        time.sleep(config.WAIT_TIME_BETWEEN_API_CALLS)
                        attempt += 1
                    else:
                        logging.exception(
                        f"Error fetching data for {symbol}: {e}")
                        break
            start_epoch_time = chunk_end_time
            
            total_data[date_col] = pd.to_datetime(total_data[columns_def.TICKER_COLS[0]], unit='s')
            total_data[date_col] = total_data[date_col].dt.tz_localize('UTC').dt.tz_convert(config.TIMEZONE)
            total_data[date_col] = total_data[date_col].dt.tz_localize(None).dt.round('5min')
        return total_data

    def schedule_data_updates(self):
        """
        Schedule regular data updates during trading hours.
        """
        IST = pytz.timezone(config.TIMEZONE)

        def delayed_job():
            """
            Delayed job execution to ensure trading hours alignment.
            """
            # Wait for 30 seconds before executing the actual job
            time.sleep(5)
            self.update_data_regularly()

        # Scheduling the data updates only during the trading hours
        self.scheduler.add_job(
            delayed_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute=f'*/{config.DATA_FETCH_CRON_INTERVAL_MIN}',
            timezone=IST,
            id='update_data_regularly_job'
        )

    def update_data_regularly(self):
        """
        Regularly update the data during trading hours.
        """
        try:
            IST = pytz.timezone(config.TIMEZONE)
            now = datetime.now(IST)
            logging.debug(f"Attempting data update at {now}")
            if _time(9, 0) <= now.time() <= _time(15, 0):
                for symbol in self.symbols:
                    last_update = now.timestamp() - 5 * 60
                    self.update_data(symbol, self.data[symbol])
                return self.data
            else:
                logging.debug("Outside trading hours")
        except Exception as e:
            logging.exception("Error in scheduled data update")

    def backup_data(self) -> None:
        now = datetime.now()
        logging.info(
            f"Starting ticker data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}")
        for symbol, df in self.data.items():
            try:
                df.to_csv(os.path.join(
                    self.file_path, f"{symbol}_{config.TICKER_FILE_SUFF}.csv"), index=False)
                logging.info(
                    f"Data backup completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logging.exception(f"Error backing up data for {symbol}: {e}")