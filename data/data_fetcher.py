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
from config import config, log_config, columns_def
log_config.setup_logging()


class DataHandler:
    def __init__(self, fyres_instance, scheduler):
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

    
    def register_callback(self, callback):
        self.callback = callback

    def load_historical_data(self):
        if self.callback:
            self.callback(self.data)


    def get_scheduler(self):
        return self.scheduler

    def configure_scheduler(self):

        # Adding jobs to the scheduler
        self.scheduler.add_job(self.backup_data, 'interval',
                               hours=config.BACKUP_INTERVAL_HOURS, id='backup_data_job')
        # self.schedule_data_updates()

    def load_or_initialize_data(self, symbol: str) -> pd.DataFrame:
        symbol_file = os.path.join(
            self.file_path, f"{symbol}_{config.TICKER_FILE_SUFF}.parquet")
        if os.path.exists(symbol_file):
            try:
                df = pd.read_parquet(symbol_file)
            except Exception as e:
                logging.exception(f"Error loading data for {symbol}: {e}")
                return pd.DataFrame()
        else:
            df = self.fetch_full_year_data(symbol)
        self.update_data(symbol, df)
        self.data[symbol].to_parquet(symbol_file)

    def fetch_full_year_data(self, symbol: str) -> pd.DataFrame:
        now = datetime.now().timestamp()
        intial_time = now - self.data_len
        return self.fetch_data(symbol, intial_time, now)

    def update_data(self, symbol: str, df: pd.DataFrame) -> None:
        last_timestamp = df['epoch_time'].max() if not df.empty else 0
        now = datetime.now().timestamp()
        ## TODO: uncomment below condition for upadtion 
        # if (now - last_timestamp) > (self.data_len):
        #     df = self.fetch_full_year_data(symbol)
        # else:
        #     missing_data = self.fetch_data(symbol, last_timestamp, now)
        #     df = pd.concat([df, missing_data]).drop_duplicates(
        #         subset='epoch_time').reset_index(drop=True)

        intial_time = now - self.data_len
        self.data[symbol] = df[df['epoch_time'] > intial_time]
    

    # Function to convert epoch to datetime
    def fetch_data(self, symbol: str, start_epoch_time: float, end_epoch_time: float) -> pd.DataFrame:
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
                                            for key, value in config.BASE_PAYLOAD.items()}
            
            ## API call to fetch data
            while attempt < config.MAX_API_CALL_ATTEMPT:
                try:
                    cs_data = self.fyres.history(inp_payload)
                    df = pd.DataFrame(
                        cs_data['candles'], columns=columns_def.TICKER_COLS[:6])
                    df[date_col] = pd.to_datetime(
                        df[columns_def.TICKER_COLS[0]], unit='s')
                    df[date_col] = df[date_col].dt.tz_localize(
                        'UTC').dt.tz_convert(config.TIMEZONE)
                    df[date_col] = df[date_col].dt.tz_localize(
                        None).dt.floor('T')
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
                df.to_parquet(os.path.join(
                    self.file_path, f"{symbol}_{config.TICKER_FILE_SUFF}.parquet"))
                logging.info(
                    f"Data backup completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logging.exception(f"Error backing up data for {symbol}: {e}")





# Commenting out the function call to prevent execution in the PCI
# if __name__=="__main__":
#     # Code for standalone testing or execution
# # Write the list of dictionaries to a JSON file
# # with open(os.path.join(self.file_path, f"{symbol}_data.json"), 'w') as json_file:
# #     json.dump(data, json_file)
#         symbol_file = os.path.join(self.file_path, f"{symbol}_data.json")
#         if os.path.exists(symbol_file):
#             try:
#                 with open('data.json', 'r') as json_file:
#                     data = json.load(json_file)