from apscheduler.schedulers.background import BackgroundScheduler
from fyers_apiv3 import fyersModel
from src.feature_engineering.orderbook_features_extraction import OrderBookDataTransformer
from typing import Dict, Callable
import pandas as pd
import requests
import json
import os, time
import logging
from datetime import datetime, timedelta
from src.utils.utils import load_symbols, get_NSE_symbol
from src.config import config, log_config
log_config.setup_logging()


class OrderBookHandler:
    def __init__(self, fyers_instance, scheduler: BackgroundScheduler):
        self.fyers = fyers_instance
        self.scheduler = scheduler
        self.transformer = OrderBookDataTransformer()  # Initialize once
        self.symbols = load_symbols(config.SYMBOLS_PATH)
        self.path = config.ORDERBOOK_FILENAME
        self.callbacks = []
        if config.TRADE_MODE == "LIVE":
            self.data = {symbol: pd.DataFrame() for symbol in self.symbols}
            self.initialize_scheduler()
        else:
            self.data = {symbol: self.load_existing_data(
                symbol) for symbol in self.symbols}

    @staticmethod
    def extract_info_df(data: dict, symbol: str):
        """
        Extracts and formats basic information from the raw data.
        """
        order_df = pd.DataFrame([{
            "symbol": symbol,
            "total_buy_qty": data.get("totalbuyqty", 0),
            "total_sell_qty": data.get("totalsellqty", 0),
            "bids": data.get("bids", []),
            "asks": data.get("ask", []),
            "open": data.get("o", 0),
            "high": data.get("h", 0),
            "low": data.get("l", 0),
            "close": data.get("c", 0),
            "change_percent": data.get("chp", 0),
            "tick_size": data.get("tick_Size", 0),
            "change": data.get("ch", 0),
            "last_traded_qty": data.get("ltq", 0),
            "last_traded_time": datetime.fromtimestamp(data.get("ltt", 0)).strftime('%Y-%m-%d %H:%M:%S'),
            "last_traded_price": data.get("ltp", 0),
            "volume": data.get("v", 0),
            "average_traded_price": data.get("atp", 0),
            "lower_circuit": data.get("lower_ckt", 0),
            "upper_circuit": data.get("upper_ckt", 0),
            "expiry": data.get("expiry", ""),
            "open_interest": data.get("oi", 0),
            "open_interest_flag": data.get("oiflag", False),
            "previous_day_open_interest": data.get("pdoi", 0),
            "open_interest_percent": data.get("oipercent", 0.0)
        }])
        return order_df

    def load_existing_data(self, symbol) -> pd.DataFrame:
        try:
            file_path = os.path.join(
                self.path, f"{symbol}_{config.ORDERBOOK_FILE_SUFF}.csv")
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['last_traded_time'] = pd.to_datetime(
                    df['last_traded_time']).dt.round('5min')
                return df
        except Exception as e:
            return pd.DataFrame()

    def register_callback(self, callback):
        self.callbacks.append(callback)

    def execute_callbacks(self):
        for callback in self.callbacks:
            callback(self.data)

    def fetch_order_book_data(self):
        for symbol in self.symbols:
            self.fetch_data_for_symbol(symbol)
        logging.info(
            f"fetching order book data for symbols completed")
        return self.data


    def fetch_data_for_symbol(self, symbol):
        attempt = 0
        while attempt < config.MAX_API_CALL_ATTEMPT:
            try:
                smb_key = get_NSE_symbol(symbol)
                data = {"symbol": smb_key, "ohlcv_flag": "1"}
                response = self.fyers.depth(data=data)
                order_book_data = response.get("d", {}).get(smb_key, {})
                structured_df = self.extract_info_df(order_book_data, symbol)
                structured_df['last_traded_time'] = pd.to_datetime(structured_df['last_traded_time']).dt.tz_localize(
                    None).dt.round('5min').astype(str)
                self.process_order_book_data(symbol, structured_df)
                logging.info(
                    f"Order book data for symbol {symbol} fetched successfully.")
                break
            except UnboundLocalError as ule:
                logging.exception(
                    f"UnboundLocalError occurred while fetching order book for {symbol}: {ule}. Retrying after {config.WAIT_TIME_BETWEEN_API_CALLS} seconds.")
                time.sleep(config.WAIT_TIME_BETWEEN_API_CALLS)
                attempt += 1
            except Exception as e:
                logging.exception(
                    f"Exception occurred while fetching order book for {symbol}: {e}")
            break

    def process_order_book_data(self, symbol, data):
        self.data[symbol] = pd.concat(
            [self.data[symbol], data]).reset_index(drop=True)
        self.trim_data(symbol)

    def trim_data(self, symbol):
        start_tm = datetime.now() - pd.DateOffset(years=config.BACKTEST_DATA_LENGTH_YEARS)
        self.data[symbol] = self.data[symbol][pd.to_datetime(
            self.data[symbol]['last_traded_time']) > start_tm]

    def backup_hourly(self):
        now = datetime.now()
        logging.info(
            f"Starting order data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}")

        for symbol, df in self.data.items():
            file_path = os.path.join(
                self.path, f"{symbol}_{config.ORDERBOOK_FILE_SUFF}.csv")
            try:
                if not os.path.exists(file_path):
                    df.to_csv(file_path, index=False)
                else:
                    existing_df = pd.read_csv(file_path)
                    updated_df = pd.concat([existing_df, df], ignore_index=True)
                    updated_df.to_csv(file_path,index=False)
                logging.info(
                    f"Order Book Data backup {symbol} completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logging.exception(
                    f"Error backing up Order Book data for {symbol}: {e}")

    def initialize_scheduler(self):
        # self.scheduler.add_job(self.fetch_order_book_data,
        #                        'cron', minute=f'*/{config.DATA_FETCH_CRON_INTERVAL_MIN}', id='order_book_job')
        self.scheduler.add_job(self.backup_hourly, 'cron',
                               hour='*', id='order_book_backup_job')


# Example Usage
# config = OrderBookConfig(base_path="path/to/data", symbols_file="symbols.txt", backup_path="path/to/backup")
# fyers_instance = fyersModel.FyersModel(client_id="your_client_id", token="your_token", log_path="your_log_path")
# order_book_handler = OrderBookHandler(fyers_instance, config)
# order_book_handler.register_callback(your_callback_function)
# To stop: order_book_handler.stop()
