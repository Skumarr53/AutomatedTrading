from apscheduler.schedulers.background import BackgroundScheduler
from fyers_apiv3 import fyersModel
from src.feature_engineering.orderbook_features_extraction import OrderBookDataTransformer
from typing import Dict, Callable, Optional
import pandas as pd
import requests
import json
import os, time
import asyncio
from loguru import logger
from datetime import datetime, timedelta
from src.utils.utils import load_symbols, get_NSE_symbol
from src import config

# Try to import InfluxDB client (optional dependency)
try:
    from src.utils.influx_client import (
        InfluxDBClient_Wrapper,
        InfluxDBConfig,
        OrderBookDataPoint,
        DataBucket,
        create_influx_config_from_hydra,
    )
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    logger.warning("InfluxDB client not available for OrderBookHandler")




class OrderBookHandler:
    def __init__(self, fyers_instance, scheduler: BackgroundScheduler):
        self.fyers = fyers_instance
        self.scheduler = scheduler
        self.transformer = OrderBookDataTransformer()  # Initialize once
        self.path = config.paths.orderbook_filename
        self.callbacks = []
        
        # InfluxDB storage configuration
        self._influx_client: Optional[InfluxDBClient_Wrapper] = None
        self._influx_connected = False
        self._setup_influxdb_storage()
        
        # Load data for all symbols (from InfluxDB first, then CSV fallback)
        self.data = {symbol: self.load_existing_data(symbol) for symbol in config.symbols}
        if config.trading_config.trade_mode == "LIVE":
            self.initialize_scheduler()
    
    def _setup_influxdb_storage(self) -> None:
        """Initialize InfluxDB storage backend."""
        if not INFLUX_AVAILABLE:
            logger.info("InfluxDB not available for orderbook, using CSV storage only")
            return
            
        # Check if InfluxDB is enabled in config
        influx_enabled = getattr(config, 'influxdb', {}).get('enabled', False)
        if not influx_enabled:
            logger.info("InfluxDB disabled in config, using CSV storage for orderbook")
            return
            
        try:
            influx_config = create_influx_config_from_hydra(config)
            self._influx_client = InfluxDBClient_Wrapper(influx_config)
            
            # Try to connect (synchronously for initialization)
            loop = asyncio.new_event_loop()
            self._influx_connected = loop.run_until_complete(self._influx_client.connect())
            loop.close()
            
            if self._influx_connected:
                logger.info("InfluxDB connected successfully for orderbook storage")
            else:
                logger.warning("InfluxDB connection failed for orderbook, using CSV fallback")
                
        except Exception as e:
            logger.warning(f"InfluxDB setup failed for orderbook: {e}, using CSV storage")
            self._influx_connected = False            

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
            # "expiry": data.get("expiry", ""),
            "open_interest": data.get("oi", 0),
            "open_interest_flag": data.get("oiflag", False),
            "previous_day_open_interest": data.get("pdoi", 0),
            "open_interest_percent": data.get("oipercent", 0.0)
        }])
        return order_df

    def load_existing_data(self, symbol) -> pd.DataFrame:
        """Load existing orderbook data from InfluxDB (primary) or CSV (fallback)."""
        # Try InfluxDB first
        if self._influx_connected and self._influx_client:
            df = self._load_from_influxdb(symbol)
            if not df.empty:
                logger.info(f"Loaded {len(df)} orderbook records from InfluxDB for {symbol}")
                return df
        
        # Fallback to CSV
        return self._load_from_csv(symbol)
    
    def _load_from_influxdb(self, symbol: str) -> pd.DataFrame:
        """Load orderbook data from InfluxDB."""
        if not self._influx_client or not self._influx_connected:
            return pd.DataFrame()
            
        try:
            # Calculate time range based on backtest_data_length_years
            end_time = datetime.now()
            start_time = end_time - timedelta(days=365 * config.backtest_data_load.backtest_data_length_years)
            
            loop = asyncio.new_event_loop()
            df = loop.run_until_complete(
                self._influx_client.query_orderbook_data([symbol], start_time, end_time)
            )
            loop.close()
            
            if not df.empty and 'last_traded_time' in df.columns:
                df['last_traded_time'] = pd.to_datetime(df['last_traded_time'], errors='coerce')
                df = df.dropna(subset=['last_traded_time'])
                df['last_traded_time'] = df['last_traded_time'].dt.round('5min')
                    
            return df
            
        except Exception as e:
            logger.warning(f"Failed to load orderbook from InfluxDB for {symbol}: {e}")
            return pd.DataFrame()
    
    def _load_from_csv(self, symbol: str) -> pd.DataFrame:
        """Load orderbook data from CSV file."""
        try:
            file_path = os.path.join(
                self.path, f"{symbol}_{config.backtest_data_load.orderbook_file_suffix}.csv")
            if os.path.exists(file_path):
                df = pd.read_csv(
                        file_path,
                        on_bad_lines="skip",
                        engine="python",
                    )
                df['last_traded_time'] = pd.to_datetime(df['last_traded_time'], errors='coerce')
                df = df.dropna(subset=['last_traded_time'])
                df['last_traded_time'] = df['last_traded_time'].dt.round('5min')
                return df
        except Exception as e:
            logger.warning(f"Failed to load orderbook CSV for {symbol}: {e}")
        return pd.DataFrame()

    def register_callback(self, callback):
        
        self.callbacks.append(callback)

    def execute_callbacks(self):
        for callback in self.callbacks:
            callback(self.data)

    def fetch_order_book_data(self):
        for symbol in config.symbols:
            self.fetch_data_for_symbol(symbol)
        logger.info(
            f"fetching order book data for symbols completed")
        return self.data

    def fetch_data_for_symbol(self, symbol):
        attempt = 0
        while attempt < config.scheduler.max_api_call_attempts:
            try:
                smb_key = get_NSE_symbol(symbol)
                data = {"symbol": smb_key, "ohlcv_flag": "1"}
                response = self.fyers.depth(data=data)
                order_book_data = response.get("d", {}).get(smb_key, {})
                structured_df = self.extract_info_df(order_book_data, symbol)
                structured_df['last_traded_time'] = pd.to_datetime(structured_df['last_traded_time']).dt.tz_localize(
                    None).dt.round('5min').astype(str)
                self.process_order_book_data(symbol, structured_df)
                logger.info(
                    f"Order book data for symbol {symbol} fetched successfully.")
                break
            except UnboundLocalError as ule:
                logger.error(
                    f"UnboundLocalError occurred while fetching order book for {symbol}: {ule}. Retrying after {config.scheduler.wait_time_between_api_calls} seconds.")
                time.sleep(config.scheduler.wait_time_between_api_calls)
                attempt += 1
            except Exception as e:
                logger.error(
                    f"Exception occurred while fetching order book for {symbol}: {e}")
            break

    def process_order_book_data(self, symbol, data):
        self.data[symbol] = pd.concat(
            [self.data[symbol], data]).reset_index(drop=True)
        self.trim_data(symbol)
        
        # Save to InfluxDB if connected
        self._save_to_influxdb(symbol, data)
    
    def _save_to_influxdb(self, symbol: str, df: pd.DataFrame) -> None:
        """Save orderbook data to InfluxDB."""
        if not self._influx_client or not self._influx_connected:
            return
            
        try:
            # Convert DataFrame to OrderBookDataPoint objects
            data_points: list[OrderBookDataPoint] = []
            
            for _, row in df.iterrows():
                try:
                    # Handle timestamp conversion
                    if "last_traded_time" in df.columns:
                        timestamp = pd.to_datetime(row["last_traded_time"])
                    else:
                        timestamp = datetime.now()
                    
                    # Parse bids and asks (may be string representation of list)
                    bids = row.get("bids", None)
                    asks = row.get("asks", None)
                    
                    # Convert string representation to list if needed
                    import ast
                    if isinstance(bids, str):
                        try:
                            bids = ast.literal_eval(bids)
                        except (ValueError, SyntaxError):
                            bids = None
                    if isinstance(asks, str):
                        try:
                            asks = ast.literal_eval(asks)
                        except (ValueError, SyntaxError):
                            asks = None
                        
                    data_points.append(OrderBookDataPoint(
                        symbol=symbol,
                        timestamp=timestamp.to_pydatetime() if hasattr(timestamp, 'to_pydatetime') else timestamp,
                        total_buy_qty=int(row.get("total_buy_qty", 0)),
                        total_sell_qty=int(row.get("total_sell_qty", 0)),
                        last_traded_price=float(row.get("last_traded_price", 0)),
                        last_traded_qty=int(row.get("last_traded_qty", 0)),
                        volume=int(row.get("volume", 0)),
                        average_traded_price=float(row.get("average_traded_price", 0)),
                        lower_circuit=float(row.get("lower_circuit", 0)),
                        upper_circuit=float(row.get("upper_circuit", 0)),
                        change_percent=float(row.get("change_percent", 0)),
                        # Additional fields
                        bids=bids,
                        asks=asks,
                        open=float(row["open"]) if pd.notna(row.get("open")) else None,
                        high=float(row["high"]) if pd.notna(row.get("high")) else None,
                        low=float(row["low"]) if pd.notna(row.get("low")) else None,
                        close=float(row["close"]) if pd.notna(row.get("close")) else None,
                        tick_size=float(row["tick_size"]) if pd.notna(row.get("tick_size")) else None,
                        change=float(row["change"]) if pd.notna(row.get("change")) else None,
                        expiry=str(row["expiry"]) if pd.notna(row.get("expiry")) else None,
                        open_interest=int(row["open_interest"]) if pd.notna(row.get("open_interest")) else None,
                        open_interest_flag=bool(row["open_interest_flag"]) if pd.notna(row.get("open_interest_flag")) else None,
                        previous_day_open_interest=int(row["previous_day_open_interest"]) if pd.notna(row.get("previous_day_open_interest")) else None,
                        open_interest_percent=float(row["open_interest_percent"]) if pd.notna(row.get("open_interest_percent")) else None,
                    ))
                except (ValueError, KeyError) as e:
                    logger.debug(f"Skipping row due to conversion error: {e}")
                    continue
            
            if data_points:
                loop = asyncio.new_event_loop()
                success = loop.run_until_complete(
                    self._influx_client.write_orderbook_batch(data_points)
                )
                loop.close()
                if success:
                    logger.debug(f"Saved {len(data_points)} orderbook records to InfluxDB for {symbol}")
                else:
                    logger.warning(f"Failed to save orderbook to InfluxDB for {symbol}")
                    
        except Exception as e:
            logger.warning(f"Failed to save orderbook to InfluxDB for {symbol}: {e}")

    def trim_data(self, symbol):
        # TODO:
        start_tm = datetime.now() - pd.DateOffset(years=config.backtest_data_load.backtest_data_length_years)
        self.data[symbol] = self.data[symbol][pd.to_datetime(
            self.data[symbol]['last_traded_time']) > start_tm]

    def backup_hourly(self):
        now = datetime.now()
        logger.info(
            f"Starting order data backup at {now.strftime('%Y-%m-%d %H:%M:%S')}")

        for symbol, df in self.data.items():
            file_path = os.path.join(
                self.path, f"{symbol}_{config.backtest_data_load.orderbook_file_suffix}.csv")
            try:
                if not os.path.exists(file_path):
                    df.to_csv(file_path, index=False)
                else:
                    existing_df = pd.read_csv(
                        file_path,
                        on_bad_lines="skip",
                        engine="python",
                    )
                    updated_df = pd.concat([existing_df, df], ignore_index=True)
                    updated_df.to_csv(file_path,index=False)
                logger.info(
                    f"Order Book Data backup {symbol} completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                logger.error(
                    f"Error backing up Order Book data for {symbol}: {e}")

    def initialize_scheduler(self):

        self.scheduler.add_job(self.backup_hourly, 'cron',
                               hour='*', id='order_book_backup_job')


# Example Usage
# config = OrderBookConfig(base_path="path/to/data", symbols_file="symbols.txt", backup_path="path/to/backup")
# fyers_instance = fyersModel.FyersModel(client_id="your_client_id", token="your_token", log_path="your_log_path")
# order_book_handler = OrderBookHandler(fyers_instance, config)
# order_book_handler.register_callback(your_callback_function)
# To stop: order_book_handler.stop()
