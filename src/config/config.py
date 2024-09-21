import os
from dotenv import load_dotenv  # Correct import statement
from src.utils.utils import determine_mode, load_config

# Load environment variables from .env file in the project root
load_dotenv()



# Constants
REDIRECT_URL = "https://www.google.com/"
RESPONSE_TYPE = "code"
GRANT_TYPE = "authorization_code"
MANUAL_SET_TRADE_MODE = None #'BACKTEST'  # 'LIVE' None 'BACKTEST'

# Environment variables
CLIENT_ID = os.environ.get('FYERS_CLIENT_ID')
SECRET_KEY = os.environ.get('FYERS_SECRET_ID')
TOTP_SECRET = os.environ.get('TOTP_SECRET')
USER_PIN = os.environ.get('USER_PIN')
USER_NAME = os.environ.get('USER_NAME', 'dummy-user')  # default if not set
ENV = os.environ.get('ENVIRONMENT', 'uat')



## Scheduler configuration
BACKUP_INTERVAL_HOURS = 1  # Backup job interval in hours
# Cron schedule for data fetching
DATA_FETCH_CRON_INTERVAL_MIN = 5
TRADE_RUN_INTERVAL_MIN = 5 
CHUNK_SIZE_DAYS = 90  # Chunk size for data fetching
TIMEZONE = 'Asia/Kolkata'  # Timezone for scheduling jobs

WAIT_TIME_BETWEEN_API_CALLS = 10 # seconds
MAX_API_CALL_ATTEMPT = 3
BACKTEST_DATA_LENGTH_YEARS = 3
N_OPERATIONS_HOURS_DAILY = 8
N_OPERATIONS_DAYS_WEEKLY = 5
VOLUME_MEAN_WINDOWS = [3, 5]
TECH_INDS_MAX_LENGTH = 234  # tech indicators compute window size
CS_PATTERNS_MAX_LENGTH = 96
FIB_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]

## Custom model 
CUSTOM_MODEL_WINDOWS = [5, 15, 30, 60, 180]

# File paths
ORDERBOOK_FILENAME = 'backups/OderBookData'
TICKER_FILENAME = 'backups/TickerData'
SYMBOLS_PATH = 'src/config/stock_symbols.txt'
ORDERBOOK_FILE_SUFF = 'orderbook_data'
TICKER_FILE_SUFF = 'ticker_data'
MODEL_CONFIG_FILENAME = '../src/config/model_config.yaml'
MODEL_PARAM_FILE = 'model_artifacts'
CUSTOM_MODEL_BEST_PARAM_PATH = '{}_CustiomModelPipeline.joblib'


TRADE_MODE = MANUAL_SET_TRADE_MODE if MANUAL_SET_TRADE_MODE is not None else determine_mode()
MODEL_CONFIG = load_config(MODEL_CONFIG_FILENAME)





# ['epoch_time', 'open', 'high', 'low', 'close', 'volume', 'date', 'candlestick_length', 'body_length', 'body_mid_point', 'is_green', 'body_to_length_ratio', 'candlestick_length_prev_1', 'body_length_prev_1', 'body_mid_point_prev_1', 'is_green_prev_1', 'body_to_length_ratio_prev_1', 'candlestick_length_prev_2', 'body_length_prev_2', 'body_mid_point_prev_2', 'is_green_prev_2', 'body_to_length_ratio_prev_2', 'high_1h', 'low_1h', 'high_5h', 'low_5h', 'high_1d', 'low_1d', 'high_3d', 'low_3d', 'high_5d', 'low_5d', 'high_14d', 'low_14d', 'high_52w', 'low_52w', 'volume_pct_change_last_interval', 'volume_pct_change_mean_3', 'volume_pct_change_mean_5', 'hour_of_day', 'day_of_week', 'month_of_year', 'quarter_of_year', 'candlestick_gap', 'bollinger_upperband_param1', 'bollinger_middleband_param1', 'bollinger_lowerband_param1', 'bollinger_upperband_param2', 'bollinger_middleband_param2', 'bollinger_lowerband_param2', 'bollinger_upperband_param3', 'bollinger_middleband_param3', 'bollinger_lowerband_param3', 'rsi_param1', 'rsi_param2', 'rsi_param3', 'macd_param1', 'macd_signal_param1', 'macd_hist_param1', 'macd_param2', 'macd_signal_param2', 'macd_hist_param2', 'macd_param3', 'macd_signal_param3', 'macd_hist_param3', 'stochastic_k_param1', 'stochastic_d_param1', 'stochastic_k_param2', 'stochastic_d_param2', 'stochastic_k_param3', 'stochastic_d_param3', 'adx_param1', 'adx_param2', 'adx_param3', 'ema_short_param1', 'ema_long_param1', 'ema_short_param2', 'ema_long_param2', 'ema_short_param3', 'ema_long_param3', 'vwap', 'atr_param1', 'atr_param2', 'atr_param3', 'obv', 'sar', 'cci_param1', 'cci_param2', 'cci_param3', 'ichimoku_conversion_line_param1', 'ichimoku_base_line_param1', 'ichimoku_leading_span_a_param1', 'ichimoku_leading_span_b_param1', 'ichimoku_lagging_span_param1', 'ichimoku_price_above_cloud_param1', 'ichimoku_conversion_line_param2', 'ichimoku_base_line_param2', 'ichimoku_leading_span_a_param2', 'ichimoku_leading_span_b_param2', 'ichimoku_lagging_span_param2', 'ichimoku_price_above_cloud_param2', 'ichimoku_conversion_line_param3', 'ichimoku_base_line_param3', 'ichimoku_leading_span_a_param3', 'ichimoku_leading_span_b_param3', 'ichimoku_lagging_span_param3', 'ichimoku_price_above_cloud_param3', 'symbol', 'total_buy_qty', 'total_sell_qty', 'bids', 'asks', 'open', 'high', 'low', 'close', 'change_percent', 'tick_size', 'change', 'last_traded_qty', 'last_traded_time', 'last_traded_price', 'volume', 'average_traded_price', 'lower_circuit', 'upper_circuit', 'expiry', 'open_interest', 'open_interest_flag', 'previous_day_open_interest', 'open_interest_percent', 'weighted_bid_price', 'total_bid_volume', 'weighted_ask_price', 'total_ask_volume', 'spread', 'buy_sell_pressure_ratio', 'intraday_price_range', 'price_movement_open_close']
