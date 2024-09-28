import time, pytz
from datetime import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from src.auth.fyers_auth import AuthCodeGenerator
from src.data.data_fetcher import DataHandler
from src.utils.utils import load_symbols
from typing import Callable
from src.feature_engineering.technical_indicators import TechnicalIndicators
from src.financial_analysis.trading_strategies import TradingStrategies
from src.feature_engineering.feature_aggregator import DataAggregator
from src.data.order_book_handler import OrderBookHandler
from src.pipelines.custom_pipelines import CustomModelPipeline
from src.config import log_config, config

# Setup logging
log_config.setup_logging()
if config.ENV == "prod":
    logging.getLogger('apscheduler').setLevel(logging.ERROR)
else:
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)


class MarketAnalysisApp:
    """
    Market Analysis Application for handling authorization, data fetching,
    computing technical indicators, executing trading strategies, and
    handling order book data.
    """
    def __init__(self):
        self.trading_mode = config.TRADE_MODE
        self.setup_based_on_mode()
    
    def setup_based_on_mode(self):
        self.symbols = load_symbols(config.SYMBOLS_PATH)
        self.generator = AuthCodeGenerator()
        self._setup_authorization()
        self.scheduler = BackgroundScheduler() if self.trading_mode == 'LIVE' else None 
        self._setup_data_handling()
        self.order_data_handler = OrderBookHandler(
            self.fyers_instance, self.scheduler)
        self.data_aggregator = DataAggregator()
        self.strategy_module = TradingStrategies()
        self.last_data_collection_time = None
        # self.custom_model = CustomModelPipeline(model_id = 'COMB')
        self.timezone = pytz.timezone(config.TIMEZONE)

    def _setup_data_handling(self):
        self.indicators = TechnicalIndicators()
        self.ticker_data_handler = DataHandler(self.fyers_instance, self.scheduler)
        self.ticker_data_handler.register_callback(
                self.indicators.get_stock_indicators)
        # self.indicators.register_callback(self.execute_strategies)
    
    def execute_strategies(self, indicators_data):
        """
        Execute trading strategies based on the indicators data.
        """
        try:
            strategy_decisions = self.strategy_module.execute_technical_strategy(
                indicators_data)
            # Process the strategy decisions further as needed
        except Exception as e:
            logging.exception("Strategy execution failed")
            raise


    def _schedule_job(self, func: Callable, interval: int, job_id: str, max_instances: int=1) -> None:
        """Schedules a single job with a delay mechanism."""
        self.scheduler.add_job(
            func,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute=f'*/{interval}',
            timezone=self.timezone,
            id=job_id,
            max_instances=max_instances
        )
        logging.info(f"Scheduled {job_id} every {interval} minutes.")

    def configure_scheduler(self):
        """
        Schedule regular data updates during trading hours.
        """
        self._schedule_job(
            self.data_collection, config.DATA_FETCH_CRON_INTERVAL_MIN, "data_collection", max_instances=2)
        # self._schedule_job(self.start_live_trading, config.TRADE_RUN_INTERVAL_MIN, "start_live_trading")
        self.scheduler.start()

    def data_collection(self):
        self.order_data_handler.fetch_order_book_data()
        time.sleep(5)
        # self.ticker_data_handler.update_data_regularly()
        self.last_data_collection_time = datetime.now()

    def start_live_trading(self):
        time.sleep(10)
        ## TODO Turn assert on 
        # assert (datetime.now() - self.last_data_collection_time).seconds < 60, 'Data Collection and Trading Excecution not in sync'
        for symbol in self.symbols:
            data_agg = self.data_aggregator.aggregate_features(
                self.ticker_data_handler.data[symbol], self.order_data_handler.data[symbol])
        pass

    def start_backtesting(self):
        ## TODO fill backtest logic
        for symbol in self.symbols:
            data_agg = self.data_aggregator.aggregate_features(
                self.ticker_data_handler.data[symbol], self.order_data_handler.data[symbol])
            
            ## Run Custom pipeline
            self.custom_model.run()
            
            
            
        pass
    
    def _setup_authorization(self):
        """
        Setup authorization for Fyers API.
        """
        try:
            self.fyers_instance = self.generator.initialize_fyers_model()
            logging.info("Authorization successful.")
        except Exception as e:
            logging.exception("Authorization failed")
            raise



def main():
    app = MarketAnalysisApp()
    if app.trading_mode == 'LIVE':
        app.configure_scheduler()
    else:
        app.start_backtesting()
    while True:
        time.sleep(5)

if __name__ == "__main__":
    main()
