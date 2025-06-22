from src import config
import time
from datetime import datetime
from loguru import logger
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
# from src.trading_logic.trading_bot import TradingBot
from src.mlflow_utils.model_loader import PredictionExecutor
from src.auth.fyers_auth import AuthCodeGenerator
from src.data.data_fetcher import DataHandler
from typing import Callable
from src.feature_engineering.technical_indicators import TechnicalIndicators
from src.financial_analysis.trading_strategies import TradingStrategies
from src.feature_engineering.feature_aggregator import DataAggregator
from src.data.order_book_handler import OrderBookHandler
from src.pipelines.base_pipeline import MLPipelineBase
from src.utils.utils import determine_mode, get_timezone
# Utility classes
from src.mlflow_utils.model_loader import ModelCache, MLflowModelLoader

# TODO: use the following snippet for alerts across services


class MarketAnalysisApp:
    """
    Market Analysis Application for handling authorization, data fetching,
    computing technical indicators, executing trading strategies, and
    handling order book data.
    """
    def __init__(self):
        # self.trading_mode = config.trading_config.trade_mode
        self.trading_mode = (
            config.trading_config.trade_mode
            if config.trading_config.trade_mode
            else determine_mode()
        )
        self.setup_based_on_mode()

    def setup_based_on_mode(self):
        # config.symbols = config.symbols #config.symbols
        self.generator = AuthCodeGenerator()
        self._setup_authorization()
        self.scheduler = (
            BackgroundScheduler() if self.trading_mode == 'LIVE' else None
        )
        self._setup_data_handling()
        self.order_data_handler = OrderBookHandler(
            self.fyers_instance, self.scheduler)
        self.data_aggregator = DataAggregator()
        self.strategy_module = TradingStrategies()
        self.last_data_collection_time = None
        self.custom_model = MLPipelineBase()
        # self.trading_bot = TradingBot()

        # Add model loading components for LIVE mode
        if self.trading_mode == 'LIVE':
            self.model_cache = ModelCache(max_cache_size=500)
            self.model_loader = MLflowModelLoader(
                config=config,
                model_cache=self.model_cache
            )

    def generate_live_predictions(
        self, data_agg: pd.DataFrame, symbol: str
    ) -> dict:
        """Generate predictions using loaded models."""
        try:
            prediction_executor = PredictionExecutor(
                model_loader=self.model_loader,
                data=data_agg
            )
            return prediction_executor.run_predictions(
                stock_symbols=[symbol],
                time_periods=config.model_settings.run_ids,
                metrics=config.model_settings.model_targets
            )
        except Exception as e:
            logger.error(f"Prediction failed for {symbol}: {str(e)}")
            raise
    
    def execute_strategies(self, indicators_data: dict, predictions: dict) -> None:
        """Execute trading strategies combining indicators and model predictions."""
        try:
            # Merge technical indicators with model predictions
            enhanced_data = {
                **indicators_data,
                'model_predictions': predictions
            } 
            
            strategy_decisions = self.strategy_module.execute_technical_strategy(enhanced_data)
            self._execute_trades_based_on_decisions(strategy_decisions)
        except Exception as e:
            logger.error("Strategy execution failed")
            raise

    def _setup_data_handling(self):
        self.indicators = TechnicalIndicators()
        self.ticker_data_handler = DataHandler(self.fyers_instance, self.scheduler)
        self.ticker_data_handler.register_callback(
            self.indicators.get_stock_indicators)
        # self.indicators.register_callback(self.execute_strategies)


    def _schedule_job(self, func: Callable, job_id: str) -> None:
        """Schedules a single job with a delay mechanism."""
        self.scheduler.add_job(
            func,
            'cron',
            day_of_week=config.scheduler.day_of_week,
            hour=config.scheduler.hour,
            minute=f'*/{config.scheduler.data_fetch_cron_interval_min}',
            timezone=get_timezone(),
            id=job_id,
            max_instances=config.scheduler.max_instances
        )
        logger.info(f"Scheduled {job_id} every {config.scheduler.data_fetch_cron_interval_min} minutes.")

    def configure_scheduler(self):
        """
        Schedule regular data updates during trading hours.
        """
        self._schedule_job(
            self.data_collection, "data_collection")
        # self._schedule_job(self.start_live_trading, config.scheduler.data_fetch_cron_interval_min, "start_live_trading")
        self.scheduler.start()

    def data_collection(self):
        self.order_data_handler.fetch_order_book_data()
        self.ticker_data_handler.update_data_regularly()
        self.last_data_collection_time = datetime.now()
        self.start_live_trading()

    def start_live_trading(self):
        # time.sleep(10)
        ## TODO Turn assert on 
        # assert (datetime.now() - self.last_data_collection_time).seconds < 60, 'Data Collection and Trading Excecution not in sync'
        for symbol in config.symbols:
            ticker_data = self.ticker_data_handler.data[symbol]#.tail(1)
            order_book_data = self.order_data_handler.data[symbol]#.tail(1)
            
            # Aggregate features
            data_agg = self.data_aggregator.aggregate_features(ticker_data, order_book_data)

            predictions = self.generate_live_predictions(data_agg, symbol)

    def start_backtesting(self):
        """Run backtesting for all configured symbols."""
        if getattr(config.training, 'combine_all_symbols', False):
            combined = []
            for symbol in config.symbols:
                data_agg = self.data_aggregator.aggregate_features(
                    self.ticker_data_handler.data[symbol],
                    self.order_data_handler.data[symbol]
                )
                data_agg['symbol'] = symbol
                combined.append(data_agg)

            if combined:
                combined_df = pd.concat(combined, ignore_index=True)
                self.custom_model.train(combined_df, 'ALL_SYMBOLS')
        else:
            for symbol in config.symbols:
                data_agg = self.data_aggregator.aggregate_features(
                    self.ticker_data_handler.data[symbol],
                    self.order_data_handler.data[symbol]
                )
                self.custom_model.train(data_agg, symbol)
    
    def _setup_authorization(self):
        """
        Setup authorization for Fyers API.
        """
        try:
            self.fyers_instance = self.generator.initialize_fyers_model()
            logger.info("Authorization successful.")
        except Exception as e:
            logger.error("Authorization failed")
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
