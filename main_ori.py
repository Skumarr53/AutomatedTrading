import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from src.auth.fyers_auth import AuthCodeGenerator
from src.data.data_fetcher import DataHandler
from src.financial_analysis.technical_indicators import TechnicalIndicators
from src.financial_analysis.trading_strategies import TradingStrategies
from src.utils.utils import determine_mode 
from data.order_book_handler import OrderBookHandler
from config import log_config, config

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
        self.scheduler = BackgroundScheduler()
        self.generator = AuthCodeGenerator()
        self.fyers_instance = None
        self.data_handler = None
        self.indicators = TechnicalIndicators()
        self.strategy_module = TradingStrategies()
        self.order_book_handler = None  # Initialize the order book handler attribute
        self.trading_mode = config.TRADE_MODE if config.TRADE_MODE else determine_mode()  

    def initialize(self):
        """
        Initialize the application by setting up authorization, data handling, 
        order book handling, computing technical indicators, and executing trading strategies.
        """
        self._setup_authorization()
        self._setup_data_handling()
        self._setup_order_book_handling()  # Setup order book handling

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

    def _setup_data_handling(self):
        """
        Setup data handling and callbacks for processing the data.
        """
        try:
            self.data_handler = DataHandler(
                self.fyers_instance, self.scheduler)
            self.data_handler.register_callback(
                self.indicators.get_stock_indicators)
            self.indicators.register_callback(self.execute_strategies)
        except Exception as e:
            logging.exception("Data handling setup failed")
            raise

    def _setup_order_book_handling(self):
        """
        Setup order book data handling and callbacks.
        """
        try:
            self.order_book_handler = OrderBookHandler(
                self.fyers_instance, self.scheduler)
            # Register any callbacks needed for order book data
            # For example:
            # self.order_book_handler.register_callback(self.some_order_book_process)
        except Exception as e:
            logging.exception("Order book handling setup failed")
            raise

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

    def start(self):
        """
        Start the application.
        """
        try:
            self.scheduler.start()
        except Exception as e:
            logging.exception("Scheduler failed to start")
            raise

    @staticmethod
    def some_other_process(strategy_decisions):
        """
        Placeholder for additional processes post strategy execution.
        """
        # Process the strategy decisions
        pass


def main():
    """
    Main function to run the application.
    """
    app = MarketAnalysisApp()
    app.initialize()
    app.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        app.scheduler.shutdown()
        logging.info("Application shutdown successfully")


if __name__ == "__main__":
    main()
