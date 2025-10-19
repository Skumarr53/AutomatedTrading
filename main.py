from src import config
import time, pytz
from scripts.telegram_notifier import send_telegram_message
from datetime import datetime
from loguru import logger
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from src.mlflow_utils.mlflow_server import start_mlflow_server, is_mlflow_server_running
# from src.trading_logic.trading_bot import TradingBot
from src.mlflow_utils.model_loader import PredictionExecutor
from src.auth.fyers_auth import AuthCodeGenerator
from src.data.data_fetcher import DataHandler
from src.utils.utils import load_symbols
import pandas as pd
from typing import Callable
from src.feature_engineering.technical_indicators import TechnicalIndicators
from src.financial_analysis.trading_strategies import TradingStrategies
from src.feature_engineering.feature_aggregator import DataAggregator
from src.data.order_book_handler import OrderBookHandler
from src.pipelines.base_pipeline import MLPipelineBase
from src.utils.utils import determine_mode, get_timezone, get_NSE_symbol
from src.mlflow_utils.model_loader import ModelCache, MLflowModelLoader
from src.trading_logic.trade_decision_maker import TradeDecisionMaker
from src.trading_logic.fyers_trade_executor import FyersTradeExecutor


print(1)

## TODO: use the following snippet for alert across 
# send_telegram_message(
# type: Good or Bad 
# message=str('Service has started')
# )

class MarketAnalysisApp:
    """
    Market Analysis Application for handling authorization, data fetching,
    computing technical indicators, executing trading strategies, and
    handling order book data.
    """
    def __init__(self):
        # self.trading_mode = cuonfig.trading_config.trade_mode
        self.trading_mode = config.trading_config.trade_mode if config.trading_config.trade_mode else determine_mode() 
        self.setup_based_on_mode()
    
    def setup_based_on_mode(self):
        # config.symbols = config.symbols #config.symbols
        self.generator = AuthCodeGenerator()
        self._setup_authorization()
        self.scheduler = BackgroundScheduler() if self.trading_mode == 'LIVE' else None 
        self._setup_data_handling()
        self.order_data_handler = OrderBookHandler(
            self.fyers_instance, self.scheduler)
        self.data_aggregator = DataAggregator()
        self.strategy_module = TradingStrategies()
        self.last_data_collection_time = None
        self.custom_model = MLPipelineBase()
        if not is_mlflow_server_running():
            start_mlflow_server()
        # self.trading_bot = TradingBot()

        # Add model loading components for LIVE mode
        if self.trading_mode == 'LIVE':
            self.model_cache = ModelCache(max_cache_size=500)
            self.model_loader = MLflowModelLoader(
                config=config,
                model_cache=self.model_cache,
                tracking_uri="http://localhost:5000"
            )
            self.trade_decision_maker = TradeDecisionMaker(cooldown_minutes=5)
            self.trade_executor = FyersTradeExecutor(
                fyers=self.fyers_instance
            )
            logger.info("Trade executor initialized successfully")
        else:
            # Initialize for BACKTEST mode to avoid AttributeError
            self.model_cache = ModelCache(max_cache_size=500)
            self.model_loader = MLflowModelLoader(
                config=config,
                model_cache=self.model_cache,
                tracking_uri="http://localhost:5000"
            )
            self.trade_decision_maker = TradeDecisionMaker(cooldown_minutes=5)
            self.trade_executor = None

    def generate_live_predictions(self, data_agg: pd.DataFrame, symbol: str) -> dict:
        """Generate predictions using loaded models."""
        try:
            # Add symbol column if using combined model
            if getattr(config.training, 'combine_all_symbols', False):
                data_agg_with_symbol = data_agg.copy()
                data_agg_with_symbol['symbol'] = symbol
                prediction_symbol = 'ALL_SYMBOLS'
            else:
                data_agg_with_symbol = data_agg
                prediction_symbol = symbol
                
            prediction_executor = PredictionExecutor(
                model_loader=self.model_loader,
                data=data_agg_with_symbol
            )
            return prediction_executor.run_predictions(
                stock_symbols=[prediction_symbol],
                time_periods=config.model_settings.run_ids,
                metrics=config.model_settings.model_targets,
                actual_symbol=symbol  # Pass actual symbol for output
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
    
    def execute_strategies(self, indicators_data):
        """
        Execute trading strategies based on the indicators data.
        """
        try:
            strategy_decisions = self.strategy_module.execute_technical_strategy(
                indicators_data)
            # Process the strategy decisions further as needed
        except Exception as e:
            logger.error("Strategy execution failed")
            raise


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
        """
        Execute live trading for all symbols:
        1. Get latest data and aggregate features
        2. Generate ML model predictions
        3. Compute technical indicators
        4. Make trading decision using weighted signals
        5. Execute trades based on decision
        """
        # time.sleep(10)
        ## TODO Turn assert on 
        # assert (datetime.now() - self.last_data_collection_time).seconds < 60, 'Data Collection and Trading Excecution not in sync'
        
        current_time = datetime.now()
        
        for symbol in config.symbols:
            try:
                logger.info(f"Processing trading signals for {symbol}")
                
                # Get data for symbol
                ticker_data = self.ticker_data_handler.data[symbol]
                order_book_data = self.order_data_handler.data[symbol]
                
                # Aggregate features
                data_agg = self.data_aggregator.aggregate_features(ticker_data, order_book_data)
                
                # Generate ML predictions for multiple timeframes
                predictions = self.generate_live_predictions(data_agg, symbol)
                symbol_predictions = predictions.get(symbol, {})
                
                # Filter predictions to get only PctChange predictions for decision making
                pct_change_predictions = {
                    k.split('_')[0]: v for k, v in symbol_predictions.items() 
                    if 'PctChange' in k
                }
                
                # Compute weighted signal score from predictions
                weighted_score = self.trade_decision_maker.compute_weighted_signal(pct_change_predictions)
                
                # Determine trade signal based on weighted score
                # Score > 3.0 indicates bullish (BUY), < 2.0 indicates bearish (SELL)
                if weighted_score >= 3.0:
                    trade_signal = "BUY"
                elif weighted_score <= 2.0:
                    trade_signal = "SELL"
                else:
                    trade_signal = "HOLD"
                
                # Format symbol for Fyers API (NSE:SYMBOL-EQ)
                fyers_symbol = get_NSE_symbol(symbol)
                
                # Check current position for the symbol
                current_position = self.trade_executor.get_position_for_symbol(fyers_symbol)
                position_qty = current_position.get('netQty', 0) if current_position else 0
                
                # Determine action based on signal and current position
                action_taken = False
                
                if trade_signal == "BUY" and position_qty <= 0:
                    # Want to BUY and don't have a long position
                    if position_qty < 0:
                        logger.info(f"{symbol}: Closing SHORT position before going LONG")
                        self.trade_executor.exit_position(fyers_symbol)
                    
                    # Check cooldown period before executing
                    if not self.trade_decision_maker.is_cooldown_over(current_time):
                        logger.info(f"{symbol}: BUY signal {trade_signal} (score: {weighted_score:.2f}) - Cooldown active, skipping")
                        continue
                    
                    # Confirm signal (requires consistency across checks)
                    signal_confirmed = self.trade_decision_maker.confirm_signal(trade_signal, current_time)
                    
                    if signal_confirmed:
                        # Get current price from data
                        current_price = data_agg['close'].iloc[-1] if 'close' in data_agg.columns else None
                        
                        if current_price:
                            ## EDIT: buy at market price avaiable as current_price may not exactly match and assuming the diff wont drastic
                            # Calculate position size based on risk management
                            qty = self.trade_executor.calculate_position_size(
                                symbol=fyers_symbol,
                                price=current_price,
                                risk_percent=2.0,  # Risk 2% of account
                                max_position_size=100
                            )
                            
                            if qty > 0:
                                logger.warning(f"{symbol}: EXECUTING BUY - Qty: {qty}, Price: ₹{current_price:.2f}, Score: {weighted_score:.2f}")
                                response = self.trade_executor.place_market_order(
                                    symbol=fyers_symbol,
                                    qty=qty,
                                    side="BUY",
                                    current_price=current_price
                                )
                                
                                if response:
                                    self.trade_decision_maker.last_trade_time = current_time
                                    action_taken = True
                                    logger.success(f"✓ {symbol}: BUY order executed successfully")
                            else:
                                logger.warning(f"{symbol}: Insufficient funds or calculated qty is 0")
                        else:
                            logger.error(f"{symbol}: Cannot determine current price")
                    else:
                        logger.info(f"{symbol}: BUY signal not confirmed yet, waiting for consistency")
                
                elif trade_signal == "SELL" and position_qty >= 0:
                    # Want to SELL and don't have a short position
                    if position_qty > 0:
                        logger.info(f"{symbol}: Closing LONG position")
                        
                        # Check cooldown period before executing
                        if not self.trade_decision_maker.is_cooldown_over(current_time):
                            logger.info(f"{symbol}: SELL signal (score: {weighted_score:.2f}) - Cooldown active, skipping")
                            continue
                        
                        # Confirm signal
                        signal_confirmed = self.trade_decision_maker.confirm_signal(trade_signal, current_time)
                        
                        if signal_confirmed:
                            logger.warning(f"{symbol}: EXECUTING SELL (Exit) - Score: {weighted_score:.2f}")
                            success = self.trade_executor.exit_position(fyers_symbol, current_price=current_price)
                            
                            if success:
                                self.trade_decision_maker.last_trade_time = current_time
                                action_taken = True
                                logger.success(f"✓ {symbol}: SELL (Exit) executed successfully")
                        else:
                            logger.info(f"{symbol}: SELL signal not confirmed yet, waiting for consistency")
                    else:
                        # No position, could initiate SHORT if strategy allows
                        logger.info(f"{symbol}: SELL signal but no LONG position to exit (SHORT trading not implemented)")
                
                elif trade_signal == "HOLD":
                    logger.debug(f"{symbol}: HOLD signal - no action taken")
                
                # Log the decision summary
                if not action_taken:
                    logger.info(f"{symbol}: Signal={trade_signal}, Score={weighted_score:.2f}, Position={position_qty}, Action=NONE")
                
                logger.debug(f"{symbol}: Predictions = {symbol_predictions}")
                    
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue

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
