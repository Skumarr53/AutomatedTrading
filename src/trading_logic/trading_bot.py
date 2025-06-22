# src/trading_logic/trading_bot.py
from datetime import datetime
from src.trading_logic.trade_decision_maker import TradeDecisionMaker
from src.trading_logic.trade_volatility_filter import TradeVolatilityFilter
from src.trading_logic.trade_stop_loss_handler import TradeStopLossHandler

class TradingBot:
    def __init__(self):
        self.decision_maker = TradeDecisionMaker()
        self.volatility_filter = TradeVolatilityFilter()
        self.stop_loss_handler = TradeStopLossHandler()

    def run_strategy(self, market_data, model_predictions, current_time: datetime):
        """
        Main routine to evaluate trade execution.
        """

        # Compute weighted decision
        combined_score = self.decision_maker.compute_weighted_signal(model_predictions)
        trade_signal = "LONG" if combined_score >= 2.5 else "SHORT"

        # Check volatility filter
        atr_value = market_data["atr"]
        price = market_data["price"]
        if not self.volatility_filter.check_volatility_filter(atr_value, price):
            return

        # Check cooldown
        if not self.decision_maker.is_cooldown_over(current_time):
            return

        # Confirm signal
        if not self.decision_maker.confirm_signal(trade_signal, current_time):
            return

        # Determine stop-loss
        stop_loss_price = self.stop_loss_handler.determine_stop_loss(price, atr_value, combined_score, trade_signal)
        if stop_loss_price is None:
            return

        print(f"[TRADE] Executing {trade_signal} at {price:.2f}, Stop-loss: {stop_loss_price:.2f}")
        self.decision_maker.last_trade_time = current_time
