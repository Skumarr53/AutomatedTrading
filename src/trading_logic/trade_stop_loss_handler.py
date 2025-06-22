# src/trading_logic/trade_stop_loss_handler.py
class TradeStopLossHandler:
    """
    Computes ATR-based stop-loss levels dynamically.
    """

    def __init__(self, atr_multiplier: float = 1.5, use_adaptive_stop_loss: bool = False):
        """
        Initialize stop-loss handler.
        """
        self.atr_multiplier = atr_multiplier
        self.use_adaptive_stop_loss = use_adaptive_stop_loss

    def determine_stop_loss(self, entry_price: float, atr_value: float, combined_score: float, trade_direction: str) -> Optional[float]:
        """
        Calculate the stop-loss price based on ATR and confidence level.
        """
        if atr_value is None:
            return None

        if self.use_adaptive_stop_loss:
            confidence = (combined_score - 1) / 3.0  
            adaptive_multiplier = self.atr_multiplier
            if confidence > 0.8:
                adaptive_multiplier *= 0.8  
            elif confidence < 0.3:
                adaptive_multiplier *= 1.2  
            stop_loss_distance = atr_value * adaptive_multiplier
        else:
            stop_loss_distance = atr_value * self.atr_multiplier

        return entry_price - stop_loss_distance if trade_direction == "LONG" else entry_price + stop_loss_distance
