# src/trading_logic/trade_decision_maker.py
from datetime import datetime
from typing import Dict, Optional

class TradeDecisionMaker:
    """
    Determines trading decisions based on model predictions and ensures signal reliability
    using weighted timeframes, confirmation mechanism, and cooldown period.
    """

    def __init__(self, cooldown_minutes: int = 5):
        """
        Initialize decision maker.
        
        Args:
            cooldown_minutes (int): Time interval before executing another trade.
        """
        self.weights = {'5m': 1, '15m': 2, '1h': 4}  # Exponential weighting for signals
        self.cooldown_minutes = cooldown_minutes
        self.last_trade_time: Optional[datetime] = None
        self.pending_signal: Optional[str] = None
        self.pending_signal_time: Optional[datetime] = None

    def convert_signal_to_score(self, signal: str) -> int:
        """Convert model prediction categories to numeric scores."""
        mapping = {'Low': 1, 'Medium Low': 2, 'Neutral': 2.5, 'Medium High': 3, 'High': 4}
        return mapping.get(signal, 0)

    def compute_weighted_signal(self, predictions: Dict[str, str]) -> float:
        """
        Compute an aggregated score for predictions across timeframes using exponential weighting.
        """
        total_weight = weighted_sum = 0
        for tf, signal in predictions.items():
            score = self.convert_signal_to_score(signal)
            w = self.weights.get(tf, 1)
            weighted_sum += score * w
            total_weight += w
        return weighted_sum / total_weight if total_weight > 0 else 0

    def is_cooldown_over(self, current_time: datetime) -> bool:
        """Check if the cooldown period after the last trade has passed."""
        if self.last_trade_time is None:
            return True
        elapsed_minutes = (current_time - self.last_trade_time).total_seconds() / 60.0
        return elapsed_minutes >= self.cooldown_minutes

    def confirm_signal(self, signal: str, current_time: datetime) -> bool:
        """
        Confirms the signal by requiring it to persist across multiple checks.
        """
        if signal is None:
            self.pending_signal = None
            return False
        if self.pending_signal is None or signal != self.pending_signal:
            self.pending_signal = signal
            self.pending_signal_time = current_time
            return False
        return True
