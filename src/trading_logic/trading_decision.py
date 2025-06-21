"""Trading decision module."""
from typing import Iterable
import pandas as pd

class TradingDecision:
    """Simple decision engine based on model predictions and strategy signals."""

    def __init__(self,
                 buy_labels: Iterable[str] = ("High", "Medium High"),
                 sell_labels: Iterable[str] = ("Medium Low", "Low")) -> None:
        self.buy_labels = set(buy_labels)
        self.sell_labels = set(sell_labels)

    def decide_row(self, row: pd.Series) -> str:
        """Return BUY, SELL or HOLD for a single row of data."""
        strategy_signal = row.get("Majority_Vote_Strategy")
        if strategy_signal in {"BUY", "SELL"}:
            return strategy_signal

        pred_label = row.get("prediction_pct_change")
        if pred_label in self.buy_labels:
            return "BUY"
        if pred_label in self.sell_labels:
            return "SELL"
        return "HOLD"

    def generate_decisions(self, df: pd.DataFrame) -> pd.Series:
        """Generate decision column for a DataFrame."""
        return df.apply(self.decide_row, axis=1)
