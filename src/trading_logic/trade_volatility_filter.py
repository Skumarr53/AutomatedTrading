# src/trading_logic/trade_volatility_filter.py
from typing import Optional

class TradeVolatilityFilter:
    """
    Ensures trade execution only occurs if the market volatility is above a required threshold.
    """

    def __init__(self, use_atr_filter: bool = True, atr_filter_multiplier: float = 1.0, fixed_vol_threshold: float = 0.005):
        """
        Initialize volatility filter.
        """
        self.use_atr_filter = use_atr_filter
        self.atr_filter_multiplier = atr_filter_multiplier
        self.fixed_vol_threshold = fixed_vol_threshold

    def check_volatility_filter(self, atr_value: Optional[float], price: float) -> bool:
        """
        Determine if the current market volatility meets the required threshold.
        """
        if atr_value is None:
            return False
        if self.use_atr_filter:
            return atr_value >= atr_value * self.atr_filter_multiplier
        return atr_value >= price * self.fixed_vol_threshold
