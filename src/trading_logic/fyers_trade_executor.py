"""Fyers trading execution module."""
from typing import Optional
from loguru import logger
from fyers_apiv3 import fyersModel

class FyersTradeExecutor:
    """Execute trades using the Fyers API."""

    def __init__(self, fyers: fyersModel) -> None:
        self.fyers = fyers

    def place_order(self, symbol: str, qty: int, side: str, price: Optional[float] = None) -> None:
        """Place a market order via the Fyers API."""
        try:
            order = {
                "symbol": symbol,
                "qty": qty,
                "type": 2,  # Market order
                "side": 1 if side == "BUY" else -1,
                "productType": "INTRADAY",
                "limitPrice": price or 0,
                "stopPrice": 0,
                "validity": "DAY",
                "disclosedQty": 0,
                "offlineOrder": False,
                "orderTag": "codex",
            }
            self.fyers.place_order(order)
            logger.info("Placed %s order for %s qty %d", side, symbol, qty)
        except Exception as exc:
            logger.error("Failed to place order: %s", exc)
