"""Fyers trading execution module."""
from typing import Optional, Dict, Any, List
from loguru import logger
from fyers_apiv3 import fyersModel
import time


class FyersTradeExecutor:
    """Execute trades using the Fyers API with comprehensive order management."""

    def __init__(self, fyers: fyersModel, default_product_type: str = "CNC") -> None:
        """
        Initialize the Fyers Trade Executor.
        
        Args:
            fyers: Initialized FyersModel instance
            default_product_type: Default product type ('INTRADAY' or 'CNC')
        """
        self.fyers = fyers
        self.default_product_type = default_product_type
        self._cache = {
            'funds': None,
            'funds_timestamp': 0,
            'positions': None,
            'positions_timestamp': 0
        }
        self.cache_ttl = 30  # Cache TTL in seconds

    def get_funds(self, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get account funds and available balance.
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            Dictionary with fund details or None on error
        """
        current_time = time.time()
        
        # Return cached data if valid
        if use_cache and self._cache['funds'] and (current_time - self._cache['funds_timestamp']) < self.cache_ttl:
            logger.debug("Returning cached funds data")
            return self._cache['funds']
        
        try:
            response = self.fyers.funds()
            
            if response and response.get('code') == 200:
                fund_data = response.get('fund_limit', [])
                if fund_data:
                    funds_info = {
                        'total_balance': fund_data[0].get('equityAmount', 0),
                        'available_balance': fund_data[0].get('availableBalance', 0),
                        'used_margin': fund_data[0].get('utilized_amount', 0),
                        'collateral': fund_data[0].get('collateral', 0),
                        'raw_response': fund_data[0]
                    }
                    
                    # Update cache
                    self._cache['funds'] = funds_info
                    self._cache['funds_timestamp'] = current_time
                    
                    logger.info(f"Available Balance: ₹{funds_info['available_balance']:.2f}")
                    return funds_info
            
            logger.warning(f"Unexpected funds response: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch funds: {e}")
            return None

    def get_positions(self, use_cache: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Get current open positions.
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            List of position dictionaries or None on error
        """
        current_time = time.time()
        
        # Return cached data if valid
        if use_cache and self._cache['positions'] and (current_time - self._cache['positions_timestamp']) < self.cache_ttl:
            logger.debug("Returning cached positions data")
            return self._cache['positions']
        
        try:
            response = self.fyers.positions()
            
            if response and response.get('code') == 200:
                positions = response.get('netPositions', [])
                
                # Update cache
                self._cache['positions'] = positions
                self._cache['positions_timestamp'] = current_time
                
                logger.info(f"Current positions count: {len(positions)}")
                return positions
            
            logger.warning(f"Unexpected positions response: {response}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return None

    def get_holdings(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get current holdings (long-term investments).
        
        Returns:
            List of holding dictionaries or None on error
        """
        try:
            response = self.fyers.holdings()
            
            if response and response.get('code') == 200:
                holdings = response.get('holdings', [])
                logger.info(f"Current holdings count: {len(holdings)}")
                return holdings
            
            logger.warning(f"Unexpected holdings response: {response}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to fetch holdings: {e}")
            return None

    def get_position_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get position details for a specific symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'NSE:SBIN-EQ')
            
        Returns:
            Position dictionary or None if no position exists
        """
        positions = self.get_positions()
        
        if positions:
            for pos in positions:
                if pos.get('symbol') == symbol:
                    logger.info(f"Found position for {symbol}: qty={pos.get('netQty', 0)}, P&L={pos.get('pl', 0)}")
                    return pos
        
        logger.debug(f"No position found for {symbol}")
        return None

    @staticmethod
    def validate_symbol_format(symbol: str) -> bool:
        """
        Validate if symbol is in correct Fyers format (EXCHANGE:SYMBOL-TYPE).
        
        Args:
            symbol: Symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check for correct format: NSE:SYMBOL-EQ or BSE:SYMBOL-EQ
        if ':' not in symbol or '-' not in symbol:
            logger.error(f"Invalid symbol format: '{symbol}'. Expected format: 'NSE:SYMBOL-EQ'")
            return False
        
        exchange, rest = symbol.split(':', 1)
        if exchange not in ['NSE', 'BSE', 'MCX', 'NFO']:
            logger.error(f"Invalid exchange: '{exchange}'. Use NSE, BSE, MCX, or NFO")
            return False
        
        return True

    def calculate_position_size(self, symbol: str, price: float, risk_percent: float = 2.0, 
                               max_position_size: int = 100) -> int:
        """
        Calculate position size based on available funds and risk management.
        
        Args:
            symbol: Trading symbol
            price: Current price per share
            risk_percent: Percentage of account to risk (default: 2%)
            max_position_size: Maximum quantity allowed (default: 100)
            
        Returns:
            Calculated quantity to trade
        """
        funds = self.get_funds()
        
        if not funds:
            logger.warning("Cannot calculate position size - funds unavailable")
            return 0
        
        available_balance = funds.get('available_balance', 0)
        
        if available_balance <= 0:
            logger.warning("Insufficient funds available")
            return 0
        
        # Calculate quantity based on risk percentage
        risk_amount = available_balance * (risk_percent / 100)
        calculated_qty = int(risk_amount / price)
        
        # Apply maximum position size limit
        final_qty = min(calculated_qty, max_position_size)
        
        logger.info(f"Position size for {symbol}: {final_qty} shares (Price: ₹{price:.2f}, Risk: {risk_percent}%)")
        return final_qty

    def place_order(self, symbol: str, qty: int, side: str, 
                   order_type: str = "MARKET", price: Optional[float] = None,
                   product_type: Optional[str] = None, 
                   stop_loss: float = 0, take_profit: float = 0) -> Optional[Dict[str, Any]]:
        """
        Place an order via the Fyers API.
        
        Args:
            symbol: Trading symbol (e.g., 'NSE:SBIN-EQ')
            qty: Quantity to trade
            side: 'BUY' or 'SELL'
            order_type: 'MARKET' or 'LIMIT' (default: 'MARKET')
            price: Limit price (required for LIMIT orders)
            product_type: 'INTRADAY' or 'CNC' (default: from init)
            stop_loss: Stop loss price (default: 0)
            take_profit: Take profit price (default: 0)
            
        Returns:
            API response dictionary or None on error
        """
        # Validate symbol format
        if not self.validate_symbol_format(symbol):
            logger.error(f"Order rejected: Invalid symbol format '{symbol}'. Use format 'NSE:SYMBOL-EQ'")
            return None
        
        if qty <= 0:
            logger.error(f"Invalid quantity: {qty}")
            return None
        
        if order_type == "LIMIT" and price is None:
            logger.error("Price must be specified for LIMIT orders")
            return None
        
        product_type = product_type or self.default_product_type
        
        order_data = {
            "symbol": symbol,
            "qty": qty,
            "type": 2 if order_type == "LIMIT" else 1,  # 2: LIMIT, 1: MARKET
            "side": 1 if side.upper() == "BUY" else -1,  # 1: BUY, -1: SELL
            "productType": product_type,
            "limitPrice": price, #if order_type == "LIMIT" else 0.1,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "orderTag": "AutoTrader"
        }
        
        try:
            logger.info(f"Placing {order_type} {side} order: {symbol}, Qty: {qty}, Price: {price or 'MARKET'}")
            response = self.fyers.place_order(order_data)
            
            if response and response.get('code') == 200:
                order_id = response.get('id')
                logger.success(f"✓ Order placed successfully! Order ID: {order_id}")
                
                # Invalidate position cache
                self._cache['positions'] = None
                self._cache['funds'] = None
                
                return response
            else:
                logger.error(f"Order placement failed: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Exception while placing order: {e}")
            return None

    def place_market_order(self, symbol: str, qty: int, side: str, 
                          product_type: Optional[str] = None,
                          current_price: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Convenience method to place a market order.
        
        Args:
            symbol: Trading symbol
            qty: Quantity to trade
            side: 'BUY' or 'SELL'
            product_type: Product type (default: from init)
            
        Returns:
            API response or None
        """
        if current_price:
            limit_price = current_price
        else:
            # Fallback: use a reasonable default (this may still fail)
            logger.warning(f"No current_price provided for MARKET order on {symbol}, using default")
            limit_price = 0.01
    
        return self.place_order(symbol, qty, side, order_type="MARKET", 
                           price=limit_price, product_type=product_type)

    def place_limit_order(self, symbol: str, qty: int, side: str, price: float,
                         product_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Convenience method to place a limit order.
        
        Args:
            symbol: Trading symbol
            qty: Quantity to trade
            side: 'BUY' or 'SELL'
            price: Limit price
            product_type: Product type (default: from init)
            
        Returns:
            API response or None
        """
        return self.place_order(symbol, qty, side, order_type="LIMIT", price=price, product_type=product_type)

    def get_order_book(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get all orders (pending, executed, cancelled).
        
        Returns:
            List of order dictionaries or None on error
        """
        try:
            response = self.fyers.orderbook()
            
            if response and response.get('code') == 200:
                orders = response.get('orderBook', [])
                logger.info(f"Order book entries: {len(orders)}")
                return orders
            
            logger.warning(f"Unexpected orderbook response: {response}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to fetch order book: {e}")
            return None

    def get_tradebook(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get all executed trades for the day.
        
        Returns:
            List of trade dictionaries or None on error
        """
        try:
            response = self.fyers.tradebook()
            
            if response and response.get('code') == 200:
                trades = response.get('tradeBook', [])
                logger.info(f"Trade book entries: {len(trades)}")
                return trades
            
            logger.warning(f"Unexpected tradebook response: {response}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to fetch trade book: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if cancellation successful, False otherwise
        """
        try:
            response = self.fyers.cancel_order({"id": order_id})
            
            if response and response.get('code') == 200:
                logger.success(f"✓ Order {order_id} cancelled successfully")
                return True
            
            logger.error(f"Failed to cancel order {order_id}: {response}")
            return False
            
        except Exception as e:
            logger.error(f"Exception while cancelling order: {e}")
            return False

    def exit_position(self, symbol: str, current_price: Optional[float] = None) -> bool:
        """
        Exit (close) an existing position.
        
        Args:
            symbol: Trading symbol
            current_price: Current market price for the order
            
        Returns:
            True if exit successful, False otherwise
        """
        position = self.get_position_for_symbol(symbol)
        
        if not position:
            logger.warning(f"No position to exit for {symbol}")
            return False
        
        net_qty = position.get('netQty', 0)
        
        if net_qty == 0:
            logger.info(f"Position already flat for {symbol}")
            return True
        
        # Determine exit side (opposite of current position)
        exit_side = "SELL" if net_qty > 0 else "BUY"
        exit_qty = abs(net_qty)
        
        logger.info(f"Exiting position for {symbol}: {exit_side} {exit_qty}")
        response = self.place_market_order(symbol, exit_qty, exit_side, current_price=current_price)
        
        return response is not None
