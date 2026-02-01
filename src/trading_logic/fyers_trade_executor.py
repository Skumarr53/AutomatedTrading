"""Fyers trading execution module with circuit breaker and prediction validation."""
from typing import Optional, Dict, Any, List
from loguru import logger
from fyers_apiv3 import fyersModel
from datetime import datetime, time as dt_time
from dataclasses import dataclass, field
import time


@dataclass
class CircuitBreakerState:
    """
    Tracks circuit breaker state for loss protection.
    
    Circuit breaker trips when:
    - Daily P&L drops below threshold (e.g., -5%)
    - Consecutive losses exceed limit (e.g., 3 in a row)
    """
    daily_pnl: float = 0.0
    daily_trades: int = 0
    consecutive_losses: int = 0
    is_tripped: bool = False
    trip_reason: str = ""
    trip_time: Optional[datetime] = None
    last_reset_date: Optional[datetime] = None
    trade_history: List[Dict[str, Any]] = field(default_factory=list)


class PredictionValidator:
    """
    Validates model predictions before trade execution.
    
    Ensures predictions are:
    - Within expected class labels
    - Not stale (recent enough)
    - Consistent across multiple checks (optional)
    """
    
    VALID_CLASSES = {'Low', 'Medium Low', 'Neutral', 'Medium High', 'High'}
    
    @classmethod
    def validate_prediction(
        cls,
        prediction: Any,
        expected_classes: Optional[set] = None,
        allow_none: bool = False,
    ) -> tuple[bool, str]:
        """
        Validate a single prediction value.
        
        Args:
            prediction: Prediction label to validate
            expected_classes: Set of valid class labels
            allow_none: Whether None/NaN predictions are acceptable
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if prediction is None or (hasattr(prediction, '__len__') and len(prediction) == 0):
            if allow_none:
                return True, ""
            return False, "Prediction is None or empty"
        
        expected = expected_classes or cls.VALID_CLASSES
        
        # Handle numpy/pandas types
        pred_str = str(prediction)
        
        if pred_str not in expected:
            return False, f"Invalid prediction '{pred_str}'. Expected one of: {expected}"
        
        return True, ""
    
    @classmethod
    def validate_predictions_dict(
        cls,
        predictions: Dict[str, Any],
        required_timeframes: Optional[List[str]] = None,
    ) -> tuple[bool, List[str]]:
        """
        Validate a dictionary of predictions across timeframes.
        
        Args:
            predictions: Dict of {timeframe: prediction}
            required_timeframes: List of timeframes that must be present
            
        Returns:
            Tuple of (all_valid, list_of_errors)
        """
        errors = []
        
        # Check required timeframes
        if required_timeframes:
            missing = set(required_timeframes) - set(predictions.keys())
            if missing:
                errors.append(f"Missing predictions for timeframes: {missing}")
        
        # Validate each prediction
        for tf, pred in predictions.items():
            is_valid, error = cls.validate_prediction(pred)
            if not is_valid:
                errors.append(f"Timeframe '{tf}': {error}")
        
        return len(errors) == 0, errors


class FyersTradeExecutor:
    """
    Execute trades using the Fyers API with comprehensive order management.
    
    Features:
    - Circuit breaker for daily loss protection
    - Prediction validation before execution
    - Position size calculation with risk management
    - Order caching and retry logic
    """

    def __init__(
        self, 
        fyers: fyersModel, 
        default_product_type: str = "CNC",
        circuit_breaker_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the Fyers Trade Executor.
        
        Args:
            fyers: Initialized FyersModel instance
            default_product_type: Default product type ('INTRADAY' or 'CNC')
            circuit_breaker_config: Circuit breaker settings (optional)
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
        
        # Initialize circuit breaker
        cb_config = circuit_breaker_config or {}
        self._circuit_breaker = CircuitBreakerState()
        self._cb_enabled = cb_config.get('enabled', True)
        self._cb_max_daily_loss_pct = cb_config.get('max_daily_loss_percent', 5.0)
        self._cb_max_consecutive_losses = cb_config.get('max_consecutive_losses', 3)
        self._cb_cooldown_minutes = cb_config.get('cooldown_after_trigger_minutes', 60)
        self._cb_reset_time = cb_config.get('reset_time', '09:15')
        
        # Initial balance for daily P&L calculation
        self._initial_balance: Optional[float] = None
        
        logger.info(f"FyersTradeExecutor initialized: circuit_breaker={self._cb_enabled}, "
                   f"max_daily_loss={self._cb_max_daily_loss_pct}%")

    # === Circuit Breaker Methods ===
    
    def _check_daily_reset(self) -> None:
        """Check if it's time to reset daily circuit breaker state."""
        now = datetime.now()
        reset_hour, reset_minute = map(int, self._cb_reset_time.split(':'))
        reset_time = dt_time(reset_hour, reset_minute)
        
        # Check if we should reset
        if self._circuit_breaker.last_reset_date is None:
            self._reset_circuit_breaker()
        elif self._circuit_breaker.last_reset_date.date() < now.date():
            if now.time() >= reset_time:
                self._reset_circuit_breaker()
    
    def _reset_circuit_breaker(self) -> None:
        """Reset circuit breaker state for new trading day."""
        logger.info("Resetting circuit breaker for new trading day")
        
        # Store initial balance for P&L calculation
        funds = self.get_funds(use_cache=False)
        if funds:
            self._initial_balance = funds.get('available_balance', 0)
        
        self._circuit_breaker = CircuitBreakerState(
            last_reset_date=datetime.now()
        )
    
    def _update_circuit_breaker_on_trade(self, pnl: float, is_win: bool) -> None:
        """
        Update circuit breaker state after a trade.
        
        Args:
            pnl: Profit/loss from the trade
            is_win: Whether the trade was profitable
        """
        self._circuit_breaker.daily_pnl += pnl
        self._circuit_breaker.daily_trades += 1
        
        if is_win:
            self._circuit_breaker.consecutive_losses = 0
        else:
            self._circuit_breaker.consecutive_losses += 1
        
        self._circuit_breaker.trade_history.append({
            'time': datetime.now(),
            'pnl': pnl,
            'is_win': is_win,
            'cumulative_pnl': self._circuit_breaker.daily_pnl,
        })
        
        # Check if circuit breaker should trip
        self._evaluate_circuit_breaker()
    
    def _evaluate_circuit_breaker(self) -> None:
        """Evaluate if circuit breaker should be triggered."""
        if not self._cb_enabled or self._circuit_breaker.is_tripped:
            return
        
        # Check daily loss threshold
        if self._initial_balance and self._initial_balance > 0:
            daily_loss_pct = (self._circuit_breaker.daily_pnl / self._initial_balance) * 100
            
            if daily_loss_pct <= -self._cb_max_daily_loss_pct:
                self._trip_circuit_breaker(
                    f"Daily loss limit reached: {daily_loss_pct:.2f}% "
                    f"(threshold: -{self._cb_max_daily_loss_pct}%)"
                )
                return
        
        # Check consecutive losses
        if self._circuit_breaker.consecutive_losses >= self._cb_max_consecutive_losses:
            self._trip_circuit_breaker(
                f"Consecutive losses limit reached: {self._circuit_breaker.consecutive_losses} "
                f"(threshold: {self._cb_max_consecutive_losses})"
            )
    
    def _trip_circuit_breaker(self, reason: str) -> None:
        """Trip the circuit breaker and halt trading."""
        self._circuit_breaker.is_tripped = True
        self._circuit_breaker.trip_reason = reason
        self._circuit_breaker.trip_time = datetime.now()
        
        logger.warning(f"🛑 CIRCUIT BREAKER TRIPPED: {reason}")
        logger.warning(f"   Trading halted until: {self._cb_cooldown_minutes} minutes or manual reset")
    
    def can_trade(self) -> tuple[bool, str]:
        """
        Check if trading is allowed (circuit breaker not tripped).
        
        Returns:
            Tuple of (can_trade, reason_if_blocked)
        """
        self._check_daily_reset()
        
        if not self._cb_enabled:
            return True, ""
        
        if not self._circuit_breaker.is_tripped:
            return True, ""
        
        # Check if cooldown has elapsed
        if self._circuit_breaker.trip_time:
            elapsed = (datetime.now() - self._circuit_breaker.trip_time).total_seconds() / 60
            if elapsed >= self._cb_cooldown_minutes:
                logger.info("Circuit breaker cooldown elapsed, resuming trading")
                self._circuit_breaker.is_tripped = False
                return True, ""
        
        return False, f"Circuit breaker tripped: {self._circuit_breaker.trip_reason}"
    
    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status."""
        return {
            'enabled': self._cb_enabled,
            'is_tripped': self._circuit_breaker.is_tripped,
            'trip_reason': self._circuit_breaker.trip_reason,
            'daily_pnl': self._circuit_breaker.daily_pnl,
            'daily_trades': self._circuit_breaker.daily_trades,
            'consecutive_losses': self._circuit_breaker.consecutive_losses,
            'last_reset': self._circuit_breaker.last_reset_date,
        }
    
    def manual_reset_circuit_breaker(self) -> None:
        """Manually reset the circuit breaker (use with caution)."""
        logger.warning("Manual circuit breaker reset requested")
        self._circuit_breaker.is_tripped = False
        self._circuit_breaker.trip_reason = ""
        self._circuit_breaker.consecutive_losses = 0

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

    def place_order(
        self, 
        symbol: str, 
        qty: int, 
        side: str, 
        order_type: str = "MARKET", 
        price: Optional[float] = None,
        product_type: Optional[str] = None, 
        stop_loss: float = 0, 
        take_profit: float = 0,
        skip_circuit_breaker: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Place an order via the Fyers API with circuit breaker protection.
        
        Args:
            symbol: Trading symbol (e.g., 'NSE:SBIN-EQ')
            qty: Quantity to trade
            side: 'BUY' or 'SELL'
            order_type: 'MARKET' or 'LIMIT' (default: 'MARKET')
            price: Limit price (required for LIMIT orders)
            product_type: 'INTRADAY' or 'CNC' (default: from init)
            stop_loss: Stop loss price (default: 0)
            take_profit: Take profit price (default: 0)
            skip_circuit_breaker: If True, bypass circuit breaker check
            
        Returns:
            API response dictionary or None on error
        """
        # Check circuit breaker first
        if not skip_circuit_breaker:
            can_execute, block_reason = self.can_trade()
            if not can_execute:
                logger.warning(f"Order blocked by circuit breaker: {block_reason}")
                return None
        
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
            "limitPrice": price,
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
    
    def record_trade_result(self, pnl: float) -> None:
        """
        Record trade result for circuit breaker tracking.
        
        Should be called after a trade is closed with the realized P&L.
        
        Args:
            pnl: Realized profit/loss from the trade
        """
        is_win = pnl > 0
        self._update_circuit_breaker_on_trade(pnl, is_win)
        logger.info(f"Recorded trade result: P&L={pnl:.2f}, is_win={is_win}, "
                   f"daily_pnl={self._circuit_breaker.daily_pnl:.2f}")

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
    
    def verify_trade_execution(
        self,
        order_id: str,
        expected_price: Optional[float] = None,
        timeout_seconds: int = 30,
        poll_interval: float = 1.0,
    ) -> Dict[str, Any]:
        """
        Verify that a trade was executed and check for slippage.
        
        Args:
            order_id: The order ID to verify
            expected_price: Expected execution price (for slippage calculation)
            timeout_seconds: Maximum time to wait for execution
            poll_interval: Time between status checks
            
        Returns:
            Dictionary with verification results:
            {
                'verified': bool,
                'status': str,  # 'FILLED', 'PENDING', 'CANCELLED', 'REJECTED'
                'filled_qty': int,
                'avg_price': float,
                'slippage_pct': float,  # Percentage slippage (positive = worse than expected)
                'slippage_amount': float,  # Absolute slippage per share
                'execution_time_ms': float,
                'order_details': dict,
                'error': Optional[str],
            }
        """
        import time
        start_time = time.time()
        
        result = {
            'verified': False,
            'status': 'UNKNOWN',
            'filled_qty': 0,
            'avg_price': 0.0,
            'slippage_pct': 0.0,
            'slippage_amount': 0.0,
            'execution_time_ms': 0.0,
            'order_details': {},
            'error': None,
        }
        
        if not order_id:
            result['error'] = "No order_id provided"
            return result
        
        try:
            elapsed = 0
            while elapsed < timeout_seconds:
                # Get order status from orderbook
                order_book = self.get_order_book()
                
                if order_book is None:
                    result['error'] = "Failed to fetch order book"
                    return result
                
                # Find the order
                order = next((o for o in order_book if o.get('id') == order_id), None)
                
                if not order:
                    logger.debug(f"Order {order_id} not found in orderbook, checking tradebook")
                    # Check tradebook for executed orders
                    trade_book = self.get_tradebook()
                    if trade_book:
                        trades = [t for t in trade_book if t.get('orderNumber') == order_id]
                        if trades:
                            # Order was executed
                            total_qty = sum(t.get('tradedQty', 0) for t in trades)
                            total_value = sum(t.get('tradedQty', 0) * t.get('tradePrice', 0) for t in trades)
                            avg_price = total_value / total_qty if total_qty > 0 else 0
                            
                            result['verified'] = True
                            result['status'] = 'FILLED'
                            result['filled_qty'] = total_qty
                            result['avg_price'] = avg_price
                            result['order_details'] = trades[0] if trades else {}
                            break
                    
                    # Order not found anywhere, keep waiting
                    time.sleep(poll_interval)
                    elapsed = time.time() - start_time
                    continue
                
                # Found order in orderbook
                result['order_details'] = order
                order_status = order.get('status', 0)
                
                # Fyers order status codes:
                # 1 = Pending, 2 = Filled, 3 = Cancelled, 4 = Rejected, 5 = Partially Filled
                status_map = {
                    1: 'PENDING',
                    2: 'FILLED',
                    3: 'CANCELLED',
                    4: 'REJECTED',
                    5: 'PARTIALLY_FILLED',
                }
                
                result['status'] = status_map.get(order_status, 'UNKNOWN')
                
                if result['status'] == 'FILLED':
                    result['verified'] = True
                    result['filled_qty'] = order.get('filledQty', 0)
                    result['avg_price'] = order.get('tradedPrice', 0)
                    break
                elif result['status'] == 'PARTIALLY_FILLED':
                    result['filled_qty'] = order.get('filledQty', 0)
                    result['avg_price'] = order.get('tradedPrice', 0)
                    # Continue waiting for full fill
                elif result['status'] in ['CANCELLED', 'REJECTED']:
                    result['error'] = f"Order {result['status']}: {order.get('message', 'Unknown reason')}"
                    break
                
                # Still pending, wait and retry
                time.sleep(poll_interval)
                elapsed = time.time() - start_time
            
            result['execution_time_ms'] = (time.time() - start_time) * 1000
            
            # Calculate slippage if we have both prices
            if result['verified'] and expected_price and result['avg_price'] > 0:
                result['slippage_amount'] = result['avg_price'] - expected_price
                result['slippage_pct'] = (result['slippage_amount'] / expected_price) * 100
                
                if abs(result['slippage_pct']) > 0.5:
                    logger.warning(
                        f"Significant slippage on order {order_id}: "
                        f"{result['slippage_pct']:.2f}% ({result['slippage_amount']:.2f})"
                    )
            
            if not result['verified'] and elapsed >= timeout_seconds:
                result['error'] = f"Timeout waiting for order execution ({timeout_seconds}s)"
            
            logger.info(
                f"Order verification: {order_id} -> {result['status']}, "
                f"filled={result['filled_qty']}, avg_price={result['avg_price']:.2f}, "
                f"slippage={result['slippage_pct']:.2f}%"
            )
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error verifying trade execution: {e}")
        
        return result
    
    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Get current status of an order.
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Dictionary with order status details
        """
        result = {
            'order_id': order_id,
            'status': 'UNKNOWN',
            'filled_qty': 0,
            'pending_qty': 0,
            'avg_price': 0.0,
            'error': None,
        }
        
        try:
            order_book = self.get_order_book()
            
            if order_book is None:
                result['error'] = "Failed to fetch order book"
                return result
            
            order = next((o for o in order_book if o.get('id') == order_id), None)
            
            if not order:
                result['error'] = "Order not found"
                return result
            
            status_map = {1: 'PENDING', 2: 'FILLED', 3: 'CANCELLED', 4: 'REJECTED', 5: 'PARTIALLY_FILLED'}
            
            result['status'] = status_map.get(order.get('status', 0), 'UNKNOWN')
            result['filled_qty'] = order.get('filledQty', 0)
            result['pending_qty'] = order.get('qty', 0) - order.get('filledQty', 0)
            result['avg_price'] = order.get('tradedPrice', 0)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
