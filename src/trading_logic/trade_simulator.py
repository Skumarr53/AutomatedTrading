# src/trading_logic/trade_simulator.py

import json
import os
import logging
from typing import Any, Dict, Optional, List

import pandas as pd
from src.config import config


class TradeSimulator:
    """
    Simulates trade executions based on generated trading signals.

    The `TradeSimulator` class manages the execution of trades by handling positions,
    calculating shares to trade, managing capital, applying transaction costs, and
    recording trade history. It supports both BACKTEST and LIVE trading modes.
    
    Attributes:
        mode (str): Operational mode ('BACKTEST' or 'LIVE').
        initial_capital (float): Starting capital for trading.
        transaction_cost (float): Fixed cost per transaction.
        positions (Dict[str, Dict[str, Any]]): Dictionary tracking current positions per symbol.
        trade_history (List[Dict[str, Any]]): List recording the history of executed trades.
    """

    def __init__(self, initial_capital: float = 10000.0, transaction_cost: float = 20.0) -> None:
        """
        Initializes the TradeSimulator with capital and transaction cost settings.

        Args:
            initial_capital (float, optional): Starting capital for trading. Defaults to 10000.0.
            transaction_cost (float, optional): Fixed cost per transaction. Defaults to 20.0.
        """
        self.mode: str = config.trading_config.trade_mode
        self.initial_capital: float = initial_capital
        self.transaction_cost: float = transaction_cost
        self.positions: Dict[str, Dict[str, Any]] = self.load_positions() if self.mode == 'LIVE' else {}
        self.trade_history: List[Dict[str, Any]] = []
        self.positions_file_path = config.paths.positions_file_path
        logging.info("TradeSimulator initialized in '%s' mode with initial capital: %.2f and transaction cost: %.2f",
                     self.mode, self.initial_capital, self.transaction_cost)

    def load_positions(self) -> Dict[str, Any]:
        """
        Loads existing positions from a JSON file in LIVE mode.

        Returns:
            Dict[str, Any]: Dictionary of current positions per symbol.

        Raises:
            FileNotFoundError: If the positions file does not exist.
            json.JSONDecodeError: If the positions file contains invalid JSON.
        """
        if os.path.exists(self.positions_file_path):
            try:
                with open(self.positions_file_path, 'r') as file:
                    positions = json.load(file)
                logging.info("Loaded existing positions from '%s'.", self.positions_file_path)
                return positions
            except json.JSONDecodeError as e:
                logging.error("Invalid JSON format in positions file: %s", e)
                raise
        else:
            logging.warning("Positions file '%s' does not exist. Starting with empty positions.",
                            self.positions_file_path)
            return {}

    def update_positions_file(self) -> None:
        """
        Updates the positions file by saving the current positions to a JSON file.

        Raises:
            IOError: If the file cannot be written.
        """
        try:
            with open(self.positions_file_path, 'w') as file:
                json.dump(self.positions, file, indent=4)
            logging.info("Positions updated and saved to '%s'.", self.positions_file_path)
        except IOError as e:
            logging.error("Failed to write positions to file: %s", e)
            raise

    def fetch_current_balance(self) -> float:
        """
        Fetches the current balance from the brokerage account in LIVE mode.

        Note: This method should be implemented to integrate with a real brokerage API.

        Returns:
            float: Current account balance.

        Raises:
            NotImplementedError: If not implemented.
        """
        # TODO: Implement real-time balance fetching from brokerage account
        raise NotImplementedError("fetch_current_balance method must be implemented for LIVE mode.")

    def execute_trade(self, signal: str, symbol: str, price: float, date: str) -> None:
        """
        Executes a trade based on the provided signal.

        Args:
            signal (str): Trading signal ('BUY' or 'SELL').
            symbol (str): Stock symbol to trade.
            price (float): Current price of the stock.
            date (str): Date and time of the trade.

        Raises:
            ValueError: If an invalid signal is provided.
        """
        logging.debug("Executing trade: %s for %s at price %.2f on %s", signal, symbol, price, date)
        if signal not in {'BUY', 'SELL'}:
            logging.error("Invalid trade signal '%s' received for symbol '%s'.", signal, symbol)
            raise ValueError(f"Invalid trade signal '{signal}'. Must be 'BUY' or 'SELL'.")

        if signal == 'BUY':
            if symbol in self.positions and self.positions[symbol]['type'] == 'short':
                self.close_position(symbol, price, date)
            self.open_position('long', symbol, price, date)
        elif signal == 'SELL':
            if symbol in self.positions and self.positions[symbol]['type'] == 'long':
                self.close_position(symbol, price, date)
            self.open_position('short', symbol, price, date)

    def open_position(self, position_type: str, symbol: str, price: float, date: str) -> None:
        """
        Opens a new position (long or short) for a given symbol.

        Args:
            position_type (str): Type of position ('long' or 'short').
            symbol (str): Stock symbol to trade.
            price (float): Entry price of the stock.
            date (str): Date and time of the trade.

        Raises:
            ValueError: If the position type is invalid or insufficient capital.
        """
        logging.debug("Opening %s position for %s at price %.2f on %s", position_type, symbol, price, date)
        shares: int = self.calculate_shares(price, position_type)
        cost: float = shares * price + self.transaction_cost

        if self.initial_capital >= cost:
            self.initial_capital -= cost
            self.positions[symbol] = {
                'type': position_type,
                'entry_price': price,
                'shares': shares,
                'entry_date': date,
                'max_swing_high': price if position_type == 'long' else price,
                'trailing_stop_loss': self.calculate_dynamic_trailing_stop_loss(price, price, position_type)
            }
            self.record_trade('OPEN', position_type, symbol, price, shares, date)
            self.update_positions_file()
            logging.info("Opened %s position for %s: %d shares at %.2f on %s",
                         position_type.upper(), symbol, shares, price, date)
        else:
            logging.warning("Insufficient capital to open %s position for %s: Required %.2f, Available %.2f",
                            position_type.upper(), symbol, cost, self.initial_capital)

    def close_position(self, symbol: str, price: float, date: str) -> None:
        """
        Closes an existing position for a given symbol.

        Args:
            symbol (str): Stock symbol to close.
            price (float): Current price of the stock.
            date (str): Date and time of the trade.

        Raises:
            KeyError: If the symbol does not have an open position.
        """
        logging.debug("Closing position for %s at price %.2f on %s", symbol, price, date)
        position: Optional[Dict[str, Any]] = self.positions.pop(symbol, None)
        if position:
            profit_loss: float
            if position['type'] == 'long':
                profit_loss = (price - position['entry_price']) * position['shares']
            else:  # 'short' position
                profit_loss = (position['entry_price'] - price) * position['shares']

            net_profit_loss: float = profit_loss - self.transaction_cost
            self.initial_capital += net_profit_loss
            self.record_trade('CLOSE', position['type'], symbol, price, position['shares'], date, position['entry_date'])
            self.update_positions_file()
            logging.info("Closed %s position for %s: %d shares at %.2f on %s. P/L: %.2f",
                         position['type'].upper(), symbol, position['shares'], price, date, net_profit_loss)
        else:
            logging.warning("Attempted to close non-existent position for symbol '%s'.", symbol)

    def calculate_shares(self, price: float, position_type: str) -> int:
        """
        Calculates the number of shares to trade based on the current capital and position type.

        Args:
            price (float): Current price of the stock.
            position_type (str): Type of position ('long' or 'short').

        Returns:
            int: Number of shares to trade.

        Raises:
            ValueError: If the position type is invalid.
        """
        initial_stop_loss: float = price * 0.05  # 5% of entry price
        trade_size: float = (0.01 * self.initial_capital) / initial_stop_loss  # 1% risk
        shares: int = max(int(trade_size), 1)  # Ensure at least 1 share is traded

        if position_type not in {'long', 'short'}:
            logging.error("Invalid position type '%s'. Must be 'long' or 'short'.", position_type)
            raise ValueError(f"Invalid position type '{position_type}'. Must be 'long' or 'short'.")

        logging.debug("Calculated shares: %d for %s position at price %.2f", shares, position_type, price)
        return shares

    def calculate_dynamic_trailing_stop_loss(self, current_price: float, entry_price: float, position_type: str) -> float:
        """
        Calculates a dynamic trailing stop loss percentage based on the return.

        Args:
            current_price (float): Current price of the stock.
            entry_price (float): Entry price of the stock.
            position_type (str): Type of position ('long' or 'short').

        Returns:
            float: Trailing stop loss percentage.

        Raises:
            ValueError: If the position type is invalid.
        """
        if position_type == 'long':
            return_percentage: float = ((current_price - entry_price) / entry_price) * 100
        elif position_type == 'short':
            return_percentage = ((entry_price - current_price) / entry_price) * 100
        else:
            logging.error("Invalid position type '%s' for trailing stop loss calculation.", position_type)
            raise ValueError(f"Invalid position type '{position_type}'. Must be 'long' or 'short'.")

        # Adjusting trailing stop loss based on return
        if return_percentage < 5:
            trailing_stop_loss: float = 0.0
        elif 5 <= return_percentage < 10:
            trailing_stop_loss = 50.0
        else:
            trailing_stop_loss = max(50.0 - (return_percentage - 10.0), 10.0)

        logging.debug("Calculated trailing stop loss: %.2f%% for %s position.", trailing_stop_loss, position_type)
        return trailing_stop_loss

    def check_trailing_stop_loss(self, symbol: str, current_price: float, date: str) -> None:
        """
        Checks and applies trailing stop loss conditions for a given symbol.

        Args:
            symbol (str): Stock symbol to check.
            current_price (float): Current price of the stock.
            date (str): Current date and time.

        Raises:
            ValueError: If the symbol does not have an open position.
        """
        position: Optional[Dict[str, Any]] = self.positions.get(symbol)
        if position:
            updated_trailing_stop_loss: float = self.calculate_dynamic_trailing_stop_loss(
                current_price, position['entry_price'], position['type']
            )
            logging.debug("Checking trailing stop loss for %s at price %.2f with trailing stop %.2f%%",
                          symbol, current_price, updated_trailing_stop_loss)

            if position['type'] == 'long':
                if current_price > position['max_swing_high']:
                    position['max_swing_high'] = current_price
                    logging.debug("Updated max swing high for %s to %.2f", symbol, current_price)
                if current_price <= position['max_swing_high'] * (1 - updated_trailing_stop_loss / 100):
                    logging.info("Trailing stop loss triggered for %s. Closing position.", symbol)
                    self.close_position(symbol, current_price, date)
            elif position['type'] == 'short':
                if current_price < position['max_swing_high']:
                    position['max_swing_high'] = current_price
                    logging.debug("Updated max swing high for %s to %.2f", symbol, current_price)
                if current_price >= position['max_swing_high'] * (1 + updated_trailing_stop_loss / 100):
                    logging.info("Trailing stop loss triggered for %s. Closing position.", symbol)
                    self.close_position(symbol, current_price, date)
        else:
            logging.warning("No open position found for symbol '%s' to check trailing stop loss.", symbol)

    def record_trade(self, action: str, position_type: str, symbol: str, price: float, shares: int,
                    date: str, entry_date: Optional[str] = None) -> None:
        """
        Records a trade action in the trade history.

        Args:
            action (str): Action type ('OPEN' or 'CLOSE').
            position_type (str): Type of position ('long' or 'short').
            symbol (str): Stock symbol traded.
            price (float): Trade execution price.
            shares (int): Number of shares traded.
            date (str): Date and time of the trade.
            entry_date (Optional[str], optional): Entry date for 'CLOSE' actions. Defaults to None.
        """
        holding_time: float = self.calculate_holding_time(date, entry_date) if entry_date else 0.0
        trade: Dict[str, Any] = {
            'action': action,
            'position_type': position_type,
            'symbol': symbol,
            'price': price,
            'shares': shares,
            'date': date,
            'balance_after_trade': self.initial_capital,
            'holding_time': holding_time
        }
        self.trade_history.append(trade)
        logging.debug("Recorded trade: %s", trade)
        # TODO: Store trade history in local storage or database for later analysis

    def calculate_holding_time(self, exit_date: str, entry_date: str) -> float:
        """
        Calculates the holding time of a trade.

        Args:
            exit_date (str): Date and time when the trade was closed.
            entry_date (str): Date and time when the trade was opened.

        Returns:
            float: Holding time in days.

        Raises:
            ValueError: If date formats are incorrect.
        """
        try:
            exit_dt = pd.to_datetime(exit_date)
            entry_dt = pd.to_datetime(entry_date)
            holding_time: float = (exit_dt - entry_dt).days + (exit_dt - entry_dt).seconds / 86400
            logging.debug("Calculated holding time: %.2f days for trade from %s to %s",
                          holding_time, entry_date, exit_date)
            return holding_time
        except Exception as e:
            logging.error("Error calculating holding time: %s", e)
            return 0.0


# Example of setting up and using the TradeSimulator class
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')

    # Mock configuration setup
    class Config:
        TRADE_MODE = 'BACKTEST'  # Change to 'LIVE' as needed
        POSITIONS_FILE_PATH = 'positions.json'

    config = Config()

    # Initialize TradeSimulator
    trade_simulator = TradeSimulator(initial_capital=10000.0, transaction_cost=20.0)

    # Example trade execution
    try:
        # Execute a BUY trade
        trade_simulator.execute_trade('BUY', 'AAPL', 150.0, '2023-10-01 10:00:00')

        # Execute a SELL trade
        trade_simulator.execute_trade('SELL', 'AAPL', 155.0, '2023-10-02 10:00:00')

        # Check trailing stop loss (example)
        trade_simulator.check_trailing_stop_loss('AAPL', 148.0, '2023-10-03 10:00:00')

        # Display trade history
        print("Trade History:")
        for trade in trade_simulator.trade_history:
            print(trade)
    except NotImplementedError as nie:
        logging.error(nie)
    except Exception as e:
        logging.error("An error occurred during trade simulation: %s", e)
