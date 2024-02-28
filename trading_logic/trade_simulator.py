# Incorporating the suggested changes and enhancements into the Trade Simulator
import json
import os
from config import config

class TradeSimulator:
    def __init__(self, initial_capital=10000, transaction_cost=20):
        self.mode = config.TRADE_MODE
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.positions = self.load_positions() if self.mode == 'LIVE' else {}  # Dictionary to track positions
        self.trade_history = []  # List to record trade history

    def load_positions(self):
      if os.path.exists(config.POSITIONS_FILE_PATH):
          with open(config.POSITIONS_FILE_PATH, 'r') as file:
              return json.load(file)
      return {}
    
    def update_positions_file(self):
        with open(config.POSITIONS_FILE_PATH, 'w') as file:
            json.dump(self.positions, file, indent=4)

    def fetch_current_balance(self):
        # TODO: Implement real-time balance fetching from brokerage account
        return self.initial_capital

    def execute_trade(self, signal, symbol, price, date):
        self.initial_capital = self.fetch_current_balance()
        if signal == 'BUY':
            if symbol not in self.positions or self.positions[symbol]['type'] == 'short':
                self.close_position(symbol, price, date)
            self.open_position('long', symbol, price, date)

        elif signal == 'SELL':
            if symbol not in self.positions or self.positions[symbol]['type'] == 'long':
                self.close_position(symbol, price, date)
            self.open_position('short', symbol, price, date)

    def open_position(self, position_type, symbol, price, date):
        shares = self.calculate_shares(price, position_type)
        cost = shares * price + self.transaction_cost
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
            self.record_trade('open', position_type, symbol, price, shares, date)
            self.update_positions_file()

    def close_position(self, symbol, price, date):
        position = self.positions.pop(symbol, None)
        if position:
            profit_loss = (price - position['entry_price']) * position['shares']
            self.initial_capital += profit_loss - self.transaction_cost
            self.record_trade('close', position['type'], symbol, price, position['shares'], date, position['entry_date'])
            self.update_positions_file()

    def calculate_shares(self, price, position_type):
        initial_stop_loss = price * 0.05  # 5% of entry price
        trade_size = (0.01 * self.initial_capital) / initial_stop_loss
        return trade_size

    def calculate_dynamic_trailing_stop_loss(self, current_price, entry_price, position_type):
        if position_type == 'long':
            return_percentage = ((current_price - entry_price) / entry_price) * 100
        else:  # 'short' position
            return_percentage = ((entry_price - current_price) / entry_price) * 100

        # Adjusting trailing stop loss based on return
        if return_percentage < 5:
            trailing_stop_loss = 0
        elif 5 <= return_percentage < 10:
            trailing_stop_loss = 50
        else:
            trailing_stop_loss = max(50 - (return_percentage - 10), 10)

        return trailing_stop_loss

    def check_trailing_stop_loss(self, symbol, current_price):
        position = self.positions.get(symbol)
        if position:
            updated_trailing_stop_loss = self.calculate_dynamic_trailing_stop_loss(
                current_price, position['entry_price'], position['type'])
            if position['type'] == 'long':
                if current_price > position['max_swing_high']:
                    position['max_swing_high'] = current_price
                if current_price <= position['max_swing_high'] * (1 - updated_trailing_stop_loss / 100):
                    self.close_position(symbol, current_price, 'current_date')  # current_date to be replaced with actual date
            else:  # 'short' position
                if current_price < position['max_swing_high']:
                    position['max_swing_high'] = current_price
                if current_price >= position['max_swing_high'] * (1 + updated_trailing_stop_loss / 100):
                    self.close_position(symbol, current_price, 'current_date')

    def record_trade(self, action, position_type, symbol, price, shares, date, entry_date=None):
        holding_time = self.calculate_holding_time(date, entry_date) if entry_date else 0
        trade = {
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
        # TODO: Store trade history in local storage for later analysis

    def calculate_holding_time(self, exit_date, entry_date):
        # TODO: Implement logic