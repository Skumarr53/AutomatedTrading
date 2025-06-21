import pandas as pd
import numpy as np
from typing import List, Dict

class PerformanceMetrics:
    """Calculate portfolio performance metrics from trade history."""

    def __init__(self, trade_history: List[Dict[str, any]], starting_capital: float, transaction_cost: float = 0.0) -> None:
        self.trade_history = trade_history
        self.starting_capital = starting_capital
        self.transaction_cost = transaction_cost

    def _history_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.trade_history)
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def roi(self) -> float:
        df = self._history_df()
        if df.empty:
            return 0.0
        final_balance = df.iloc[-1]['balance_after_trade']
        return (final_balance - self.starting_capital) / self.starting_capital * 100

    def equity_curve(self) -> pd.DataFrame:
        df = self._history_df()
        return df[['date', 'balance_after_trade']].sort_values('date') if not df.empty else df

    def max_drawdown(self) -> float:
        curve = self.equity_curve()
        if curve.empty:
            return 0.0
        roll_max = curve['balance_after_trade'].cummax()
        drawdown = (curve['balance_after_trade'] - roll_max) / roll_max
        return drawdown.min() * 100

    def win_rate(self) -> float:
        df = self._history_df()
        closes = df[df['action'] == 'CLOSE']
        if closes.empty:
            return 0.0
        wins = closes[closes['net_profit_loss'] > 0]
        return len(wins) / len(closes) * 100

    def average_holding_period(self) -> float:
        df = self._history_df()
        closes = df[df['action'] == 'CLOSE']
        if closes.empty:
            return 0.0
        return closes['holding_time'].mean()

    def total_transaction_costs(self) -> float:
        df = self._history_df()
        return len(df) * self.transaction_cost

    def diversification(self) -> int:
        df = self._history_df()
        return df['symbol'].nunique() if not df.empty else 0

    def summary(self) -> Dict[str, float]:
        return {
            'ROI(%)': self.roi(),
            'Sharpe': self.sharpe_ratio(),
            'Max Drawdown(%)': self.max_drawdown(),
            'Win Rate(%)': self.win_rate(),
            'Avg Holding Period(days)': self.average_holding_period(),
            'Transaction Costs': self.total_transaction_costs(),
            'Symbols Traded': self.diversification(),
        }

    def sharpe_ratio(self, risk_free_rate: float = 0.0) -> float:
        df = self.equity_curve()
        if df.empty or len(df) < 2:
            return 0.0
        returns = df['balance_after_trade'].pct_change().dropna()
        if returns.empty:
            return 0.0
        excess = returns - risk_free_rate/252
        return np.sqrt(252) * excess.mean() / excess.std() if excess.std() != 0 else 0.0

