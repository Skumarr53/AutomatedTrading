# src/backtesting/backtest_result.py
"""
Data structures for backtest results and performance metrics.

Provides structured output from backtesting operations including:
- Individual trade records
- Aggregate performance metrics
- Model comparison reports
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


class TradeDirection(str, Enum):
    """Trade direction."""
    LONG = "LONG"
    SHORT = "SHORT"
    HOLD = "HOLD"


class TradeStatus(str, Enum):
    """Trade status."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


@dataclass
class TradeRecord:
    """
    Record of a single trade in the backtest.
    
    Captures all relevant details about a trade for analysis:
    - Entry and exit timing
    - Price and position details
    - Profit/loss calculations
    - Model prediction information
    """
    trade_id: str
    symbol: str
    direction: TradeDirection
    entry_time: datetime
    entry_price: float
    quantity: int
    
    # Exit details (optional until trade is closed)
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    status: TradeStatus = TradeStatus.OPEN
    
    # Position sizing
    position_value: float = 0.0
    
    # Stop loss / take profit
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    
    # Model prediction info
    model_name: Optional[str] = None
    prediction_confidence: Optional[float] = None
    timeframe_agreement: Optional[int] = None
    
    # P&L (calculated when closed)
    gross_pnl: float = 0.0
    transaction_cost: float = 0.0
    net_pnl: float = 0.0
    pnl_percent: float = 0.0
    
    # Timing
    holding_period_minutes: float = 0.0
    
    # Exit reason
    exit_reason: Optional[str] = None  # "signal", "stop_loss", "take_profit", "timeout"
    
    def close(
        self, 
        exit_time: datetime, 
        exit_price: float, 
        transaction_cost: float = 0.0,
        exit_reason: str = "signal"
    ) -> None:
        """
        Close the trade and calculate P&L.
        
        Args:
            exit_time: Time of trade exit
            exit_price: Exit price
            transaction_cost: Transaction costs (fees, slippage, etc.)
            exit_reason: Why the trade was closed
        """
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.status = TradeStatus.CLOSED
        self.transaction_cost = transaction_cost
        self.exit_reason = exit_reason
        
        # Calculate P&L
        if self.direction == TradeDirection.LONG:
            self.gross_pnl = (exit_price - self.entry_price) * self.quantity
        else:  # SHORT
            self.gross_pnl = (self.entry_price - exit_price) * self.quantity
        
        self.net_pnl = self.gross_pnl - transaction_cost
        self.pnl_percent = (self.net_pnl / self.position_value * 100) if self.position_value > 0 else 0.0
        
        # Calculate holding period
        self.holding_period_minutes = (exit_time - self.entry_time).total_seconds() / 60
    
    def is_win(self) -> bool:
        """Check if trade was profitable."""
        return self.net_pnl > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = asdict(self)
        d['direction'] = self.direction.value
        d['status'] = self.status.value
        d['entry_time'] = self.entry_time.isoformat() if self.entry_time else None
        d['exit_time'] = self.exit_time.isoformat() if self.exit_time else None
        return d


@dataclass
class PerformanceMetrics:
    """
    Aggregate performance metrics from a backtest.
    
    Includes standard metrics for evaluating trading strategy performance:
    - Return metrics (total, annualized)
    - Risk metrics (max drawdown, volatility)
    - Trade statistics (win rate, profit factor)
    """
    # Return metrics
    total_return_pct: float = 0.0
    annualized_return_pct: float = 0.0
    final_balance: float = 0.0
    
    # Risk metrics
    max_drawdown_pct: float = 0.0
    max_drawdown_duration_days: float = 0.0
    volatility_pct: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    
    # Trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate_pct: float = 0.0
    profit_factor: float = 0.0
    
    # Average trade metrics
    avg_trade_pnl: float = 0.0
    avg_win_pnl: float = 0.0
    avg_loss_pnl: float = 0.0
    avg_holding_period_minutes: float = 0.0
    
    # Best/Worst
    best_trade_pnl: float = 0.0
    worst_trade_pnl: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    
    # Exposure
    avg_exposure_pct: float = 0.0
    max_exposure_pct: float = 0.0
    
    @classmethod
    def calculate(
        cls,
        trades: List[TradeRecord],
        equity_curve: pd.Series,
        initial_capital: float,
        trading_days: int = 252,
    ) -> "PerformanceMetrics":
        """
        Calculate performance metrics from trades and equity curve.
        
        Args:
            trades: List of closed trades
            equity_curve: Series of portfolio values over time
            initial_capital: Starting capital
            trading_days: Trading days per year for annualization
            
        Returns:
            PerformanceMetrics instance
        """
        metrics = cls()
        
        if not trades:
            metrics.final_balance = initial_capital
            return metrics
        
        # Filter to closed trades only
        closed_trades = [t for t in trades if t.status == TradeStatus.CLOSED]
        
        if not closed_trades:
            metrics.final_balance = initial_capital
            return metrics
        
        # === Return Metrics ===
        final_balance = initial_capital + sum(t.net_pnl for t in closed_trades)
        metrics.final_balance = final_balance
        metrics.total_return_pct = ((final_balance - initial_capital) / initial_capital) * 100
        
        # Annualized return (approximate)
        if len(equity_curve) > 1:
            days = (equity_curve.index[-1] - equity_curve.index[0]).days
            if days > 0:
                years = days / 365
                if years > 0:
                    metrics.annualized_return_pct = (
                        ((final_balance / initial_capital) ** (1 / years)) - 1
                    ) * 100
        
        # === Risk Metrics ===
        if len(equity_curve) > 1:
            # Max drawdown
            rolling_max = equity_curve.cummax()
            drawdown = (equity_curve - rolling_max) / rolling_max
            metrics.max_drawdown_pct = abs(drawdown.min()) * 100
            
            # Drawdown duration
            in_drawdown = drawdown < 0
            if in_drawdown.any():
                dd_periods = (~in_drawdown).cumsum()
                dd_durations = in_drawdown.groupby(dd_periods).sum()
                if len(dd_durations) > 0:
                    metrics.max_drawdown_duration_days = dd_durations.max()
            
            # Volatility (daily returns)
            daily_returns = equity_curve.pct_change().dropna()
            if len(daily_returns) > 1:
                metrics.volatility_pct = daily_returns.std() * np.sqrt(trading_days) * 100
                
                # Sharpe ratio (assuming 0% risk-free rate)
                mean_return = daily_returns.mean()
                std_return = daily_returns.std()
                if std_return > 0:
                    metrics.sharpe_ratio = (mean_return / std_return) * np.sqrt(trading_days)
                
                # Sortino ratio (downside deviation)
                negative_returns = daily_returns[daily_returns < 0]
                if len(negative_returns) > 0:
                    downside_std = negative_returns.std()
                    if downside_std > 0:
                        metrics.sortino_ratio = (mean_return / downside_std) * np.sqrt(trading_days)
            
            # Calmar ratio
            if metrics.max_drawdown_pct > 0:
                metrics.calmar_ratio = metrics.annualized_return_pct / metrics.max_drawdown_pct
        
        # === Trade Statistics ===
        metrics.total_trades = len(closed_trades)
        metrics.winning_trades = sum(1 for t in closed_trades if t.is_win())
        metrics.losing_trades = metrics.total_trades - metrics.winning_trades
        metrics.win_rate_pct = (metrics.winning_trades / metrics.total_trades * 100) if metrics.total_trades > 0 else 0
        
        # Profit factor
        total_wins = sum(t.net_pnl for t in closed_trades if t.net_pnl > 0)
        total_losses = abs(sum(t.net_pnl for t in closed_trades if t.net_pnl < 0))
        metrics.profit_factor = (total_wins / total_losses) if total_losses > 0 else float('inf')
        
        # === Average Trade Metrics ===
        pnls = [t.net_pnl for t in closed_trades]
        metrics.avg_trade_pnl = np.mean(pnls) if pnls else 0
        
        wins = [t.net_pnl for t in closed_trades if t.is_win()]
        metrics.avg_win_pnl = np.mean(wins) if wins else 0
        
        losses = [t.net_pnl for t in closed_trades if not t.is_win()]
        metrics.avg_loss_pnl = np.mean(losses) if losses else 0
        
        holding_periods = [t.holding_period_minutes for t in closed_trades]
        metrics.avg_holding_period_minutes = np.mean(holding_periods) if holding_periods else 0
        
        # === Best/Worst ===
        metrics.best_trade_pnl = max(pnls) if pnls else 0
        metrics.worst_trade_pnl = min(pnls) if pnls else 0
        
        # Consecutive wins/losses
        is_win = [t.is_win() for t in closed_trades]
        metrics.max_consecutive_wins = cls._max_consecutive(is_win, True)
        metrics.max_consecutive_losses = cls._max_consecutive(is_win, False)
        
        return metrics
    
    @staticmethod
    def _max_consecutive(results: List[bool], target: bool) -> int:
        """Count maximum consecutive occurrences of target."""
        max_count = 0
        current_count = 0
        for r in results:
            if r == target:
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        return max_count
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class BacktestResult:
    """
    Complete result of a single backtest run.
    
    Contains:
    - Configuration used
    - All trades executed
    - Performance metrics
    - Equity curve data
    """
    # Identification
    backtest_id: str
    symbol: str
    model_name: str
    experiment_name: str
    
    # Configuration
    start_date: datetime
    end_date: datetime
    initial_capital: float
    transaction_cost: float
    
    # Results
    trades: List[TradeRecord] = field(default_factory=list)
    metrics: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    
    # Time series data
    equity_curve: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    predictions: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    # Execution info
    execution_time_seconds: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    errors: List[str] = field(default_factory=list)
    
    def calculate_metrics(self, trading_days: int = 252) -> None:
        """Calculate performance metrics from trades."""
        self.metrics = PerformanceMetrics.calculate(
            trades=self.trades,
            equity_curve=self.equity_curve,
            initial_capital=self.initial_capital,
            trading_days=trading_days,
        )
    
    def summary(self) -> Dict[str, Any]:
        """Get summary of backtest results."""
        return {
            'backtest_id': self.backtest_id,
            'symbol': self.symbol,
            'model_name': self.model_name,
            'period': f"{self.start_date.date()} to {self.end_date.date()}",
            'initial_capital': self.initial_capital,
            'final_balance': self.metrics.final_balance,
            'total_return_pct': round(self.metrics.total_return_pct, 2),
            'sharpe_ratio': round(self.metrics.sharpe_ratio, 2),
            'max_drawdown_pct': round(self.metrics.max_drawdown_pct, 2),
            'total_trades': self.metrics.total_trades,
            'win_rate_pct': round(self.metrics.win_rate_pct, 2),
            'profit_factor': round(self.metrics.profit_factor, 2),
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'backtest_id': self.backtest_id,
            'symbol': self.symbol,
            'model_name': self.model_name,
            'experiment_name': self.experiment_name,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'initial_capital': self.initial_capital,
            'transaction_cost': self.transaction_cost,
            'trades': [t.to_dict() for t in self.trades],
            'metrics': self.metrics.to_dict(),
            'execution_time_seconds': self.execution_time_seconds,
            'created_at': self.created_at.isoformat(),
            'errors': self.errors,
        }


@dataclass
class ComparisonReport:
    """
    Report comparing multiple backtest results.
    
    Useful for comparing:
    - Different models on the same symbol
    - Same model across different symbols
    - Different configurations
    """
    report_id: str
    created_at: datetime = field(default_factory=datetime.now)
    
    # Results being compared
    results: List[BacktestResult] = field(default_factory=list)
    
    # Comparison metadata
    comparison_type: str = "model"  # "model", "symbol", "config"
    
    def add_result(self, result: BacktestResult) -> None:
        """Add a backtest result to comparison."""
        self.results.append(result)
    
    def get_comparison_table(self) -> pd.DataFrame:
        """Get comparison as a DataFrame."""
        if not self.results:
            return pd.DataFrame()
        
        rows = []
        for r in self.results:
            rows.append({
                'Model': r.model_name,
                'Symbol': r.symbol,
                'Total Return (%)': round(r.metrics.total_return_pct, 2),
                'Sharpe Ratio': round(r.metrics.sharpe_ratio, 2),
                'Max Drawdown (%)': round(r.metrics.max_drawdown_pct, 2),
                'Win Rate (%)': round(r.metrics.win_rate_pct, 2),
                'Profit Factor': round(r.metrics.profit_factor, 2),
                'Total Trades': r.metrics.total_trades,
                'Avg Trade P&L': round(r.metrics.avg_trade_pnl, 2),
            })
        
        return pd.DataFrame(rows)
    
    def get_best_model(self, metric: str = 'sharpe_ratio') -> Optional[BacktestResult]:
        """Get the best performing model by specified metric."""
        if not self.results:
            return None
        
        metric_map = {
            'sharpe_ratio': lambda r: r.metrics.sharpe_ratio,
            'total_return': lambda r: r.metrics.total_return_pct,
            'win_rate': lambda r: r.metrics.win_rate_pct,
            'profit_factor': lambda r: r.metrics.profit_factor,
            'max_drawdown': lambda r: -r.metrics.max_drawdown_pct,  # Lower is better
        }
        
        getter = metric_map.get(metric, lambda r: r.metrics.sharpe_ratio)
        return max(self.results, key=getter)
    
    def summary(self) -> Dict[str, Any]:
        """Get comparison summary."""
        if not self.results:
            return {'error': 'No results to compare'}
        
        best_sharpe = self.get_best_model('sharpe_ratio')
        best_return = self.get_best_model('total_return')
        
        return {
            'report_id': self.report_id,
            'models_compared': len(self.results),
            'symbols': list(set(r.symbol for r in self.results)),
            'best_by_sharpe': best_sharpe.model_name if best_sharpe else None,
            'best_by_return': best_return.model_name if best_return else None,
            'created_at': self.created_at.isoformat(),
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'report_id': self.report_id,
            'created_at': self.created_at.isoformat(),
            'comparison_type': self.comparison_type,
            'results': [r.to_dict() for r in self.results],
            'summary': self.summary(),
        }
