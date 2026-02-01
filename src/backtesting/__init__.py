# src/backtesting/__init__.py
"""
Backtesting Framework for Trading Models

Provides tools for:
- Historical backtesting with MLflow models
- Model comparison and performance analysis
- Report generation with visualizations
"""

from src.backtesting.backtest_result import (
    BacktestResult,
    TradeRecord,
    ComparisonReport,
    PerformanceMetrics,
)
from src.backtesting.backtest_engine import BacktestEngine

__all__ = [
    "BacktestEngine",
    "BacktestResult",
    "TradeRecord",
    "ComparisonReport",
    "PerformanceMetrics",
]
