import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from loguru import logger

# Try to import Prometheus client
try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.debug("prometheus_client not installed, metrics will be logged only")


# === Data Ingestion Metrics ===

class DataIngestionMetrics:
    """
    Prometheus metrics for data ingestion monitoring.
    
    Tracks:
    - Ingestion success/failure counts
    - Records ingested per symbol
    - Ingestion latency
    - Data freshness
    """
    
    _instance: Optional["DataIngestionMetrics"] = None
    
    def __init__(self, port: int = 8000):
        """Initialize metrics (singleton pattern)."""
        if not PROMETHEUS_AVAILABLE:
            self._enabled = False
            logger.warning("Prometheus metrics disabled (prometheus_client not installed)")
            return
        
        self._enabled = True
        self._port = port
        self._server_started = False
        
        # Define metrics (only once)
        try:
            # Counters
            self.ingestion_success_total = Counter(
                'data_ingestion_success_total',
                'Total number of successful data ingestion operations',
                ['symbol', 'actor_id']
            )
            
            self.ingestion_failure_total = Counter(
                'data_ingestion_failure_total',
                'Total number of failed data ingestion operations',
                ['symbol', 'actor_id', 'error_type']
            )
            
            self.records_ingested_total = Counter(
                'data_records_ingested_total',
                'Total number of records ingested',
                ['symbol', 'data_type', 'actor_id']
            )
            
            # Gauges
            self.data_freshness_seconds = Gauge(
                'data_freshness_seconds',
                'Age of the most recent data in seconds',
                ['symbol', 'data_type']
            )
            
            self.ingestion_queue_size = Gauge(
                'data_ingestion_queue_size',
                'Number of symbols waiting to be ingested',
                ['actor_id']
            )
            
            self.active_connections = Gauge(
                'data_ingestion_active_connections',
                'Number of active database connections',
                ['database']
            )
            
            # Histograms
            self.ingestion_latency_seconds = Histogram(
                'data_ingestion_latency_seconds',
                'Time taken to ingest data for a symbol',
                ['symbol', 'actor_id'],
                buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
            )
            
            self.api_latency_seconds = Histogram(
                'data_api_latency_seconds',
                'Time taken for API calls',
                ['endpoint'],
                buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0)
            )
            
        except ValueError:
            # Metrics already registered (happens in testing)
            logger.debug("Metrics already registered, reusing existing")
            self._reuse_existing_metrics()
    
    def _reuse_existing_metrics(self):
        """Reuse already registered metrics."""
        for metric in REGISTRY._names_to_collectors.values():
            name = getattr(metric, '_name', '')
            if name == 'data_ingestion_success_total':
                self.ingestion_success_total = metric
            elif name == 'data_ingestion_failure_total':
                self.ingestion_failure_total = metric
            elif name == 'data_records_ingested_total':
                self.records_ingested_total = metric
            elif name == 'data_freshness_seconds':
                self.data_freshness_seconds = metric
            elif name == 'data_ingestion_queue_size':
                self.ingestion_queue_size = metric
            elif name == 'data_ingestion_active_connections':
                self.active_connections = metric
            elif name == 'data_ingestion_latency_seconds':
                self.ingestion_latency_seconds = metric
            elif name == 'data_api_latency_seconds':
                self.api_latency_seconds = metric
    
    @classmethod
    def get_instance(cls, port: int = 8000) -> "DataIngestionMetrics":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls(port=port)
        return cls._instance
    
    def start_server(self) -> bool:
        """Start Prometheus metrics HTTP server."""
        if not self._enabled or self._server_started:
            return False
        
        try:
            start_http_server(self._port)
            self._server_started = True
            logger.info(f"Prometheus metrics server started on port {self._port}")
            return True
        except Exception as e:
            logger.warning(f"Could not start metrics server: {e}")
            return False
    
    def record_ingestion_success(self, symbol: str, actor_id: str, records: int, latency_seconds: float):
        """Record a successful ingestion operation."""
        if not self._enabled:
            logger.debug(f"Ingestion success: {symbol}, {records} records, {latency_seconds:.3f}s")
            return
        
        self.ingestion_success_total.labels(symbol=symbol, actor_id=actor_id).inc()
        self.records_ingested_total.labels(symbol=symbol, data_type='ticker', actor_id=actor_id).inc(records)
        self.ingestion_latency_seconds.labels(symbol=symbol, actor_id=actor_id).observe(latency_seconds)
    
    def record_ingestion_failure(self, symbol: str, actor_id: str, error_type: str):
        """Record a failed ingestion operation."""
        if not self._enabled:
            logger.debug(f"Ingestion failure: {symbol}, error: {error_type}")
            return
        
        self.ingestion_failure_total.labels(symbol=symbol, actor_id=actor_id, error_type=error_type).inc()
    
    def update_data_freshness(self, symbol: str, data_type: str, age_seconds: float):
        """Update data freshness gauge."""
        if not self._enabled:
            return
        
        self.data_freshness_seconds.labels(symbol=symbol, data_type=data_type).set(age_seconds)
    
    def update_queue_size(self, actor_id: str, size: int):
        """Update ingestion queue size."""
        if not self._enabled:
            return
        
        self.ingestion_queue_size.labels(actor_id=actor_id).set(size)
    
    def update_active_connections(self, database: str, count: int):
        """Update active database connections."""
        if not self._enabled:
            return
        
        self.active_connections.labels(database=database).set(count)
    
    def record_api_latency(self, endpoint: str, latency_seconds: float):
        """Record API call latency."""
        if not self._enabled:
            return
        
        self.api_latency_seconds.labels(endpoint=endpoint).observe(latency_seconds)


# Convenience function to get metrics instance
def get_ingestion_metrics(port: int = 8000) -> DataIngestionMetrics:
    """Get the data ingestion metrics instance."""
    return DataIngestionMetrics.get_instance(port=port)


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

