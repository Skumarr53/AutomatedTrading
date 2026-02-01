# src/backtesting/report_generator.py
"""
Report Generator for Backtest Results

Generates HTML and PDF reports with:
- Performance summary
- Equity curves
- Drawdown charts
- Trade log
- Model comparison tables
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from loguru import logger

from src.backtesting.backtest_result import BacktestResult, ComparisonReport

# Optional imports for charting
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.debug("plotly not installed, charts will be disabled")


class ReportGenerator:
    """
    Generate HTML and PDF reports from backtest results.
    
    Example:
        generator = ReportGenerator(output_dir="./reports")
        generator.generate_html(result, "backtest_report.html")
    """
    
    def __init__(
        self, 
        output_dir: str = "./backtest_reports",
        include_charts: bool = True,
        include_trade_log: bool = True,
    ):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory for report output
            include_charts: Whether to include charts
            include_trade_log: Whether to include detailed trade log
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.include_charts = include_charts and PLOTLY_AVAILABLE
        self.include_trade_log = include_trade_log
    
    def generate_html(
        self, 
        result: BacktestResult, 
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate HTML report from backtest result.
        
        Args:
            result: BacktestResult to report on
            filename: Output filename (default: auto-generated)
            
        Returns:
            Path to generated report
        """
        if filename is None:
            filename = f"backtest_{result.symbol}_{result.backtest_id}.html"
        
        output_path = self.output_dir / filename
        
        html_content = self._build_html_report(result)
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Report generated: {output_path}")
        return str(output_path)
    
    def generate_comparison_html(
        self,
        report: ComparisonReport,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate HTML comparison report.
        
        Args:
            report: ComparisonReport with multiple results
            filename: Output filename
            
        Returns:
            Path to generated report
        """
        if filename is None:
            filename = f"comparison_{report.report_id}.html"
        
        output_path = self.output_dir / filename
        
        html_content = self._build_comparison_html(report)
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Comparison report generated: {output_path}")
        return str(output_path)
    
    def _build_html_report(self, result: BacktestResult) -> str:
        """Build HTML content for single backtest."""
        metrics = result.metrics
        
        # Generate charts if available
        equity_chart = ""
        drawdown_chart = ""
        if self.include_charts and not result.equity_curve.empty:
            equity_chart = self._create_equity_chart(result)
            drawdown_chart = self._create_drawdown_chart(result)
        
        # Generate trade log table
        trade_log = ""
        if self.include_trade_log and result.trades:
            trade_log = self._create_trade_log_table(result.trades)
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Backtest Report - {result.symbol}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .header h1 {{
            font-size: 2rem;
            margin-bottom: 10px;
        }}
        .header .meta {{
            opacity: 0.9;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .card h2 {{
            color: #667eea;
            margin-bottom: 15px;
            border-bottom: 2px solid #f0f0f0;
            padding-bottom: 10px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .metric {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }}
        .metric .value {{
            font-size: 1.5rem;
            font-weight: bold;
            color: #333;
        }}
        .metric .label {{
            color: #666;
            font-size: 0.9rem;
        }}
        .metric.positive .value {{
            color: #28a745;
        }}
        .metric.negative .value {{
            color: #dc3545;
        }}
        .chart {{
            width: 100%;
            margin: 20px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #e0e0e0;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .win {{
            color: #28a745;
        }}
        .loss {{
            color: #dc3545;
        }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #666;
            font-size: 0.9rem;
        }}
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Backtest Report: {result.symbol}</h1>
            <div class="meta">
                <p>Model: {result.model_name}</p>
                <p>Period: {result.start_date.strftime('%Y-%m-%d')} to {result.end_date.strftime('%Y-%m-%d')}</p>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
        
        <div class="card">
            <h2>Performance Summary</h2>
            <div class="metrics-grid">
                <div class="metric {'positive' if metrics.total_return_pct > 0 else 'negative'}">
                    <div class="value">{metrics.total_return_pct:.2f}%</div>
                    <div class="label">Total Return</div>
                </div>
                <div class="metric">
                    <div class="value">${metrics.final_balance:,.0f}</div>
                    <div class="label">Final Balance</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.sharpe_ratio:.2f}</div>
                    <div class="label">Sharpe Ratio</div>
                </div>
                <div class="metric negative">
                    <div class="value">{metrics.max_drawdown_pct:.2f}%</div>
                    <div class="label">Max Drawdown</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.total_trades}</div>
                    <div class="label">Total Trades</div>
                </div>
                <div class="metric {'positive' if metrics.win_rate_pct > 50 else 'negative'}">
                    <div class="value">{metrics.win_rate_pct:.1f}%</div>
                    <div class="label">Win Rate</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.profit_factor:.2f}</div>
                    <div class="label">Profit Factor</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.avg_holding_period_minutes:.0f}min</div>
                    <div class="label">Avg Holding Period</div>
                </div>
            </div>
        </div>
        
        {f'<div class="card"><h2>Equity Curve</h2><div class="chart">{equity_chart}</div></div>' if equity_chart else ''}
        
        {f'<div class="card"><h2>Drawdown</h2><div class="chart">{drawdown_chart}</div></div>' if drawdown_chart else ''}
        
        <div class="card">
            <h2>Trade Statistics</h2>
            <div class="metrics-grid">
                <div class="metric positive">
                    <div class="value">{metrics.winning_trades}</div>
                    <div class="label">Winning Trades</div>
                </div>
                <div class="metric negative">
                    <div class="value">{metrics.losing_trades}</div>
                    <div class="label">Losing Trades</div>
                </div>
                <div class="metric positive">
                    <div class="value">${metrics.avg_win_pnl:.2f}</div>
                    <div class="label">Avg Win</div>
                </div>
                <div class="metric negative">
                    <div class="value">${metrics.avg_loss_pnl:.2f}</div>
                    <div class="label">Avg Loss</div>
                </div>
                <div class="metric positive">
                    <div class="value">${metrics.best_trade_pnl:.2f}</div>
                    <div class="label">Best Trade</div>
                </div>
                <div class="metric negative">
                    <div class="value">${metrics.worst_trade_pnl:.2f}</div>
                    <div class="label">Worst Trade</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.max_consecutive_wins}</div>
                    <div class="label">Max Consecutive Wins</div>
                </div>
                <div class="metric">
                    <div class="value">{metrics.max_consecutive_losses}</div>
                    <div class="label">Max Consecutive Losses</div>
                </div>
            </div>
        </div>
        
        {f'<div class="card"><h2>Trade Log</h2>{trade_log}</div>' if trade_log else ''}
        
        <div class="footer">
            <p>Report generated by AutomatedTrading Backtest Engine</p>
            <p>Execution time: {result.execution_time_seconds:.2f} seconds</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _create_equity_chart(self, result: BacktestResult) -> str:
        """Create equity curve chart as HTML."""
        if not PLOTLY_AVAILABLE or result.equity_curve.empty:
            return ""
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=result.equity_curve.index,
            y=result.equity_curve.values,
            mode='lines',
            name='Equity',
            line=dict(color='#667eea', width=2),
            fill='tozeroy',
            fillcolor='rgba(102, 126, 234, 0.1)',
        ))
        
        # Add initial capital line
        fig.add_hline(
            y=result.initial_capital,
            line_dash="dash",
            line_color="gray",
            annotation_text="Initial Capital"
        )
        
        fig.update_layout(
            title="Portfolio Equity Curve",
            xaxis_title="Date",
            yaxis_title="Equity ($)",
            template="plotly_white",
            height=400,
            margin=dict(l=50, r=50, t=50, b=50),
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_drawdown_chart(self, result: BacktestResult) -> str:
        """Create drawdown chart as HTML."""
        if not PLOTLY_AVAILABLE or result.equity_curve.empty:
            return ""
        
        # Calculate drawdown
        rolling_max = result.equity_curve.cummax()
        drawdown = (result.equity_curve - rolling_max) / rolling_max * 100
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=drawdown.index,
            y=drawdown.values,
            mode='lines',
            name='Drawdown',
            line=dict(color='#dc3545', width=2),
            fill='tozeroy',
            fillcolor='rgba(220, 53, 69, 0.2)',
        ))
        
        fig.update_layout(
            title="Drawdown Chart",
            xaxis_title="Date",
            yaxis_title="Drawdown (%)",
            template="plotly_white",
            height=300,
            margin=dict(l=50, r=50, t=50, b=50),
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)
    
    def _create_trade_log_table(self, trades: list) -> str:
        """Create HTML table for trade log."""
        if not trades:
            return ""
        
        rows = []
        for t in trades:
            pnl_class = "win" if t.net_pnl > 0 else "loss"
            rows.append(f"""
            <tr>
                <td>{t.trade_id}</td>
                <td>{t.direction.value}</td>
                <td>{t.entry_time.strftime('%Y-%m-%d %H:%M') if t.entry_time else '-'}</td>
                <td>${t.entry_price:.2f}</td>
                <td>{t.exit_time.strftime('%Y-%m-%d %H:%M') if t.exit_time else '-'}</td>
                <td>${t.exit_price:.2f if t.exit_price else 0:.2f}</td>
                <td>{t.quantity}</td>
                <td class="{pnl_class}">${t.net_pnl:.2f}</td>
                <td>{t.exit_reason or '-'}</td>
            </tr>
            """)
        
        return f"""
        <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>Trade ID</th>
                        <th>Direction</th>
                        <th>Entry Time</th>
                        <th>Entry Price</th>
                        <th>Exit Time</th>
                        <th>Exit Price</th>
                        <th>Quantity</th>
                        <th>Net P&L</th>
                        <th>Exit Reason</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
        """
    
    def _build_comparison_html(self, report: ComparisonReport) -> str:
        """Build HTML content for comparison report."""
        comparison_table = report.get_comparison_table()
        
        # Create comparison chart if available
        comparison_chart = ""
        if self.include_charts and len(report.results) > 0:
            comparison_chart = self._create_comparison_chart(report)
        
        # Build table rows
        table_rows = ""
        if not comparison_table.empty:
            for _, row in comparison_table.iterrows():
                table_rows += "<tr>"
                for val in row:
                    table_rows += f"<td>{val}</td>"
                table_rows += "</tr>"
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Model Comparison Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .card h2 {{
            color: #667eea;
            margin-bottom: 15px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e0e0e0;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
    </style>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Model Comparison Report</h1>
            <p>Report ID: {report.report_id}</p>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Models Compared: {len(report.results)}</p>
        </div>
        
        {f'<div class="card"><h2>Performance Comparison</h2>{comparison_chart}</div>' if comparison_chart else ''}
        
        <div class="card">
            <h2>Comparison Table</h2>
            <table>
                <thead>
                    <tr>
                        {"".join(f"<th>{col}</th>" for col in comparison_table.columns)}
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _create_comparison_chart(self, report: ComparisonReport) -> str:
        """Create comparison bar chart."""
        if not PLOTLY_AVAILABLE:
            return ""
        
        df = report.get_comparison_table()
        if df.empty:
            return ""
        
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=("Sharpe Ratio", "Total Return (%)", "Win Rate (%)"),
        )
        
        # Sharpe Ratio
        fig.add_trace(
            go.Bar(x=df['Model'], y=df['Sharpe Ratio'], name='Sharpe'),
            row=1, col=1
        )
        
        # Total Return
        fig.add_trace(
            go.Bar(x=df['Model'], y=df['Total Return (%)'], name='Return'),
            row=1, col=2
        )
        
        # Win Rate
        fig.add_trace(
            go.Bar(x=df['Model'], y=df['Win Rate (%)'], name='Win Rate'),
            row=1, col=3
        )
        
        fig.update_layout(
            height=400,
            showlegend=False,
            template="plotly_white",
        )
        
        return fig.to_html(full_html=False, include_plotlyjs=False)


def generate_report(result: BacktestResult, output_path: str) -> str:
    """Convenience function to generate a report."""
    generator = ReportGenerator()
    return generator.generate_html(result, output_path)
