#!/usr/bin/env python3
"""
Backtest CLI Tool

Run backtests on trading models and generate reports.

Usage:
    # Run backtest for single symbol
    python scripts/run_backtest.py --symbol RELIANCE --days 30
    
    # Compare multiple experiments
    python scripts/run_backtest.py --compare --experiments TradingModels_Production TradingModels_Staging
    
    # Run preset
    python scripts/run_backtest.py --preset quick
    
    # Generate HTML report
    python scripts/run_backtest.py --symbol RELIANCE --report backtest_report.html
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.backtesting.backtest_engine import BacktestEngine, quick_backtest
from src.backtesting.backtest_result import BacktestResult, ComparisonReport


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime."""
    if date_str == "today":
        return datetime.now()
    elif date_str.startswith("-"):
        # Relative date like "-30d" or "-1y"
        amount = int(date_str[1:-1])
        unit = date_str[-1].lower()
        
        if unit == 'd':
            return datetime.now() - timedelta(days=amount)
        elif unit == 'w':
            return datetime.now() - timedelta(weeks=amount)
        elif unit == 'm':
            return datetime.now() - timedelta(days=amount * 30)
        elif unit == 'y':
            return datetime.now() - timedelta(days=amount * 365)
        else:
            raise ValueError(f"Unknown date unit: {unit}")
    else:
        # Absolute date
        return datetime.strptime(date_str, "%Y-%m-%d")


def print_result(result: BacktestResult) -> None:
    """Print backtest result to console."""
    summary = result.summary()
    
    print("\n" + "=" * 60)
    print("BACKTEST RESULT")
    print("=" * 60)
    print(f"Symbol: {summary['symbol']}")
    print(f"Model: {summary['model_name']}")
    print(f"Period: {summary['period']}")
    print("-" * 60)
    print(f"Initial Capital: ${summary['initial_capital']:,.0f}")
    print(f"Final Balance: ${summary['final_balance']:,.0f}")
    print(f"Total Return: {summary['total_return_pct']:.2f}%")
    print(f"Sharpe Ratio: {summary['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {summary['max_drawdown_pct']:.2f}%")
    print("-" * 60)
    print(f"Total Trades: {summary['total_trades']}")
    print(f"Win Rate: {summary['win_rate_pct']:.2f}%")
    print(f"Profit Factor: {summary['profit_factor']:.2f}")
    print("=" * 60)
    
    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")


def print_comparison(report: ComparisonReport) -> None:
    """Print comparison report to console."""
    print("\n" + "=" * 70)
    print("MODEL COMPARISON REPORT")
    print("=" * 70)
    
    summary = report.summary()
    print(f"Models Compared: {summary['models_compared']}")
    print(f"Symbols: {', '.join(summary['symbols'])}")
    print(f"Best by Sharpe: {summary['best_by_sharpe']}")
    print(f"Best by Return: {summary['best_by_return']}")
    print("-" * 70)
    
    # Print comparison table
    df = report.get_comparison_table()
    if not df.empty:
        print("\nComparison Table:")
        print(df.to_string(index=False))
    
    print("=" * 70)


async def run_single_backtest(
    symbol: str,
    start_date: datetime,
    end_date: datetime,
    experiment: str,
    model_stage: str,
    initial_capital: float,
    transaction_cost: float,
) -> BacktestResult:
    """Run a single backtest."""
    engine = BacktestEngine(
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        transaction_cost=transaction_cost,
    )
    
    try:
        result = await engine.run_backtest(
            symbol=symbol,
            experiment_name=experiment,
            model_stage=model_stage,
        )
    finally:
        await engine.close()
    
    return result


async def run_comparison(
    symbols: List[str],
    experiments: List[str],
    start_date: datetime,
    end_date: datetime,
    model_stage: str,
    initial_capital: float,
    transaction_cost: float,
) -> ComparisonReport:
    """Run model comparison."""
    engine = BacktestEngine(
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        transaction_cost=transaction_cost,
    )
    
    try:
        report = await engine.compare_models(
            symbols=symbols,
            experiment_names=experiments,
            model_stage=model_stage,
        )
    finally:
        await engine.close()
    
    return report


def get_preset_config(preset: str) -> dict:
    """Get configuration for a preset."""
    presets = {
        'quick': {
            'days': 30,
            'symbols': ['RELIANCE'],
        },
        'standard': {
            'days': 90,
            'symbols': ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK'],
        },
        'full': {
            'days': 365,
            'symbols': [],  # Will use config
        },
    }
    return presets.get(preset, presets['standard'])


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run backtests on trading models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Single symbol backtest
    python scripts/run_backtest.py --symbol RELIANCE --days 30
    
    # Multiple symbols
    python scripts/run_backtest.py --symbols RELIANCE TCS INFY --days 90
    
    # Compare experiments
    python scripts/run_backtest.py --compare --experiments Exp1 Exp2 --symbols RELIANCE
    
    # Use preset
    python scripts/run_backtest.py --preset quick
    
    # Custom date range
    python scripts/run_backtest.py --symbol RELIANCE --start 2024-01-01 --end 2024-12-31
    
    # Save report
    python scripts/run_backtest.py --symbol RELIANCE --report report.json
        """
    )
    
    # Symbol selection
    parser.add_argument("--symbol", type=str, help="Single symbol to backtest")
    parser.add_argument("--symbols", nargs="+", help="Multiple symbols to backtest")
    
    # Date range
    parser.add_argument("--days", type=int, default=30, help="Number of days to backtest (default: 30)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD or -30d)")
    parser.add_argument("--end", type=str, default="today", help="End date (default: today)")
    
    # Model selection
    parser.add_argument(
        "--experiment", 
        type=str, 
        default="TradingModels_Production",
        help="MLflow experiment name"
    )
    parser.add_argument(
        "--model-stage",
        type=str,
        default="Production",
        choices=["Production", "Staging", "None"],
        help="Model stage (default: Production)"
    )
    
    # Comparison mode
    parser.add_argument("--compare", action="store_true", help="Compare multiple experiments")
    parser.add_argument("--experiments", nargs="+", help="Experiments to compare")
    
    # Preset
    parser.add_argument(
        "--preset",
        type=str,
        choices=["quick", "standard", "full"],
        help="Use a preset configuration"
    )
    
    # Capital settings
    parser.add_argument("--capital", type=float, default=100000, help="Initial capital (default: 100000)")
    parser.add_argument("--txn-cost", type=float, default=20, help="Transaction cost (default: 20)")
    
    # Output
    parser.add_argument("--report", type=str, help="Save report to file (JSON)")
    parser.add_argument("--quiet", action="store_true", help="Suppress detailed output")
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    
    # Apply preset if specified
    if args.preset:
        preset_config = get_preset_config(args.preset)
        if not args.days:
            args.days = preset_config['days']
        if not args.symbols and not args.symbol:
            args.symbols = preset_config['symbols']
    
    # Determine symbols
    if args.symbol:
        symbols = [args.symbol]
    elif args.symbols:
        symbols = args.symbols
    else:
        # Try to get from config
        try:
            from src import config
            symbols = list(config.symbols)[:5]  # First 5 symbols
        except Exception:
            symbols = ["RELIANCE"]
    
    # Determine date range
    if args.start:
        start_date = parse_date(args.start)
    else:
        start_date = datetime.now() - timedelta(days=args.days)
    
    end_date = parse_date(args.end)
    
    if not args.quiet:
        logger.info("=" * 60)
        logger.info("Backtest Configuration")
        logger.info("=" * 60)
        logger.info(f"Symbols: {symbols}")
        logger.info(f"Period: {start_date.date()} to {end_date.date()}")
        logger.info(f"Capital: ${args.capital:,.0f}")
        logger.info(f"Experiment: {args.experiment}")
        logger.info("=" * 60)
    
    try:
        if args.compare:
            # Comparison mode
            experiments = args.experiments or ["TradingModels_Production", "TradingModels_Staging"]
            
            report = await run_comparison(
                symbols=symbols,
                experiments=experiments,
                start_date=start_date,
                end_date=end_date,
                model_stage=args.model_stage,
                initial_capital=args.capital,
                transaction_cost=args.txn_cost,
            )
            
            if not args.quiet:
                print_comparison(report)
            
            # Save report if requested
            if args.report:
                with open(args.report, 'w') as f:
                    json.dump(report.to_dict(), f, indent=2, default=str)
                logger.info(f"Report saved to {args.report}")
        else:
            # Single backtest mode (can be multiple symbols)
            results = []
            
            for symbol in symbols:
                result = await run_single_backtest(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    experiment=args.experiment,
                    model_stage=args.model_stage,
                    initial_capital=args.capital,
                    transaction_cost=args.txn_cost,
                )
                results.append(result)
                
                if not args.quiet:
                    print_result(result)
            
            # Save report if requested
            if args.report and results:
                report_data = {
                    'created_at': datetime.now().isoformat(),
                    'results': [r.to_dict() for r in results],
                }
                with open(args.report, 'w') as f:
                    json.dump(report_data, f, indent=2, default=str)
                logger.info(f"Report saved to {args.report}")
    
    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
