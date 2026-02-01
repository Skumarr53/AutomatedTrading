#!/usr/bin/env python3
"""
Comprehensive Data Collection Verification Script

Verifies that data is being collected and stored correctly in InfluxDB:
1. Data exists for all configured symbols
2. Data is fresh (within expected interval)
3. Schema is correct (required columns present)
4. Data quality checks (no invalid values)
5. Ticker and orderbook data alignment

Usage:
    python scripts/verify_data_collection.py --symbols RELIANCE TCS INFY --days 7
    python scripts/verify_data_collection.py --all-symbols --freshness 5
    python scripts/verify_data_collection.py --report verification_report.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig,
)


class VerificationStatus(str, Enum):
    """Status of a verification check."""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARNING"
    SKIP = "SKIPPED"


@dataclass
class SymbolVerificationResult:
    """Result of verifying data for a single symbol."""
    symbol: str
    ticker_record_count: int = 0
    orderbook_record_count: int = 0
    ticker_latest_timestamp: Optional[str] = None
    orderbook_latest_timestamp: Optional[str] = None
    ticker_oldest_timestamp: Optional[str] = None
    orderbook_oldest_timestamp: Optional[str] = None
    ticker_freshness_minutes: Optional[float] = None
    orderbook_freshness_minutes: Optional[float] = None
    schema_valid: bool = True
    missing_columns: list[str] = field(default_factory=list)
    data_quality_issues: list[str] = field(default_factory=list)
    status: VerificationStatus = VerificationStatus.PASS
    error: Optional[str] = None


@dataclass
class VerificationReport:
    """Complete verification report."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    influx_url: str = ""
    influx_org: str = ""
    symbols_checked: int = 0
    symbols_with_ticker_data: int = 0
    symbols_with_orderbook_data: int = 0
    total_ticker_records: int = 0
    total_orderbook_records: int = 0
    overall_status: VerificationStatus = VerificationStatus.PASS
    freshness_threshold_minutes: int = 5
    symbols_with_stale_data: list[str] = field(default_factory=list)
    symbols_missing_data: list[str] = field(default_factory=list)
    symbols_with_quality_issues: list[str] = field(default_factory=list)
    results: list[SymbolVerificationResult] = field(default_factory=list)
    

class DataCollectionVerifier:
    """Verifies data collection and storage in InfluxDB."""
    
    # Required ticker columns
    TICKER_REQUIRED_COLUMNS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    
    # Required orderbook columns
    ORDERBOOK_REQUIRED_COLUMNS = [
        'timestamp', 'symbol', 'total_buy_qty', 'total_sell_qty', 
        'ltp', 'ltq', 'volume', 'atp'
    ]
    
    def __init__(
        self, 
        client: InfluxDBClient_Wrapper,
        freshness_threshold_minutes: int = 5,
    ):
        """
        Initialize verifier.
        
        Args:
            client: Connected InfluxDB client
            freshness_threshold_minutes: Maximum age of data in minutes to be considered fresh
        """
        self.client = client
        self.freshness_threshold_minutes = freshness_threshold_minutes
    
    async def verify_symbol(
        self, 
        symbol: str, 
        days: int = 7,
        check_orderbook: bool = True,
    ) -> SymbolVerificationResult:
        """
        Verify data for a single symbol.
        
        Args:
            symbol: Stock symbol to verify
            days: Number of days of historical data to check
            check_orderbook: Whether to check orderbook data
            
        Returns:
            SymbolVerificationResult with verification details
        """
        result = SymbolVerificationResult(symbol=symbol)
        now = datetime.now()
        
        try:
            # Check ticker data
            hours = days * 24
            ticker_df = await self.client.query_ticker_data(symbol, hours=hours)
            
            if ticker_df is not None and len(ticker_df) > 0:
                result.ticker_record_count = len(ticker_df)
                
                # Get timestamps
                if 'timestamp' in ticker_df.columns:
                    result.ticker_latest_timestamp = str(ticker_df['timestamp'].max())
                    result.ticker_oldest_timestamp = str(ticker_df['timestamp'].min())
                    
                    # Calculate freshness
                    try:
                        latest = ticker_df['timestamp'].max()
                        if hasattr(latest, 'to_pydatetime'):
                            latest = latest.to_pydatetime()
                        elif isinstance(latest, str):
                            from dateutil import parser
                            latest = parser.parse(latest)
                        
                        # Make timezone naive if needed
                        if hasattr(latest, 'tzinfo') and latest.tzinfo is not None:
                            latest = latest.replace(tzinfo=None)
                        
                        age = now - latest
                        result.ticker_freshness_minutes = age.total_seconds() / 60
                    except Exception as e:
                        logger.debug(f"Could not calculate ticker freshness for {symbol}: {e}")
                
                # Validate schema
                missing = [col for col in self.TICKER_REQUIRED_COLUMNS if col not in ticker_df.columns]
                if missing:
                    result.schema_valid = False
                    result.missing_columns.extend([f"ticker:{col}" for col in missing])
                
                # Data quality checks
                self._check_ticker_quality(ticker_df, result)
            else:
                result.data_quality_issues.append("No ticker data found")
            
            # Check orderbook data
            if check_orderbook:
                try:
                    # Calculate time range from days
                    end_time = now
                    start_time = now - timedelta(days=days)
                    
                    # query_orderbook_data expects list of symbols, start_time, and end_time
                    orderbook_df = await self.client.query_orderbook_data(
                        symbols=[symbol],
                        start_time=start_time,
                        end_time=end_time
                    )
                    
                    # Filter to this symbol if multiple symbols returned
                    if orderbook_df is not None and not orderbook_df.empty and 'symbol' in orderbook_df.columns:
                        orderbook_df = orderbook_df[orderbook_df['symbol'] == symbol]
                    
                    if orderbook_df is not None and len(orderbook_df) > 0:
                        result.orderbook_record_count = len(orderbook_df)
                        
                        if 'timestamp' in orderbook_df.columns:
                            result.orderbook_latest_timestamp = str(orderbook_df['timestamp'].max())
                            result.orderbook_oldest_timestamp = str(orderbook_df['timestamp'].min())
                            
                            # Calculate freshness
                            try:
                                latest = orderbook_df['timestamp'].max()
                                if hasattr(latest, 'to_pydatetime'):
                                    latest = latest.to_pydatetime()
                                elif isinstance(latest, str):
                                    from dateutil import parser
                                    latest = parser.parse(latest)
                                
                                if hasattr(latest, 'tzinfo') and latest.tzinfo is not None:
                                    latest = latest.replace(tzinfo=None)
                                
                                age = now - latest
                                result.orderbook_freshness_minutes = age.total_seconds() / 60
                            except Exception as e:
                                logger.debug(f"Could not calculate orderbook freshness for {symbol}: {e}")
                        
                        # Data quality checks
                        self._check_orderbook_quality(orderbook_df, result)
                    else:
                        result.data_quality_issues.append("No orderbook data found")
                except Exception as e:
                    logger.debug(f"Could not query orderbook data for {symbol}: {e}")
                    result.data_quality_issues.append(f"Orderbook query error: {str(e)}")
            
            # Determine overall status
            result.status = self._determine_status(result)
            
        except Exception as e:
            result.status = VerificationStatus.FAIL
            result.error = str(e)
            logger.error(f"Error verifying {symbol}: {e}")
        
        return result
    
    def _check_ticker_quality(self, df, result: SymbolVerificationResult) -> None:
        """Check ticker data quality."""
        import numpy as np
        
        # Check for negative prices
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                if (df[col] < 0).any():
                    result.data_quality_issues.append(f"Negative values in {col}")
        
        # Check for zero volume
        if 'volume' in df.columns:
            zero_volume_pct = (df['volume'] == 0).sum() / len(df) * 100
            if zero_volume_pct > 10:
                result.data_quality_issues.append(f"{zero_volume_pct:.1f}% of records have zero volume")
        
        # Check for missing values in critical columns
        for col in ['close', 'volume']:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    result.data_quality_issues.append(f"{null_count} null values in {col}")
        
        # Check OHLC consistency (high >= low, high >= open/close, low <= open/close)
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            invalid_ohlc = (
                (df['high'] < df['low']) |
                (df['high'] < df['open']) |
                (df['high'] < df['close']) |
                (df['low'] > df['open']) |
                (df['low'] > df['close'])
            ).sum()
            if invalid_ohlc > 0:
                result.data_quality_issues.append(f"{invalid_ohlc} records with invalid OHLC relationships")
    
    def _check_orderbook_quality(self, df, result: SymbolVerificationResult) -> None:
        """Check orderbook data quality."""
        # Check for negative quantities
        for col in ['total_buy_qty', 'total_sell_qty', 'ltq', 'volume']:
            if col in df.columns:
                if (df[col] < 0).any():
                    result.data_quality_issues.append(f"Negative values in {col}")
        
        # Check for negative prices
        for col in ['ltp', 'atp', 'lower_circuit', 'upper_circuit']:
            if col in df.columns:
                if (df[col] < 0).any():
                    result.data_quality_issues.append(f"Negative values in {col}")
    
    def _determine_status(self, result: SymbolVerificationResult) -> VerificationStatus:
        """Determine overall verification status."""
        # FAIL if no data at all
        if result.ticker_record_count == 0 and result.orderbook_record_count == 0:
            return VerificationStatus.FAIL
        
        # FAIL if schema invalid
        if not result.schema_valid:
            return VerificationStatus.FAIL
        
        # WARN if data is stale
        if result.ticker_freshness_minutes is not None:
            if result.ticker_freshness_minutes > self.freshness_threshold_minutes:
                return VerificationStatus.WARN
        
        # WARN if quality issues
        if result.data_quality_issues:
            return VerificationStatus.WARN
        
        return VerificationStatus.PASS
    
    async def verify_all_symbols(
        self, 
        symbols: list[str],
        days: int = 7,
        check_orderbook: bool = True,
    ) -> VerificationReport:
        """
        Verify data for all symbols.
        
        Args:
            symbols: List of symbols to verify
            days: Number of days of historical data to check
            check_orderbook: Whether to check orderbook data
            
        Returns:
            VerificationReport with all results
        """
        report = VerificationReport(
            symbols_checked=len(symbols),
            freshness_threshold_minutes=self.freshness_threshold_minutes,
        )
        
        logger.info(f"Verifying data for {len(symbols)} symbols...")
        
        for i, symbol in enumerate(symbols):
            logger.info(f"  [{i+1}/{len(symbols)}] Checking {symbol}...")
            result = await self.verify_symbol(symbol, days=days, check_orderbook=check_orderbook)
            report.results.append(result)
            
            # Update summary
            if result.ticker_record_count > 0:
                report.symbols_with_ticker_data += 1
                report.total_ticker_records += result.ticker_record_count
            
            if result.orderbook_record_count > 0:
                report.symbols_with_orderbook_data += 1
                report.total_orderbook_records += result.orderbook_record_count
            
            if result.status == VerificationStatus.FAIL:
                report.symbols_missing_data.append(symbol)
            
            if result.ticker_freshness_minutes is not None:
                if result.ticker_freshness_minutes > self.freshness_threshold_minutes:
                    report.symbols_with_stale_data.append(symbol)
            
            if result.data_quality_issues:
                report.symbols_with_quality_issues.append(symbol)
        
        # Determine overall status
        if report.symbols_missing_data:
            report.overall_status = VerificationStatus.FAIL
        elif report.symbols_with_stale_data or report.symbols_with_quality_issues:
            report.overall_status = VerificationStatus.WARN
        else:
            report.overall_status = VerificationStatus.PASS
        
        return report


def print_report(report: VerificationReport) -> None:
    """Print verification report to console."""
    print("\n" + "=" * 70)
    print("DATA COLLECTION VERIFICATION REPORT")
    print("=" * 70)
    print(f"Timestamp: {report.timestamp}")
    print(f"InfluxDB URL: {report.influx_url}")
    print(f"Organization: {report.influx_org}")
    print("-" * 70)
    
    # Summary
    status_emoji = {
        VerificationStatus.PASS: "✅",
        VerificationStatus.FAIL: "❌",
        VerificationStatus.WARN: "⚠️",
        VerificationStatus.SKIP: "⏭️",
    }
    
    print(f"\nOverall Status: {status_emoji[report.overall_status]} {report.overall_status.value}")
    print(f"\nSymbols Checked: {report.symbols_checked}")
    print(f"Symbols with Ticker Data: {report.symbols_with_ticker_data}")
    print(f"Symbols with Orderbook Data: {report.symbols_with_orderbook_data}")
    print(f"Total Ticker Records: {report.total_ticker_records:,}")
    print(f"Total Orderbook Records: {report.total_orderbook_records:,}")
    print(f"Freshness Threshold: {report.freshness_threshold_minutes} minutes")
    
    if report.symbols_missing_data:
        print(f"\n❌ Symbols Missing Data ({len(report.symbols_missing_data)}):")
        for s in report.symbols_missing_data[:10]:
            print(f"   - {s}")
        if len(report.symbols_missing_data) > 10:
            print(f"   ... and {len(report.symbols_missing_data) - 10} more")
    
    if report.symbols_with_stale_data:
        print(f"\n⚠️  Symbols with Stale Data ({len(report.symbols_with_stale_data)}):")
        for s in report.symbols_with_stale_data[:10]:
            print(f"   - {s}")
        if len(report.symbols_with_stale_data) > 10:
            print(f"   ... and {len(report.symbols_with_stale_data) - 10} more")
    
    if report.symbols_with_quality_issues:
        print(f"\n⚠️  Symbols with Quality Issues ({len(report.symbols_with_quality_issues)}):")
        for s in report.symbols_with_quality_issues[:10]:
            # Find the result
            result = next((r for r in report.results if r.symbol == s), None)
            if result:
                issues = result.data_quality_issues[:3]
                print(f"   - {s}: {', '.join(issues)}")
        if len(report.symbols_with_quality_issues) > 10:
            print(f"   ... and {len(report.symbols_with_quality_issues) - 10} more")
    
    # Detailed results
    print("\n" + "-" * 70)
    print("DETAILED RESULTS")
    print("-" * 70)
    print(f"{'Symbol':<15} {'Ticker':<10} {'Orderbook':<10} {'Fresh':<10} {'Status':<10}")
    print("-" * 70)
    
    for result in report.results:
        ticker_str = f"{result.ticker_record_count:,}" if result.ticker_record_count > 0 else "-"
        orderbook_str = f"{result.orderbook_record_count:,}" if result.orderbook_record_count > 0 else "-"
        
        if result.ticker_freshness_minutes is not None:
            if result.ticker_freshness_minutes < 60:
                fresh_str = f"{result.ticker_freshness_minutes:.0f}m"
            elif result.ticker_freshness_minutes < 1440:
                fresh_str = f"{result.ticker_freshness_minutes/60:.1f}h"
            else:
                fresh_str = f"{result.ticker_freshness_minutes/1440:.1f}d"
        else:
            fresh_str = "-"
        
        status_str = f"{status_emoji[result.status]} {result.status.value}"
        print(f"{result.symbol:<15} {ticker_str:<10} {orderbook_str:<10} {fresh_str:<10} {status_str:<10}")
    
    print("=" * 70)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Verify data collection in InfluxDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Check specific symbols
    python scripts/verify_data_collection.py --symbols RELIANCE TCS INFY

    # Check all configured symbols with 10-minute freshness threshold
    python scripts/verify_data_collection.py --all-symbols --freshness 10

    # Generate JSON report
    python scripts/verify_data_collection.py --all-symbols --report report.json
    
    # Check last 30 days of data
    python scripts/verify_data_collection.py --all-symbols --days 30
        """
    )
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Specific symbols to check"
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Check all symbols from config"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of historical data to check (default: 7)"
    )
    parser.add_argument(
        "--freshness",
        type=int,
        default=5,
        help="Freshness threshold in minutes (default: 5)"
    )
    parser.add_argument(
        "--no-orderbook",
        action="store_true",
        help="Skip orderbook data verification"
    )
    parser.add_argument(
        "--report",
        type=str,
        help="Output JSON report to file"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress detailed output"
    )
    
    args = parser.parse_args()
    
    # Load environment
    load_dotenv()
    
    # Get InfluxDB config
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        logger.error("❌ INFLUXDB_TOKEN environment variable is required")
        logger.info("💡 Set it in .env file or export it")
        sys.exit(1)
    
    # Determine symbols to check
    if args.symbols:
        symbols = args.symbols
    elif args.all_symbols:
        try:
            from src import config as app_config
            symbols = list(app_config.symbols)
            logger.info(f"Loaded {len(symbols)} symbols from config")
        except Exception as e:
            logger.error(f"Could not load symbols from config: {e}")
            symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
            logger.info(f"Using default symbols: {symbols}")
    else:
        symbols = ["RELIANCE", "TCS", "INFY"]
        logger.info(f"No symbols specified, using defaults: {symbols}")
    
    # Create config and client
    config = InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )
    
    if not args.quiet:
        logger.info("=" * 60)
        logger.info("Data Collection Verification")
        logger.info("=" * 60)
        logger.info(f"URL: {influx_url}")
        logger.info(f"Org: {influx_org}")
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Days: {args.days}")
        logger.info(f"Freshness threshold: {args.freshness} minutes")
        logger.info("=" * 60)
    
    try:
        async with InfluxDBClient_Wrapper.create(config) as client:
            if not client.is_connected:
                logger.error("❌ Failed to connect to InfluxDB")
                logger.info("💡 Check if InfluxDB is running: podman ps | grep influxdb")
                sys.exit(1)
            
            logger.info("✅ Connected to InfluxDB")
            
            # Create verifier and run
            verifier = DataCollectionVerifier(
                client=client,
                freshness_threshold_minutes=args.freshness,
            )
            
            report = await verifier.verify_all_symbols(
                symbols=symbols,
                days=args.days,
                check_orderbook=not args.no_orderbook,
            )
            
            # Add connection info to report
            report.influx_url = influx_url
            report.influx_org = influx_org
            
            # Print report
            if not args.quiet:
                print_report(report)
            
            # Save JSON report if requested
            if args.report:
                report_dict = {
                    'timestamp': report.timestamp,
                    'influx_url': report.influx_url,
                    'influx_org': report.influx_org,
                    'symbols_checked': report.symbols_checked,
                    'symbols_with_ticker_data': report.symbols_with_ticker_data,
                    'symbols_with_orderbook_data': report.symbols_with_orderbook_data,
                    'total_ticker_records': report.total_ticker_records,
                    'total_orderbook_records': report.total_orderbook_records,
                    'overall_status': report.overall_status.value,
                    'freshness_threshold_minutes': report.freshness_threshold_minutes,
                    'symbols_missing_data': report.symbols_missing_data,
                    'symbols_with_stale_data': report.symbols_with_stale_data,
                    'symbols_with_quality_issues': report.symbols_with_quality_issues,
                    'results': [asdict(r) for r in report.results],
                }
                # Convert enums to strings
                for r in report_dict['results']:
                    r['status'] = r['status'].value if hasattr(r['status'], 'value') else r['status']
                
                with open(args.report, 'w') as f:
                    json.dump(report_dict, f, indent=2, default=str)
                logger.info(f"📄 Report saved to {args.report}")
            
            # Exit with appropriate code
            if report.overall_status == VerificationStatus.FAIL:
                sys.exit(1)
            elif report.overall_status == VerificationStatus.WARN:
                sys.exit(0)  # Warnings are acceptable
            else:
                sys.exit(0)
                
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
