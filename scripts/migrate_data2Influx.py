#!/usr/bin/env python3
"""
CSV to InfluxDB Migration Script

Migrates existing ticker and order book CSV data to InfluxDB.
Uses async batch writes for high performance.

Usage:
    python scripts/migrate_data2Influx.py --ticker-dir backups/TickerData --orderbook-dir backups/OrderBookData
    
Environment Variables Required:
    INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig,
    OrderBookDataPoint,
    TickerDataPoint,
)


def load_symbols(symbols_file: str) -> list[str]:
    """
    Load stock symbols from a file.
    
    Args:
        symbols_file: Path to the file containing stock symbols.
        
    Returns:
        List of stock symbols.
    """
    try:
        with open(symbols_file, "r") as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        logger.error(f"Symbols file not found: {symbols_file}")
        return []


def parse_ticker_csv(file_path: str, symbol: str) -> list[TickerDataPoint]:
    """
    Parse ticker CSV file into TickerDataPoint objects.
    
    Args:
        file_path: Path to the CSV file
        symbol: Stock symbol
        
    Returns:
        List of TickerDataPoint objects
    """
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", engine="python")
        
        # Handle different date column names
        date_col = "date" if "date" in df.columns else "epoch_time"
        
        if date_col == "epoch_time":
            df["timestamp"] = pd.to_datetime(df["epoch_time"], unit="s")
        else:
            df["timestamp"] = pd.to_datetime(df["date"])
        
        data_points: list[TickerDataPoint] = []
        for _, row in df.iterrows():
            try:
                data_points.append(
                    TickerDataPoint(
                        symbol=symbol,
                        timestamp=row["timestamp"].to_pydatetime(),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=int(row["volume"]),
                        exchange="NSE",
                    )
                )
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping invalid row in {file_path}: {e}")
                continue
                
        return data_points
        
    except Exception as e:
        logger.error(f"Failed to parse ticker CSV {file_path}: {e}")
        return []


def parse_orderbook_csv(file_path: str, symbol: str) -> list[OrderBookDataPoint]:
    """
    Parse order book CSV file into OrderBookDataPoint objects.
    
    Args:
        file_path: Path to the CSV file
        symbol: Stock symbol
        
    Returns:
        List of OrderBookDataPoint objects
    """
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", engine="python")
        
        # Handle different timestamp column names
        time_col = "last_traded_time" if "last_traded_time" in df.columns else "timestamp"
        df["timestamp"] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        
        data_points: list[OrderBookDataPoint] = []
        for _, row in df.iterrows():
            try:
                # Parse bids and asks (may be string representation of list)
                bids = row.get("bids", None)
                asks = row.get("asks", None)
                
                # Convert string representation to list if needed
                import ast
                if isinstance(bids, str):
                    try:
                        bids = ast.literal_eval(bids)
                    except (ValueError, SyntaxError):
                        bids = None
                if isinstance(asks, str):
                    try:
                        asks = ast.literal_eval(asks)
                    except (ValueError, SyntaxError):
                        asks = None
                
                data_points.append(
                    OrderBookDataPoint(
                        symbol=symbol,
                        timestamp=row["timestamp"].to_pydatetime(),
                        total_buy_qty=int(row.get("total_buy_qty", 0)),
                        total_sell_qty=int(row.get("total_sell_qty", 0)),
                        last_traded_price=float(row.get("last_traded_price", 0)),
                        last_traded_qty=int(row.get("last_traded_qty", 0)),
                        volume=int(row.get("volume", 0)),
                        average_traded_price=float(row.get("average_traded_price", 0)),
                        lower_circuit=float(row.get("lower_circuit", 0)),
                        upper_circuit=float(row.get("upper_circuit", 0)),
                        change_percent=float(row.get("change_percent", 0)),
                        # Additional fields
                        bids=bids,
                        asks=asks,
                        open=float(row["open"]) if pd.notna(row.get("open")) else None,
                        high=float(row["high"]) if pd.notna(row.get("high")) else None,
                        low=float(row["low"]) if pd.notna(row.get("low")) else None,
                        close=float(row["close"]) if pd.notna(row.get("close")) else None,
                        tick_size=float(row["tick_size"]) if pd.notna(row.get("tick_size")) else None,
                        change=float(row["change"]) if pd.notna(row.get("change")) else None,
                        expiry=str(row["expiry"]) if pd.notna(row.get("expiry")) else None,
                        open_interest=int(row["open_interest"]) if pd.notna(row.get("open_interest")) else None,
                        open_interest_flag=bool(row["open_interest_flag"]) if pd.notna(row.get("open_interest_flag")) else None,
                        previous_day_open_interest=int(row["previous_day_open_interest"]) if pd.notna(row.get("previous_day_open_interest")) else None,
                        open_interest_percent=float(row["open_interest_percent"]) if pd.notna(row.get("open_interest_percent")) else None,
                    )
                )
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping invalid row in {file_path}: {e}")
                continue
                
        return data_points
        
    except Exception as e:
        logger.error(f"Failed to parse orderbook CSV {file_path}: {e}")
        return []


async def migrate_ticker_data(
    client: InfluxDBClient_Wrapper,
    ticker_dir: str,
    symbols: list[str],
    batch_size: int = 1000,
) -> tuple[int, int]:
    """
    Migrate ticker data from CSV files to InfluxDB.
    
    Args:
        client: InfluxDB client wrapper
        ticker_dir: Directory containing ticker CSV files
        symbols: List of symbols to migrate
        batch_size: Number of points per write batch
        
    Returns:
        Tuple of (successful_count, failed_count)
    """
    success_count = 0
    fail_count = 0
    
    for symbol in tqdm(symbols, desc="Migrating ticker data"):
        file_path = os.path.join(ticker_dir, f"{symbol}_ticker_data.csv")
        
        if not os.path.exists(file_path):
            logger.debug(f"Ticker file not found for {symbol}, skipping")
            continue
            
        data_points = parse_ticker_csv(file_path, symbol)
        
        if not data_points:
            fail_count += 1
            continue
            
        # Write in batches
        for i in range(0, len(data_points), batch_size):
            batch = data_points[i : i + batch_size]
            success = await client.write_ticker_batch(batch, bucket=DataBucket.TICKER_DATA)
            
            if not success:
                logger.warning(f"Failed to write batch for {symbol}")
                fail_count += 1
                break
        else:
            success_count += 1
            logger.info(f"Migrated {len(data_points)} ticker records for {symbol}")
            
    return success_count, fail_count


async def migrate_orderbook_data(
    client: InfluxDBClient_Wrapper,
    orderbook_dir: str,
    symbols: list[str],
    batch_size: int = 1000,
) -> tuple[int, int]:
    """
    Migrate order book data from CSV files to InfluxDB.
    
    Args:
        client: InfluxDB client wrapper
        orderbook_dir: Directory containing order book CSV files
        symbols: List of symbols to migrate
        batch_size: Number of points per write batch
        
    Returns:
        Tuple of (successful_count, failed_count)
    """
    success_count = 0
    fail_count = 0
    
    for symbol in tqdm(symbols, desc="Migrating orderbook data"):
        file_path = os.path.join(orderbook_dir, f"{symbol}_orderbook_data.csv")
        
        if not os.path.exists(file_path):
            logger.debug(f"Orderbook file not found for {symbol}, skipping")
            continue
            
        data_points = parse_orderbook_csv(file_path, symbol)
        
        if not data_points:
            fail_count += 1
            continue
            
        # Write in batches
        for i in range(0, len(data_points), batch_size):
            batch = data_points[i : i + batch_size]
            success = await client.write_orderbook_batch(batch, bucket=DataBucket.ORDER_BOOK)
            
            if not success:
                logger.warning(f"Failed to write batch for {symbol}")
                fail_count += 1
                break
        else:
            success_count += 1
            logger.info(f"Migrated {len(data_points)} orderbook records for {symbol}")
            
    return success_count, fail_count


async def main(
    ticker_dir: str,
    orderbook_dir: str,
    symbols_file: str,
    batch_size: int,
    skip_ticker: bool,
    skip_orderbook: bool,
) -> None:
    """Main migration function."""
    # Load environment variables
    load_dotenv()
    
    # Get InfluxDB config from environment
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        logger.error("INFLUXDB_TOKEN environment variable is required")
        sys.exit(1)
    
    config = InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
        batch_size=batch_size,
    )
    
    # Load symbols
    symbols = load_symbols(symbols_file)
    if not symbols:
        logger.error("No symbols loaded, exiting")
        sys.exit(1)
        
    logger.info(f"Loaded {len(symbols)} symbols for migration")
    
    async with InfluxDBClient_Wrapper.create(config) as client:
        if not client.is_connected:
            logger.error("Failed to connect to InfluxDB")
            sys.exit(1)
        
        results = {"ticker": (0, 0), "orderbook": (0, 0)}
        
        # Migrate ticker data
        if not skip_ticker:
            results["ticker"] = await migrate_ticker_data(
                client, ticker_dir, symbols, batch_size
            )
        
        # Migrate order book data
        if not skip_orderbook:
            results["orderbook"] = await migrate_orderbook_data(
                client, orderbook_dir, symbols, batch_size
            )
        
        # Print summary
        logger.info("=" * 50)
        logger.info("Migration Summary:")
        logger.info(f"  Ticker: {results['ticker'][0]} success, {results['ticker'][1]} failed")
        logger.info(f"  Orderbook: {results['orderbook'][0]} success, {results['orderbook'][1]} failed")
        logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate CSV data to InfluxDB")
    parser.add_argument(
        "--ticker-dir",
        default="backups/TickerData",
        help="Directory containing ticker CSV files",
    )
    parser.add_argument(
        "--orderbook-dir",
        default="backups/OrderBookData",
        help="Directory containing order book CSV files",
    )
    parser.add_argument(
        "--symbols-file",
        default="src/config/stock_symbols.txt",
        help="File containing stock symbols",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of points per write batch",
    )
    parser.add_argument(
        "--skip-ticker",
        action="store_true",
        help="Skip ticker data migration",
    )
    parser.add_argument(
        "--skip-orderbook",
        action="store_true",
        help="Skip order book data migration",
    )
    
    args = parser.parse_args()
    
    asyncio.run(
        main(
            ticker_dir=args.ticker_dir,
            orderbook_dir=args.orderbook_dir,
            symbols_file=args.symbols_file,
            batch_size=args.batch_size,
            skip_ticker=args.skip_ticker,
            skip_orderbook=args.skip_orderbook,
        )
    )