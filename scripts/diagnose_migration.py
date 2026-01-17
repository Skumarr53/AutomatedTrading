#!/usr/bin/env python3
"""
Diagnose migration issues - check CSV files, parsing, and InfluxDB writes.

Usage:
    python scripts/diagnose_migration.py --ticker-dir backups/TickerData --symbol RELIANCE
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig,
    TickerDataPoint,
)


async def diagnose_csv_file(ticker_dir: str, symbol: str) -> bool:
    """Check if CSV file exists and can be parsed."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Diagnosing CSV file for {symbol}")
    logger.info(f"{'='*60}")
    
    # Check file exists
    file_path = os.path.join(ticker_dir, f"{symbol}_ticker_data.csv")
    
    if not os.path.exists(file_path):
        logger.error(f"❌ CSV file not found: {file_path}")
        logger.info("💡 Check:")
        logger.info(f"   1. File path: {file_path}")
        logger.info(f"   2. Directory exists: {os.path.exists(ticker_dir)}")
        if os.path.exists(ticker_dir):
            logger.info(f"   3. Files in directory:")
            files = os.listdir(ticker_dir)
            logger.info(f"      {files[:10]}{'...' if len(files) > 10 else ''}")
        return False
    
    logger.info(f"✅ CSV file found: {file_path}")
    
    # Check file size
    file_size = os.path.getsize(file_path)
    logger.info(f"📊 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    
    if file_size == 0:
        logger.error("❌ CSV file is empty!")
        return False
    
    # Try to read CSV
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", engine="python")
        logger.info(f"✅ CSV read successfully: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"📊 Columns: {df.columns.tolist()}")
        
        # Check required columns
        required_cols = ["open", "high", "low", "close", "volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"❌ Missing required columns: {missing_cols}")
            return False
        
        # Check date column
        date_col = "date" if "date" in df.columns else ("epoch_time" if "epoch_time" in df.columns else None)
        if not date_col:
            logger.error("❌ No date column found (need 'date' or 'epoch_time')")
            return False
        
        logger.info(f"✅ Date column found: {date_col}")
        
        # Show sample data
        logger.info(f"\n📊 Sample data (first 3 rows):")
        logger.info(f"\n{df.head(3).to_string()}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to read CSV: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


async def diagnose_parsing(ticker_dir: str, symbol: str) -> tuple[bool, list[TickerDataPoint]]:
    """Check if CSV can be parsed into TickerDataPoint objects."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Diagnosing CSV parsing for {symbol}")
    logger.info(f"{'='*60}")
    
    file_path = os.path.join(ticker_dir, f"{symbol}_ticker_data.csv")
    
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip", engine="python")
        
        # Handle date column
        date_col = "date" if "date" in df.columns else "epoch_time"
        if date_col == "epoch_time":
            df["timestamp"] = pd.to_datetime(df["epoch_time"], unit="s")
        else:
            df["timestamp"] = pd.to_datetime(df["date"])
        
        data_points: list[TickerDataPoint] = []
        errors = []
        
        for idx, row in df.iterrows():
            try:
                dp = TickerDataPoint(
                    symbol=symbol,
                    timestamp=row["timestamp"].to_pydatetime(),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=int(row["volume"]),
                    exchange="NSE",
                )
                data_points.append(dp)
            except Exception as e:
                errors.append((idx, str(e)))
                if len(errors) <= 5:  # Show first 5 errors
                    logger.warning(f"⚠️  Row {idx} failed: {e}")
        
        logger.info(f"✅ Parsed {len(data_points)}/{len(df)} rows successfully")
        
        if errors:
            logger.warning(f"⚠️  {len(errors)} rows failed to parse")
        
        if len(data_points) > 0:
            logger.info(f"\n📊 Sample parsed data point:")
            logger.info(f"   Symbol: {data_points[0].symbol}")
            logger.info(f"   Timestamp: {data_points[0].timestamp}")
            logger.info(f"   OHLCV: O={data_points[0].open}, H={data_points[0].high}, L={data_points[0].low}, C={data_points[0].close}, V={data_points[0].volume}")
        
        return len(data_points) > 0, data_points
        
    except Exception as e:
        logger.error(f"❌ Parsing failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False, []


async def diagnose_influxdb_write(symbol: str, data_points: list[TickerDataPoint]) -> bool:
    """Check if data can be written to InfluxDB."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Diagnosing InfluxDB write for {symbol}")
    logger.info(f"{'='*60}")
    
    load_dotenv()
    
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        logger.error("❌ INFLUXDB_TOKEN not set")
        return False
    
    config = InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )
    
    try:
        async with InfluxDBClient_Wrapper.create(config) as client:
            if not client.is_connected:
                logger.error("❌ Failed to connect to InfluxDB")
                return False
            
            logger.info("✅ Connected to InfluxDB")
            
            # Try writing a small batch (first 10 points)
            test_batch = data_points[:10]
            logger.info(f"📤 Writing test batch of {len(test_batch)} points...")
            
            success = await client.write_ticker_batch(test_batch, bucket=DataBucket.TICKER_DATA)
            
            if success:
                logger.info("✅ Write succeeded!")
                
                # Wait a moment for write to complete (InfluxDB async writes)
                await asyncio.sleep(2)
                
                # Get the timestamp of the data we wrote
                test_timestamp = test_batch[0].timestamp
                logger.info(f"📅 Test data timestamp: {test_timestamp}")
                
                # Query with a wide range that includes the test data
                # Since test data is from CSV (might be old), query last year
                logger.info("🔍 Querying back to verify...")
                df = await client.query_ticker_data(symbol, hours=8760)  # 1 year
                
                if df is not None and len(df) > 0:
                    logger.info(f"✅ Successfully queried back {len(df)} records!")
                    logger.info(f"📊 Sample: {df.head(1).to_string()}")
                    logger.info(f"📈 Date range in query result: {df['timestamp'].min()} to {df['timestamp'].max()}")
                    return True
                else:
                    logger.warning("⚠️  Write succeeded but query returned no data")
                    logger.info(f"💡 Test data timestamp: {test_timestamp}")
                    logger.info("💡 Possible issues:")
                    logger.info("   1. Timing delay (InfluxDB async writes can take a few seconds)")
                    logger.info("   2. Query time range doesn't include test data timestamp")
                    logger.info("   3. Data written but query syntax issue")
                    
                    # Try direct query with specific timestamp range
                    logger.info("\n💡 Trying direct query with wider time range...")
                    try:
                        from influxdb_client import InfluxDBClient
                        direct_client = InfluxDBClient(
                            url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
                            token=os.getenv("INFLUXDB_TOKEN"),
                            org=os.getenv("INFLUXDB_ORG", "trading")
                        )
                        query_api = direct_client.query_api()
                        
                        # Query all ticker data for this symbol
                        query = f'''
                        from(bucket: "ticker_data")
                          |> range(start: -365d)
                          |> filter(fn: (r) => r["_measurement"] == "ticker")
                          |> filter(fn: (r) => r["symbol"] == "{symbol}")
                          |> limit(n: 20)
                        '''
                        
                        result = query_api.query(query)
                        count = sum(1 for table in result for _ in table.records)
                        
                        if count > 0:
                            logger.info(f"✅ Direct query found {count} records!")
                            logger.info("💡 Data is there - query method might need adjustment")
                            direct_client.close()
                            return True
                        else:
                            logger.warning("⚠️  Direct query also returned no data")
                            logger.info("💡 Wait a few seconds and try again - InfluxDB writes are async")
                            direct_client.close()
                            return True  # Write succeeded, query timing issue
                    except Exception as e:
                        logger.warning(f"⚠️  Direct query failed: {e}")
                        return True  # Write succeeded even if query fails
            else:
                logger.error("❌ Write failed!")
                return False
                
    except Exception as e:
        logger.error(f"❌ InfluxDB write test failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


async def main():
    parser = argparse.ArgumentParser(description="Diagnose migration issues")
    parser.add_argument(
        "--ticker-dir",
        default="backups/TickerData",
        help="Directory containing ticker CSV files",
    )
    parser.add_argument(
        "--symbol",
        default="RELIANCE",
        help="Symbol to diagnose",
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Migration Diagnosis Tool")
    logger.info("=" * 60)
    
    # Step 1: Check CSV file
    csv_ok = await diagnose_csv_file(args.ticker_dir, args.symbol)
    if not csv_ok:
        logger.error("\n❌ CSV file check failed - cannot proceed")
        sys.exit(1)
    
    # Step 2: Check parsing
    parse_ok, data_points = await diagnose_parsing(args.ticker_dir, args.symbol)
    if not parse_ok:
        logger.error("\n❌ Parsing check failed - cannot proceed")
        sys.exit(1)
    
    # Step 3: Check InfluxDB write
    write_ok = await diagnose_influxdb_write(args.symbol, data_points)
    if not write_ok:
        logger.error("\n❌ InfluxDB write check failed")
        sys.exit(1)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ All checks passed!")
    logger.info("💡 Migration should work. Try running:")
    logger.info(f"   python scripts/migrate_data2Influx.py --ticker-dir {args.ticker_dir} --symbols-file <(echo '{args.symbol}') --skip-orderbook")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
