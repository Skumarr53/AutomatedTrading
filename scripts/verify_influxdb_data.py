#!/usr/bin/env python3
"""
Quick verification script to check if data exists in InfluxDB.

Usage:
    python scripts/verify_influxdb_data.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig,
)


async def verify_data():
    """Verify data exists in InfluxDB."""
    load_dotenv()
    
    # Get config from environment
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        logger.error("❌ INFLUXDB_TOKEN environment variable is required")
        logger.info("💡 Set it in .env file or export it")
        return False
    
    config = InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )
    
    logger.info("=" * 60)
    logger.info("InfluxDB Data Verification")
    logger.info("=" * 60)
    logger.info(f"URL: {influx_url}")
    logger.info(f"Org: {influx_org}")
    logger.info(f"Bucket: {DataBucket.TICKER_DATA.value}")
    logger.info("=" * 60)
    
    try:
        async with InfluxDBClient_Wrapper.create(config) as client:
            # Check connection
            if not client.is_connected:
                logger.error("❌ Failed to connect to InfluxDB")
                logger.info("💡 Check if InfluxDB is running: podman ps | grep influxdb")
                return False
            
            logger.info("✅ Connected to InfluxDB")
            
            # Check bucket exists
            try:
                bucket_names = await client.list_buckets()
                logger.info(f"📦 Available buckets: {bucket_names}")
                
                if DataBucket.TICKER_DATA.value not in bucket_names:
                    logger.warning(f"⚠️  Bucket '{DataBucket.TICKER_DATA.value}' not found!")
                    logger.info("💡 Buckets found: " + ", ".join(bucket_names) if bucket_names else "None")
                    logger.info("💡 The bucket should be created automatically on first write.")
                    logger.info("💡 Proceeding with data query to check if data exists...")
                    # Don't return False here - bucket might be created on first write
                else:
                    logger.info(f"✅ Bucket '{DataBucket.TICKER_DATA.value}' exists")
                
            except Exception as e:
                logger.warning(f"⚠️  Error listing buckets: {e}")
                logger.info("💡 Proceeding with data query anyway...")
                # Don't return False - bucket might exist but API call failed
            
            # Try to query data
            logger.info("\n🔍 Querying data...")
            
            # Query using the client's method
            try:
                # Try last 24 hours first
                df = await client.query_ticker_data("RELIANCE", hours=10000)
                
                if df is not None and len(df) > 0:
                    logger.info(f"✅ Found {len(df)} records for RELIANCE (last 24h)")
                    logger.info("\n📊 Sample data:")
                    logger.info(f"\n{df.head().to_string()}")
                    logger.info(f"\n📈 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                    return True
                else:
                    logger.warning("⚠️  No data found for RELIANCE in last 24 hours")
                    logger.info("💡 Trying to query more historical data (last 30 days)...")
                    
                    # Try querying more data (30 days = 720 hours)
                    df_all = await client.query_ticker_data("RELIANCE", hours=720)
                    if df_all is not None and len(df_all) > 0:
                        logger.info(f"✅ Found {len(df_all)} total records for RELIANCE (last 30 days)")
                        logger.info(f"📈 Date range: {df_all['timestamp'].min()} to {df_all['timestamp'].max()}")
                        logger.info("\n📊 Sample data:")
                        logger.info(f"\n{df_all.head().to_string()}")
                        return True
                    else:
                        logger.warning("⚠️  No data found in last 30 days, trying 1 year...")
                        # Try 1 year (365 * 24 = 8760 hours)
                        df_year = await client.query_ticker_data("RELIANCE", hours=8760)
                        if df_year is not None and len(df_year) > 0:
                            logger.info(f"✅ Found {len(df_year)} records for RELIANCE (last year)")
                            logger.info(f"📈 Date range: {df_year['timestamp'].min()} to {df_year['timestamp'].max()}")
                            return True
                        else:
                            logger.error("❌ No data found for RELIANCE at all")
                            logger.info("💡 Possible issues:")
                            logger.info("   1. Migration didn't complete successfully")
                            logger.info("   2. Wrong symbol name (check CSV files)")
                            logger.info("   3. Data written to different bucket")
                            logger.info("   4. Data exists but query syntax issue")
                            return False
                    
            except Exception as e:
                logger.error(f"❌ Error querying data: {e}")
                logger.info("💡 Trying direct InfluxDB query...")
                
                # Try direct query
                try:
                    from influxdb_client import InfluxDBClient
                    direct_client = InfluxDBClient(
                        url=influx_url,
                        token=influx_token,
                        org=influx_org
                    )
                    query_api = direct_client.query_api()
                    
                    # Flux query
                    query = f'''
                    from(bucket: "{DataBucket.TICKER_DATA.value}")
                      |> range(start: -300d)
                      |> filter(fn: (r) => r["_measurement"] == "ticker")
                      |> filter(fn: (r) => r["symbol"] == "RELIANCE")
                      |> limit(n: 10)
                    '''
                    
                    logger.info(f"🔍 Executing direct query...")
                    logger.debug(f"Query: {query}")
                    result = query_api.query(query)
                    
                    count = 0
                    sample_record = None
                    for table in result:
                        for record in table.records:
                            count += 1
                            if count == 1 and sample_record is None:
                                sample_record = record
                    
                    if count > 0:
                        logger.info(f"✅ Found {count} records with direct query!")
                        if sample_record:
                            logger.info(f"   Measurement: {sample_record.get_measurement()}")
                            logger.info(f"   Symbol tag: {sample_record.values.get('symbol')}")
                            logger.info(f"   Time: {sample_record.get_time()}")
                            logger.info(f"   Fields: {[k for k in sample_record.values.keys() if not k.startswith('_')]}")
                        return True
                    else:
                        logger.error("❌ No records found with direct query")
                        logger.info("💡 This suggests data might not have been written during migration")
                        logger.info("💡 Check migration logs for errors")
                        logger.info("💡 Try this query in InfluxDB UI Data Explorer:")
                        logger.info(f"\n{query}")
                        
                        # Try a simpler query to see if ANY data exists
                        logger.info("\n💡 Trying simpler query to check if ANY ticker data exists...")
                        simple_query = f'''
                        from(bucket: "{DataBucket.TICKER_DATA.value}")
                          |> range(start: -365d)
                          |> filter(fn: (r) => r["_measurement"] == "ticker")
                          |> limit(n: 1)
                        '''
                        simple_result = query_api.query(simple_query)
                        simple_count = sum(1 for table in simple_result for _ in table.records)
                        if simple_count > 0:
                            logger.info(f"✅ Found ticker data exists! ({simple_count} sample record)")
                            logger.info("💡 Issue might be with symbol name - check what symbols exist:")
                            logger.info("   Run: python scripts/verify_influxdb_data.py --all-symbols")
                        else:
                            logger.error("❌ No ticker data found at all in bucket")
                            logger.info("💡 Migration likely didn't write data successfully")
                        return False
                        
                except Exception as e2:
                    logger.error(f"❌ Direct query also failed: {e2}")
                    return False
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


async def check_all_symbols():
    """Check data for all symbols."""
    load_dotenv()
    
    influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    influx_token = os.getenv("INFLUXDB_TOKEN")
    influx_org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not influx_token:
        logger.error("❌ INFLUXDB_TOKEN not set")
        return
    
    config = InfluxDBConfig(
        url=influx_url,
        token=influx_token,
        org=influx_org,
    )
    
    # Load symbols from config
    try:
        from src import config as app_config
        symbols = app_config.symbols[:10]  # Check first 10 symbols
    except:
        symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
    
    logger.info(f"\n🔍 Checking data for {len(symbols)} symbols...\n")
    
    async with InfluxDBClient_Wrapper.create(config) as client:
        if not client.is_connected:
            logger.error("❌ Not connected to InfluxDB")
            return
        
        results = {}
        for symbol in symbols:
            try:
                df = await client.query_ticker_data(symbol, days=7)
                count = len(df) if df is not None else 0
                results[symbol] = count
                status = "✅" if count > 0 else "❌"
                logger.info(f"{status} {symbol}: {count} records")
            except Exception as e:
                results[symbol] = -1
                logger.error(f"❌ {symbol}: Error - {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info("Summary:")
        total = sum(v for v in results.values() if v > 0)
        found = sum(1 for v in results.values() if v > 0)
        logger.info(f"  Symbols with data: {found}/{len(symbols)}")
        logger.info(f"  Total records: {total}")
        logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify InfluxDB data")
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Check all symbols instead of just RELIANCE"
    )
    
    args = parser.parse_args()
    
    if args.all_symbols:
        asyncio.run(check_all_symbols())
    else:
        success = asyncio.run(verify_data())
        sys.exit(0 if success else 1)
