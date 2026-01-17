#!/usr/bin/env python3
"""
Inspect what's actually in InfluxDB - useful for debugging migration issues.

Usage:
    python scripts/inspect_influxdb.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()


def inspect_influxdb():
    """Inspect InfluxDB contents."""
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN")
    org = os.getenv("INFLUXDB_ORG", "trading")
    
    if not token:
        logger.error("❌ INFLUXDB_TOKEN not set")
        return
    
    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()
    
    logger.info("=" * 60)
    logger.info("InfluxDB Inspection")
    logger.info("=" * 60)
    
    # Check all measurements
    logger.info("\n📊 Checking measurements in ticker_data bucket...")
    query = '''
    from(bucket: "ticker_data")
      |> range(start: -365d)
      |> group(columns: ["_measurement"])
      |> distinct(column: "_measurement")
    '''
    
    try:
        result = query_api.query(query)
        measurements = []
        for table in result:
            for record in table.records:
                measurements.append(record.get_value())
        
        if measurements:
            logger.info(f"✅ Measurements found: {measurements}")
        else:
            logger.warning("⚠️  No measurements found")
    except Exception as e:
        logger.error(f"❌ Error querying measurements: {e}")
    
    # Check all symbols
    logger.info("\n📊 Checking symbols in ticker_data bucket...")
    query = '''
    from(bucket: "ticker_data")
      |> range(start: -365d)
      |> filter(fn: (r) => r["_measurement"] == "ticker")
      |> group(columns: ["symbol"])
      |> distinct(column: "symbol")
    '''
    
    try:
        result = query_api.query(query)
        symbols = []
        for table in result:
            for record in table.records:
                symbol = record.values.get("symbol")
                if symbol:
                    symbols.append(symbol)
        
        if symbols:
            logger.info(f"✅ Symbols found ({len(symbols)}): {symbols[:10]}{'...' if len(symbols) > 10 else ''}")
        else:
            logger.warning("⚠️  No symbols found")
    except Exception as e:
        logger.error(f"❌ Error querying symbols: {e}")
    
    # Check data count
    logger.info("\n📊 Checking data count...")
    query = '''
    from(bucket: "ticker_data")
      |> range(start: -365d)
      |> filter(fn: (r) => r["_measurement"] == "ticker")
      |> count()
    '''
    
    try:
        result = query_api.query(query)
        total_count = 0
        for table in result:
            for record in table.records:
                total_count = record.get_value()
        
        if total_count > 0:
            logger.info(f"✅ Total records: {total_count}")
        else:
            logger.warning("⚠️  No records found")
    except Exception as e:
        logger.error(f"❌ Error counting records: {e}")
    
    # Check sample record
    logger.info("\n📊 Checking sample record...")
    query = '''
    from(bucket: "ticker_data")
      |> range(start: -365d)
      |> filter(fn: (r) => r["_measurement"] == "ticker")
      |> limit(n: 1)
    '''
    
    try:
        result = query_api.query(query)
        for table in result:
            for record in table.records:
                logger.info("✅ Sample record found:")
                logger.info(f"   Measurement: {record.get_measurement()}")
                logger.info(f"   Time: {record.get_time()}")
                logger.info(f"   Field: {record.get_field()}")
                logger.info(f"   Value: {record.get_value()}")
                logger.info(f"   All values: {record.values}")
                break
        else:
            logger.warning("⚠️  No sample record found")
    except Exception as e:
        logger.error(f"❌ Error getting sample: {e}")
    
    client.close()
    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    inspect_influxdb()
