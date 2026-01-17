#!/usr/bin/env python3
"""
Script to delete existing InfluxDB data to remove duplicates.

Usage:
    python scripts/delete_influxdb_data.py [--bucket BUCKET_NAME] [--all]
    
Options:
    --bucket: Specific bucket to delete (default: ticker_data)
    --all: Delete all buckets (ticker_data, order_book, trades, system_metrics)
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
from loguru import logger
from src.utils.influx_client import (
    DataBucket,
    InfluxDBClient_Wrapper,
    InfluxDBConfig
)

from dotenv import load_dotenv
load_dotenv()

async def delete_bucket_data(bucket: DataBucket) -> bool:
    """Delete all data from a specific bucket."""
    # Get config from environment variables or use defaults
    influx_config = InfluxDBConfig(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "dK9BCc_QTsnH38kLbtSEJ_HkuRvC_DQEB8QGRqVACTW6yjKMQ4KIYtIdJSTjPZ2U96SeTYAb-eIBQbnzinxm2w=="),
        org=os.getenv("INFLUXDB_ORG", "trading")
    )
    
    if not influx_config.token:
        logger.error("INFLUXDB_TOKEN environment variable is required")
        return False
    
    async with InfluxDBClient_Wrapper.create(influx_config) as client:
        if not client.is_connected:
            logger.error("Failed to connect to InfluxDB")
            return False
        
        logger.info(f"Deleting all data from bucket: {bucket.value}")
        success = await client.delete_data(
            bucket=bucket,
            predicate=None  # Delete all data
        )
        
        if success:
            logger.success(f"Successfully deleted data from {bucket.value}")
        else:
            logger.error(f"Failed to delete data from {bucket.value}")
        
        return success


async def main():
    """Main function to delete InfluxDB data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Delete InfluxDB data")
    parser.add_argument(
        "--bucket",
        choices=["ticker_data", "order_book", "trades", "system_metrics"],
        default="ticker_data",
        help="Bucket to delete (default: ticker_data)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Delete all buckets"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt"
    )
    
    args = parser.parse_args()
    
    if args.all:
        buckets = [
            DataBucket.TICKER_DATA,
            DataBucket.ORDER_BOOK,
            DataBucket.TRADES,
            DataBucket.METRICS
        ]
        logger.warning("⚠️  This will delete ALL data from ALL buckets!")
    else:
        bucket_map = {
            "ticker_data": DataBucket.TICKER_DATA,
            "order_book": DataBucket.ORDER_BOOK,
            "trades": DataBucket.TRADES,
            "system_metrics": DataBucket.METRICS
        }
        buckets = [bucket_map[args.bucket]]
        logger.warning(f"⚠️  This will delete ALL data from bucket: {args.bucket}")
    
    if not args.confirm:
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Deletion cancelled")
            return
    
    logger.info("Starting data deletion...")
    
    success_count = 0
    for bucket in buckets:
        if await delete_bucket_data(bucket):
            success_count += 1
    
    if success_count == len(buckets):
        logger.success(f"✅ Successfully deleted data from {success_count} bucket(s)")
    else:
        logger.error(f"❌ Failed to delete data from {len(buckets) - success_count} bucket(s)")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
