#!/usr/bin/env python3
"""
Company Metadata Management Script

This script provides command-line utilities for managing the company metadata cache:
- Fetch metadata for symbols
- Refresh stale entries
- Update cache with new symbols
- View cache statistics
- Export cache to CSV

Usage:
    python manage_company_metadata.py fetch --symbols RELIANCE TCS INFY
    python manage_company_metadata.py refresh-stale
    python manage_company_metadata.py update-from-config
    python manage_company_metadata.py stats
    python manage_company_metadata.py export --output metadata.csv
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional
from loguru import logger

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data.company_metadata import CompanyMetadataFetcher
from src import config
from src.utils.utils import load_symbols


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    log_level = "DEBUG" if verbose else "INFO"
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )


def fetch_metadata(symbols: List[str], force_refresh: bool = False, cache_dir: str = "./data/cache"):
    """
    Fetch metadata for specified symbols.
    
    Args:
        symbols: List of stock symbols
        force_refresh: Force refresh even if cache is valid
        cache_dir: Directory for cache storage
    """
    logger.info(f"Fetching metadata for {len(symbols)} symbols...")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    result_df = fetcher.fetch_multiple(symbols, force_refresh=force_refresh)
    
    logger.success(f"Successfully fetched metadata for {len(result_df)} symbols")
    
    # Display summary
    print("\n" + "="*80)
    print("METADATA FETCH SUMMARY")
    print("="*80)
    print(f"Total symbols processed: {len(result_df)}")
    print(f"\nData sources breakdown:")
    for source, count in result_df['data_source'].value_counts().items():
        print(f"  - {source}: {count}")
    
    print(f"\nSector distribution:")
    for sector, count in result_df['sector'].value_counts().head(10).items():
        print(f"  - {sector}: {count}")
    
    print("\n" + "="*80)


def refresh_stale(cache_dir: str = "./data/cache"):
    """
    Refresh all stale entries in the cache.
    
    Args:
        cache_dir: Directory for cache storage
    """
    logger.info("Refreshing stale metadata entries...")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    
    # Get stats before refresh
    stats_before = fetcher.get_cache_stats()
    stale_count = stats_before['stale_entries']
    
    if stale_count == 0:
        logger.info("No stale entries found. Cache is up to date.")
        return
    
    logger.info(f"Found {stale_count} stale entries to refresh")
    fetcher.refresh_stale_entries()
    
    # Get stats after refresh
    stats_after = fetcher.get_cache_stats()
    
    logger.success(f"Successfully refreshed {stale_count} stale entries")
    print("\n" + "="*80)
    print("REFRESH SUMMARY")
    print("="*80)
    print(f"Entries refreshed: {stale_count}")
    print(f"Total entries in cache: {stats_after['total_entries']}")
    print(f"Remaining stale entries: {stats_after['stale_entries']}")
    print("="*80)


def update_from_config(cache_dir: str = "./data/cache"):
    """
    Update cache with symbols from config file.
    
    Args:
        cache_dir: Directory for cache storage
    """
    logger.info("Updating metadata cache from config...")
    
    # Load symbols from config
    symbols = config.symbols
    
    if not symbols:
        logger.warning("No symbols found in config")
        return
    
    logger.info(f"Found {len(symbols)} symbols in config: {symbols}")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    fetcher.update_symbols(symbols)
    
    # Get final stats
    stats = fetcher.get_cache_stats()
    
    logger.success(f"Cache updated successfully")
    print("\n" + "="*80)
    print("UPDATE SUMMARY")
    print("="*80)
    print(f"Total entries in cache: {stats['total_entries']}")
    print(f"Stale entries: {stats['stale_entries']}")
    print("\nSector distribution:")
    for sector, count in list(stats['sectors'].items())[:10]:
        print(f"  - {sector}: {count}")
    print("="*80)


def show_stats(cache_dir: str = "./data/cache"):
    """
    Display cache statistics.
    
    Args:
        cache_dir: Directory for cache storage
    """
    logger.info("Retrieving cache statistics...")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    stats = fetcher.get_cache_stats()
    
    print("\n" + "="*80)
    print("CACHE STATISTICS")
    print("="*80)
    print(f"Cache file: {stats['cache_file_path']}")
    print(f"Total entries: {stats['total_entries']}")
    print(f"Stale entries: {stats['stale_entries']}")
    
    if stats['data_sources']:
        print("\nData sources:")
        for source, count in stats['data_sources'].items():
            print(f"  - {source}: {count}")
    
    if stats['sectors']:
        print("\nTop 10 sectors:")
        for sector, count in list(stats['sectors'].items())[:10]:
            print(f"  - {sector}: {count}")
    
    if stats['market_cap_distribution']:
        print("\nMarket cap distribution:")
        for bucket, count in stats['market_cap_distribution'].items():
            print(f"  - {bucket}: {count}")
    
    print("="*80)


def export_cache(output_path: Optional[str] = None, cache_dir: str = "./data/cache"):
    """
    Export cache to CSV.
    
    Args:
        output_path: Path for output CSV file
        cache_dir: Directory for cache storage
    """
    logger.info("Exporting cache to CSV...")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    
    if fetcher.metadata_df.empty:
        logger.warning("Cache is empty, nothing to export")
        return
    
    fetcher.export_to_csv(output_path)
    logger.success(f"Cache exported successfully to {output_path or 'default location'}")


def initialize_cache(cache_dir: str = "./data/cache"):
    """
    Initialize cache for the first time with all symbols from config.
    
    Args:
        cache_dir: Directory for cache storage
    """
    logger.info("Initializing metadata cache...")
    
    # Load symbols from config
    symbols = config.symbols
    
    if not symbols:
        logger.error("No symbols found in config file")
        return
    
    logger.info(f"Initializing cache with {len(symbols)} symbols from config")
    
    fetcher = CompanyMetadataFetcher(cache_dir=cache_dir)
    
    # Check if cache already exists
    if not fetcher.metadata_df.empty:
        logger.warning(f"Cache already exists with {len(fetcher.metadata_df)} entries")
        response = input("Do you want to fetch metadata anyway? (y/n): ")
        if response.lower() != 'y':
            logger.info("Initialization cancelled")
            return
    
    # Fetch all symbols
    result_df = fetcher.fetch_multiple(symbols, force_refresh=False)
    
    logger.success(f"Cache initialized with {len(result_df)} entries")
    
    # Show stats
    show_stats(cache_dir)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Manage company metadata cache",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize cache with symbols from config
  python manage_company_metadata.py init
  
  # Fetch metadata for specific symbols
  python manage_company_metadata.py fetch --symbols RELIANCE TCS INFY
  
  # Refresh stale entries
  python manage_company_metadata.py refresh-stale
  
  # Update cache with symbols from config
  python manage_company_metadata.py update-from-config
  
  # Show cache statistics
  python manage_company_metadata.py stats
  
  # Export cache to CSV
  python manage_company_metadata.py export --output metadata.csv
        """
    )
    
    parser.add_argument(
        'command',
        choices=['init', 'fetch', 'refresh-stale', 'update-from-config', 'stats', 'export'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        help='List of symbols (for fetch command)'
    )
    
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Force refresh even if cache is valid'
    )
    
    parser.add_argument(
        '--output',
        help='Output path for export command'
    )
    
    parser.add_argument(
        '--cache-dir',
        default='./data/cache',
        help='Directory for cache storage (default: ./data/cache)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    try:
        if args.command == 'init':
            initialize_cache(args.cache_dir)
        
        elif args.command == 'fetch':
            if not args.symbols:
                logger.error("--symbols required for fetch command")
                sys.exit(1)
            fetch_metadata(args.symbols, args.force_refresh, args.cache_dir)
        
        elif args.command == 'refresh-stale':
            refresh_stale(args.cache_dir)
        
        elif args.command == 'update-from-config':
            update_from_config(args.cache_dir)
        
        elif args.command == 'stats':
            show_stats(args.cache_dir)
        
        elif args.command == 'export':
            export_cache(args.output, args.cache_dir)
    
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        if args.verbose:
            logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()

