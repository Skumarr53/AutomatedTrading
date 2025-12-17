"""
Company Metadata Fetcher and Cache Manager

This module provides functionality to fetch, cache, and manage company-specific metadata
such as sector, industry, market cap, country, etc. The data is cached locally to minimize
API calls and improve performance.

Supported data sources:
- yfinance (primary)
- Fallback to NSE India data where applicable
"""

import os
import json
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from loguru import logger
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class CompanyMetadataFetcher:
    """
    Fetches and caches company metadata from various sources.
    
    Attributes:
        cache_file (Path): Path to the parquet cache file
        metadata_df (pd.DataFrame): In-memory cache of company metadata
        cache_expiry_days (int): Number of days before cache is considered stale
    """
    
    # Market cap buckets (in INR crores)
    MARKET_CAP_BUCKETS = {
        'micro': (0, 500),
        'small': (500, 5000),
        'mid': (5000, 20000),
        'large': (20000, 50000),
        'mega': (50000, float('inf'))
    }
    
    def __init__(
        self, 
        cache_dir: str = "./data/cache",
        cache_file_name: str = "company_metadata.parquet",
        cache_expiry_days: int = 30
    ):
        """
        Initialize the metadata fetcher.
        
        Args:
            cache_dir: Directory to store the cache file
            cache_file_name: Name of the cache file
            cache_expiry_days: Number of days before cache entries are considered stale
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / cache_file_name
        self.cache_expiry_days = cache_expiry_days
        self.metadata_df: Optional[pd.DataFrame] = None
        
        # Load existing cache
        self._load_cache()
    
    def _load_cache(self) -> None:
        """Load the metadata cache from disk if it exists."""
        if self.cache_file.exists():
            try:
                self.metadata_df = pd.read_parquet(self.cache_file)
                logger.info(f"Loaded company metadata cache with {len(self.metadata_df)} entries")
            except Exception as e:
                logger.error(f"Failed to load cache file: {e}")
                self.metadata_df = self._create_empty_dataframe()
        else:
            logger.info("No existing cache found, creating new cache")
            self.metadata_df = self._create_empty_dataframe()
    
    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the expected schema."""
        return pd.DataFrame(columns=[
            'symbol',
            'company_name',
            'sector',
            'industry',
            'sub_industry',
            'country',
            'market_cap',
            'market_cap_bucket',
            'listing_date',
            'exchange',
            'currency',
            'employees',
            'website',
            'business_summary',
            'last_updated',
            'data_source'
        ])
    
    def _save_cache(self) -> None:
        """Save the current metadata cache to disk."""
        try:
            self.metadata_df.to_parquet(self.cache_file, index=False)
            logger.info(f"Saved metadata cache to {self.cache_file}")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def _is_stale(self, last_updated: str) -> bool:
        """
        Check if a cache entry is stale based on the last_updated timestamp.
        
        Args:
            last_updated: ISO format timestamp string
            
        Returns:
            True if the entry is stale, False otherwise
        """
        try:
            last_update_date = datetime.fromisoformat(last_updated)
            return (datetime.now() - last_update_date).days > self.cache_expiry_days
        except (ValueError, TypeError):
            return True
    
    def _determine_market_cap_bucket(self, market_cap: float) -> str:
        """
        Determine the market cap bucket for a given market cap value.
        
        Args:
            market_cap: Market capitalization in INR crores
            
        Returns:
            Market cap bucket name
        """
        if pd.isna(market_cap):
            return 'unknown'
        
        for bucket_name, (lower, upper) in self.MARKET_CAP_BUCKETS.items():
            if lower <= market_cap < upper:
                return bucket_name
        
        return 'unknown'
    
    def _fetch_from_yfinance(self, symbol: str) -> Optional[Dict]:
        """
        Fetch company metadata from yfinance.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Dictionary containing company metadata or None if fetch fails
        """
        try:
            # For NSE stocks, yfinance expects .NS suffix
            yf_symbol = f"{symbol}.NS" if not symbol.endswith('.NS') else symbol
            ticker = yf.Ticker(yf_symbol)
            info = ticker.info
            
            if not info or 'symbol' not in info:
                logger.warning(f"No data found for {symbol} from yfinance")
                return None
            
            # Convert market cap from USD to INR crores (approximate conversion)
            market_cap_usd = info.get('marketCap', None)
            market_cap_inr_crores = None
            if market_cap_usd:
                # Approximate: 1 USD = 83 INR, 1 crore = 10 million
                market_cap_inr_crores = (market_cap_usd * 83) / 10000000
            
            metadata = {
                'symbol': symbol,
                'company_name': info.get('longName') or info.get('shortName', 'Unknown'),
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'sub_industry': info.get('industryDisp', info.get('industry', 'Unknown')),
                'country': info.get('country', 'India'),
                'market_cap': market_cap_inr_crores,
                'market_cap_bucket': self._determine_market_cap_bucket(market_cap_inr_crores),
                'listing_date': self._parse_listing_date(info.get('firstTradeDateEpochUtc')),
                'exchange': info.get('exchange', 'NSE'),
                'currency': info.get('currency', 'INR'),
                'employees': info.get('fullTimeEmployees'),
                'website': info.get('website'),
                'business_summary': info.get('longBusinessSummary', ''),
                'last_updated': datetime.now().isoformat(),
                'data_source': 'yfinance'
            }
            
            logger.info(f"Successfully fetched metadata for {symbol} from yfinance")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to fetch metadata for {symbol} from yfinance: {e}")
            return None
    
    def _parse_listing_date(self, epoch_timestamp: Optional[int]) -> Optional[str]:
        """
        Parse listing date from epoch timestamp.
        
        Args:
            epoch_timestamp: Unix epoch timestamp
            
        Returns:
            ISO format date string or None
        """
        if epoch_timestamp:
            try:
                return datetime.fromtimestamp(epoch_timestamp).date().isoformat()
            except (ValueError, OSError):
                return None
        return None
    
    def _fetch_fallback(self, symbol: str) -> Optional[Dict]:
        """
        Fallback method to create basic metadata when API fetch fails.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Dictionary with minimal metadata
        """
        logger.warning(f"Using fallback metadata for {symbol}")
        return {
            'symbol': symbol,
            'company_name': symbol,
            'sector': 'Unknown',
            'industry': 'Unknown',
            'sub_industry': 'Unknown',
            'country': 'India',
            'market_cap': None,
            'market_cap_bucket': 'unknown',
            'listing_date': None,
            'exchange': 'NSE',
            'currency': 'INR',
            'employees': None,
            'website': None,
            'business_summary': '',
            'last_updated': datetime.now().isoformat(),
            'data_source': 'fallback'
        }
    
    def fetch_metadata(self, symbol: str, force_refresh: bool = False) -> Dict:
        """
        Fetch metadata for a single symbol, using cache if available.
        
        Args:
            symbol: Stock ticker symbol
            force_refresh: Force refresh even if cache is valid
            
        Returns:
            Dictionary containing company metadata
        """
        # Check if symbol exists in cache and is not stale
        if not force_refresh and symbol in self.metadata_df['symbol'].values:
            row = self.metadata_df[self.metadata_df['symbol'] == symbol].iloc[0]
            if not self._is_stale(row['last_updated']):
                logger.debug(f"Using cached metadata for {symbol}")
                return row.to_dict()
            else:
                logger.info(f"Cache for {symbol} is stale, refreshing...")
        
        # Fetch fresh data
        metadata = self._fetch_from_yfinance(symbol)
        
        # Use fallback if fetch failed
        if metadata is None:
            metadata = self._fetch_fallback(symbol)
        
        # Update cache
        self._update_cache_entry(metadata)
        
        return metadata
    
    def fetch_multiple(
        self, 
        symbols: List[str], 
        force_refresh: bool = False,
        max_workers: int = 5
    ) -> pd.DataFrame:
        """
        Fetch metadata for multiple symbols in parallel.
        
        Args:
            symbols: List of stock ticker symbols
            force_refresh: Force refresh even if cache is valid
            max_workers: Maximum number of parallel workers
            
        Returns:
            DataFrame containing metadata for all symbols
        """
        logger.info(f"Fetching metadata for {len(symbols)} symbols...")
        
        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(self.fetch_metadata, symbol, force_refresh): symbol
                for symbol in symbols
            }
            
            # Collect results as they complete
            results = []
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    metadata = future.result()
                    results.append(metadata)
                except Exception as e:
                    logger.error(f"Error fetching metadata for {symbol}: {e}")
                    # Add fallback for failed symbols
                    results.append(self._fetch_fallback(symbol))
        
        logger.info(f"Completed fetching metadata for {len(results)} symbols")
        return pd.DataFrame(results)
    
    def _update_cache_entry(self, metadata: Dict) -> None:
        """
        Update or insert a single cache entry.
        
        Args:
            metadata: Dictionary containing company metadata
        """
        symbol = metadata['symbol']
        
        # Remove existing entry if present
        self.metadata_df = self.metadata_df[self.metadata_df['symbol'] != symbol]
        
        # Add new entry
        new_row = pd.DataFrame([metadata])
        self.metadata_df = pd.concat([self.metadata_df, new_row], ignore_index=True)
        
        # Save to disk
        self._save_cache()
    
    def get_cached_metadata(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get cached metadata for specified symbols or all symbols.
        
        Args:
            symbols: List of symbols to retrieve, or None for all
            
        Returns:
            DataFrame containing cached metadata
        """
        if symbols is None:
            return self.metadata_df.copy()
        
        return self.metadata_df[self.metadata_df['symbol'].isin(symbols)].copy()
    
    def refresh_stale_entries(self, max_workers: int = 5) -> None:
        """
        Refresh all stale entries in the cache.
        
        Args:
            max_workers: Maximum number of parallel workers
        """
        if self.metadata_df.empty:
            logger.info("No cache entries to refresh")
            return
        
        # Find stale entries
        stale_symbols = []
        for _, row in self.metadata_df.iterrows():
            if self._is_stale(row['last_updated']):
                stale_symbols.append(row['symbol'])
        
        if stale_symbols:
            logger.info(f"Refreshing {len(stale_symbols)} stale entries")
            self.fetch_multiple(stale_symbols, force_refresh=True, max_workers=max_workers)
        else:
            logger.info("No stale entries found")
    
    def update_symbols(self, current_symbols: List[str], max_workers: int = 5) -> None:
        """
        Update cache to match current symbol list (add new, optionally remove old).
        
        Args:
            current_symbols: Current list of symbols in use
            max_workers: Maximum number of parallel workers
        """
        cached_symbols = set(self.metadata_df['symbol'].tolist())
        current_symbols_set = set(current_symbols)
        
        # Find new symbols
        new_symbols = list(current_symbols_set - cached_symbols)
        
        if new_symbols:
            logger.info(f"Found {len(new_symbols)} new symbols to add: {new_symbols}")
            self.fetch_multiple(new_symbols, force_refresh=False, max_workers=max_workers)
        else:
            logger.info("No new symbols to add")
        
        # Optionally remove symbols no longer in use
        # Commented out to keep historical data
        # removed_symbols = cached_symbols - current_symbols_set
        # if removed_symbols:
        #     logger.info(f"Removing {len(removed_symbols)} old symbols")
        #     self.metadata_df = self.metadata_df[self.metadata_df['symbol'].isin(current_symbols)]
        #     self._save_cache()
    
    def get_cache_stats(self) -> Dict:
        """
        Get statistics about the current cache.
        
        Returns:
            Dictionary with cache statistics
        """
        if self.metadata_df.empty:
            return {
                'total_entries': 0,
                'stale_entries': 0,
                'data_sources': {},
                'sectors': {},
                'market_cap_distribution': {}
            }
        
        stale_count = sum(
            self._is_stale(row['last_updated']) 
            for _, row in self.metadata_df.iterrows()
        )
        
        return {
            'total_entries': len(self.metadata_df),
            'stale_entries': stale_count,
            'data_sources': self.metadata_df['data_source'].value_counts().to_dict(),
            'sectors': self.metadata_df['sector'].value_counts().head(10).to_dict(),
            'market_cap_distribution': self.metadata_df['market_cap_bucket'].value_counts().to_dict(),
            'cache_file_path': str(self.cache_file)
        }
    
    def export_to_csv(self, output_path: Optional[str] = None) -> None:
        """
        Export the cache to CSV format for inspection.
        
        Args:
            output_path: Path to save CSV file, defaults to cache directory
        """
        if output_path is None:
            output_path = str(self.cache_dir / "company_metadata.csv")
        
        self.metadata_df.to_csv(output_path, index=False)
        logger.info(f"Exported metadata to {output_path}")

