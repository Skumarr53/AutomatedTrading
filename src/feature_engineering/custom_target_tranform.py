# src/preprocessing/custom_target_transform.py

from typing import Optional
import re
import warnings
import numpy as np
import pandas as pd
from functools import partial
from joblib import Memory
from loguru import logger
from src import config

# Suppress pandas warnings in this module
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

cache_dir = './pipeline_cache'
memory = Memory(location=cache_dir, verbose=0)

class TargetTransform:
    """
    FeatureExtractor encapsulates the logic for target transformations, including 
    categorizing ATR and percent changes based on statistical thresholds.
    
    IMPORTANT: To prevent target leakage, mu/sigma for categorization thresholds
    are computed ONLY on the training portion of data (controlled by train_ratio).
    This ensures test data categories are determined using thresholds that don't
    include future information.
    """
    
    def __init__(self, train_ratio: float = 0.8) -> None:
        """
        Initializes the FeatureExtractor instance.
        
        Args:
            train_ratio: Proportion of data to use for computing statistics (mu, sigma).
                        This should match the train/test split ratio to prevent leakage.
                        Default is 0.8 (80% train, 20% test).
        """
        self.interval_min = config.scheduler.data_fetch_cron_interval_min
        self.train_ratio = train_ratio
        # Cache for computed statistics to ensure consistency
        self._stats_cache: dict = {}


    @staticmethod
    def drop_nulls(df: pd.DataFrame, categories: pd.Series) -> tuple:
        """
        Drops rows from the DataFrame and corresponding null values from the Series based on null indices in the Series.

        Parameters:
        - df (pd.DataFrame): The DataFrame from which to drop rows.
        - categories (pd.Series): The Series from which to drop null values.

        Returns:
        - tuple: A tuple containing the cleaned DataFrame and the cleaned Series.
        """
        # Step 1: Identify null indices and drop them from the DataFrame and Series in one go
        null_indices = categories.index[categories.isnull()]

        # Step 2: Drop the rows from the DataFrame and clean the Series
        df_cleaned = df.drop(index=null_indices)
        categories_cleaned = categories.dropna()

        return df_cleaned, categories_cleaned


    @staticmethod
    def extract_window_size(run_id: str) -> int:
        # Extract the number from the string
        match = re.match(r'(\d+)(min|h)', run_id)
        if not match:
            raise ValueError(f"Invalid time string format: {run_id}")
        
        value, unit = match.groups()
        window_size = int(value)
        
        # Convert hours to minutes if necessary
        if unit == 'h':
            window_size *= 60
        
        # Ensure the result is a multiple of 5
        if window_size % 5 != 0:
            window_size += (5 - window_size % 5)
        
        return window_size
    
    def fill_missing_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing dates in the DataFrame with NaN values.

        Args:
            df (pd.DataFrame): The DataFrame to fill missing dates in.
        
        Returns:
            pd.DataFrame: DataFrame with filled timestamps.
        """
        min_date = df.index.min()
        max_date = df.index.max()

        # Create a date range with a 5-minute frequency (use 'min' instead of deprecated 'T')
        complete_date_range = pd.date_range(start=min_date, end=max_date, freq=f'{self.interval_min}min')

        df_filled = df.reindex(complete_date_range)

        df_filled.index = pd.to_datetime(df_filled.index)
        return df_filled

    def _categorize(self, mu: float, sigma: float, value: float) -> Optional[str]:
        """
        Categorizes a given value based on its distance from the mean (`mu`) in terms of standard deviations (`sigma`).

        The function divides the values into five categories:
        - 'High': Values greater than `mu + 1.5 * sigma`.
        - 'Medium High': Values between `mu + 0.5 * sigma` and `mu + 1.5 * sigma`.
        - 'Neutral': Values between `mu - 0.5 * sigma` and `mu + 0.5 * sigma`.
        - 'Medium Low': Values between `mu - 1.5 * sigma` and `mu - 0.5 * sigma`.
        - 'Low': Values less than `mu - 1.5 * sigma`.

        Args:
            mu (float): The mean of the values.
            sigma (float): The standard deviation of the values.
            value (float): The value to be categorized.

        Returns:
            Optional[str]: The category into which the value falls ('High', 'Medium High', 'Neutral', 'Medium Low', 'Low'),
                           or None if the value is NaN.
        """
        if sigma == 0:
            raise ValueError("Standard deviation (sigma) must be non-zero")

        if pd.isna(value):
            return None
        elif value > mu + 1.5 * sigma:
            return 'High'
        elif mu + 0.5 * sigma < value <= mu + 1.5 * sigma:
            return 'Medium High'
        elif mu - 0.5 * sigma <= value <= mu + 0.5 * sigma:
            return 'Neutral'
        elif mu - 1.5 * sigma < value < mu - 0.5 * sigma:
            return 'Medium Low'
        else:
            return 'Low'

    def _calculate_window_max_percent_change(self, series: pd.Series, window_periods: int) -> pd.Series:
        """
        Computes the largest absolute percent change (either maximum or minimum) within a specified forward window size,
        excluding the current time step (t=0), for each time step in the series.

        This function calculates the percent change between the current value at time step t=0 and the largest 
        or smallest value within a forward-looking window, but excludes the current time step (t=0) from this window. 
        It returns the largest percent change, whether from the max or min value within the window.

        Args:
            series (pd.Series): Time series of prices captured at regular intervals (e.g., 5-minute intervals).
            window_periods (int): Number of periods corresponding to the window size (e.g., 3 for a 15-minute window if the data is in 5-minute intervals).

        Returns:
            pd.Series: A series of the largest absolute percent change (max or min) within the window for each time step, 
                       excluding the current step (t=0).

        Raises:
            ValueError: If `window_periods` is less than 1.
        """
        if window_periods < 1:
            raise ValueError("window_periods must be greater than or equal to 1")

        if window_periods > 1:
            # Shift the series to exclude the current step (t=0) from the window
            reversed_series = series.iloc[::-1]
            series_shifted = reversed_series.shift(1)  # Forward looking window
            rolling_max = series_shifted.rolling(window=window_periods, min_periods=window_periods).max().iloc[::-1]
            rolling_min = series_shifted.rolling(window=window_periods, min_periods=window_periods).min().iloc[::-1]

            # Calculate the percent changes for max and min
            pct_change_max = ((rolling_max - series) / series) * 100
            pct_change_min = ((rolling_min - series) / series) * 100

            # Take the larger absolute percent change (max or min)
            pct_change = pct_change_max.where(pct_change_max.abs() >= pct_change_min.abs(), pct_change_min)
        else:
            # If window_periods is 1, calculate percent change without deprecated fill_method
            pct_change = series.pct_change(periods=window_periods, fill_method=None) * 100
            pct_change.replace({0.0: np.nan}, inplace=True)

        return pct_change

    def _calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series, window_periods: int) -> pd.Series:
        """
        Calculates the Average True Range (ATR) over a specified window size.

        Args:
            high (pd.Series): Series of high prices.
            low (pd.Series): Series of low prices.
            close (pd.Series): Series of closing prices.
            window_periods (int): Number of periods over which to calculate the ATR.

        Returns:
            pd.Series: The ATR values.
        """
        high, low, close = high.iloc[::-1], low.iloc[::-1], close.iloc[::-1]
        # Calculate True Range (TR)
        prev_close = close.shift(-1)
        tr = pd.concat([
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)

        # Calculate ATR
        atr = tr.rolling(window=window_periods, min_periods=1).mean()

        return atr.iloc[::-1]

    def categorize_percent_change(self, data: pd.DataFrame, run_id: str) -> pd.Series:
        """
        Computes the largest absolute percent change (either maximum or minimum) within a specified forward window size 
        for each time step, then categorizes the changes into buckets based on standard deviations from the mean.

        The function first extracts the window size from the provided `run_id` and determines the largest absolute percent 
        change within that window (either the max or min price). It then categorizes the percent change values into 'High', 
        'Medium High', 'Neutral', 'Medium Low', or 'Low' based on the number of standard deviations from the mean.

        Args:
            data (pd.DataFrame): DataFrame with time series data. May contain multiple symbols.
            run_id (str): Unique identifier that contains the window size information (in minutes).

        Returns:
            tuple: (df, categories) where categories is a series containing categorized labels 
            based on the largest forward absolute percent change (max or min) within the specified window.
        """
        df = data.copy()
        
        logger.debug(f"categorize_percent_change - Input shape: {df.shape}, run_id: {run_id}")

        # Check if data contains multiple symbols
        has_symbol_col = 'symbol' in df.columns
        
        if has_symbol_col:
            # Process each symbol separately using groupby
            logger.info("Processing target computation for multiple symbols separately using groupby")
            
            # Apply the computation per symbol group
            grouped_results = df.groupby('symbol', group_keys=False).apply(
                lambda group: self._categorize_percent_change_single_with_categories(group, run_id)
            )
            
            # Separate df and categories from the result
            df = grouped_results.drop(columns=['_target_category'])
            categories = grouped_results['_target_category']
            
            logger.debug(f"Combined result shape: {df.shape}, categories shape: {categories.shape}")
        else:
            # Single symbol, process as before
            df, categories = self._categorize_percent_change_single(df, run_id)

        return df, categories
    
    def _categorize_percent_change_single(self, df: pd.DataFrame, run_id: str) -> tuple:
        """
        Internal method to process percent change categorization for a single symbol.
        
        CRITICAL: Statistics (mu, sigma) are computed ONLY on the training portion
        of the data to prevent target leakage. The same thresholds are then applied
        to categorize all data (including test data).
        
        Args:
            df (pd.DataFrame): DataFrame for a single symbol
            run_id (str): Unique identifier that contains the window size information
            
        Returns:
            tuple: (df, categories)
        """
        # Extract window_size from run_id
        window_size = self.extract_window_size(run_id)  # in minutes

        # Number of periods corresponding to the window size (since data is at 5 min intervals)
        window_periods = window_size // self.interval_min

        df = self.fill_missing_timestamps(df)

        target = df['close']

        pct_change = self._calculate_window_max_percent_change(target, window_periods)

        # CRITICAL FIX: Compute mu/sigma ONLY on training portion to prevent target leakage
        # If we compute on all data, we leak future distribution information into training labels
        n_samples = len(pct_change.dropna())
        train_end_idx = int(n_samples * self.train_ratio)
        
        # Get training portion for statistics computation (exclude NaN values)
        pct_change_clean = pct_change.dropna()
        train_pct_change = pct_change_clean.iloc[:train_end_idx]
        
        if len(train_pct_change) < 10:
            logger.warning(f"Very few training samples ({len(train_pct_change)}) for statistics. "
                          f"Using all data as fallback (not recommended for production).")
            mu = pct_change.mean()
            sigma = pct_change.std()
        else:
            # Compute stats ONLY on training portion
            mu = train_pct_change.mean()
            sigma = train_pct_change.std()
        
        logger.debug(f"Symbol stats (train-only, n={len(train_pct_change)}) - mu: {mu:.4f}, sigma: {sigma:.4f}")

        get_categories = partial(self._categorize, mu, sigma)

        df, pct_change = self.drop_nulls(df, pct_change)

        # Apply categorization to ALL data using training-derived thresholds
        categories = pct_change.apply(get_categories)

        return df, categories
    
    def _categorize_percent_change_single_with_categories(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """
        Internal method for groupby operation - returns DataFrame with categories as a column.
        
        Args:
            df (pd.DataFrame): DataFrame for a single symbol
            run_id (str): Unique identifier that contains the window size information
            
        Returns:
            pd.DataFrame: DataFrame with added '_target_category' column
        """
        processed_df, categories = self._categorize_percent_change_single(df, run_id)
        processed_df['_target_category'] = categories
        return processed_df

    def categorize_atr(self, data: pd.DataFrame, run_id: str) -> pd.Series:
        """
        Calculates the ATR over a specified window size and categorizes the ATR values.

        Args:
            data (pd.DataFrame): DataFrame with time series data. May contain multiple symbols.
            run_id (str): Unique identifier that contains the window size information (in minutes).

        Returns:
            tuple: (df, categories) where categories is a series containing categorized ATR values.
        """
        df = data.copy()

        logger.debug(f"categorize_atr - Input shape: {df.shape}, run_id: {run_id}")

        # Check if data contains multiple symbols
        has_symbol_col = 'symbol' in df.columns
        
        if has_symbol_col:
            # Process each symbol separately using groupby
            logger.info("Processing ATR computation for multiple symbols separately using groupby")
            
            # Apply the computation per symbol group
            grouped_results = df.groupby('symbol', group_keys=False).apply(
                lambda group: self._categorize_atr_single_with_categories(group, run_id)
            )
            
            # Separate df and categories from the result
            df = grouped_results.drop(columns=['_target_category'])
            categories = grouped_results['_target_category']
            
            logger.debug(f"Combined result shape: {df.shape}, categories shape: {categories.shape}")
        else:
            # Single symbol, process as before
            df, categories = self._categorize_atr_single(df, run_id)

        return df, categories
    
    def _categorize_atr_single(self, df: pd.DataFrame, run_id: str) -> tuple:
        """
        Internal method to process ATR categorization for a single symbol.
        
        CRITICAL: Statistics (mu, sigma) are computed ONLY on the training portion
        of the data to prevent target leakage. The same thresholds are then applied
        to categorize all data (including test data).
        
        Args:
            df (pd.DataFrame): DataFrame for a single symbol
            run_id (str): Unique identifier that contains the window size information
            
        Returns:
            tuple: (df, categories)
        """
        columns_names_config = config.columns.common_columns

        # Extract window_size from run_id
        window_size = self.extract_window_size(run_id)  # in minutes

        # Number of periods corresponding to the window size (since data is at 5 min intervals)
        window_periods = window_size // self.interval_min

        df = self.fill_missing_timestamps(df)

        high, low, close = df[columns_names_config.high], df[columns_names_config.low], df[columns_names_config.close]

        # Calculate ATR
        atr = self._calculate_atr(high, low, close, window_periods)

        # CRITICAL FIX: Compute mu/sigma ONLY on training portion to prevent target leakage
        n_samples = len(atr.dropna())
        train_end_idx = int(n_samples * self.train_ratio)
        
        # Get training portion for statistics computation
        atr_clean = atr.dropna()
        train_atr = atr_clean.iloc[:train_end_idx]
        
        if len(train_atr) < 10:
            logger.warning(f"Very few training samples ({len(train_atr)}) for ATR statistics. "
                          f"Using all data as fallback (not recommended for production).")
            mu = atr.mean()
            sigma = atr.std()
        else:
            # Compute stats ONLY on training portion
            mu = train_atr.mean()
            sigma = train_atr.std()
        
        logger.debug(f"Symbol ATR stats (train-only, n={len(train_atr)}) - mu: {mu:.4f}, sigma: {sigma:.4f}")

        df, atr = self.drop_nulls(df, atr)

        # Categorize ATR values using training-derived thresholds
        get_categories = partial(self._categorize, mu, sigma)
        categories = atr.apply(get_categories)

        return df, categories
    
    def _categorize_atr_single_with_categories(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """
        Internal method for groupby operation - returns DataFrame with categories as a column.
        
        Args:
            df (pd.DataFrame): DataFrame for a single symbol
            run_id (str): Unique identifier that contains the window size information
            
        Returns:
            pd.DataFrame: DataFrame with added '_target_category' column
        """
        processed_df, categories = self._categorize_atr_single(df, run_id)
        processed_df['_target_category'] = categories
        return processed_df
