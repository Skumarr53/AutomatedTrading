# src/feature_engineering/custom_features_extraction.py
import pandas as pd
import numpy as np
from typing import Dict, List
from src.config import config


class FeatureExtraction:
    """
    Extracts and generates custom features from trading data for use in automated trading systems.

    Attributes:
        mode (str): The trading mode, either 'BACKTEST' or 'LIVE'.
        volume_max_window (int): The maximum window size for volume-based feature calculations.
        periods (Dict[str, int]): Dictionary mapping time frame labels to their corresponding rolling window sizes.
    """

    def __init__(self) -> None:
        """
        Initializes the FeatureExtraction instance with the trading mode and sets up rolling window periods.
        """
        self.mode: str = config.TRADE_MODE
        self.volume_max_window: int = max(config.VOLUME_MEAN_WINDOWS)
        self.periods: Dict[str, int] = self._initialize_time_frame_windows()

    def _initialize_time_frame_windows(self) -> Dict[str, int]:
        """
        Initializes the rolling window sizes for various time frames considering market hours.

        Returns:
            Dict[str, int]: A dictionary mapping time frame labels (e.g., '1h', '1d') to their corresponding window sizes.
        """
        return {
            '1h': self._rolling_window_market_hours(1),
            '5h': self._rolling_window_market_hours(5),
            '1d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY),
            '3d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 3),
            '5d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 5),
            '14d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 14),
            '52w': self._rolling_window_market_hours(
                config.N_OPERATIONS_HOURS_DAILY * config.N_OPERATIONS_DAYS_WEEKLY * 52
            )
        }

    def _calculate_market_hours(self, data: pd.DataFrame) -> pd.Series:
        """
        Calculate market hours considering only 9-5 on weekdays.

        Args:
            data (pd.DataFrame): DataFrame containing a 'date' column with datetime information.

        Returns:
            pd.Series: A boolean Series indicating whether each timestamp falls within market hours.
        """
        market_open: pd.Series = data['date'].dt.floor('D') + pd.to_timedelta('9 hours')
        market_close: pd.Series = data['date'].dt.floor('D') + pd.to_timedelta('17 hours')
        return (data['date'] >= market_open) & (data['date'] <= market_close)

    def _rolling_window_market_hours(self, hours: int) -> int:
        """
        Adjust rolling window size to account for market hours.

        Args:
            hours (int): Number of hours for the rolling window.

        Returns:
            int: Adjusted rolling window size based on the trading run interval.
        """
        return hours * 60 // config.TRADE_RUN_INTERVAL_MIN  # Convert hours to number of intervals

    def _add_high_low_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add High and Low features for different time frames, considering market hours.

        Args:
            data (pd.DataFrame): DataFrame containing 'high' and 'low' price columns.

        Returns:
            pd.DataFrame: DataFrame with added high and low features for each specified time frame.
        """
        features: pd.DataFrame = pd.DataFrame(
            index=data.index if self.mode == "BACKTEST" else data.index[-1:]
        )

        if self.mode == "BACKTEST":
            for label, window in self.periods.items():
                features[f'high_{label}'] = data['high'].rolling(window=window).max()
                features[f'low_{label}'] = data['low'].rolling(window=window).min()
        else:
            for label, window in self.periods.items():
                features[f'high_{label}'] = data['high'].tail(window).max()
                features[f'low_{label}'] = data['low'].tail(window).min()

        return features

    def _add_candlestick_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add features derived from the last three candlesticks.

        Args:
            data (pd.DataFrame): DataFrame containing 'open', 'high', 'low', and 'close' price columns.

        Returns:
            pd.DataFrame: DataFrame with added candlestick-related features.
        """
        if self.mode == "LIVE":
            data = data.iloc[-3:]
        features: pd.DataFrame = pd.DataFrame(index=data.index)
        features['candlestick_length'] = data['high'] - data['low']
        features['body_length'] = abs(data['close'] - data['open'])
        features['body_mid_point'] = data['open'] + (features['body_length'] / 2)
        features['is_green'] = data['close'] > data['open']
        features['body_to_length_ratio'] = features['body_length'] / features['candlestick_length']
        feat_cols: List[str] = features.columns.to_list()

        # Carry forward the features for the last two candles into the current record
        for shift in range(1, 3):
            shifted_features: pd.DataFrame = features[feat_cols].shift(shift)
            shifted_features.columns = [f"{col}_prev_{shift}" for col in feat_cols]
            features = pd.concat([features, shifted_features], axis=1)

        return features

    def _add_volume_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add volume-based features.

        Args:
            data (pd.DataFrame): DataFrame containing a 'volume' column.

        Returns:
            pd.DataFrame: DataFrame with added volume-related features.
        """
        if self.mode == "LIVE":
            data = data.iloc[-(self.volume_max_window + 1):]
        features: pd.DataFrame = pd.DataFrame(
            index=data.index if self.mode == "BACKTEST" else data.index[-1:]
        )
        if self.mode == "BACKTEST":
            features['volume_pct_change_last_interval'] = data['volume'].pct_change() * 100
        else:
            features['volume_pct_change_last_interval'] = data['volume'].pct_change().iloc[-1:] * 100

        for period in config.VOLUME_MEAN_WINDOWS:
            # Calculate percent change compared to the rolling mean of the given period
            rolling_mean: pd.Series = data['volume'].rolling(window=period).mean()
            pct_change_from_rolling_mean: pd.Series = (
                data['volume'] - rolling_mean
            ) / rolling_mean * 100
            if self.mode == "BACKTEST":
                features[f'volume_pct_change_mean_{period}'] = pct_change_from_rolling_mean
            else:
                features[f'volume_pct_change_mean_{period}'] = pct_change_from_rolling_mean.iloc[-1:]

        return features

    def _add_time_based_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Add time-based features.

        Args:
            data (pd.DataFrame): DataFrame with datetime index.

        Returns:
            pd.DataFrame: DataFrame with added time-based features such as hour of day, day of week, etc.
        """
        if self.mode == "LIVE":
            data = data.iloc[-1:]
        features: pd.DataFrame = pd.DataFrame(index=data.index)
        features['hour_of_day'] = data.index.hour
        features['day_of_week'] = data.index.weekday
        features['month_of_year'] = data.index.month
        features['quarter_of_year'] = data.index.quarter

        return features

    def _add_gap_analysis_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze gaps between candlesticks.

        Args:
            data (pd.DataFrame): DataFrame containing 'open' and 'close' price columns.

        Returns:
            pd.DataFrame: DataFrame with added candlestick gap features.
        """
        features: pd.DataFrame = pd.DataFrame(
            index=data.index if self.mode == "BACKTEST" else data.index[-1:]
        )
        if self.mode == "BACKTEST":
            features['candlestick_gap'] = data['open'] - data['close'].shift(1)
        else:
            if len(data) >= 2:
                features['candlestick_gap'] = data.iloc[-1]['open'] - data.iloc[-2]['close']
            else:
                features['candlestick_gap'] = np.nan  # Handle case with insufficient data

        return features

    def generate_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate all features for the provided trading data.

        Args:
            data (pd.DataFrame): DataFrame containing trading data with appropriate columns.

        Returns:
            pd.DataFrame: DataFrame containing all generated custom features.
        """
        candlestick_features: pd.DataFrame = self._add_candlestick_features(data)
        high_low_features: pd.DataFrame = self._add_high_low_features(data)
        volume_features: pd.DataFrame = self._add_volume_features(data)
        time_based_features: pd.DataFrame = self._add_time_based_features(data)
        gap_analysis_features: pd.DataFrame = self._add_gap_analysis_features(data)

        # Combine all new feature DataFrames
        custom_dfs: List[pd.DataFrame] = [
            candlestick_features,
            high_low_features,
            volume_features,
            time_based_features,
            gap_analysis_features
        ]
        if self.mode == "LIVE":
            for df in custom_dfs:
                assert len(df) == 1, f"DataFrame {df} does not have a length of 1"
        all_features: pd.DataFrame = pd.concat(custom_dfs, axis=1)
        return all_features
