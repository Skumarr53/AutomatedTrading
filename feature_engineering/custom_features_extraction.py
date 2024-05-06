import pandas as pd
import numpy as np
from config import config
from typing import Dict


class FeatureExtraction:
    def __init__(self):
        self.mode = config.TRADE_MODE
        self.volume_max_window = max(config.VOLUME_MEAN_WINDOWS)
        self.periods = self._initialize_time_frame_windows()
    
    def _initialize_time_frame_windows(self) -> Dict[str, int]:
        """
        Initializes the rolling window sizes for various time frames considering market hours.
        """
        return {
            '1h': self._rolling_window_market_hours(1),
            '5h': self._rolling_window_market_hours(5),
            '1d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY),
            '3d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 3),
            '5d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 5),
            '14d': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * 14),
            '52w': self._rolling_window_market_hours(config.N_OPERATIONS_HOURS_DAILY * config.N_OPERATIONS_DAYS_WEEKLY * 52)
        }

    def _calculate_market_hours(self, data: pd.DataFrame):
        """Calculate market hours considering only 9-5 on weekdays."""
        market_open = data['date'].dt.floor('D') + pd.to_timedelta('9 hours')
        market_close = data['date'].dt.floor('D') + pd.to_timedelta('17 hours')
        return (data['date'] >= market_open) & (data['date'] <= market_close)

    def _rolling_window_market_hours(self, hours):
        """Adjust rolling window size to account for market hours."""
        return hours * 60 // config.TRADE_RUN_INTERVAL_MIN  # Convert hours to number of 5-min intervals in market hours

    def _add_high_low_features(self, data: pd.DataFrame):
        """Add High and Low features for different time frames, considering market hours."""
        features = pd.DataFrame(index=data.index if self.mode == "BACKTEST" else data.index[-1:])

        if self.mode == "BACKTEST":
            for label, window in self.periods.items():
                features[f'high_{label}'] = data['high'].rolling(window=window).max()  
                features[f'low_{label}'] = data['low'].rolling(window=window).min() 
        else:
            for label, window in self.periods.items():
                features[f'high_{label}'] = data['high'].tail(window).max()
                features[f'high_{label}'] = data['low'].tail(window).max()
        return features

    def _add_candlestick_features(self, data: pd.DataFrame):
        """Add features derived from the last three candlesticks."""

        if self.mode == "LIVE": data = data.iloc[-3:]
        features = pd.DataFrame(index=data.index)
        features['candlestick_length'] = data['high'] - data['low']
        features['body_length'] = abs(data['close'] - data['open'])
        features['body_mid_point'] = data['open'] + (features['body_length'] / 2)
        features['is_green'] = data['close'] > data['open']
        features['body_to_length_ratio'] = features['body_length'] / features['candlestick_length']
        feat_cols = features.columns.to_list()
        
        # Carry forward the features for the last two candles into the current record
        for shift in range(1, 3):
            shifted_features = features[feat_cols].shift(shift)
            shifted_features.columns = [
                f"{col}_prev_{shift}" for col in feat_cols]
            features = pd.concat([features, shifted_features], axis=1) 
        
        return features 

    def _add_volume_features(self, data: pd.DataFrame):
        """Add volume-based features."""
        if self.mode == "LIVE":
            data = data.iloc[-(self.volume_max_window+1):]
        features = pd.DataFrame(
                index=data.index if self.mode == "BACKTEST" else data.index[-1:])
        features['volume_pct_change_last_interval'] = data['volume'].pct_change() * 100 if self.mode == "BACKTEST" else data['volume'].pct_change().iloc[-1:] * 100

        for period in config.VOLUME_MEAN_WINDOWS:
            # Calculate percent change compared to the rolling mean of the given period
            rolling_mean = data['volume'].rolling(window=period).mean()
            pct_change_from_rolling_mean = (
                data['volume'] - rolling_mean) / rolling_mean * 100
            features[f'volume_pct_change_mean_{period}'] = pct_change_from_rolling_mean if self.mode == "BACKTEST" else pct_change_from_rolling_mean.iloc[-1:]
        
        return features 

    def _add_time_based_features(self, data: pd.DataFrame):
        """Add time-based features."""
        if self.mode == "LIVE":
            data = data.iloc[-1:]
        features = pd.DataFrame(index=data.index)
        features['hour_of_day'] = data.index.hour
        features['day_of_week'] = data.index.weekday
        features['month_of_year'] = data.index.month
        features['quarter_of_year'] = data.index.quarter

        return features

    def _add_gap_analysis_features(self, data: pd.DataFrame):
        """Analyze gaps between candlesticks."""
        features = pd.DataFrame(index=data.index if self.mode ==
                            "BACKTEST" else data.index[-1:])
        if self.mode == "BACKTEST":
            features['candlestick_gap'] = (
                data['open'] - data['close'].shift(1))
        else:
            features['candlestick_gap'] = data.iloc[-1]['open'] - data.iloc[-2]['close']
        
        return features

    def generate_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate all features for each symbol and return a dictionary of new features DataFrames."""
        candlestick_features = self._add_candlestick_features(data)
        high_low_features = self._add_high_low_features(data)
        volume_features = self._add_volume_features(data)
        time_based_features = self._add_time_based_features(data)
        gap_analysis_features = self._add_gap_analysis_features(data)

        # Combine all new feature DataFrames
        custom_dfs = [candlestick_features, high_low_features, volume_features, time_based_features, gap_analysis_features]
        if self.mode == "LIVE": 
            for df in custom_dfs: assert len(df) == 1, f"DataFrame {df} does not have length of 1"
        all_features = pd.concat(custom_dfs, axis=1)
        return all_features
