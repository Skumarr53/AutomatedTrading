# src/feature_engineering/custom_target_tranform.py
from typing import Dict, Any
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field

# Setting up basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@dataclass
class FeatureExtractor:
    """
    Extracts and generates custom target features from ticker data for automated trading systems.

    Attributes:
        ticker_data (Dict[str, pd.DataFrame]): Dictionary containing ticker data for each symbol.
    """
    ticker_data: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Post-initialization processing to validate the provided ticker data.
        """
        self.validate_data()

    def validate_data(self) -> None:
        """
        Ensures that all DataFrames in ticker_data contain the 'close' column.

        Raises:
            ValueError: If any DataFrame is missing the 'close' column.
        """
        for symbol, data in self.ticker_data.items():
            if 'close' not in data.columns:
                logging.error(f"Data for {symbol} is missing 'close' column.")
                raise ValueError(f"Data for {symbol} is missing 'close' column.")

    def extract_features(self, windows: Dict[str, pd.Timedelta]) -> Dict[str, pd.DataFrame]:
        """
        Extracts high and low price features for specified window sizes.

        Args:
            windows (Dict[str, pd.Timedelta]): 
                A dictionary of window sizes with keys as identifiers and pd.Timedelta as values.

        Returns:
            Dict[str, pd.DataFrame]: 
                A dictionary with the same keys as ticker_data, each containing a DataFrame of extracted features.
        """
        features: Dict[str, pd.DataFrame] = {}
        for symbol, data in self.ticker_data.items():
            features[symbol] = self._calculate_features_for_symbol(data, windows)
        return features

    def _calculate_features_for_symbol(
        self, 
        data: pd.DataFrame, 
        windows: Dict[str, pd.Timedelta]
    ) -> pd.DataFrame:
        """
        Calculates high and low prices for a single symbol over specified window sizes.

        Args:
            data (pd.DataFrame): 
                DataFrame containing ticker data for a symbol. Must include a 'close' column.
            windows (Dict[str, pd.Timedelta]): 
                A dictionary of window sizes with keys as identifiers and pd.Timedelta as values.

        Returns:
            pd.DataFrame: 
                DataFrame with extracted high and low price features for each specified window.
        """
        feature_data: pd.DataFrame = pd.DataFrame(index=data.index)
        for window_name, window_size in windows.items():
            rolling_window = data['close'].rolling(
                window=window_size, closed='both'
            )
            feature_data[f'{window_name}_high'] = rolling_window.max()
            feature_data[f'{window_name}_low'] = rolling_window.min()
        return feature_data


# Example usage
if __name__ == "__main__":
    # Example data setup
    data_example: Dict[str, pd.DataFrame] = {
        'AAPL': pd.DataFrame({
            'close': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'time': pd.date_range(start='1/1/2022', periods=10, freq='5T')
        }).set_index('time')
    }

    windows: Dict[str, pd.Timedelta] = {
        '5min': pd.Timedelta(minutes=5),
        '1hour': pd.Timedelta(hours=1),
        '8hour': pd.Timedelta(hours=8)
    }

    extractor: FeatureExtractor = FeatureExtractor(ticker_data=data_example)
    features: Dict[str, pd.DataFrame] = extractor.extract_features(windows=windows)
    for symbol, feature_df in features.items():
        print(f"Features for {symbol}:\n{feature_df}\n")
