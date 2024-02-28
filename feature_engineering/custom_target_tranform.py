from typing import Dict
import pandas as pd
import logging
from dataclasses import dataclass

# Setting up basic configuration for logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class FeatureExtractor:
    ticker_data: Dict[str, pd.DataFrame]

    def __post_init__(self):
        self.validate_data()

    def validate_data(self):
        """Ensure that all DataFrame have 'close' column."""
        for symbol, data in self.ticker_data.items():
            if 'close' not in data.columns:
                logging.error(f"Data for {symbol} is missing 'close' column.")
                raise ValueError(
                    f"Data for {symbol} is missing 'close' column.")

    def extract_features(self, windows: Dict[str, pd.Timedelta]) -> Dict[str, pd.DataFrame]:
        """
        Extract high and low prices for specified window sizes.

        :param windows: A dictionary of window sizes with keys as identifiers and pd.Timedelta as values.
        :return: A dictionary with the same keys as the input data, each containing a DataFrame of extracted features.
        """
        features = {}
        for symbol, data in self.ticker_data.items():
            features[symbol] = self._calculate_features_for_symbol(
                data, windows)
        return features

    def _calculate_features_for_symbol(self, data: pd.DataFrame, windows: Dict[str, pd.Timedelta]) -> pd.DataFrame:
        """
        Calculate high and low prices for a single symbol over specified window sizes.

        :param data: DataFrame containing ticker data for a symbol.
        :param windows: A dictionary of window sizes.
        :return: DataFrame with extracted features.
        """
        feature_data = pd.DataFrame(index=data.index)
        for window_name, window_size in windows.items():
            rolling_window = data['close'].rolling(
                window=window_size, closed='both')
            feature_data[f'{window_name}_high'] = rolling_window.max()
            feature_data[f'{window_name}_low'] = rolling_window.min()
        return feature_data


# Example usage
if __name__ == "__main__":
    # Example data setup
    data_example = {
        'AAPL': pd.DataFrame({
            'close': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'time': pd.date_range(start='1/1/2022', periods=10, freq='5T')
        }).set_index('time')
    }

    windows = {
        '5min': pd.Timedelta(minutes=5),
        '1hour': pd.Timedelta(hours=1),
        '8hour': pd.Timedelta(hours=8)
    }

    extractor = FeatureExtractor(ticker_data=data_example)
    features = extractor.extract_features(windows=windows)
    for symbol, feature_df in features.items():
        print(f"Features for {symbol}:\n{feature_df}\n")
