# src/feature_engineering/candlestick_patterns_features.py
import pandas as pd
import talib
from typing import Dict, List
from src.config import config


class CandlestickPatternRecognizer:
    """
    Recognizes various candlestick patterns in trading data.

    Attributes:
        mode (str): The trading mode, either 'BACKTEST' or 'LIVE'.
    """

    def __init__(self) -> None:
        """
        Initializes the CandlestickPatternRecognizer with the trading mode from configuration.
        """
        self.mode: str = config.trading_config.trade_mode

    @staticmethod
    def Engulfing(df: pd.DataFrame) -> Dict[str, List[int]]:
        """
        Identifies bullish and bearish engulfing candlestick patterns in the provided DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing trading data with 'open', 'high', 'low', and 'close' columns.

        Returns:
            Dict[str, List[int]]: Dictionary with keys 'BullishEngulfing' and 'BearishEngulfing',
                each mapping to a list of integers indicating the presence (1) or absence (0) of the pattern.
        """
        engulfing = talib.CDLENGULFING(df['open'], df['high'], df['low'], df['close'])

        # Create separate features for bullish and bearish engulfing
        bullish_engulfing = [1 if engulfing_value > 0 else 0 for engulfing_value in engulfing]
        bearish_engulfing = [1 if engulfing_value < 0 else 0 for engulfing_value in engulfing]

        # Create a dictionary with the separate features
        features = {
            'BullishEngulfing': bullish_engulfing,
            'BearishEngulfing': bearish_engulfing
        }
        return features

    def recognize_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recognizes specified candlestick patterns within the provided trading data.

        Args:
            df (pd.DataFrame): DataFrame containing trading data with 'open', 'high', 'low', and 'close' columns.

        Returns:
            pd.DataFrame: DataFrame containing binary indicators for each recognized candlestick pattern.
        """
        if self.mode == 'LIVE':
            df = df.iloc[-config.backtest_data_load.cs_patterns_max_length:]

        patterns: Dict[str, pd.Series] = {
            'Doji': talib.CDLDOJI(df['open'], df['high'], df['low'], df['close']),
            'Hammer': talib.CDLHAMMER(df['open'], df['high'], df['low'], df['close']),
            'InvertedHammer': talib.CDLINVERTEDHAMMER(df['open'], df['high'], df['low'], df['close']),
            'MorningStar': talib.CDLMORNINGSTAR(df['open'], df['high'], df['low'], df['close'], penetration=0),
            'EveningStar': talib.CDLEVENINGSTAR(df['open'], df['high'], df['low'], df['close'], penetration=0),
            'ShootingStar': talib.CDLSHOOTINGSTAR(df['open'], df['high'], df['low'], df['close']),
            'Harami': talib.CDLHARAMI(df['open'], df['high'], df['low'], df['close']),
            'PiercingLine': talib.CDLPIERCING(df['open'], df['high'], df['low'], df['close']),
            'ThreeBlackCrows': talib.CDL3BLACKCROWS(df['open'], df['high'], df['low'], df['close']),
        }

        engulf_patterns: Dict[str, List[int]] = self.Engulfing(df)

        # Convert pattern indicators to DataFrame
        combined_patterns: Dict[str, List[int]] = {**engulf_patterns, **{k: (v > 0).astype(int).tolist() for k, v in patterns.items()}}
        pattern_df: pd.DataFrame = pd.DataFrame(combined_patterns)

        return pattern_df


# Example usage
if __name__ == "__main__":
    # Mock data for demonstration
    data: Dict[str, pd.DataFrame] = {
        'AAPL': pd.DataFrame({
            'open': [100, 102, 104, 103, 105],
            'high': [101, 103, 105, 104, 106],
            'low': [99, 101, 103, 102, 104],
            'close': [100, 102, 104, 103, 105],
        })
    }

    recognizer: CandlestickPatternRecognizer = CandlestickPatternRecognizer()
    for symbol, df in data.items():
        patterns: pd.DataFrame = recognizer.recognize_patterns(df)
        print(f"Patterns for {symbol}:")
        print(patterns)
