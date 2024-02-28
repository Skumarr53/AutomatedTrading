import pandas as pd
import talib
from typing import Dict
from config import config


class CandlestickPatternRecognizer:
    def __init__(self):
        self.mode = config.TRADE_MODE
    
    @staticmethod
    def Engulfing(df: pd.DataFrame):
        engulfing = talib.CDLENGULFING(df['open'], df['high'], df['low'], df['close'])

        # Create separate features for bullish and bearish engulfing
        bullish_engulfing = [max(engulfing_value, 0) for engulfing_value in engulfing]
        bearish_engulfing = [min(engulfing_value, 0) for engulfing_value in engulfing]

        # Create a dictionary with the separate features
        features = {
            'BullishEngulfing': bullish_engulfing,
            'BearishEngulfing': bearish_engulfing
        }
        return features
    
    def recognize_patterns(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Recognize specified candlestick patterns for each symbol's data.
        """
        pattern_results = {}
        for symbol, df in data.items():
            if self.mode == 'LIVE':
                df = df.iloc[-config.CS_PATTERNS_MAX_LENGTH:]
            patterns = {
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
            Engulf_patterns = self.Engulfing(df)

            # Convert pattern indicators to DataFrame
            patterns = {**Engulf_patterns, **patterns}
            pattern_df = pd.DataFrame(patterns)
            pattern_results[symbol] = pattern_df if self.mode == 'LIVE' else pattern_df.iloc[-1:]
        return pattern_results


# Example usage
if __name__ == "__main__":
    # Mock data for demonstration
    data = {
        'AAPL': pd.DataFrame({
            'open': [100, 102, 104, 103, 105],
            'high': [101, 103, 105, 104, 106],
            'low': [99, 101, 103, 102, 104],
            'close': [100, 102, 104, 103, 105],
        })
    }

    recognizer = CandlestickPatternRecognizer(data)
    patterns = recognizer.recognize_patterns()
    for symbol, pattern_df in patterns.items():
        print(f"Patterns for {symbol}:")
        print(pattern_df)
