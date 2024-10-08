# src/financial_analysis/trading_strategies.py

import pandas as pd
import logging
import numpy as np
from typing import Dict, Any


class TradingStrategies:
    """
    Implements various trading strategies based on technical indicators.

    This class provides static methods for different trading strategies that analyze
    technical indicators to generate trading signals. It also includes a majority voting
    strategy to consolidate decisions from multiple strategies.
    """

    @staticmethod
    def bollinger_rsi_volume_strategy(indicators: Dict[str, Dict[str, Any]]) -> str:
        """
        Determines a trading signal based on Bollinger Bands, RSI, and Volume indicators.

        Buy Signal:
            Current price (`cp`) is less than or equal to the lower Bollinger Band.
            RSI (`crsi`) is below 30.
            Current volume (`cvol`) is greater than the average volume.

        Sell Signal:
            Current price (`cp`) is greater than or equal to the upper Bollinger Band.
            RSI (`crsi`) is above 70.

        Hold Signal:
            No buy or sell conditions are met.

        Args:
            indicators (Dict[str, Dict[str, Any]]): 
                A dictionary containing technical indicators with the following structure:
                {
                    'bollinger': {
                        'lowerband': pd.Series,
                        'middleband': pd.Series,
                        'upperband': pd.Series
                    },
                    'rsi': {
                        'rsi': pd.Series
                    },
                    'volume': pd.Series
                }

        Returns:
            str: 'BUY', 'SELL', or 'HOLD' based on the strategy conditions.
        """
        lower_band: pd.Series = indicators['bollinger']['lowerband']
        mid_band: pd.Series = indicators['bollinger']['middleband']
        cp: pd.Series = mid_band  # Assuming 'mid_band' represents the current price
        crsi: pd.Series = indicators['rsi']['rsi']
        cvol: pd.Series = indicators['volume']
        average_volume: float = cvol.mean()

        # Fetch the latest values
        latest_cp: float = cp.iloc[-1]
        latest_crsi: float = crsi.iloc[-1]
        latest_cvol: float = cvol.iloc[-1]
        latest_lower_band: float = lower_band.iloc[-1]
        latest_upper_band: float = indicators['bollinger']['upperband'].iloc[-1]

        if latest_cp <= latest_lower_band and latest_crsi < 30 and latest_cvol > average_volume:
            return 'BUY'
        elif latest_cp >= latest_upper_band and latest_crsi > 70:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def macd_stochastic_adx_strategy(indicators: Dict[str, Dict[str, Any]]) -> str:
        """
        Determines a trading signal based on MACD, Stochastic Oscillator, and ADX indicators.

        Buy Signal:
            MACD value (`m_val`) is greater than the MACD signal (`m_sig`).
            Stochastic %K (`sk`) is above 20.
            ADX (`adx`) is above 25.

        Sell Signal:
            MACD value (`m_val`) is less than the MACD signal (`m_sig`).
            Stochastic %K (`sk`) is below 80.

        Hold Signal:
            No buy or sell conditions are met.

        Args:
            indicators (Dict[str, Dict[str, Any]]): 
                A dictionary containing technical indicators with the following structure:
                {
                    'macd': {
                        'macd': pd.Series,
                        'signal': pd.Series,
                        'hist': pd.Series
                    },
                    'stochastic': {
                        'stochastic_k': pd.Series,
                        'stochastic_d': pd.Series
                    },
                    'adx': {
                        'adx': pd.Series
                    }
                }

        Returns:
            str: 'BUY', 'SELL', or 'HOLD' based on the strategy conditions.
        """
        m_val: pd.Series = indicators['macd']['macd']
        m_sig: pd.Series = indicators['macd']['signal']
        sk: pd.Series = indicators['stochastic']['stochastic_k']
        adx_val: pd.Series = indicators['adx']['adx']

        # Fetch the latest values
        latest_m_val: float = m_val.iloc[-1]
        latest_m_sig: float = m_sig.iloc[-1]
        latest_sk: float = sk.iloc[-1]
        latest_adx: float = adx_val.iloc[-1]

        if latest_m_val > latest_m_sig and latest_sk > 20 and latest_adx > 25:
            return 'BUY'
        elif latest_m_val < latest_m_sig and latest_sk < 80:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def ema_atr_obv_strategy(indicators: Dict[str, Dict[str, Any]]) -> str:
        """
        Determines a trading signal based on EMA, ATR, and OBV indicators.

        Buy Signal:
            Short-term EMA (`ema_short`) is greater than long-term EMA (`ema_long`).
            ATR (`atr`) is above its average value.
            OBV (`obv`) is above its average value.

        Sell Signal:
            Short-term EMA (`ema_short`) is less than long-term EMA (`ema_long`).

        Hold Signal:
            No buy or sell conditions are met.

        Args:
            indicators (Dict[str, Dict[str, Any]]): 
                A dictionary containing technical indicators with the following structure:
                {
                    'ema': {
                        'ema_short': pd.Series,
                        'ema_long': pd.Series
                    },
                    'atr': {
                        'atr': pd.Series
                    },
                    'obv': {
                        'obv': pd.Series
                    }
                }

        Returns:
            str: 'BUY', 'SELL', or 'HOLD' based on the strategy conditions.
        """
        ema_short: pd.Series = indicators['ema']['ema_short']
        ema_long: pd.Series = indicators['ema']['ema_long']
        atr: pd.Series = indicators['atr']['atr']
        obv: pd.Series = indicators['obv']['obv']

        # Calculate average ATR and OBV
        average_atr: float = atr.mean()
        average_obv: float = obv.mean()

        # Fetch the latest values
        latest_ema_short: float = ema_short.iloc[-1]
        latest_ema_long: float = ema_long.iloc[-1]
        latest_atr: float = atr.iloc[-1]
        latest_obv: float = obv.iloc[-1]

        if latest_ema_short > latest_ema_long and latest_atr > average_atr and latest_obv > average_obv:
            return 'BUY'
        elif latest_ema_short < latest_ema_long:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def sar_vwap_rsi_strategy(indicators: Dict[str, Dict[str, Any]]) -> str:
        """
        Determines a trading signal based on SAR, VWAP, and RSI indicators.

        Buy Signal:
            SAR (`cp`) is above VWAP (`vwap`).
            RSI (`rsi`) is between 50 and 70.

        Sell Signal:
            SAR (`cp`) is below VWAP (`vwap`).

        Hold Signal:
            No buy or sell conditions are met.

        Args:
            indicators (Dict[str, Dict[str, Any]]): 
                A dictionary containing technical indicators with the following structure:
                {
                    'sar': {
                        'sar': pd.Series
                    },
                    'vwap': {
                        'vwap': pd.Series
                    },
                    'rsi': {
                        'rsi': pd.Series
                    }
                }

        Returns:
            str: 'BUY', 'SELL', or 'HOLD' based on the strategy conditions.
        """
        cp: pd.Series = indicators['sar']['sar']
        vwap: pd.Series = indicators['vwap']['vwap']
        rsi: pd.Series = indicators['rsi']['rsi']

        # Fetch the latest values
        latest_cp: float = cp.iloc[-1]
        latest_vwap: float = vwap.iloc[-1]
        latest_rsi: float = rsi.iloc[-1]

        if latest_cp > latest_vwap and 50 < latest_rsi < 70:
            return 'BUY'
        elif latest_cp < latest_vwap:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def fibonacci_ichimoku_cci_strategy(indicators: Dict[str, Dict[str, Any]]) -> str:
        """
        Determines a trading signal based on Fibonacci Retracements, Ichimoku Cloud, and CCI indicators.

        Buy Signal:
            Current price (`cp`) is greater than or equal to a Fibonacci level.
            Price is above the Ichimoku Cloud.
            CCI (`cci`) is above -100.

        Sell Signal:
            Current price (`cp`) is below a Fibonacci level.

        Hold Signal:
            No buy or sell conditions are met.

        Args:
            indicators (Dict[str, Dict[str, Any]]): 
                A dictionary containing technical indicators with the following structure:
                {
                    'fibonacci': {
                        'price': float,
                        'levels': List[float]
                    },
                    'ichimoku': {
                        'price_above_cloud': bool
                    },
                    'cci': {
                        'cci': pd.Series
                    }
                }

        Returns:
            str: 'BUY', 'SELL', or 'HOLD' based on the strategy conditions.
        """
        cp: float = indicators['fibonacci']['price']
        levels: List[float] = indicators['fibonacci']['levels']
        ichimoku_price_above_cloud: bool = indicators['ichimoku']['price_above_cloud']
        cci_val: float = indicators['cci']['cci'].iloc[-1]

        for level in levels:
            if cp >= level and ichimoku_price_above_cloud and cci_val > -100:
                return 'BUY'
            elif cp < level:
                return 'SELL'
        return 'HOLD'

    @staticmethod
    def majority_voting_strategy(decision_dict: Dict[str, str]) -> str:
        """
        Determines the final trading signal based on majority voting from multiple strategies.

        Majority Rule:
            If 60% or more of the strategies agree on 'BUY', return 'BUY'.
            If 60% or more of the strategies agree on 'SELL', return 'SELL'.
            Otherwise, return 'NONE' indicating no clear majority.

        Args:
          decision_dict (Dict[str, str]): 
            A dictionary where keys are strategy names and values are their respective decisions ('BUY', 'SELL', 'HOLD').

        Returns:
            str: 'BUY', 'SELL', or 'NONE' based on the majority voting outcome.
        """
        vote_count: Dict[str, int] = {'BUY': 0, 'SELL': 0, 'HOLD': 0}
        total_votes: int = 0

        for strategy_name, decision in decision_dict.items():
            if decision in vote_count:
                vote_count[decision] += 1
                total_votes += 1

        for outcome, count in vote_count.items():
            if total_votes > 0 and (count / total_votes) >= 0.6:  # 60% majority
                return outcome
        return 'NONE'  # No clear majority

    def execute_technical_strategy(self, all_indicators_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        """
        Executes all defined technical strategies for each stock symbol and consolidates decisions.

        Args:
            all_indicators_data (Dict[str, Dict[str, Any]]): 
                A dictionary where keys are stock symbols and values are dictionaries of technical indicators.

        Returns:
            Dict[str, Dict[str, str]]: 
                A nested dictionary where the first key is the stock symbol and the second key is the strategy name,
                mapping to their respective decisions ('BUY', 'SELL', 'HOLD', 'NONE').
        """
        strategy_decision: Dict[str, Dict[str, str]] = {}

        for symbol, indicators in all_indicators_data.items():
            decision = {
                'Bollinger_RSI_Volume': self.bollinger_rsi_volume_strategy(indicators),
                'MACD_Stochastic_ADX': self.macd_stochastic_adx_strategy(indicators),
                'EMA_ATR_OBV': self.ema_atr_obv_strategy(indicators),
                'SAR_VWAP_RSI': self.sar_vwap_rsi_strategy(indicators),
                'Fibonacci_Ichimoku_CCI': self.fibonacci_ichimoku_cci_strategy(indicators)
            }
            decision['Majority_Vote_Strategy'] = self.majority_voting_strategy(decision)
            strategy_decision[symbol] = decision

        return strategy_decision


# Example of setting up and using the TradingStrategies class
if __name__ == "__main__":
    # Example indicator data setup for demonstration
    example_indicators = {
        'AAPL': {
            'bollinger': {
                'lowerband': pd.Series([140, 142, 144]),
                'middleband': pd.Series([150, 152, 154]),
                'upperband': pd.Series([160, 162, 164])
            },
            'rsi': {
                'rsi': pd.Series([25, 35, 45])
            },
            'volume': pd.Series([10000, 15000, 20000]),
            'macd': {
                'macd': pd.Series([1.2, 1.5, 1.8]),
                'signal': pd.Series([1.0, 1.3, 1.6]),
                'hist': pd.Series([0.2, 0.2, 0.2])
            },
            'stochastic': {
                'stochastic_k': pd.Series([25, 50, 75]),
                'stochastic_d': pd.Series([20, 55, 70])
            },
            'adx': {
                'adx': pd.Series([30, 35, 40])
            },
            'ema': {
                'ema_short': pd.Series([145, 147, 149]),
                'ema_long': pd.Series([140, 142, 144])
            },
            'atr': {
                'atr': pd.Series([1.5, 1.6, 1.7])
            },
            'obv': {
                'obv': pd.Series([5000, 6000, 7000])
            },
            'sar': {
                'sar': pd.Series([151, 153, 155])
            },
            'vwap': {
                'vwap': pd.Series([150, 152, 154])
            },
            'cci': {
                'cci': pd.Series([50, 60, 70])
            },
            'fibonacci': {
                'price': 155,
                'levels': [150, 145, 140]
            },
            'ichimoku': {
                'price_above_cloud': True
            }
        }
    }

    # Initialize TradingStrategies instance
    ts: TradingStrategies = TradingStrategies()

    # Execute strategies
    strategy_results: Dict[str, Dict[str, str]] = ts.execute_technical_strategy(example_indicators)

    # Print the results
    for symbol, decisions in strategy_results.items():
        print(f"Trading decisions for {symbol}:")
        for strategy, decision in decisions.items():
            print(f"  {strategy}: {decision}")
        print()
