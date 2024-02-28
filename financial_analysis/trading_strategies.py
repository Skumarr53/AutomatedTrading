# Reviewing and optimizing the financial_analysis/trading_strategies.py script

import logging
import numpy as np


class TradingStrategies:
    """
    Class for implementing various trading strategies based on technical indicators.
    """

    @staticmethod
    def bollinger_rsi_volume_strategy(indicators):
        """
        Strategy based on Bollinger Bands, RSI, and Volume.
        """
        lower_band = indicators['bollinger']['lowerband']
        mid_band = indicators['bollinger']['middleband']
        cp = mid_band  # current price
        crsi = indicators['rsi']['rsi']
        cvol = indicators['volume']
        if cp <= lower_band and crsi < 30 and cvol > np.mean(cvol):
            return 'BUY'
        elif cp >= indicators['bollinger']['upperband'] and crsi > 70:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def macd_stochastic_adx_strategy(indicators):
        """
        Strategy based on MACD, Stochastic Oscillator, and ADX.
        """
        m_val = indicators['macd']['macd']
        m_sig = indicators['macd']['signal']
        sk = indicators['stochastic']['stochastic_k']
        if m_val > m_sig and sk > 20 and indicators['adx']['adx'] > 25:
            return 'BUY'
        elif m_val < m_sig and sk < 80:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def ema_atr_obv_strategy(indicators):
        """
        Strategy based on EMA, ATR, and OBV.
        """
        ema_short = indicators['ema']['ema_short']
        ema_long = indicators['ema']['ema_long']
        atr = indicators['atr']['atr']
        obv = indicators['obv']['obv']
        if ema_short > ema_long and atr > np.mean(atr) and obv > np.mean(obv):
            return 'BUY'
        elif ema_short < ema_long:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def sar_vwap_rsi_strategy(indicators):
        """
        Strategy based on SAR, VWAP, and RSI.
        """
        cp = indicators['sar']['sar']
        vwap = indicators['vwap']['vwap']
        rsi = indicators['rsi']['rsi']
        if cp > vwap and rsi > 50 and rsi < 70:
            return 'BUY'
        elif cp < vwap:
            return 'SELL'
        return 'HOLD'

    @staticmethod
    def fibonacci_ichimoku_cci_strategy(indicators):
        """
        Strategy based on Fibonacci Retracements, Ichimoku Cloud, and CCI.
        """
        cp = indicators['fibonacci']['price']
        for level in indicators['fibonacci']['levels']:
            if cp >= level and indicators['ichimoku']['price_above_cloud'] and indicators['cci']['cci'] > -100:
                return 'BUY'
            elif cp < level:
                return 'SELL'
        return 'HOLD'
    
    @staticmethod
    def majority_voting_strategy(decision_dict:dict):
        vote_count = {'BUY': 0, 'SELL': 0, 'HOLD': 0}
        for strategy_name in decision_dict.keys():
            vote_count[decision_dict[strategy_name]] += 1
            
            total_votes = sum(vote_count.values())
            for outcome, count in vote_count.items():
                if count / total_votes >= 0.6:  # 60% majority
                    return outcome
        return 'NONE'  # No clear majority

    def execute_technical_strategy(self, all_indicators_data):
        """
        Execute technical strategies for all provided indicator data.
        """
        strategy_decision = {}

        for symbol, indicators in all_indicators_data.items():
            decision = {
                'Bollinger_RSI_Volume': self.bollinger_rsi_volume_strategy(indicators),
                'MACD_Stochastic_ADX': self.macd_stochastic_adx_strategy(indicators),
                'EMA_ATR_OBV': self.ema_atr_obv_strategy(indicators),
                'SAR_VWAP_RSI': self.sar_vwap_rsi_strategy(indicators),
                'Fibonacci_Ichimoku_CCI': self.fibonacci_ichimoku_cci_strategy(indicators)
            }
            decision['Majority_Vote_Strategy'] = self.majority_voting_strategy(
                decision)
            strategy_decision[symbol] = decision

        return strategy_decision

# Commenting out the function call to prevent execution in the PCI environment
# Note: The class is assumed to be instantiated and used with appropriate indicator data in the actual application.
