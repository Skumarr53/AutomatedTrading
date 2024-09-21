from dataclasses import dataclass, field
from typing import Callable, Dict, List
import pandas as pd
import src.feature_engineering.indicators as ind
from src.config import config


@dataclass
class TechnicalIndicators:
    mode: str = config.TRADE_MODE
    callback: Callable = field(default=None)
    ## TODO include fibonachi as well
    indicators_functions = [
            ind.bollinger_bands, ind.rsi, ind.macd, ind.stochastic_oscillator,
            ind.adx, ind.ema, ind.vwap, ind.atr,
            ind.obv, ind.sar, ind.cci, ind.ichimoku_cloud
    ]  # ind.fibonacci_retracements

    def get_stock_indicators(self, all_stock_data: Dict[str, pd.DataFrame]) -> None:
        indicators_data = {symbol: self._compute_indicators(data)
                           for symbol, data in all_stock_data.items() if not data.empty}
        return indicators_data

    def compute_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._truncate_data_for_live_mode(
            data) if self.mode == 'LIVE' else data
        indicators_df = self._gather_indicators(data)
        return indicators_df

    def _truncate_data_for_live_mode(self, data: pd.DataFrame) -> pd.DataFrame:
        #max_period = (14 * config.N_OPERATIONS_HOURS_DAILY * 60) // config.TRADE_RUN_INTERVAL_MIN
        return data.tail(config.TECH_INDS_MAX_LENGTH+1)


    def _gather_indicators(self, data: pd.DataFrame) -> pd.DataFrame:

        # Initialize an empty DataFrame to store the results
        indicators_df = pd.DataFrame(
            index=data.index if self.mode == 'BACKTEST' else data.index[-1:])

        # Iterate over each indicator function, compute the indicator, and merge the results into the DataFrame
        for func in self.indicators_functions:
            indicator_result = func(data)

            if func.__name__ == 'fibonacci_retracements':
                for key, dat in indicator_result.items():
                    indicators_df = pd.concat([indicators_df, dat], axis=1)
            else:
                for key, series in indicator_result.items():
                    indicators_df[key] = series

        return indicators_df 


# Example of setting up and using the TechnicalIndicators class
# if __name__ == "__main__":
#     ti = TechnicalIndicators(mode="BACKTEST")
#     stock_data = {...}  # Assume stock_data is populated with symbol: DataFrame pairs
#     ti.get_stock_indicators(stock_data)
