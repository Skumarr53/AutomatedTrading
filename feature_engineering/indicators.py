import talib, sys
import pandas as pd
from typing import Dict, List
from config import config
from config.config import MODEL_CONFIG


def rolling_pipe(dataframe, window, fctn):
    return pd.concat([dataframe.iloc[i-window: i].pipe(fctn)
                      if i >= window else None
                      for i in range(1, len(dataframe)+1)], axis=1).T

def get_param(func_name: str) -> List[Dict[str, int]]:
    function_params = {key[len(func_name)+2:]: value for key,
                       value in MODEL_CONFIG['TECHNICAL_INDICATORS_PARAMS'].items() if key.startswith(func_name)}
    return [{key: values[i] for key, values in function_params.items()}
            for i in range(3)]


def calc_fib_levels(df):
    recent_high = df['high'].max()
    recent_low = df['low'].min()
    retracements = {f'fib_level_{str(int(level * 1000))}': recent_low +
                    (recent_high - recent_low) * level for level in config.FIB_LEVELS}
    return pd.Series(retracements, name=df.iloc[-1].name)


def calc_ichimoku_cloud(data, i, param):
    conversion_line = (data['high'].rolling(window=param["conversion_line_period"]).max(
    ) + data['low'].rolling(window=param["conversion_line_period"]).min()) / 2
    base_line = (data['high'].rolling(window=param["base_line_periods"]).max(
    ) + data['low'].rolling(window=param["base_line_periods"]).min()) / 2
    leading_span_a = (conversion_line + base_line) / 2
    leading_span_b = (data['high'].rolling(window=param["lagging_span2_periods"]).max(
    ) + data['low'].rolling(window=param["lagging_span2_periods"]).min()) / 2
    lagging_span = data['close'].shift(-param["displacement"])
    price_above_cloud = data['close'] > max(
        leading_span_a.iloc[-param["displacement"]], leading_span_b.iloc[-param["displacement"]])
    return {
        f"ichimoku_conversion_line_param{i+1}": conversion_line,
        f"ichimoku_base_line_param{i+1}": base_line,
        f"ichimoku_leading_span_a_param{i+1}": leading_span_a,
        f"ichimoku_leading_span_b_param{i+1}": leading_span_b,
        f"ichimoku_lagging_span_param{i+1}": lagging_span,
        f"ichimoku_price_above_cloud_param{i+1}": price_above_cloud
    }


def bollinger_bands(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name = sys._getframe().f_code.co_name
    params = get_param(f_name)

    results = {}
    for i, param in enumerate(params):
        upperband, middleband, lowerband = talib.BBANDS(
            data['close'], **param)
        results.update(
            {f"bollinger_upperband_param{i+1}": upperband, f"bollinger_middleband_param{i+1}": middleband, f"bollinger_lowerband_param{i+1}": lowerband})
    return results



def rsi(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update({f"rsi_param{i+1}": talib.RSI(data['close'], **param)})
    return results


def macd(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name = sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}

    for i, param in enumerate(params):
        macd_val, signal, hist = talib.MACD(
            data['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        results.update({f"macd_param{i+1}": macd_val,
                       f"macd_signal_param{i+1}": signal, f"macd_hist_param{i+1}": hist})
    return results


def stochastic_oscillator(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        k, d = talib.STOCH(data['high'], data['low'],
                        data['close'], **param)
        results.update({f"stochastic_k_param{i+1}": k,
                       f"stochastic_d_param{i+1}": d})
    return results


def adx(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update(
            {f"adx_param{i+1}": talib.ADX(data['high'], data['low'], data['close'], **param)})
    return results


def ema(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update({f"ema_short_param{i+1}": talib.EMA(data['close'], timeperiod=param['short_period']),
                        f"ema_long_param{i+1}": talib.EMA(data['close'], timeperiod=param['long_period'])})
    return results


def vwap(data: pd.DataFrame) -> Dict[str, pd.Series]:
    typical_price = (data['high'] + data['low'] + data['close']) / 3
    vol_series = data['volume']
    vwap_value = (typical_price * vol_series).cumsum() / vol_series.cumsum()
    return {"vwap": vwap_value}


def atr(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update({f"atr_param{i+1}": talib.ATR(
            data['high'], data['low'], data['close'], **param)})
    return results


def obv(data: pd.DataFrame) -> Dict[str, pd.Series]:
    return {"obv": talib.OBV(data['close'], data['volume'])}


def sar(data: pd.DataFrame) -> Dict[str, pd.Series]:
    return {"sar": talib.SAR(data['high'], data['low'])}


def cci(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update({f"cci_param{i+1}": talib.CCI(data['high'], data['low'],
                          data['close'], **param)})
    return results


def fibonacci_retracements(data: pd.DataFrame) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        fib_res_df = data.pipe(rolling_pipe,fctn=calc_fib_levels, window=param['window'])
        fib_res_df.columns = [f"{col}_param{i+1}" for col in fib_res_df.columns]
        results.update({f"levels_param{i+1}": fib_res_df})
    return results


def ichimoku_cloud(data: pd.DataFrame, conversion_line_period=9, base_line_periods=26, lagging_span2_periods=52, displacement=26) -> Dict[str, pd.Series]:
    f_name=sys._getframe().f_code.co_name
    params = get_param(f_name)
    results = {}
    
    for i,param in enumerate(params):
        results.update(calc_ichimoku_cloud(data, i, param))
    return results
