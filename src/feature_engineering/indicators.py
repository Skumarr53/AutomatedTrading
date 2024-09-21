# src/feature_engineering/indicators.py
import talib
import sys
import pandas as pd
from typing import Dict, List, Any, Callable
from src.config import config
from src.config.config import MODEL_CONFIG


def rolling_pipe(dataframe: pd.DataFrame, window: int, fctn: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
    """
    Applies a rolling window function to the dataframe and concatenates the results.

    Args:
        dataframe (pd.DataFrame): The input DataFrame to process.
        window (int): The size of the rolling window.
        fctn (Callable[[pd.DataFrame], pd.Series]): The function to apply to each rolling window.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated results of the rolling function.
    """
    return pd.concat([
        dataframe.iloc[i - window: i].pipe(fctn) if i >= window else None
        for i in range(1, len(dataframe) + 1)
    ], axis=1).T


def get_param(func_name: str) -> List[Dict[str, int]]:
    """
    Retrieves the parameters for a given technical indicator function from the configuration.

    Args:
        func_name (str): The name of the technical indicator function.

    Returns:
        List[Dict[str, int]]: A list of dictionaries containing parameters for the function.
    """
    function_params = {
        key[len(func_name) + 2:]: value
        for key, value in MODEL_CONFIG['TECHNICAL_INDICATORS_PARAMS'].items()
        if key.startswith(func_name)
    }
    return [
        {key: values[i] for key, values in function_params.items()}
        for i in range(3)
    ]


def calc_fib_levels(df: pd.DataFrame) -> pd.Series:
    """
    Calculates Fibonacci retracement levels based on the recent high and low prices.

    Args:
        df (pd.DataFrame): DataFrame containing 'high' and 'low' columns.

    Returns:
        pd.Series: A Series containing Fibonacci levels with the index matching the last row of the input DataFrame.
    """
    recent_high: float = df['high'].max()
    recent_low: float = df['low'].min()
    retracements: Dict[str, float] = {
        f'fib_level_{str(int(level * 1000))}': recent_low + (recent_high - recent_low) * level
        for level in config.FIB_LEVELS
    }
    return pd.Series(retracements, name=df.iloc[-1].name)


def calc_ichimoku_cloud(data: pd.DataFrame, i: int, param: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates Ichimoku Cloud components based on the provided parameters.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.
        i (int): The index of the parameter set.
        param (Dict[str, Any]): Dictionary containing Ichimoku parameters.

    Returns:
        Dict[str, Any]: A dictionary containing Ichimoku Cloud components.
    """
    conversion_line = (
        data['high'].rolling(window=param["conversion_line_period"]).max() +
        data['low'].rolling(window=param["conversion_line_period"]).min()
    ) / 2
    base_line = (
        data['high'].rolling(window=param["base_line_periods"]).max() +
        data['low'].rolling(window=param["base_line_periods"]).min()
    ) / 2
    leading_span_a = (conversion_line + base_line) / 2
    leading_span_b = (
        data['high'].rolling(window=param["lagging_span2_periods"]).max() +
        data['low'].rolling(window=param["lagging_span2_periods"]).min()
    ) / 2
    lagging_span = data['close'].shift(-param["displacement"])
    price_above_cloud = data['close'] > max(
        leading_span_a.iloc[-param["displacement"]],
        leading_span_b.iloc[-param["displacement"]]
    )
    return {
        f"ichimoku_conversion_line_param{i + 1}": conversion_line,
        f"ichimoku_base_line_param{i + 1}": base_line,
        f"ichimoku_leading_span_a_param{i + 1}": leading_span_a,
        f"ichimoku_leading_span_b_param{i + 1}": leading_span_b,
        f"ichimoku_lagging_span_param{i + 1}": lagging_span,
        f"ichimoku_price_above_cloud_param{i + 1}": price_above_cloud
    }


def bollinger_bands(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates Bollinger Bands for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing a 'close' column.

    Returns:
        Dict[str, pd.Series]: A dictionary containing upper, middle, and lower Bollinger Bands for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)

    results: Dict[str, pd.Series] = {}
    for i, param in enumerate(params):
        upperband, middleband, lowerband = talib.BBANDS(data['close'], **param)
        results.update({
            f"bollinger_upperband_param{i + 1}": upperband,
            f"bollinger_middleband_param{i + 1}": middleband,
            f"bollinger_lowerband_param{i + 1}": lowerband
        })
    return results


def rsi(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Relative Strength Index (RSI) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing a 'close' column.

    Returns:
        Dict[str, pd.Series]: A dictionary containing RSI values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update({
            f"rsi_param{i + 1}": talib.RSI(data['close'], **param)
        })
    return results


def macd(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Moving Average Convergence Divergence (MACD) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing a 'close' column.

    Returns:
        Dict[str, pd.Series]: A dictionary containing MACD, signal, and histogram values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        macd_val, signal, hist = talib.MACD(
            data['close'],
            fastperiod=param.get('fastperiod', 12),
            slowperiod=param.get('slowperiod', 26),
            signalperiod=param.get('signalperiod', 9)
        )
        results.update({
            f"macd_param{i + 1}": macd_val,
            f"macd_signal_param{i + 1}": signal,
            f"macd_hist_param{i + 1}": hist
        })
    return results


def stochastic_oscillator(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Stochastic Oscillator for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing %K and %D values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        k, d = talib.STOCH(
            data['high'],
            data['low'],
            data['close'],
            **param
        )
        results.update({
            f"stochastic_k_param{i + 1}": k,
            f"stochastic_d_param{i + 1}": d
        })
    return results


def adx(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Average Directional Index (ADX) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing ADX values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update({
            f"adx_param{i + 1}": talib.ADX(
                data['high'],
                data['low'],
                data['close'],
                **param
            )
        })
    return results


def ema(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates Exponential Moving Averages (EMA) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing a 'close' column.

    Returns:
        Dict[str, pd.Series]: A dictionary containing short and long EMAs for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update({
            f"ema_short_param{i + 1}": talib.EMA(data['close'], timeperiod=param.get('short_period', 12)),
            f"ema_long_param{i + 1}": talib.EMA(data['close'], timeperiod=param.get('long_period', 26))
        })
    return results


def vwap(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Volume Weighted Average Price (VWAP) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', 'close', and 'volume' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing the VWAP values.
    """
    typical_price: pd.Series = (data['high'] + data['low'] + data['close']) / 3
    vol_series: pd.Series = data['volume']
    vwap_value: pd.Series = (typical_price * vol_series).cumsum() / vol_series.cumsum()
    return {"vwap": vwap_value}


def atr(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Average True Range (ATR) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing ATR values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update({
            f"atr_param{i + 1}": talib.ATR(
                data['high'],
                data['low'],
                data['close'],
                **param
            )
        })
    return results


def obv(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the On-Balance Volume (OBV) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'close' and 'volume' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing the OBV values.
    """
    return {"obv": talib.OBV(data['close'], data['volume'])}


def sar(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Parabolic SAR (Stop and Reverse) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high' and 'low' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing the SAR values.
    """
    return {"sar": talib.SAR(data['high'], data['low'])}


def cci(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates the Commodity Channel Index (CCI) for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing CCI values for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, int]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update({
            f"cci_param{i + 1}": talib.CCI(
                data['high'],
                data['low'],
                data['close'],
                **param
            )
        })
    return results


def fibonacci_retracements(data: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculates Fibonacci retracement levels for the provided data using rolling windows.

    Args:
        data (pd.DataFrame): DataFrame containing 'high' and 'low' columns.

    Returns:
        Dict[str, pd.Series]: A dictionary containing Fibonacci levels for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, Any]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        fib_res_df: pd.DataFrame = data.pipe(rolling_pipe, fctn=calc_fib_levels, window=param.get('window', 14))
        fib_res_df.columns = [f"{col}_param{i + 1}" for col in fib_res_df.columns]
        results.update({f"levels_param{i + 1}": fib_res_df})
    return results


def ichimoku_cloud(
    data: pd.DataFrame,
    conversion_line_period: int = 9,
    base_line_periods: int = 26,
    lagging_span2_periods: int = 52,
    displacement: int = 26
) -> Dict[str, pd.Series]:
    """
    Calculates Ichimoku Cloud components for the provided data.

    Args:
        data (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.
        conversion_line_period (int, optional): Period for the conversion line. Defaults to 9.
        base_line_periods (int, optional): Period for the base line. Defaults to 26.
        lagging_span2_periods (int, optional): Period for the leading span B. Defaults to 52.
        displacement (int, optional): Displacement for the lagging span. Defaults to 26.

    Returns:
        Dict[str, pd.Series]: A dictionary containing Ichimoku Cloud components for each parameter set.
    """
    f_name: str = sys._getframe().f_code.co_name
    params: List[Dict[str, Any]] = get_param(f_name)
    results: Dict[str, pd.Series] = {}

    for i, param in enumerate(params):
        results.update(calc_ichimoku_cloud(data, i, param))
    return results
