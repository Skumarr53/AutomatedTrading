# Coumns
TICKER_COLS = ['epoch_time', 'open', 'high', 'low', 'close', 'volume', 'date']

CUSTOM_CS_COLS = ['candlestick_length', 'body_length', 'body_mid_point', 'is_green', 'body_to_length_ratio', 'candlestick_length_prev_1', 'body_length_prev_1', 'body_mid_point_prev_1', 'is_green_prev_1', 'body_to_length_ratio_prev_1', 'candlestick_length_prev_2', 'body_length_prev_2', 'body_mid_point_prev_2', 'is_green_prev_2',
                  'body_to_length_ratio_prev_2', 'high_1h', 'low_1h', 'high_5h', 'low_5h', 'high_1d', 'low_1d', 'high_3d', 'low_3d', 'high_5d', 'low_5d', 'high_14d', 'low_14d', 'high_52w', 'low_52w', 'volume_pct_change_last_interval', 'volume_pct_change_mean_3', 'volume_pct_change_mean_5', 'hour_of_day', 'day_of_week', 'month_of_year', 'quarter_of_year', 'candlestick_gap']
INDICVATORS_COLS = ['upperband_param1', 'middleband_param1', 'lowerband_param1', 'upperband_param2', 'middleband_param2', 'lowerband_param2', 'upperband_param3', 'middleband_param3', 'lowerband_param3', 'rsi_param1', 'rsi_param2', 'rsi_param3', 'macd_param1', 'signal_param1', 'hist_param1', 'macd_param2', 'signal_param2', 'hist_param2', 'macd_param3', 'signal_param3', 'hist_param3', 'stochastic_k_param1', 'stochastic_d_param1', 'stochastic_k_param2', 'stochastic_d_param2', 'stochastic_k_param3', 'stochastic_d_param3', 'adx_param1', 'adx_param2', 'adx_param3', 'ema_short_param1', 'ema_long_param1',
                    'ema_short_param2', 'ema_long_param2', 'ema_short_param3', 'ema_long_param3', 'vwap', 'atr_param1', 'atr_param2', 'atr_param3', 'obv', 'sar', 'cci_param1', 'cci_param2', 'cci_param3', 'conversion_line_param1', 'base_line_param1', 'leading_span_a_param1', 'leading_span_b_param1', 'lagging_span_param1', 'price_above_cloud_param1', 'conversion_line_param2', 'base_line_param2', 'leading_span_a_param2', 'leading_span_b_param2', 'lagging_span_param2', 'price_above_cloud_param2', 'conversion_line_param3', 'base_line_param3', 'leading_span_a_param3', 'leading_span_b_param3', 'lagging_span_param3', 'price_above_cloud_param3']
ORDER_BOOK_COLS = ['symbol', 'total_buy_qty', 'total_sell_qty', 'open', 'high', 'low', 'close', 'change_percent', 'tick_size', 'change', 'last_traded_qty', 'last_traded_time',
                   'last_traded_price', 'volume', 'average_traded_price', 'lower_circuit', 'upper_circuit', 'expiry', 'open_interest', 'open_interest_flag', 'previous_day_open_interest', 'open_interest_percent']
ORDER_BOOK_DERV_COLS = ['weighted_bid_price', 'total_bid_volume', 'weighted_ask_price', 'total_ask_volume',
                        'spread', 'buy_sell_pressure_ratio', 'intraday_price_range', 'price_movement_open_close']


SHORT_TERM_BENF_COLS = [
    'candlestick_length', 'body_length', 'body_mid_point',  'body_to_length_ratio',
    'candlestick_length_prev_1', 'body_length_prev_1', 'body_mid_point_prev_1', 'body_to_length_ratio_prev_1',
    'candlestick_length_prev_2', 'body_length_prev_2', 'body_mid_point_prev_2', 'body_to_length_ratio_prev_2',
    'high_1h', 'low_1h', 'high_5h', 'low_5h', 'high_1d', 'low_1d',
    'volume_pct_change_last_interval', 'volume_pct_change_mean_3', 'volume_pct_change_mean_5',
    'bollinger_upperband_param1', 'bollinger_middleband_param1', 'bollinger_lowerband_param1',
    'bollinger_upperband_param2', 'bollinger_middleband_param2', 'bollinger_lowerband_param2',
    'bollinger_upperband_param3', 'bollinger_middleband_param3', 'bollinger_lowerband_param3',
    'rsi_param1', 'rsi_param2', 'rsi_param3',
    'macd_param1', 'macd_signal_param1', 'macd_hist_param1',
    'macd_param2', 'macd_signal_param2', 'macd_hist_param2',
    'macd_param3', 'macd_signal_param3', 'macd_hist_param3',
    'stochastic_k_param1', 'stochastic_d_param1',
    'stochastic_k_param2', 'stochastic_d_param2',
    'stochastic_k_param3', 'stochastic_d_param3',
    'adx_param1', 'adx_param2', 'adx_param3',
    'ema_short_param1', 'ema_long_param1', 'ema_short_param2', 'ema_long_param2', 'ema_short_param3', 'ema_long_param3',
    'vwap', 'atr_param1', 'atr_param2', 'atr_param3', 'obv', 'sar', 'cci_param1', 'cci_param2', 'cci_param3',
    'ichimoku_conversion_line_param1', 'ichimoku_base_line_param1', 'ichimoku_leading_span_a_param1', 'ichimoku_leading_span_b_param1', 'ichimoku_lagging_span_param1', 'ichimoku_price_above_cloud_param1',
    'ichimoku_conversion_line_param2', 'ichimoku_base_line_param2', 'ichimoku_leading_span_a_param2', 'ichimoku_leading_span_b_param2', 'ichimoku_lagging_span_param2', 'ichimoku_price_above_cloud_param2',
    'ichimoku_conversion_line_param3', 'ichimoku_base_line_param3', 'ichimoku_leading_span_a_param3', 'ichimoku_leading_span_b_param3', 'ichimoku_lagging_span_param3', 'ichimoku_price_above_cloud_param3'
]

LONG_TERM_BENF_COLS = [
    'epoch_time', 'open', 'high', 'low', 'close', 'volume', 'date',
    'high_14d', 'low_14d', 'high_52w', 'low_52w',
    'total_buy_qty', 'total_sell_qty',
    'change_percent', 'tick_size', 'change', 'last_traded_qty', 'last_traded_time', 'last_traded_price',
    'average_traded_price', 'lower_circuit', 'upper_circuit', 'expiry', 'open_interest', 'open_interest_flag', 'previous_day_open_interest', 'open_interest_percent',
    'weighted_bid_price', 'total_bid_volume', 'weighted_ask_price', 'total_ask_volume', 'spread', 'buy_sell_pressure_ratio', 'intraday_price_range', 'price_movement_open_close'
]


CAT_COLS = [
    'is_green', 'is_green_prev_1', 'is_green_prev_2', 'symbol', 'hour_of_day', 'day_of_week', 'month_of_year', 'quarter_of_year'
]
