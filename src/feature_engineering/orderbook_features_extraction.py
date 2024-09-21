import pandas as pd
import numpy as np
from datetime import datetime
from src.config import config

class OrderBookDataTransformer:
    def __init__(self):
        pass

    def transform(self, data: dict) -> pd.DataFrame:
        """
        Orchestrates the transformation of raw order book data for a given symbol and returns only new features.
        """
        # basic_info_df = self.extract_basic_info(data)
        condensed_info_df = self.add_condensed_order_book_info(data)
        derived_variables_df = self.add_derived_variables(
            data)

        return pd.concat([condensed_info_df, derived_variables_df], axis=1)

    def add_condensed_order_book_info(self, data: dict) -> pd.DataFrame:
        """
        Adds condensed information from bids and asks, returns a DataFrame.
        """
        bids, asks = data["bids"], data["asks"]
        weighted_bid_price, total_bid_volume = self.calculate_weighted_price_and_volume(
            bids)
        weighted_ask_price, total_ask_volume = self.calculate_weighted_price_and_volume(
            asks)
        spread = weighted_ask_price - weighted_bid_price

        condensed_info_df = pd.DataFrame([{
            'weighted_bid_price': weighted_bid_price,
            'total_bid_volume': total_bid_volume,
            'weighted_ask_price': weighted_ask_price,
            'total_ask_volume': total_ask_volume,
            'spread': spread
        }])
        return condensed_info_df

    @staticmethod
    def calculate_metrics(order):
        volumes = np.array([item['volume'] for item in order])
        prices = np.array([item['price'] for item in order])
        total_volume = volumes.sum()
        weighted_price = np.dot(prices, volumes) / \
            total_volume if total_volume else 0
        return pd.Series([total_volume, weighted_price], index=['total_volume', 'weighted_price'])
    
    def calculate_weighted_price_and_volume(self, orders):
        metrics_df = orders.apply(lambda order: self.calculate_metrics(order))
        return metrics_df['weighted_price'], metrics_df['total_volume']

    def add_derived_variables(self, basic_info_df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds additional derived variables for in-depth analysis, returns a DataFrame of new features.
        """
        derived_df = pd.DataFrame(index=basic_info_df.index)
        derived_df['buy_sell_pressure_ratio'] = basic_info_df['total_buy_qty'] / \
            basic_info_df['total_sell_qty']
        derived_df['intraday_price_range'] = basic_info_df['high'] - \
            basic_info_df['low']
        derived_df['price_movement_open_close'] = (
            basic_info_df['close'] - basic_info_df['open']) / basic_info_df['open']

        # Select only the newly added columns, excluding the original order book data columns

        return derived_df
