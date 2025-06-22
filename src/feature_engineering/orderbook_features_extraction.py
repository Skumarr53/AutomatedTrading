# src/feature_engineering/orderbook_features_extraction.py
import ast
import pandas as pd
import numpy as np
from loguru import logger
from typing import Dict, Any, List, Tuple
from src.utils.utils import get_trunc_output


class OrderBookDataTransformer:
    """
    Transforms raw order book data into structured features for automated trading systems.

    Attributes:
        None
    """

    def __init__(self) -> None:
        """
        Initializes the OrderBookDataTransformer instance.
        """
        pass

    def transform(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Orchestrates the transformation of raw order book data for a given symbol
        and returns combined condensed and derived feature DataFrames.

        Args:
            data (Dict[str, Any]): Raw order book data for a symbol containing 'bids' and 'asks'.

        Returns:
            pd.DataFrame: A concatenated DataFrame containing condensed order book information
                          and derived variables.
        """
        condensed_info_df: pd.DataFrame = self.add_condensed_order_book_info(data)
        derived_variables_df: pd.DataFrame = self.add_derived_variables(
            condensed_info_df
        )

        return pd.concat([condensed_info_df, derived_variables_df], axis=1)
    
    def add_condensed_order_book_info(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Adds condensed information from bids and asks, including weighted prices,
        total volumes, and spread, and returns it as a DataFrame.

        Args:
            data (Dict[str, Any]): Raw order book data containing 'bids' and 'asks'.

        Returns:
            pd.DataFrame: DataFrame containing condensed order book information.
        """


        clean_str2pyobj =  lambda bid: ast.literal_eval(bid.replace("'", '"').replace("\n", ',')) if isinstance(bid, str) else bid
                                     
        bids: List[Dict[str, Any]] = data.bids.apply(clean_str2pyobj)
        asks: List[Dict[str, Any]] = data.asks.apply(clean_str2pyobj)

        weighted_bid_price, total_bid_volume = self.calculate_weighted_price_and_volume(
            bids
        )
        weighted_ask_price, total_ask_volume = self.calculate_weighted_price_and_volume(
            asks
        )
        spread: float = weighted_ask_price - weighted_bid_price

        condensed_info_df: pd.DataFrame = pd.DataFrame(
                {
                    "weighted_bid_price": weighted_bid_price,
                    "total_bid_volume": total_bid_volume,
                    "weighted_ask_price": weighted_ask_price,
                    "total_ask_volume": total_ask_volume,
                    "spread": spread
                }
                , index = data.index
        )

        return get_trunc_output(condensed_info_df)

    @staticmethod
    def calculate_metrics(order: List[Dict[str, Any]]) -> pd.Series:
        """
        Calculates total volume and weighted price for a given set of orders.

        Args:
            order (List[Dict[str, Any]]): List of orders, each containing 'price' and 'volume'.

        Returns:
            pd.Series: Series containing 'total_volume' and 'weighted_price'.
        """
        volumes: np.ndarray = np.array([item["volume"] for item in order])
        prices: np.ndarray = np.array([item["price"] for item in order])
        total_volume: float = volumes.sum()
        weighted_price: float = (
            np.dot(prices, volumes) / total_volume if total_volume else 0.0
        )

        return pd.Series(
            [total_volume, weighted_price], index=["total_volume", "weighted_price"]
        )

    def calculate_weighted_price_and_volume(
        self, orders: List[Dict[str, Any]]
    ) -> Tuple[float, float]:
        """
        Calculates the weighted price and total volume for a list of orders.

        Args:
            orders (List[Dict[str, Any]]): List of orders, each containing 'price' and 'volume'.

        Returns:
            Tuple[float, float]: A tuple containing weighted price and total volume.
        """
        metrics_df: pd.DataFrame = pd.DataFrame(
            [self.calculate_metrics(order) for order in orders]
        )

        weighted_price = metrics_df["weighted_price"].values
        total_volume = metrics_df["total_volume"].values

        return weighted_price, total_volume

    def add_derived_variables(self, condensed_info_df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds additional derived variables for in-depth analysis, such as buy/sell pressure ratio,
        intraday price range, and price movement from open to close.

        Args:
            condensed_info_df (pd.DataFrame): DataFrame containing condensed order book information.

        Returns:
            pd.DataFrame: DataFrame containing derived variables.
        """
        derived_df: pd.DataFrame = pd.DataFrame(index=condensed_info_df.index)
        derived_df["buy_sell_pressure_ratio"] = (
            (
                condensed_info_df["total_bid_volume"]
                / condensed_info_df["total_ask_volume"]
            )
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0)
        )

        return derived_df
