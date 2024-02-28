from dataclasses import dataclass, field
from typing import Dict
import pandas as pd
from feature_engineering.custom_features_extraction import FeatureExtraction
from feature_engineering.technical_indicators import TechnicalIndicators
from feature_engineering.orderbook_features_extraction import OrderBookDataTransformer
from feature_engineering.candlestick_patterns_features import CandlestickPatternRecognizer
from config import config


@dataclass
class DataAggregator:
    feature_extractor: FeatureExtraction = field(
        default_factory=FeatureExtraction)
    indicator_generator: TechnicalIndicators = field(
        default_factory=TechnicalIndicators)
    order_book_transformer: OrderBookDataTransformer = field(
        default_factory=OrderBookDataTransformer)
    cs_pattern_recognizer: CandlestickPatternRecognizer = field(
        default_factory=CandlestickPatternRecognizer)
    
    def aggregate_order_book(self, order_books, is_bid=True):
        """
        Aggregates 1-minute order book snapshots into a 5-minute window.
        
        :param order_books: List of dictionaries representing 1-minute snapshots of order books
        :return: Aggregated order book with top 5 bids and asks
        """
        # Initialize containers for all bids and asks in the window
        all_ords = [] 
        all_ords.extend(order_books)

        # Sort and select top 5 bids
        if is_bid:
            top_ords = sorted(all_ords, key=lambda x: x['price'], reverse=True)[:5]
        # Sort and select top 5 asks
        else:
            top_ords = sorted(all_ords, key=lambda x: x['price'])[:5]

        return top_ords

    def aggregate_ticker_to_run_min(self,data_dict: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates 1-minute candlestick data to 5-minute intervals.

        Parameters:
            data (pd.DataFrame): DataFrame containing 1-minute candlestick data with columns:
                                ['epoch_time', 'open', 'high', 'low', 'close', 'volume', 'date']

        Returns:
            pd.DataFrame: Aggregated 5-minute candlestick data.
        """
        ticker_agg_derived = {}
        for symbol, data in data_dict.items():

            # Ensure 'date' column is in datetime format for resampling
            
            # Set 'date' as the index
            data.set_index('date', inplace=True)
            
            # Resample data to 5-minute intervals
            resampled_data = data.resample(f'{config.TRADE_RUN_INTERVAL_MIN}T').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            # Reset index to make 'date' a column again
            resampled_data.reset_index(inplace=True)

            ticker_agg_derived[symbol] = resampled_data
        
        return ticker_agg_derived

    def aggregate_features(self, ticker_data, order_book_data) -> Dict[str, pd.DataFrame]:
        """
        Aggregate features from various components and combine them with the original data.
        """
        # Aggregate features based on ticker data
        combined_ticker_data = self._aggregate_ticker_data(ticker_data)

        # Aggregate features based on order book data
        combined_order_book_data = self._aggregate_order_book_data(
            order_book_data)

        # Merge ticker and order book data features
        combined_data = self._merge_ticker_and_order_book_data(
            combined_ticker_data, combined_order_book_data)

        return combined_data

    def _aggregate_ticker_data(self, ticker_data) -> Dict[str, pd.DataFrame]:
        """
        Generate and combine features based on ticker data.
        """
        ## Add aggregate funtion
        if config.DATA_FETCH_CRON_INTERVAL_MIN != config.TRADE_RUN_INTERVAL_MIN:
            ticker_data = self.aggregate_ticker_to_run_min(ticker_data)
        
        ticker_features = self.feature_extractor.generate_features(
            ticker_data)
        indicator_features = self.indicator_generator.get_stock_indicators(
            ticker_data)
        indicator_features = self.cs_pattern_recognizer.recognize_patterns(
            ticker_data)

        combined_ticker_data = self._combine_features(ticker_data,
            ticker_features, indicator_features)
        return combined_ticker_data
    
    def aggregate_order_book_data_to_run_min(self, order_book_data_dict):
        # Convert index back to a column for resampling
        order_features_derived = {}
        for symbol, order_book_data in order_book_data_dict.items():

            order_book_data.reset_index(inplace=True)
            
            # Prepare the DataFrame for resampling by setting the index to the datetime column
            order_book_data.set_index('last_traded_time', inplace=True)

            # Aggregate main data into 5-minute intervals
            aggregated_data = order_book_data.resample(f'{config.TRADE_RUN_INTERVAL_MIN}T').agg({
                'symbol': 'last',
                'total_buy_qty': 'sum',
                'total_sell_qty': 'sum',
                'ask': lambda x: self.aggregate_order_book(x, is_bid=True),
                'bids': lambda x: self.aggregate_order_book(x, is_bid=False),
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'tick_size': 'last',
                'change': lambda x: x.iloc[-1] - x.iloc[0],
                'last_traded_qty': 'sum',
                'volume': 'sum',
                'average_traded_price': lambda x: np.average(x, weights=order_book_data.loc[x.index, 'volume']),
                'lower_circuit': 'last',
                'upper_circuit': 'last',
                'expiry': 'last',
                'open_interest': 'sum',
                'open_interest_flag': 'last',
                'previous_day_open_interest': 'last',
                'open_interest_percent': 'last'
            })

            # Calculate change_percent based on aggregated open and close
            aggregated_data['change_percent'] = (aggregated_data['close'] - aggregated_data['open']) / aggregated_data['open'] * 100

            order_features_derived[symbol] = aggregated_data

        return order_features_derived


    def _aggregate_order_book_data(self, order_book_data) -> Dict[str, pd.DataFrame]:
        """
        Generate and combine features based on order book data.
        """
        
        if config.DATA_FETCH_CRON_INTERVAL_MIN != config.TRADE_RUN_INTERVAL_MIN:
            order_book_data = self.aggregate_order_book_data_to_run_min(
                order_book_data)
        
        if config.TRADE_MODE == 'LIVE':
            order_book_data = order_book_data.iloc[-1:]

        order_book_features = self.order_book_transformer.transform(
            order_book_data)
        

        combined_order_data = self._combine_features(order_book_data,
                                                     order_book_features)
        return combined_order_data

    def _merge_ticker_and_order_book_data(self, ticker_data: Dict[str, pd.DataFrame], order_book_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Merge ticker and order book data features into a single DataFrame per symbol.
        """
        combined_data = {}
        for symbol in set(ticker_data) | set(order_book_data):
            combined_df = pd.DataFrame()
            if symbol in ticker_data:
                combined_df = pd.concat(
                    [combined_df, ticker_data[symbol]], axis=1)
            if symbol in order_book_data:
                combined_df = pd.concat(
                    [combined_df, order_book_data[symbol]], axis=1)
            combined_data[symbol] = combined_df
        return combined_data


    def _combine_features(self, *args: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Helper method to combine multiple feature DataFrames more efficiently.
        """
        combined_data = {}
        # Collect all DataFrames for each symbol before concatenating
        for data_dict in args:
            for symbol, df in data_dict.items():
                if 'asks' in df.columns:       
                    df = df.drop(
                        columns=['asks', 'bids'], errors='ignore')
                if symbol not in combined_data:

                    combined_data[symbol] = [df]
                else:
                    combined_data[symbol].append(df)

        # Concatenate all collected DataFrames for each symbol at once
        for symbol in combined_data:
            combined_data[symbol] = pd.concat(combined_data[symbol], axis=1)

        return combined_data
