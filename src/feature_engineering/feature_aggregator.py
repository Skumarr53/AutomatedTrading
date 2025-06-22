import ast
from dataclasses import dataclass, field
from typing import Dict
import pandas as pd
from src.feature_engineering.custom_features_extraction import FeatureExtraction
from src.feature_engineering.technical_indicators import TechnicalIndicators
from src.feature_engineering.orderbook_features_extraction import OrderBookDataTransformer
from src.feature_engineering.candlestick_patterns_features import CandlestickPatternRecognizer
from src import config
from src.utils.utils import get_trunc_output


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
        all_ords = order_books.item()
        all_ords = ast.literal_eval(all_ords.replace('\n',',')) if isinstance(all_ords, str) else all_ords

        # Sort and select top 5 bids
        if is_bid:
            top_ords = sorted(all_ords, key=lambda x: x['price'], reverse=True)[:5]
        # Sort and select top 5 asks
        else:
            top_ords = sorted(all_ords, key=lambda x: x['price'])[:5]

        return top_ords

    def aggregate_ticker_to_run_min(self,data: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregates 1-minute candlestick data to 5-minute intervals.

        Parameters:
            data (pd.DataFrame): DataFrame containing 1-minute candlestick data with columns:
                                ['epoch_time', 'open', 'high', 'low', 'close', 'volume', 'date']

        Returns:
            pd.DataFrame: Aggregated 5-minute candlestick data.
        """


            # Ensure 'date' column is in datetime format for resampling
            
        # Set 'date' as the index
        # data.set_index('date', inplace=True)
        
        # Resample data to 5-minute intervals
        resampled_data = data.resample(f'{config.scheduler.trade_run_interval_min}T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        # Reset index to make 'date' a column again
        # resampled_data.reset_index(inplace=True)
        
        return resampled_data
    
    @staticmethod
    def data_set_index(data, col_name):
        data.set_index(col_name, inplace=True)
        data.index = pd.to_datetime(data.index)
        return data
    
    def df_cleaup_transform(self, ticker_data, order_book_data):

        ticker_data = ticker_data[ticker_data.volume != 0]

        ticker_data = self.data_set_index(ticker_data, 'date')
        order_book_data = self.data_set_index(order_book_data, 'last_traded_time')

        order_book_data = order_book_data[~order_book_data.index.duplicated(keep='first')]
        order_book_data = order_book_data.drop(columns = [col for col in config.columns.ticker_cols if col in order_book_data.columns])
        return ticker_data, order_book_data

    def aggregate_features(self, ticker_data, order_book_data) -> Dict[str, pd.DataFrame]:
        """
        Aggregate features from various components and combine them with the original data.
        """

        ticker_data,  order_book_data = self.df_cleaup_transform(ticker_data,  order_book_data)
        # Aggregate features based on ticker data
        combined_ticker_data = self._aggregate_ticker_data(ticker_data)
        # Aggregate features based on order book data
        combined_order_book_data = self._aggregate_order_book_data(
            order_book_data)

        # comment out in PROD Merge ticker and order book data features
        dfs = []
        
        for df in [*combined_ticker_data, *combined_order_book_data]: dfs.append(df.reset_index(drop=True))
        combined_data = pd.concat(dfs, axis=1, join='inner')

        # combined_data = pd.concat([*combined_ticker_data, *combined_order_book_data], axis=1,join='inner')

        return combined_data

    def _aggregate_ticker_data(self, ticker_data) -> Dict[str, pd.DataFrame]:
        """
        Generate and combine features based on ticker data.
        """
        # ticker_data.set_index('epoch_time', inplace=True)
        # ticker_data.index = pd.to_datetime(ticker_data.index)
        ## Add aggregate funtion
        if config.scheduler.data_fetch_cron_interval_min != config.scheduler.trade_run_interval_min:
            ticker_data = self.aggregate_ticker_to_run_min(ticker_data)
        

        ticker_features = self.feature_extractor.generate_features(
            ticker_data)
        indicator_features = self.indicator_generator.compute_indicators(
            ticker_data)
        cs_pattern_features = self.cs_pattern_recognizer.recognize_patterns(
            ticker_data)
        ticker_data = get_trunc_output(ticker_data)
        return ticker_data, ticker_features, indicator_features, cs_pattern_features
    
    def aggregate_order_book_data_to_run_min(self, order_book_data):
        # Convert index back to a column for resampling
        # order_features_derived = {}
        # for symbol, order_book_data in order_book_data_dict.items():

        # order_book_data.reset_index(inplace=True)
        
        # # Prepare the DataFrame for resampling by setting the index to the datetime column
        # order_book_data.set_index('last_traded_time', inplace=True)

        # Aggregate main data into 5-minute intervals
        aggregated_data = order_book_data.resample(f'{config.scheduler.trade_run_interval_min}T').agg({
            'symbol': 'last',
            'total_buy_qty': 'sum',
            'total_sell_qty': 'sum',
            'asks': lambda x: self.aggregate_order_book(x, is_bid=True),
            'bids': lambda x: self.aggregate_order_book(x, is_bid=False),
            # 'open': 'first',
            # 'high': 'max',
            # 'low': 'min',
            # 'close': 'last',
            'tick_size': 'last',
            'change': lambda x: x.iloc[-1] - x.iloc[0],
            'last_traded_qty': 'sum',
            # 'volume': 'sum',
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

        return aggregated_data

    def _aggregate_order_book_data(self, order_book_data) -> pd.DataFrame:
        """
        Generate and combine features based on order book data.
        """
        
        # if config.scheduler.data_fetch_cron_interval_min != config.scheduler.trade_run_interval_min:
        #     order_book_data = self.aggregate_order_book_data_to_run_min(
        #         order_book_data)
        
        # if config.trading_config.trade_mode == 'LIVE':
        #     order_book_data = order_book_data.iloc[-1:]
        
        order_book_data.index.rename('date', inplace=True)

        order_book_data = get_trunc_output(order_book_data)

        order_book_features = self.order_book_transformer.transform(
            order_book_data)
        
        order_book_data = order_book_data.drop(columns=['asks', 'bids'], errors='ignore')
        return order_book_data, order_book_features

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


