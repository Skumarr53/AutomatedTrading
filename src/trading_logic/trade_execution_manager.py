# src/trading_logic/trade_execution_manager.py

import os
import logging
from typing import Optional

import pandas as pd

from src.utils.utils import load_symbols
from src.trading_logic.strategy_manager import StrategyManager
from src.trading_logic.trade_simulator import TradeSimulator


class TradeExecutionManager:
    """
    Manages the execution of trading strategies by handling data loading, strategy application,
    and trade simulation.

    The `TradeExecutionManager` class orchestrates the complete trade cycle, which includes:
        1. Loading historical market data for specified symbols.
        2. Applying trading strategies to generate signals.
        3. Simulating trades based on generated signals.

    Attributes:
        base_path (str): Directory containing the CSV files with historical data.
        symbols (List[str]): List of stock symbols to process.
        strategy_manager (StrategyManager): Instance responsible for applying trading strategies.
        trade_simulator (TradeSimulator): Instance responsible for simulating trade executions.
    """

    def __init__(
        self,
        base_path: str,
        symbols_file: str,
        strategy_manager: StrategyManager,
        trade_simulator: TradeSimulator
    ) -> None:
        """
        Initializes the TradeExecutionManager with data loading, strategy execution,
        and trade simulation capabilities.

        Args:
            base_path (str): Directory containing the CSV files with historical data.
            symbols_file (str): Path to the file containing stock symbols.
            strategy_manager (StrategyManager): An instance of StrategyManager for applying strategies.
            trade_simulator (TradeSimulator): An instance of TradeSimulator for executing trades.
        """
        self.base_path: str = base_path
        self.symbols: list = load_symbols(symbols_file)
        self.strategy_manager: StrategyManager = strategy_manager
        self.trade_simulator: TradeSimulator = trade_simulator

        logging.info("TradeExecutionManager initialized with %d symbols.", len(self.symbols))

    def load_data(self) -> pd.DataFrame:
        """
        Loads and concatenates historical data for all specified symbols.

        It reads CSV files for each symbol from the `base_path` directory. Each CSV file is
        expected to contain historical market data for a specific symbol. The method adds a
        'symbol' column to each DataFrame for identification and concatenates all data into
        a single DataFrame.

        Returns:
            pd.DataFrame: A concatenated DataFrame containing historical data for all symbols.

        Raises:
            FileNotFoundError: If no data files are found in the specified directory.
        """
        all_data = pd.DataFrame()
        data_loaded = False

        for symbol in self.symbols:
            file_path = os.path.join(self.base_path, f"{symbol}_data.csv")
            if os.path.exists(file_path):
                try:
                    symbol_data = pd.read_csv(
                        file_path,
                        on_bad_lines="skip",
                        engine="python",
                        parse_dates=['date'])
                    symbol_data['symbol'] = symbol
                    all_data = pd.concat([all_data, symbol_data], ignore_index=True)
                    data_loaded = True
                    logging.info("Loaded data for symbol: %s", symbol)
                except Exception as e:
                    logging.error("Failed to load data for symbol '%s': %s", symbol, e)
            else:
                logging.warning("Data file for symbol '%s' does not exist at path: %s", symbol, file_path)

        if not data_loaded:
            raise FileNotFoundError(f"No data files found in directory: {self.base_path}")

        logging.info("All available data loaded successfully.")
        return all_data

    def execute_trade_cycle(self) -> None:
        """
        Executes the complete trade cycle, which includes data loading, strategy execution,
        and trade simulation.

        The method performs the following steps:
            1. Loads historical market data for all specified symbols.
            2. Applies all configured trading strategies to generate signals.
            3. Iterates over each row of the data with signals and executes trades based on
               the 'Majority_Vote_Strategy' signal.

        Raises:
            KeyError: If the 'Majority_Vote_Strategy' column is missing in the data.
        """
        logging.info("Starting trade execution cycle.")
        historical_data: pd.DataFrame = self.load_data()

        # Check for required columns
        required_columns = {'symbol', 'close', 'date', 'Majority_Vote_Strategy'}
        if not required_columns.issubset(historical_data.columns):
            missing = required_columns - set(historical_data.columns)
            raise KeyError(f"Missing required columns in data: {missing}")

        data_with_signals: pd.DataFrame = self.strategy_manager.apply_strategies(historical_data)

        # Iterate over each row to execute trades
        for index, row in data_with_signals.iterrows():
            signal: str = row['Majority_Vote_Strategy']
            symbol: str = row['symbol']
            close_price: float = row['close']
            trade_date: pd.Timestamp = row['date']

            logging.debug("Processing trade for symbol: %s on %s with signal: %s",
                          symbol, trade_date, signal)

            if signal in {'BUY', 'SELL'}:
                try:
                    self.trade_simulator.execute_trade(signal, symbol, close_price, trade_date)
                    logging.info("Executed %s trade for %s at price %.2f on %s.",
                                 signal, symbol, close_price, trade_date)
                except Exception as e:
                    logging.error("Failed to execute %s trade for %s on %s: %s",
                                  signal, symbol, trade_date, e)
            else:
                logging.debug("No trade executed for symbol: %s on %s (Signal: %s).",
                              symbol, trade_date, signal)

        logging.info("Trade execution cycle completed.")


# Example of setting up and using the TradeExecutionManager class
if __name__ == "__main__":
    import logging
    import pandas as pd
    from strategy_manager import StrategyManager
    from trading_logic.trade_simulator import TradeSimulator

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

    # Example trading strategy functions
    def example_buy_strategy(row: pd.Series) -> str:
        if row['close'] < row['bollinger_lower'] and row['rsi'] < 30:
            return 'BUY'
        return 'HOLD'

    def example_sell_strategy(row: pd.Series) -> str:
        if row['close'] > row['bollinger_upper'] and row['rsi'] > 70:
            return 'SELL'
        return 'HOLD'

    def majority_vote_strategy(row: pd.Series) -> str:
        buy = 0
        sell = 0
        hold = 0
        # Example: Assume strategies are 'BuyStrategy' and 'SellStrategy'
        if row['BuyStrategy'] == 'BUY':
            buy += 1
        if row['SellStrategy'] == 'SELL':
            sell += 1
        # Majority voting logic
        if buy > sell:
            return 'BUY'
        elif sell > buy:
            return 'SELL'
        else:
            return 'HOLD'

    # Define technical and additional strategies
    technical_strategies = {
        'BuyStrategy': example_buy_strategy,
        'SellStrategy': example_sell_strategy
    }

    additional_strategies = {
        'Majority_Vote_Strategy': majority_vote_strategy
    }

    # Initialize StrategyManager
    strategy_manager = StrategyManager(
        technical_strategies=technical_strategies,
        additional_strategies=additional_strategies
    )

    # Initialize TradeSimulator (Assuming TradeSimulator is properly implemented)
    trade_simulator = TradeSimulator()

    # Initialize TradeExecutionManager
    trade_execution_manager = TradeExecutionManager(
        base_path='path/to/data',
        symbols_file='path/to/symbols_file.txt',
        strategy_manager=strategy_manager,
        trade_simulator=trade_simulator
    )

    # Execute the trade cycle
    try:
        trade_execution_manager.execute_trade_cycle()
    except Exception as e:
        logging.error("An error occurred during the trade execution cycle: %s", e)
