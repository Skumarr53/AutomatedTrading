# src/trading_logic/trade_execution_manager.py

import os
from loguru import logger
from typing import Optional

import pandas as pd

from src.utils.utils import load_symbols
from src.trading_logic.strategy_manager import StrategyManager
from src.trading_logic.trade_simulator import TradeSimulator
from src.trading_logic.trading_decision import TradingDecision


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
        decision_maker (TradingDecision): Component that determines BUY/SELL/HOLD signals.
    """

    def __init__(
        self,
        base_path: str,
        symbols_file: str,
        strategy_manager: StrategyManager,
        trade_simulator: TradeSimulator,
        decision_maker: TradingDecision,
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
        # config.symbols: list = load_symbols(symbols_file)
        self.strategy_manager: StrategyManager = strategy_manager
        self.trade_simulator: TradeSimulator = trade_simulator
        self.decision_maker: TradingDecision = decision_maker

        logger.info("TradeExecutionManager initialized with %d symbols.", len(config.symbols))

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

        for symbol in config.symbols:
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
                    logger.info("Loaded data for symbol: %s", symbol)
                except Exception as e:
                    logger.error("Failed to load data for symbol '%s': %s", symbol, e)
            else:
                logger.warning("Data file for symbol '%s' does not exist at path: %s", symbol, file_path)

        if not data_loaded:
            raise FileNotFoundError(f"No data files found in directory: {self.base_path}")

        logger.info("All available data loaded successfully.")
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
        logger.info("Starting trade execution cycle.")
        historical_data: pd.DataFrame = self.load_data()

        # Check for required columns
        required_columns = {'symbol', 'close', 'date', 'Majority_Vote_Strategy'}
        if not required_columns.issubset(historical_data.columns):
            missing = required_columns - set(historical_data.columns)
            raise KeyError(f"Missing required columns in data: {missing}")

        data_with_signals: pd.DataFrame = self.strategy_manager.apply_strategies(historical_data)
        decisions = self.decision_maker.generate_decisions(data_with_signals)
        data_with_signals['decision'] = decisions

        # Iterate over each row to execute trades
        for index, row in data_with_signals.iterrows():
            signal: str = row['decision']
            symbol: str = row['symbol']
            close_price: float = row['close']
            trade_date: pd.Timestamp = row['date']

            logger.debug("Processing trade for symbol: %s on %s with signal: %s",
                          symbol, trade_date, signal)

            if signal in {'BUY', 'SELL'}:
                try:
                    self.trade_simulator.execute_trade(signal, symbol, close_price, trade_date)
                    logger.info("Executed %s trade for %s at price %.2f on %s.",
                                 signal, symbol, close_price, trade_date)
                except Exception as e:
                    logger.error("Failed to execute %s trade for %s on %s: %s",
                                  signal, symbol, trade_date, e)
            else:
                logger.debug("No trade executed for symbol: %s on %s (Signal: %s).",
                              symbol, trade_date, signal)

        logger.info("Trade execution cycle completed.")


