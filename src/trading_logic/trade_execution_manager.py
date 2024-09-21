# Import necessary modules and classes
import os
import pandas as pd
from utils.utils import load_symbols
from strategy_manager import StrategyManager
from trading_logic.trade_simulator import TradeSimulator


class TradeExecutionManager:
    def __init__(self, base_path: str, symbols_file: str, strategy_manager: StrategyManager, trade_simulator: TradeSimulator):
        """
        Initialize the TradeExecutionManager with data loading, strategy execution, and trade simulation capabilities.
        :param base_path: Directory containing the csv files.
        :param symbols_file: Path to the file containing stock symbols.
        :param strategy_manager: An instance of StrategyManager for applying strategies.
        :param trade_simulator: An instance of TradeSimulator for executing trades.
        """
        self.base_path = base_path
        self.symbols = load_symbols(symbols_file)
        self.strategy_manager = strategy_manager
        self.trade_simulator = trade_simulator

    def load_data(self) -> pd.DataFrame:
        """Load and concatenate historical data for all specified symbols."""
        all_data = pd.DataFrame()
        for symbol in self.symbols:
            file_path = os.path.join(self.base_path, f"{symbol}_data.csv")
            if os.path.exists(file_path):
                symbol_data = pd.read_csv(file_path)
                # Adding a symbol column for identification
                symbol_data['symbol'] = symbol
                all_data = pd.concat(
                    [all_data, symbol_data], ignore_index=True)
        return all_data

    def execute_trade_cycle(self):
        """Handle the complete trade cycle: data loading, strategy execution, and trade simulation."""
        historical_data = self.load_data()
        data_with_signals = self.strategy_manager.apply_strategies(
            historical_data)

        for index, row in data_with_signals.iterrows():
            signal = row['Majority_Vote_Strategy']
            if signal != 'NONE':
                self.trade_simulator.execute_trade(
                    signal, row['symbol'], row['close'], row['date'])
