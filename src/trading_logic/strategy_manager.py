# src/trading_logic/strategy_manager.py

from typing import Callable, Dict, Optional, Any
import pandas as pd
import logging


class StrategyManager:
    """
    Manages and applies various trading strategies to market data.

    The `StrategyManager` class orchestrates the application of multiple trading strategies
    (both technical and additional) to a given dataset. It facilitates the integration of
    different strategies, allowing for a consolidated approach to generating trading signals.
    
    Attributes:
        technical_strategies (Dict[str, Callable[[pd.Series], Any]]):
            A dictionary mapping strategy names to their corresponding technical strategy functions.
        additional_strategies (Dict[str, Callable[[pd.Series], Any]]):
            An optional dictionary mapping strategy names to their corresponding additional strategy functions.
    """

    def __init__(
        self,
        technical_strategies: Dict[str, Callable[[pd.Series], Any]],
        additional_strategies: Optional[Dict[str, Callable[[pd.Series], Any]]] = None
    ) -> None:
        """
        Initializes the StrategyManager with a set of trading strategies.

        Args:
            technical_strategies (Dict[str, Callable[[pd.Series], Any]]):
                A dictionary where keys are strategy names and values are functions implementing technical trading strategies.
            additional_strategies (Optional[Dict[str, Callable[[pd.Series], Any]]], optional):
                An optional dictionary where keys are strategy names and values are functions implementing additional trading strategies.
                Defaults to an empty dictionary.
        """
        self.technical_strategies: Dict[str, Callable[[pd.Series], Any]] = technical_strategies
        self.additional_strategies: Dict[str, Callable[[pd.Series], Any]] = additional_strategies or {}
        logging.info("StrategyManager initialized with %d technical strategies and %d additional strategies.",
                     len(self.technical_strategies), len(self.additional_strategies))

    def apply_strategies(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies all available trading strategies to the given market data.

        This method processes each row of the input DataFrame by applying each configured
        trading strategy. The results are appended as new columns to the DataFrame, representing
        the signals generated by each strategy.

        Args:
            data (pd.DataFrame):
                A DataFrame containing market data. It is expected to have one row per time period
                and columns corresponding to various market indicators and features.

        Returns:
            pd.DataFrame:
                A copy of the input DataFrame augmented with new columns representing the signals
                generated by each trading strategy. The columns are named after the respective strategies.
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Input data must be a pandas DataFrame.")

        data_with_signals: pd.DataFrame = data.copy()
        logging.info("Applying %d technical strategies and %d additional strategies.",
                     len(self.technical_strategies), len(self.additional_strategies))

        # Apply technical strategies
        for strategy_name, strategy_func in self.technical_strategies.items():
            try:
                logging.debug("Applying technical strategy: %s", strategy_name)
                data_with_signals[strategy_name] = data_with_signals.apply(strategy_func, axis=1)
                logging.debug("Strategy %s applied successfully.", strategy_name)
            except Exception as e:
                logging.error("Error applying technical strategy '%s': %s", strategy_name, e)
                data_with_signals[strategy_name] = None  # Assign None or a default value in case of error

        # Apply additional strategies if any
        for strategy_name, strategy_func in self.additional_strategies.items():
            try:
                logging.debug("Applying additional strategy: %s", strategy_name)
                data_with_signals[strategy_name] = data_with_signals.apply(strategy_func, axis=1)
                logging.debug("Strategy %s applied successfully.", strategy_name)
            except Exception as e:
                logging.error("Error applying additional strategy '%s': %s", strategy_name, e)
                data_with_signals[strategy_name] = None  # Assign None or a default value in case of error

        logging.info("All strategies applied successfully.")
        return data_with_signals
