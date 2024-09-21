# StrategyManager class to handle various trading strategies


class StrategyManager:
    def __init__(self, technical_strategies, additional_strategies=None):
        """
        Initializes the StrategyManager with a set of trading strategies.
        :param technical_strategies: Dictionary of technical trading strategies.
        :param additional_strategies: Optional dictionary of additional trading strategies.
        """
        self.technical_strategies = technical_strategies
        self.additional_strategies = additional_strategies or {}

    def apply_strategies(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies all available trading strategies to the given data.
        :param data: DataFrame containing market data.
        :return: DataFrame with strategy signals.
        """
        data_with_signals = data.copy()

        # Apply technical strategies
        for strategy_name, strategy_func in self.technical_strategies.items():
            data_with_signals[strategy_name] = data_with_signals.apply(strategy_func, axis=1)

        # Apply additional strategies if any
        for strategy_name, strategy_func in self.additional_strategies.items():
            data_with_signals[strategy_name] = data_with_signals.apply(strategy_func, axis=1)

        return data_with_signals