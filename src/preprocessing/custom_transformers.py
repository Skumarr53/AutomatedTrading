# src/preprocessing/custom_transformers.py

import os
import joblib
import json
from typing import Any, Dict, List, Optional, Tuple
from functools import reduce

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.feature_selection import SelectKBest, mutual_info_regression, RFE, RFECV
from sklearn.base import TransformerMixin, BaseEstimator

from src.config import config


class ColumnExtractor(BaseEstimator, TransformerMixin):
    """
    Extracts specified columns from a pandas DataFrame.

    This transformer selects a subset of columns from the input DataFrame based on the provided list.

    Attributes:
        cols (List[str]): List of column names to extract.
    """

    def __init__(self, cols: List[str]) -> None:
        """
        Initializes the ColumnExtractor with the specified columns.

        Args:
            cols (List[str]): List of column names to extract.
        """
        self.cols = cols

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'ColumnExtractor':
        """
        Fits the transformer. This is a stateless transformer, so it simply returns itself.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            ColumnExtractor: Fitted transformer.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the specified columns.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing only the selected columns.
        """
        Xcols = X[self.cols]
        return Xcols


class DFFeatureUnion(BaseEstimator, TransformerMixin):
    """
    Combines multiple feature transformers into a single transformer that operates on pandas DataFrames.

    This is similar to sklearn's FeatureUnion but tailored for pandas DataFrames, allowing for merging
    of transformed DataFrames based on their indices.

    Attributes:
        transformer_list (List[Tuple[str, TransformerMixin]]): List of (name, transformer) tuples.
        columns (List[str]): List of column names after transformation.
    """

    def __init__(self, transformer_list: List[Tuple[str, TransformerMixin]]) -> None:
        """
        Initializes the DFFeatureUnion with a list of transformers.

        Args:
            transformer_list (List[Tuple[str, TransformerMixin]]): List of (name, transformer) tuples.
        """
        self.transformer_list = transformer_list
        self.columns: List[str] = []

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFFeatureUnion':
        """
        Fits all transformers in the transformer list.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            DFFeatureUnion: Fitted transformer.
        """
        for _, transformer in self.transformer_list:
            transformer.fit(X, y)
        return self

    def get_feature_names(self) -> List[str]:
        """
        Retrieves the combined feature names after transformation.

        Returns:
            List[str]: List of feature names.
        """
        return self.columns

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame using all transformers and merges the results.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Merged DataFrame containing all transformed features.
        """
        Xts = [transformer.transform(X) for _, transformer in self.transformer_list]
        if not Xts:
            raise ValueError("No transformers provided to DFFeatureUnion.")
        Xunion = reduce(lambda X1, X2: pd.merge(X1, X2, left_index=True, right_index=True), Xts)
        self.columns = Xunion.columns.tolist()
        return Xunion


class ShortTermNormalizer(BaseEstimator, TransformerMixin):
    """
    Normalizes short-term numeric features using rolling mean and standard deviation.

    In BACKTEST mode, it calculates and stores the rolling mean and std. In LIVE mode,
    it loads the stored parameters to apply normalization.

    Attributes:
        look_back_period (int): Number of periods to look back for rolling calculations.
        params (Dict[str, Dict[str, float]]): Stored mean and std for each column.
        columns (List[str]): List of column names to normalize.
        param_dict (Dict[str, Dict[str, float]]): Dictionary to store parameters for persistence.
    """

    def __init__(self, look_back_days: int = 5) -> None:
        """
        Initializes the ShortTermNormalizer.

        Args:
            look_back_days (int, optional): Number of days to look back for rolling calculations. Defaults to 5.
        """
        # TODO
        self.look_back_period: int = look_back_days * config.backtest_data_load.n_operations_hours_daily
        self.params: Dict[str, Dict[str, float]] = {}  # To store mean and std for live mode
        self.columns: List[str] = []
        self.param_dict: Dict[str, Dict[str, float]] = {}
        self.model_param_file = config.paths.model_param_path

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'ShortTermNormalizer':
        """
        Fits the transformer by calculating rolling mean and std for each column.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            ShortTermNormalizer: Fitted transformer.
        """
        self.columns = X.columns.tolist()
        if config.trading_config.trade_mode == 'BACKTEST':
            # Assuming data is indexed by datetime
            for column in self.columns:
                rolling_windows = X[column].rolling(window=self.look_back_period)
                self.params[column] = {
                    'mean': rolling_windows.mean().iloc[-1],
                    'std': rolling_windows.std().iloc[-1]
                }
            # Store parameters for later use in LIVE mode
            self._store_params()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by normalizing each column.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Normalized DataFrame.
        """
        transformed_data = pd.DataFrame(index=X.index)
        if config.trading_config.trade_mode == 'LIVE':
            # Load parameters if in LIVE mode
            self._load_params()

        for column in self.columns:
            mean = self.params[column]['mean']
            std = self.params[column]['std']
            transformed_data[column] = (X[column] - mean) / std
        return transformed_data

    def _store_params(self) -> None:
        """
        Stores the calculated mean and std for each column to a file for later use in LIVE mode.
        """
        for column in self.columns:
            self.param_dict[column] = {
                'mean': self.params[column]['mean'],
                'std': self.params[column]['std']
            }

        # TODO
        params_path = os.path.join(self.model_param_file, 'shortterm_normalization_params.joblib')
        joblib.dump(self.param_dict, params_path)
        logging.info(f"Short-term normalization parameters stored at {params_path}")

    def _load_params(self) -> None:
        """
        Loads the stored mean and std parameters from a file in LIVE mode.
        """
        # TODO
        params_path = os.path.join(self.model_param_file, 'shortterm_normalization_params.joblib')
        if os.path.exists(params_path):
            self.params = joblib.load(params_path)
            logging.info(f"Short-term normalization parameters loaded from {params_path}")
        else:
            raise FileNotFoundError(f"Normalization parameters file not found at {params_path}")

    def get_params(self) -> Dict[str, Dict[str, float]]:
        """
        Retrieves the stored normalization parameters.

        Returns:
            Dict[str, Dict[str, float]]: Dictionary containing mean and std for each column.
        """
        return self.params


class LongTermNormalizer(BaseEstimator, TransformerMixin):
    """
    Normalizes long-term numeric features using standard scaling.

    In BACKTEST mode, it fits the scaler and stores the parameters. In LIVE mode,
    it loads the stored parameters to apply normalization.

    Attributes:
        ss (Optional[StandardScaler]): StandardScaler instance.
        mean_ (Optional[pd.Series]): Mean values for each feature.
        scale_ (Optional[pd.Series]): Scale (standard deviation) values for each feature.
    """

    def __init__(self) -> None:
        """
        Initializes the LongTermNormalizer.
        """
        self.ss: Optional[StandardScaler] = None
        self.mean_: Optional[pd.Series] = None
        self.scale_: Optional[pd.Series] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'LongTermNormalizer':
        """
        Fits the StandardScaler to the input data and stores the mean and scale.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            LongTermNormalizer: Fitted transformer.
        """
        self.ss = StandardScaler()
        self.ss.fit(X)
        self.mean_ = pd.Series(self.ss.mean_, index=X.columns)
        self.scale_ = pd.Series(self.ss.scale_, index=X.columns)

        # Store parameters for later use in LIVE mode
        self._store_params()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by applying standard scaling.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Scaled DataFrame.
        """
        if config.trading_config.trade_mode == 'LIVE':
            # Load parameters if in LIVE mode
            self._load_params()
            Xscaled = (X - self.mean_) / self.scale_
        else:
            if self.ss is None:
                raise ValueError("Scaler has not been fitted. Call fit() before transform().")
            Xss = self.ss.transform(X)
            Xscaled = pd.DataFrame(Xss, index=X.index, columns=X.columns)
        return Xscaled

    def _store_params(self) -> None:
        """
        Stores the calculated mean and scale for each column to a file for later use in LIVE mode.
        """
        params = {'mean': self.mean_, 'std': self.scale_}
        # TODO
        params_path = os.path.join(self.model_param_file, 'longterm_normalization_params.joblib')
        joblib.dump(params, params_path)
        logging.info(f"Long-term normalization parameters stored at {params_path}")

    def _load_params(self) -> None:
        """
        Loads the stored mean and scale parameters from a file in LIVE mode.
        """
        # TODO
        params_path = os.path.join(self.model_param_file, 'longterm_normalization_params.joblib')
        if os.path.exists(params_path):
            parameters = joblib.load(params_path)
            self.mean_ = parameters['mean']
            self.scale_ = parameters['std']
            logging.info(f"Long-term normalization parameters loaded from {params_path}")
        else:
            raise FileNotFoundError(f"Normalization parameters file not found at {params_path}")

    def get_params(self) -> Dict[str, pd.Series]:
        """
        Retrieves the stored normalization parameters.

        Returns:
            Dict[str, pd.Series]: Dictionary containing mean and std for each column.
        """
        return {'mean': self.mean_, 'std': self.scale_}


class DFRecursiveFeatureSelector(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination (RFE).

    Attributes:
        estimator (DecisionTreeRegressor): The estimator used for feature selection.
        n_features (int): Number of features to select.
        step (int): Number of features to remove at each iteration.
        RFE (RFE): The fitted RFE instance after fitting.
    """

    def __init__(self, estimator: DecisionTreeRegressor = DecisionTreeRegressor(),
                 n_features: int = 10, step: int = 10) -> None:
        """
        Initializes the DFRecursiveFeatureSelector.

        Args:
            estimator (DecisionTreeRegressor, optional): Estimator for RFE. Defaults to DecisionTreeRegressor().
            n_features (int, optional): Number of features to select. Defaults to 10.
            step (int, optional): Number of features to remove at each step. Defaults to 10.
        """
        self.estimator = estimator
        self.n_features = n_features
        self.step = step
        self.RFE: Optional[RFE] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFRecursiveFeatureSelector':
        """
        Fits the RFE selector to the data.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Target variable.

        Returns:
            DFRecursiveFeatureSelector: Fitted feature selector.
        """
        self.RFE = RFE(estimator=self.estimator, n_features_to_select=self.n_features, step=self.step)
        self.RFE.fit(X, y)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing only the selected features.
        """
        if self.RFE is None:
            raise ValueError("Feature selector has not been fitted. Call fit() before transform().")
        selected_features = X.columns[self.RFE.get_support()]
        return X[selected_features]


class DF_RFECV_FeatureSelection(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination with Cross-Validation (RFECV).

    Attributes:
        estimator (DecisionTreeRegressor): The estimator used for feature selection.
        cv (StratifiedKFold): Cross-validation strategy.
        step (int): Number of features to remove at each step.
        scoring (str): Scoring metric for cross-validation.
        rfevc (RFECV): The fitted RFECV instance after fitting.
    """

    def __init__(self, estimator: DecisionTreeRegressor = DecisionTreeRegressor(),
                 cv: StratifiedKFold = StratifiedKFold(n_splits=3),
                 step: int = 1, scoring: str = 'r2') -> None:
        """
        Initializes the DF_RFECV_FeatureSelection.

        Args:
            estimator (DecisionTreeRegressor, optional): Estimator for RFECV. Defaults to DecisionTreeRegressor().
            cv (StratifiedKFold, optional): Cross-validation strategy. Defaults to StratifiedKFold(n_splits=3).
            step (int, optional): Number of features to remove at each step. Defaults to 1.
            scoring (str, optional): Scoring metric for cross-validation. Defaults to 'r2'.
        """
        self.estimator = estimator
        self.cv = cv
        self.step = step
        self.scoring = scoring
        self.rfevc: Optional[RFECV] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DF_RFECV_FeatureSelection':
        """
        Fits the RFECV selector to the data.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Target variable.

        Returns:
            DF_RFECV_FeatureSelection: Fitted feature selector.
        """
        self.rfevc = RFECV(estimator=self.estimator, step=self.step, cv=self.cv, scoring=self.scoring)
        self.rfevc.fit(X, y)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing only the selected features.
        """
        if self.rfevc is None:
            raise ValueError("Feature selector has not been fitted. Call fit() before transform().")
        selected_features = X.columns[self.rfevc.get_support()]
        return X[selected_features]


class CategoricalPreprocessor(BaseEstimator, TransformerMixin):
    """
    Encodes categorical features using One-Hot Encoding.

    Attributes:
        columns (List[str]): List of categorical columns to encode.
        encoder (OneHotEncoder): Fitted OneHotEncoder instance.
    """

    def __init__(self, columns: List[str]) -> None:
        """
        Initializes the CategoricalPreprocessor.

        Args:
            columns (List[str]): List of categorical columns to encode.
        """
        self.columns = columns
        self.encoder: Optional[OneHotEncoder] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CategoricalPreprocessor':
        """
        Fits the OneHotEncoder to the specified categorical columns.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            CategoricalPreprocessor: Fitted preprocessor.
        """
        self.encoder = OneHotEncoder(sparse=False, drop='if_binary')
        self.encoder.fit(X[self.columns])
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by encoding the specified categorical columns.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with encoded categorical features.
        """
        if self.encoder is None:
            raise ValueError("Encoder has not been fitted. Call fit() before transform().")

        encoded_data = self.encoder.transform(X[self.columns])
        # Convert to DataFrame and ensure we have the right column names
        col_names = self.encoder.get_feature_names_out(input_features=self.columns)
        transformed_data = pd.DataFrame(encoded_data, columns=col_names, index=X.index)
        return transformed_data
