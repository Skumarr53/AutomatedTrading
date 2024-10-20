# src/preprocessing/custom_transformers.py

import os
import joblib
import json
from typing import Any, Dict, List, Optional, Tuple
from functools import reduce
from loguru  import logger
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.feature_selection import SelectKBest, mutual_info_regression, RFE, RFECV
from sklearn.base import TransformerMixin, BaseEstimator

from src import config


class ColumnExtractor(BaseEstimator, TransformerMixin):
    """
    Extracts specified columns from a pandas DataFrame.
    """
    def __init__(self, cols: Optional[List[str]] = None) -> None:
        """
        Initializes the ColumnExtractor with the specified columns.

        Args:
            cols (List[str], optional): List of column names to extract. Defaults to None.
        """
        self.cols = cols

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'ColumnExtractor':
        """
        Fits the transformer. Since it's stateless, it simply returns itself.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the specified columns.
        """
        if self.cols is None:
            raise ValueError("No columns specified for extraction.")
        return X[self.cols]

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {'cols': self.cols}

    def set_params(self, **params) -> 'ColumnExtractor':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self


class DFFeatureUnion(BaseEstimator, TransformerMixin):
    """
    Combines multiple feature transformers into a single transformer that operates on pandas DataFrames.
    """
    def __init__(self, transformer_list: Optional[List[Tuple[str, TransformerMixin]]] = None) -> None:
        """
        Initializes the DFFeatureUnion with a list of transformers.

        Args:
            transformer_list (List[Tuple[str, TransformerMixin]], optional): List of (name, transformer) tuples. Defaults to None.
        """
        self.transformer_list = transformer_list if transformer_list is not None else []
        self.columns: List[str] = []

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFFeatureUnion':
        """
        Fits all transformers in the transformer list.
        """
        for _, transformer in self.transformer_list:
            transformer.fit(X, y)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame using all transformers and concatenates the results.
        """
        transformed_dfs = []
        for name, transformer in self.transformer_list:
            transformed_df = transformer.transform(X)
            transformed_dfs.append(transformed_df)
        if not transformed_dfs:
            raise ValueError("No transformers provided to DFFeatureUnion.")
        X_union = pd.concat(transformed_dfs, axis=1)
        self.columns = X_union.columns.tolist()
        return X_union

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        if not deep:
            return {'transformer_list': self.transformer_list}
        else:
            out = {'transformer_list': self.transformer_list}
            for name, transformer in self.transformer_list:
                for key, value in transformer.get_params(deep=True).items():
                    out[f"{name}__{key}"] = value
            return out

    def set_params(self, **params) -> 'DFFeatureUnion':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            if '__' in key:
                name, param_name = key.split('__', 1)
                transformer = dict(self.transformer_list)[name]
                transformer.set_params(**{param_name: value})
            else:
                setattr(self, key, value)
        return self


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
        """
        self.columns = X.columns.tolist()
        # Assuming data is at 5-minute intervals
        # self.look_back_period = self.look_back_days * (12 * 24)  # 12 intervals per hour, 24 hours per day

        self.params = {}
        for column in self.columns:
            rolling_window = X[column].rolling(window=self.look_back_period, min_periods=1)
            self.params[column] = {
                'mean': rolling_window.mean().iloc[-1],
                'std': rolling_window.std(ddof=0).iloc[-1]
            }
        # Store parameters for later use
        self._store_params()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by normalizing each column.
        """
        if not self.params:
            # Load parameters if not already loaded
            self._load_params()
        transformed_data = pd.DataFrame(index=X.index)
        for column in self.columns:
            mean = self.params[column]['mean']
            std = self.params[column]['std']
            if std == 0:
                std = 1e-8  # Prevent division by zero
            transformed_data[column] = (X[column] - mean) / std
        return transformed_data

    def _store_params(self) -> None:
        """
        Stores the calculated mean and std for each column to a file for later use.
        """
        params_path = os.path.join(self.model_param_file, 'shortterm_normalization_params.joblib')
        os.makedirs(self.model_param_file, exist_ok=True)
        joblib.dump(self.params, params_path)
        logger.info(f"Short-term normalization parameters stored at {params_path}")

    def _load_params(self) -> None:
        """
        Loads the stored mean and std parameters from a file.
        """
        params_path = os.path.join(self.model_param_file, 'shortterm_normalization_params.joblib')
        if os.path.exists(params_path):
            self.params = joblib.load(params_path)
            logger.info(f"Short-term normalization parameters loaded from {params_path}")
        else:
            raise FileNotFoundError(f"Normalization parameters file not found at {params_path}")

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {
            'look_back_days': self.look_back_period,
            'model_param_file': self.model_param_file
        }

    def set_params(self, **params) -> 'ShortTermNormalizer':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self


class LongTermNormalizer(BaseEstimator, TransformerMixin):
    """
    Normalizes long-term numeric features using standard scaling.
    """
    def __init__(self, model_param_file: str = 'model_params') -> None:
        """
        Initializes the LongTermNormalizer.

        Args:
            model_param_file (str, optional): Directory to store model parameters. Defaults to 'model_params'.
        """
        self.model_param_file = model_param_file
        self.ss: Optional[StandardScaler] = None
        self.mean_: Optional[pd.Series] = None
        self.scale_: Optional[pd.Series] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'LongTermNormalizer':
        """
        Fits the StandardScaler to the input data and stores the mean and scale.
        """
        self.ss = StandardScaler()
        self.ss.fit(X)
        self.mean_ = pd.Series(self.ss.mean_, index=X.columns)
        self.scale_ = pd.Series(self.ss.scale_, index=X.columns)
        # Store parameters for later use
        self._store_params()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by applying standard scaling.
        """
        if self.mean_ is None or self.scale_ is None:
            # Load parameters if not already loaded
            self._load_params()
        X_scaled = (X - self.mean_) / self.scale_
        return X_scaled

    def _store_params(self) -> None:
        """
        Stores the calculated mean and scale for each column to a file for later use.
        """
        params = {'mean': self.mean_, 'scale': self.scale_}
        params_path = os.path.join(self.model_param_file, 'longterm_normalization_params.joblib')
        os.makedirs(self.model_param_file, exist_ok=True)
        joblib.dump(params, params_path)
        logger.info(f"Long-term normalization parameters stored at {params_path}")

    def _load_params(self) -> None:
        """
        Loads the stored mean and scale parameters from a file.
        """
        params_path = os.path.join(self.model_param_file, 'longterm_normalization_params.joblib')
        if os.path.exists(params_path):
            parameters = joblib.load(params_path)
            self.mean_ = parameters['mean']
            self.scale_ = parameters['scale']
            logger.info(f"Long-term normalization parameters loaded from {params_path}")
        else:
            raise FileNotFoundError(f"Normalization parameters file not found at {params_path}")

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {'model_param_file': self.model_param_file}

    def set_params(self, **params) -> 'LongTermNormalizer':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self


class DFRecursiveFeatureSelector(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination (RFE).
    """
    def __init__(
        self,
        estimator: Optional[BaseEstimator] = None,
        n_features_to_select: Optional[int] = None,
        step: int = 1
    ) -> None:
        """
        Initializes the DFRecursiveFeatureSelector.

        Args:
            estimator (BaseEstimator, optional): Estimator for RFE. Defaults to DecisionTreeRegressor().
            n_features_to_select (int, optional): Number of features to select. Defaults to None (half of the features).
            step (int, optional): Number of features to remove at each step. Defaults to 1.
        """
        self.estimator = estimator if estimator is not None else DecisionTreeRegressor()
        self.n_features_to_select = n_features_to_select
        self.step = step
        self.support_: Optional[pd.Index] = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> 'DFRecursiveFeatureSelector':
        """
        Fits the RFE selector to the data.
        """
        self.rfe = RFE(
            estimator=self.estimator,
            n_features_to_select=self.n_features_to_select,
            step=self.step
        )
        self.rfe.fit(X, y)
        self.support_ = X.columns[self.rfe.get_support()]
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.
        """
        if self.support_ is None:
            raise ValueError("The transformer has not been fitted yet.")
        return X[self.support_]

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {
            'estimator': self.estimator,
            'n_features_to_select': self.n_features_to_select,
            'step': self.step
        }

    def set_params(self, **params) -> 'DFRecursiveFeatureSelector':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self


class DF_RFECV_FeatureSelection(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination with Cross-Validation (RFECV).
    """
    def __init__(
        self,
        estimator: Optional[BaseEstimator] = None,
        step: int = 1,
        cv: Optional[Any] = None,
        scoring: str = 'accuracy'
    ) -> None:
        """
        Initializes the DF_RFECV_FeatureSelection.

        Args:
            estimator (BaseEstimator, optional): Estimator for RFECV. Defaults to DecisionTreeRegressor().
            step (int, optional): Number of features to remove at each step. Defaults to 1.
            cv (int or cross-validation generator, optional): Cross-validation strategy. Defaults to StratifiedKFold(n_splits=3).
            scoring (str, optional): Scoring metric for cross-validation. Defaults to 'accuracy'.
        """
        self.estimator = estimator if estimator is not None else DecisionTreeRegressor()
        self.step = step
        self.cv = cv if cv is not None else StratifiedKFold(n_splits=3)
        self.scoring = scoring
        self.support_: Optional[pd.Index] = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> 'DF_RFECV_FeatureSelection':
        """
        Fits the RFECV selector to the data.
        """
        self.rfecv = RFECV(
            estimator=self.estimator,
            step=self.step,
            cv=self.cv,
            scoring=self.scoring
        )
        self.rfecv.fit(X, y)
        self.support_ = X.columns[self.rfecv.get_support()]
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.
        """
        if self.support_ is None:
            raise ValueError("The transformer has not been fitted yet.")
        return X[self.support_]

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {
            'estimator': self.estimator,
            'step': self.step,
            'cv': self.cv,
            'scoring': self.scoring
        }

    def set_params(self, **params) -> 'DF_RFECV_FeatureSelection':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self


class CategoricalPreprocessor(BaseEstimator, TransformerMixin):
    """
    Encodes categorical features using One-Hot Encoding.
    """
    def __init__(self, columns: Optional[List[str]] = None, handle_unknown: str = 'ignore') -> None:
        """
        Initializes the CategoricalPreprocessor.

        Args:
            columns (List[str], optional): List of categorical columns to encode. Defaults to None.
            handle_unknown (str, optional): How to handle unknown categories. Defaults to 'ignore'.
        """
        self.columns = columns
        self.handle_unknown = handle_unknown
        self.encoder: Optional[OneHotEncoder] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CategoricalPreprocessor':
        """
        Fits the OneHotEncoder to the specified categorical columns.
        """
        if self.columns is None:
            raise ValueError("No columns specified for encoding.")
        self.encoder = OneHotEncoder(
            sparse=False,
            drop='if_binary',
            handle_unknown=self.handle_unknown
        )
        self.encoder.fit(X[self.columns])
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by encoding the specified categorical columns.
        """
        if self.encoder is None:
            raise ValueError("The encoder has not been fitted yet.")
        encoded_array = self.encoder.transform(X[self.columns])
        feature_names = self.encoder.get_feature_names_out(self.columns)
        encoded_df = pd.DataFrame(encoded_array, columns=feature_names, index=X.index)
        return encoded_df

    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Returns the parameters of the transformer.
        """
        return {
            'columns': self.columns,
            'handle_unknown': self.handle_unknown
        }

    def set_params(self, **params) -> 'CategoricalPreprocessor':
        """
        Sets the parameters of the transformer.
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self