# src/preprocessing/custom_transformers.py

import os
import joblib
import shap
import json
from typing import Any, Dict, List, Optional, Tuple
from functools import reduce
from loguru  import logger
import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.feature_selection import SelectKBest, mutual_info_regression, RFE, RFECV
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.exceptions import NotFittedError

from src import config


class ColumnExtractor(BaseEstimator, TransformerMixin):
    """
    Extracts specified columns from a pandas DataFrame.

    Attributes:
        cols (List[str]): List of column names to extract.
    """

    def __init__(self, cols: Optional[List[str]] = None) -> None:
        """
        Initializes the ColumnExtractor.

        Args:
            cols (Optional[List[str]]): List of column names to extract.
        """
        self.cols = cols

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'ColumnExtractor':
        """
        Fits the transformer. No operation is performed.

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
            pd.DataFrame: DataFrame containing only the specified columns.
        """
        if self.cols is None:
            raise ValueError("No columns specified for extraction.")
        missing_cols = [col for col in self.cols if col not in X.columns]
        if missing_cols:
            raise ValueError(f"The following columns are missing in the input DataFrame: {missing_cols}")
        return X[self.cols]


class DFFeatureUnion(BaseEstimator, TransformerMixin):
    """
    Combines multiple DataFrame transformers into a single transformer.

    This is similar to scikit-learn's FeatureUnion but tailored for pandas DataFrames,
    allowing for merging of transformed DataFrames based on their indices.

    Attributes:
        transformer_list (List[Tuple[str, TransformerMixin]]): List of (name, transformer) tuples.
    """

    def __init__(self, transformer_list: List[Tuple[str, TransformerMixin]]) -> None:
        """
        Initializes the DFFeatureUnion with a list of transformers.

        Args:
            transformer_list (List[Tuple[str, TransformerMixin]]): List of (name, transformer) tuples.
        """
        self.transformer_list = transformer_list

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFFeatureUnion':
        """
        Fits all transformers in the transformer list.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            DFFeatureUnion: Fitted transformer.
        """
        for name, transformer in self.transformer_list:
            logger.debug(f"Fitting transformer: {name}")
            transformer.fit(X, y)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame using all transformers and merges the results.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Merged DataFrame containing all transformed features.
        """
        transformed_dfs = []
        for name, transformer in self.transformer_list:
            logger.debug(f"Transforming with transformer: {name}")
            transformed_df = transformer.transform(X)
            transformed_dfs.append(transformed_df)
        if not transformed_dfs:
            raise ValueError("No transformers provided to DFFeatureUnion.")
        X_union = pd.concat(transformed_dfs, axis=1)
        return X_union


class ShortTermNormalizer(BaseEstimator, TransformerMixin):
    """
    Normalizes short-term numeric features using rolling mean and standard deviation.

    Attributes:
        look_back_days (int): Number of days to look back for rolling calculations.
        look_back_period (int): Number of periods to look back based on operational hours.
    """

    def __init__(self, look_back_days: int = 5) -> None:
        """
        Initializes the ShortTermNormalizer.

        Args:
            look_back_days (int, optional): Number of days to look back for rolling calculations. Defaults to 5.
        """
        self.look_back_days = look_back_days
        self.look_back_period = look_back_days * 24  # Assuming 24 operations per day; adjust as needed.

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'ShortTermNormalizer':
        """
        Fits the transformer by calculating rolling mean and std for each column.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            ShortTermNormalizer: Fitted transformer.
        """
        self.columns_ = X.columns.tolist()
        self.means_ = {}
        self.stds_ = {}
        for column in self.columns_:
            rolling = X[column].rolling(window=self.look_back_period, min_periods=1)
            self.means_[column] = rolling.mean().iloc[-1]
            self.stds_[column] = rolling.std().iloc[-1] if rolling.std().iloc[-1] != 0 else 1.0
           #logger.debug(f"Calculated mean and std for column '{column}': mean={self.means_[column]}, std={self.stds_[column]}")
        logger.info("Fitted short-term normalizer.")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by normalizing each column.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Normalized DataFrame.
        """
        X_normalized = pd.DataFrame(index=X.index)
        for column in self.columns_:
            if column not in X.columns:
                raise ValueError(f"Column '{column}' not found in input DataFrame during transform.")
            mean = self.means_[column]
            std = self.stds_[column]
            X_normalized[column] = (X[column] - mean) / std
            #logger.debug(f"Normalized column '{column}': mean={mean}, std={std}")
        logger.info("Transformed short-term normalizer.")
        return X_normalized


class LongTermNormalizer(BaseEstimator, TransformerMixin):
    """
    Normalizes long-term numeric features using standard scaling.

    Attributes:
        None
    """

    def __init__(self) -> None:
        """
        Initializes the LongTermNormalizer.
        """
        self.scaler_ = StandardScaler()

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'LongTermNormalizer':
        """
        Fits the StandardScaler to the input data.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            LongTermNormalizer: Fitted transformer.
        """
        self.scaler_.fit(X)
        self.mean_ = pd.Series(self.scaler_.mean_, index=X.columns)
        self.scale_ = pd.Series(self.scaler_.scale_, index=X.columns)
        logger.debug(f"Fitted LongTermNormalizer with mean and scale for each column.")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by applying standard scaling.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Scaled DataFrame.
        """
        X_scaled_array = self.scaler_.transform(X)
        X_scaled = pd.DataFrame(X_scaled_array, index=X.index, columns=X.columns)
        logger.debug(f"Applied standard scaling to input DataFrame.")
        return X_scaled
    

class CategoricalPreprocessor(BaseEstimator, TransformerMixin):
    """
    Encodes categorical features using One-Hot Encoding.

    Attributes:
        columns (List[str]): List of categorical columns to encode.
    """

    def __init__(self, columns: List[str]) -> None:
        """
        Initializes the CategoricalPreprocessor.

        Args:
            columns (List[str]): List of categorical columns to encode.
        """
        self.columns = columns
        self.encoder_ = OneHotEncoder(sparse=False, drop='if_binary', handle_unknown='ignore')

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CategoricalPreprocessor':
        """
        Fits the OneHotEncoder to the specified categorical columns.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Optional target variable.

        Returns:
            CategoricalPreprocessor: Fitted preprocessor.
        """
        self.encoder_.fit(X[self.columns])
        logger.debug(f"Fitted OneHotEncoder on columns: {self.columns}")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by encoding the specified categorical columns.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with encoded categorical features.
        """
        encoded_array = self.encoder_.transform(X[self.columns])
        encoded_columns = self.encoder_.get_feature_names_out(input_features=self.columns)
        encoded_df = pd.DataFrame(encoded_array, columns=encoded_columns, index=X.index)
        logger.debug(f"Transformed categorical columns: {self.columns}")
        return encoded_df


class DFRecursiveFeatureSelector(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination (RFE).

    Attributes:
        estimator (DecisionTreeClassifier): The estimator used for feature selection.
        n_features (int): Number of features to select.
        step (int): Number of features to remove at each iteration.
    """

    def __init__(self, estimator: DecisionTreeClassifier = DecisionTreeClassifier(),
                 n_features: int = 10, step: int = 1) -> None:
        """
        Initializes the DFRecursiveFeatureSelector.

        Args:
            estimator (DecisionTreeClassifier, optional): Estimator for RFE. Defaults to DecisionTreeClassifier().
            n_features (int, optional): Number of features to select. Defaults to 10.
            step (int, optional): Number of features to remove at each step. Defaults to 1.
        """
        self.estimator = estimator
        self.n_features = n_features
        self.step = step
        self.selector_ = RFE(estimator=self.estimator, n_features_to_select=self.n_features, step=self.step)

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFRecursiveFeatureSelector':
        """
        Fits the RFE selector to the data.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Target variable.

        Returns:
            DFRecursiveFeatureSelector: Fitted feature selector.
        """
        ## TODO: drop nans logic take it to top of the pipeline
        self.selector_.fit(X, y)
        selected_features = X.columns[self.selector_.get_support()]
        logger.debug(f"Selected features after RFE: {list(selected_features)}")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing only the selected features.
        """
        if not hasattr(self, 'selector_'):
            raise ValueError("The feature selector has not been fitted yet.")
        selected_features = X.columns[self.selector_.get_support()]
        logger.debug(f"Transforming data to include selected features: {list(selected_features)}")
        return X[selected_features]


class DF_RFECV_FeatureSelection(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on Recursive Feature Elimination with Cross-Validation (RFECV).

    Attributes:
        estimator (DecisionTreeClassifier): The estimator used for feature selection.
        cv (StratifiedKFold): Cross-validation strategy.
        step (int): Number of features to remove at each step.
        scoring (str): Scoring metric for cross-validation.
    """

    def __init__(self, estimator: DecisionTreeClassifier = DecisionTreeClassifier(),
                 cv: StratifiedKFold = StratifiedKFold(n_splits=3),
                 step: int = 1, scoring: str = 'accuracy') -> None:
        """
        Initializes the DF_RFECV_FeatureSelection.

        Args:
            estimator (DecisionTreeClassifier, optional): Estimator for RFECV. Defaults to DecisionTreeClassifier().
            cv (StratifiedKFold, optional): Cross-validation strategy. Defaults to StratifiedKFold(n_splits=3).
            step (int, optional): Number of features to remove at each step. Defaults to 1.
            scoring (str, optional): Scoring metric for cross-validation. Defaults to 'accuracy'.
        """
        self.estimator = estimator
        self.cv = cv
        self.step = step
        self.scoring = scoring
        self.selector_ = RFECV(estimator=self.estimator, step=self.step, cv=self.cv, scoring=self.scoring)

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DF_RFECV_FeatureSelection':
        """
        Fits the RFECV selector to the data.

        Args:
            X (pd.DataFrame): Input DataFrame.
            y (Optional[pd.Series]): Target variable.

        Returns:
            DF_RFECV_FeatureSelection: Fitted feature selector.
        """
        self.selector_.fit(X, y)
        selected_features = X.columns[self.selector_.get_support()]
        logger.debug(f"Selected features after RFECV: {list(selected_features)}")
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the chosen features.

        Args:
            X (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing only the selected features.
        """
        if not hasattr(self, 'selector_'):
            raise ValueError("The feature selector has not been fitted yet.")
        selected_features = X.columns[self.selector_.get_support()]
        logger.debug(f"Transforming data to include selected features: {list(selected_features)}")
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

class DFShapFeatureSelector(BaseEstimator, TransformerMixin):
    """
    Selects a subset of features based on SHAP (SHapley Additive exPlanations) feature importance.

    Attributes:
        estimator (BaseEstimator): The estimator used to compute SHAP values.
        n_features (int): Number of top features to select based on SHAP importance.
        feature_importances_ (pd.Series): The computed SHAP feature importances after fitting.
    """

    def __init__(self, estimator: BaseEstimator, n_features: int = 10) -> None:
        """
        Initializes the DFShapFeatureSelector.

        Args:
            estimator (BaseEstimator): Estimator for SHAP analysis. Must support the `fit` method.
            n_features (int, optional): Number of top features to select based on SHAP importance. Defaults to 10.
        
        Raises:
            ValueError: If `n_features` is not a positive integer.
        """
        if not isinstance(n_features, int) or n_features <= 0:
            raise ValueError("`n_features` must be a positive integer.")
        
        self.estimator = estimator
        self.n_features = n_features
        self.feature_importances_: Optional[pd.Series] = None

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFShapFeatureSelector':
        """
        Fits the estimator and computes SHAP feature importances.

        Args:
            X (pd.DataFrame): Input DataFrame containing features.
            y (Optional[pd.Series]): Target variable. Required if the estimator's `fit` method needs it.

        Returns:
            DFShapFeatureSelector: Fitted feature selector.
        
        Raises:
            ValueError: If `X` is not a pandas DataFrame.
        """
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input `X` must be a pandas DataFrame.")
        
        # Fit the estimator
        self.estimator.fit(X, y)
        
        # Initialize the SHAP explainer
        try:
            explainer = shap.Explainer(self.estimator, X, feature_names=X.columns)
        except Exception as e:
            raise ValueError(f"Error initializing SHAP explainer: {e}")
        
        # Compute SHAP values
        try:
            shap_values = explainer(X)
        except Exception as e:
            raise ValueError(f"Error computing SHAP values: {e}")
        
        # Aggregate SHAP values to get feature importances
        if isinstance(shap_values, list):
            # For multi-output models
            shap_abs = [np.abs(shap_value.values).mean(axis=0) for shap_value in shap_values]
            shap_mean = np.mean(shap_abs, axis=0)
        else:
            # For single-output models
            shap_mean = np.abs(shap_values.values).mean(axis=0)
        
        self.feature_importances_ = pd.Series(shap_mean, index=X.columns).sort_values(ascending=False)
        
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input DataFrame by selecting the top `n_features` based on SHAP importance.

        Args:
            X (pd.DataFrame): Input DataFrame containing features.

        Returns:
            pd.DataFrame: DataFrame containing only the selected top `n_features` features.
        
        Raises:
            NotFittedError: If the selector has not been fitted yet.
            ValueError: If `X` is not a pandas DataFrame.
        """
        if self.feature_importances_ is None:
            raise NotFittedError("Feature selector has not been fitted. Call `fit` before `transform`.")
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input `X` must be a pandas DataFrame.")
        
        # Select the top n_features
        selected_features = self.feature_importances_.iloc[:self.n_features].index
        return X[selected_features]

    def get_feature_importances(self) -> pd.Series:
        """
        Retrieves the computed SHAP feature importances.

        Returns:
            pd.Series: SHAP feature importances sorted in descending order.
        
        Raises:
            NotFittedError: If the selector has not been fitted yet.
        """
        if self.feature_importances_ is None:
            raise NotFittedError("Feature selector has not been fitted yet.")
        return self.feature_importances_



from sklearn.base import BaseEstimator, TransformerMixin
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.combine import SMOTETomek, SMOTEENN
from typing import Optional
import pandas as pd
import numpy as np

class ImbalanceHandler(BaseEstimator, TransformerMixin):
    """
    A custom transformer to handle imbalanced datasets using resampling techniques such as SMOTE and Random Oversampling.

    Attributes:
        technique (str): The resampling technique to use ('smote', 'random', 'smote_tomek', 'smote_enn').
        sampler: The imbalanced-learn sampler object used for resampling.
    """

    def __init__(self, technique: str = 'smote') -> None:
        """
        Initializes the ImbalanceHandler transformer.

        Args:
            technique (str, optional): The resampling technique to use. 
                Options: 'smote' (default), 'random', 'smote_tomek', 'smote_enn'.
        """
        self.technique = technique.lower()
        self.sampler = None

        if self.technique == 'smote':
            self.sampler = SMOTE()
        elif self.technique == 'random':
            self.sampler = RandomOverSampler()
        elif self.technique == 'smote_tomek':
            self.sampler = SMOTETomek()
        elif self.technique == 'smote_enn':
            self.sampler = SMOTEENN()
        else:
            raise ValueError("Invalid technique. Choose from 'smote', 'random', 'smote_tomek', or 'smote_enn'.")

    def fit(self, X: pd.DataFrame, y: pd.Series) -> 'ImbalanceHandler':
        """
        Fits the sampler on the data.

        Args:
            X (pd.DataFrame): Input feature DataFrame.
            y (pd.Series): Target variable Series.

        Returns:
            ImbalanceHandler: Fitted transformer.
        """
        # Fit the sampler on X, y if necessary
        if self.sampler is not None:
            self.sampler.fit_resample(X, y)
        return self

    def transform(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Transforms the data by applying the selected resampling technique to balance classes.

        Args:
            X (pd.DataFrame): Input feature DataFrame.
            y (Optional[pd.Series]): Target variable Series. Required for resampling.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: Resampled features and target.
        """
        if y is None:
            raise ValueError("The target variable 'y' is required for resampling.")

        # Apply the selected resampling technique
        X_resampled, y_resampled = self.sampler.fit_resample(X, y)

        # Log the resampling results
        logger.debug(f"Resampling technique '{self.technique}' applied.")
        logger.debug(f"Original dataset size: {len(X)}, Resampled dataset size: {len(X_resampled)}")
        
        return pd.DataFrame(X_resampled, columns=X.columns), pd.Series(y_resampled, name=y.name)
