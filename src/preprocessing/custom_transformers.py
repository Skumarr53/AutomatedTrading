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
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.base import BaseSampler
from imblearn.combine import SMOTETomek, SMOTEENN
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import shuffle
from src import config
from collections import Counter


class TargetLabelEncoder(BaseEstimator, TransformerMixin):
    """
    Custom transformer for label encoding the target variable.
    """
    def __init__(self):
        self.label_encoder = LabelEncoder()

    def fit(self, y: pd.Series) -> 'TargetLabelEncoder':
        """
        Fits the label encoder to the target variable.

        Args:
            y (pd.Series): Target variable to encode.

        Returns:
            TargetLabelEncoder: Fitted transformer.
        """
        self.label_encoder.fit(y)
        return self

    def transform(self, y: pd.Series) -> pd.Series:
        """
        Transforms the target variable by encoding labels to numeric values.

        Args:
            y (pd.Series): Target variable to encode.

        Returns:
            pd.Series: Encoded target variable.
        """
        return pd.Series(self.label_encoder.transform(y), index=y.index)

    def inverse_transform(self, y: pd.Series) -> pd.Series:
        """
        Transforms the numeric encoded labels back to original labels.

        Args:
            y (pd.Series): Encoded target variable.

        Returns:
            pd.Series: Original target variable labels.
        """
        return pd.Series(self.label_encoder.inverse_transform(y)) #, index=y.index


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

class ResamplerTransformer(BaseEstimator, TransformerMixin):
    """
    Custom transformer that applies resampling and shuffling to X and y.
    Optionally caps samples per class to a maximum limit for balanced datasets.
    """
    def __init__(
        self, 
        sampler: BaseSampler, 
        shuffle: bool = True, 
        random_state: Optional[int] = 42,
        max_samples_per_class: Optional[int] = None
    ) -> None:
        """
        Initializes the ResamplerTransformer.

        Args:
            sampler (BaseSampler): An imblearn sampler, e.g., SMOTE().
            shuffle (bool): Whether to shuffle after resampling.
            random_state (Optional[int]): Seed for reproducibility.
            max_samples_per_class (Optional[int]): Maximum samples per class after resampling.
                If None, no limit is applied. If set, all classes will be capped at this value.
        """
        self.sampler = sampler
        self.shuffle = shuffle
        self.random_state = random_state
        self.max_samples_per_class = max_samples_per_class

    def fit(self, X: pd.DataFrame, y: pd.Series) -> 'ResamplerTransformer':
        """
        Fit the sampler.

        Args:
            X (pd.DataFrame): Feature data.
            y (pd.Series): Target data.

        Returns:
            ResamplerTransformer
        """
        logger.debug("Fitting the sampler.")
        counts = Counter(y)
        min_samples = min(counts.values())
        self.sampler.k_neighbors = min(5, min_samples - 1)
        self.sampler.fit(X, y)
    
    def fit_transform(self, X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
        # Log class distribution BEFORE resampling
        try:
            y_counts_before = pd.Series(y).value_counts().to_dict()
            y_counts_before_sorted = dict(sorted(y_counts_before.items()))
            min_count = min(y_counts_before.values()) if y_counts_before else 0
            max_count = max(y_counts_before.values()) if y_counts_before else 0
            
            logger.info(f"Class distribution BEFORE resampling: {y_counts_before_sorted}")
            logger.info(f"Min class count: {min_count}, Max class count: {max_count}, Classes: {len(y_counts_before)}")
        except Exception as e:
            logger.debug(f"Failed to log class distribution before resampling: {e}")
        
        try:
            self.fit(X, y)
            
            X, y = self.sampler.fit_resample(X, y)
            
            # Apply max_samples_per_class limit if configured
            if self.max_samples_per_class is not None:
                # Determine target count: min(max_samples_per_class, current_max_class_count)
                y_counts_after_resample = pd.Series(y).value_counts().to_dict()
                current_max_count = max(y_counts_after_resample.values()) if y_counts_after_resample else 0
                target_count = min(self.max_samples_per_class, current_max_count)
                
                if current_max_count > self.max_samples_per_class:
                    logger.info(f"Capping samples per class from {current_max_count} to {target_count}")
                    
                    # Sample down each class to target_count
                    X_list = []
                    y_list = []
                    
                    for class_label in y_counts_after_resample.keys():
                        class_mask = (y == class_label)
                        class_X = X[class_mask]
                        class_y = y[class_mask]
                        
                        # If class has more samples than target, randomly sample
                        if len(class_X) > target_count:
                            indices = np.random.RandomState(self.random_state).choice(
                                len(class_X), size=target_count, replace=False
                            )
                            class_X = class_X.iloc[indices] if isinstance(class_X, pd.DataFrame) else class_X[indices]
                            class_y = class_y.iloc[indices] if isinstance(class_y, pd.Series) else class_y[indices]
                        
                        X_list.append(class_X)
                        y_list.append(class_y)
                    
                    # Combine all classes
                    X = pd.concat(X_list, ignore_index=True) if isinstance(X_list[0], pd.DataFrame) else np.vstack(X_list)
                    y = pd.concat(y_list, ignore_index=True) if isinstance(y_list[0], pd.Series) else np.hstack(y_list)
            
                
                # NEW: Shuffle the combined dataset so classes are not grouped together
                X, y = shuffle(X, y, random_state=self.random_state)
                
                # If using Pandas, reset index after shuffle to keep it clean
                if isinstance(X, pd.DataFrame):
                    X = X.reset_index(drop=True)
                    y = y.reset_index(drop=True)
            
            # Log class distribution AFTER resampling
            try:
                y_counts_after = pd.Series(y).value_counts().to_dict()
                y_counts_after_sorted = dict(sorted(y_counts_after.items()))
                min_count_after = min(y_counts_after.values()) if y_counts_after else 0
                max_count_after = max(y_counts_after.values()) if y_counts_after else 0
                is_balanced = len(set(y_counts_after.values())) == 1 if y_counts_after else False
                
                logger.info(f"Class distribution AFTER resampling: {y_counts_after_sorted}")
                logger.info(f"Min class count: {min_count_after}, Max class count: {max_count_after}")
                logger.info(f"Classes are balanced (equal samples): {is_balanced}")
            except Exception as e:
                logger.debug(f"Failed to log class distribution after resampling: {e}")


            return X, y #.values.reshape(-1, 1)
        except Exception as e:
            logger.error(f"Resampling failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            raise ValueError(f"Error transforming data: {e}\n this failed at ResamplerTransformer transform method")

    def transform(
        self, 
        X: pd.DataFrame, 
    ) -> pd.DataFrame:
        """
        Resample and optionally shuffle the data.

        Args:
            X (pd.DataFrame): Feature data.
            y (pd.Series): Target data.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: Resampled and optionally shuffled X and y.
        """
        return X


    def _shuffle(
        self, 
        X: pd.DataFrame, 
        y: pd.Series
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Shuffles X and y in unison.

        Args:
            X (pd.DataFrame): Feature data.
            y (pd.Series): Target data.

        Returns:
            Tuple[pd.DataFrame, pd.Series]
        """
        shuffled = X.sample(frac=1, random_state=self.random_state).reset_index(drop=True)
        y_shuffled = y.loc[shuffled.index].reset_index(drop=True)
        return shuffled, y_shuffled

class DFFeatureUnion(BaseEstimator, TransformerMixin):
    """
    Combines multiple DataFrame transformers into a single transformer.

    This is similar to scikit-learn's FeatureUnion but tailored for pandas DataFrames,
    allowing for merging of transformed DataFrames based on their indices.

    Attributes:
        transformer_list (List[Tuple[str, TransformerMixin]]): List of (name, transformer) tuples.
    """

    def __init__(
        self, 
        transformer_list: List[Tuple[str, TransformerMixin]]
    ) -> None:        
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

    def transform(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> pd.DataFrame:
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
            try:
                transformed_df = transformer.transform(X)
                
                # Validate transformer output
                if not isinstance(transformed_df, pd.DataFrame):
                    raise ValueError(
                        f"Transformer '{name}' returned {type(transformed_df)}, expected DataFrame"
                    )
                
                # Check for sequences in columns (lists/arrays instead of scalars)
                import numpy as np
                sequence_cols = []
                for col in transformed_df.columns:
                    try:
                        # Sample a few values
                        sample = transformed_df[col].dropna().head(5)
                        for val in sample:
                            if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                                sequence_cols.append(col)
                                logger.error(
                                    f"Transformer '{name}' column '{col}' contains sequences: "
                                    f"type={type(val)}, value={val}"
                                )
                                break
                    except Exception:
                        pass
                
                if sequence_cols:
                    raise ValueError(
                        f"Transformer '{name}' produced columns with sequences (lists/arrays): {sequence_cols}. "
                        f"All columns must contain scalar values for sklearn compatibility."
                    )
                
                # Check shape is reasonable
                if transformed_df.shape[0] != X.shape[0]:
                    raise ValueError(
                        f"Transformer '{name}' changed row count: {X.shape[0]} → {transformed_df.shape[0]}"
                    )
                
                logger.debug(f"  Transformer '{name}': {transformed_df.shape} ({len(transformed_df.columns)} columns)")
                transformed_dfs.append(transformed_df)
                
            except Exception as e:
                logger.error(f"Transformer '{name}' failed: {e}")
                raise ValueError(f"Transformer '{name}' failed: {str(e)}")
        
        if not transformed_dfs:
            raise ValueError("No transformers provided to DFFeatureUnion.")
        
        try:
            X_union = pd.concat(transformed_dfs, axis=1)
            
            # Final validation
            logger.debug(f"DFFeatureUnion output shape: {X_union.shape}")
            
            # Check for sequences in final output
            import numpy as np
            for col in X_union.columns:
                try:
                    sample = X_union[col].dropna().head(3)
                    for val in sample:
                        if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                            logger.error(f"Final output column '{col}' contains sequences: {type(val)}")
                            raise ValueError(
                                f"Column '{col}' contains sequences. "
                                f"All values must be scalars for sklearn compatibility."
                            )
                except Exception:
                    pass
            
            return X_union
            
        except Exception as e:
            logger.error(f"Failed to concatenate transformer outputs: {e}")
            logger.error(f"  Number of transformers: {len(transformed_dfs)}")
            for i, df in enumerate(transformed_dfs):
                logger.error(f"  Transformer {i}: shape={df.shape}, columns={list(df.columns)[:10]}")
            raise


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
        self.look_back_period = look_back_days * 6  # Assuming 24 operations per day; adjust as needed.

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
        normalized_data = {}
        
        for column in self.columns_:
            if column not in X.columns:
                raise ValueError(f"Column '{column}' not found in input DataFrame during transform.")
            mean = self.means_[column]
            std = self.stds_[column]
            normalized_data[column] = (X[column] - mean) / std

        # Concatenate all normalized columns into a DataFrame
        X_normalized = pd.DataFrame(normalized_data, index=X.index)
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
        # Validate input - ensure all columns are numeric and contain scalars
        import numpy as np
        
        # Check for non-numeric columns
        non_numeric = X.select_dtypes(exclude=['number']).columns.tolist()
        if non_numeric:
            raise ValueError(
                f"LongTermNormalizer received non-numeric columns: {non_numeric}. "
                f"Only numeric columns can be scaled."
            )
        
        # Check for sequences in numeric columns (shouldn't happen, but be safe)
        for col in X.columns:
            sample = X[col].dropna().head(5)
            for val in sample:
                if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                    raise ValueError(
                        f"Column '{col}' contains sequences (lists/arrays) instead of scalars. "
                        f"Cannot scale sequences."
                    )
        
        try:
            X_scaled_array = self.scaler_.transform(X)
            X_scaled = pd.DataFrame(X_scaled_array, index=X.index, columns=X.columns)
            logger.debug(f"Applied standard scaling to {len(X.columns)} columns.")
            return X_scaled
        except Exception as e:
            logger.error(f"StandardScaler.transform failed: {e}")
            logger.error(f"  Input shape: {X.shape}")
            logger.error(f"  Input dtypes: {X.dtypes.value_counts().to_dict()}")
            raise ValueError(f"Standard scaling failed: {str(e)}")
    

# class CategoricalPreprocessor(BaseEstimator, TransformerMixin):
#     """
#     Encodes categorical features using One-Hot Encoding.

#     Attributes:
#         columns (List[str]): List of categorical columns to encode.
#     """

#     def __init__(self, columns: List[str]) -> None:
#         """
#         Initializes the CategoricalPreprocessor.

#         Args:
#             columns (List[str]): List of categorical columns to encode.
#         """
#         self.columns = columns
#         self.encoder_ = OneHotEncoder(sparse=False, drop='if_binary', handle_unknown='ignore')

#     def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CategoricalPreprocessor':
#         """
#         Fits the OneHotEncoder to the specified categorical columns.

#         Args:
#             X (pd.DataFrame): Input DataFrame.
#             y (Optional[pd.Series]): Optional target variable.

#         Returns:
#             CategoricalPreprocessor: Fitted preprocessor.
#         """
#         self.encoder_.fit(X[self.columns])
#         logger.debug(f"Fitted OneHotEncoder on columns: {self.columns}")
#         return self

#     def transform(self, X: pd.DataFrame) -> pd.DataFrame:
#         """
#         Transforms the input DataFrame by encoding the specified categorical columns.

#         Args:
#             X (pd.DataFrame): Input DataFrame.

#         Returns:
#             pd.DataFrame: DataFrame with encoded categorical features.
#         """
#         encoded_array = self.encoder_.transform(X[self.columns])
#         encoded_columns = self.encoder_.get_feature_names_out(input_features=self.columns)
#         encoded_df = pd.DataFrame(encoded_array, columns=encoded_columns, index=X.index)
#         logger.debug(f"Transformed categorical columns: {self.columns}")
#         return encoded_df


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
        self.encoder = OneHotEncoder(handle_unknown='ignore', sparse=False, drop='if_binary')
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
    
    SHAP values provide model-agnostic feature importance that considers feature interactions.
    More accurate than simple importance metrics but computationally expensive for large datasets.

    Attributes:
        estimator (BaseEstimator): The estimator used to compute SHAP values. Defaults to LGBMClassifier.
        n_features (int): Number of top features to select based on SHAP importance.
        sample_size (int): Number of samples to use for SHAP calculation (for efficiency).
        feature_importances_ (pd.Series): The computed SHAP feature importances after fitting.
        selected_features_ (list): Names of selected features.
    """

    def __init__(
        self, 
        estimator: Optional[BaseEstimator] = None, 
        n_features: int = 50,
        sample_size: Optional[int] = 1000
    ) -> None:
        """
        Initializes the DFShapFeatureSelector.

        Args:
            estimator (BaseEstimator, optional): Estimator for SHAP analysis. 
                If None, uses LGBMClassifier with fast settings (recommended).
            n_features (int, optional): Number of top features to select. Defaults to 50.
            sample_size (int, optional): Max samples for SHAP calculation. None = use all.
                Recommended: 1000-5000 for efficiency. Defaults to 1000.
        
        Raises:
            ValueError: If `n_features` is not a positive integer.
        """
        if not isinstance(n_features, int) or n_features <= 0:
            raise ValueError("`n_features` must be a positive integer.")
        
        self.estimator = estimator
        self.n_features = n_features
        self.sample_size = sample_size
        self.feature_importances_: Optional[pd.Series] = None
        self.selected_features_: Optional[List[str]] = None
        self._fitted_estimator: Optional[BaseEstimator] = None
    
    def _create_default_estimator(self) -> BaseEstimator:
        """Create a default LGBMClassifier optimized for SHAP calculation."""
        try:
            from lightgbm import LGBMClassifier
            return LGBMClassifier(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                num_leaves=31,
                verbosity=-1,
                n_jobs=-1,
                random_state=42
            )
        except ImportError:
            # Fallback to sklearn if LightGBM not available
            from sklearn.ensemble import GradientBoostingClassifier
            logger.warning("LightGBM not available, using GradientBoostingClassifier for SHAP")
            return GradientBoostingClassifier(
                n_estimators=100,
                max_depth=5,
                random_state=42
            )

    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'DFShapFeatureSelector':
        """
        Fits the estimator and computes SHAP feature importances.

        Args:
            X (pd.DataFrame): Input DataFrame containing features.
            y (Optional[pd.Series]): Target variable. Required for fitting.

        Returns:
            DFShapFeatureSelector: Fitted feature selector.
        
        Raises:
            ValueError: If `X` is not a pandas DataFrame.
        """
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input `X` must be a pandas DataFrame.")
        
        if y is None:
            raise ValueError("Target variable y is required for DFShapFeatureSelector")
        
        self.feature_names_in_ = list(X.columns)
        n_features_original = len(self.feature_names_in_)
        
        # Create estimator if not provided
        self._fitted_estimator = self.estimator if self.estimator is not None else self._create_default_estimator()
        
        logger.debug(f"DFShapFeatureSelector: Fitting {type(self._fitted_estimator).__name__} on {X.shape}")
        
        # Fit the estimator
        self._fitted_estimator.fit(X, y)
        
        # Sample data for SHAP calculation if needed (for efficiency)
        if self.sample_size is not None and len(X) > self.sample_size:
            logger.debug(f"Sampling {self.sample_size} rows for SHAP calculation")
            X_sample = X.sample(n=self.sample_size, random_state=42)
        else:
            X_sample = X
        
        # Initialize the SHAP explainer
        try:
            # Use TreeExplainer for tree-based models (faster)
            if hasattr(self._fitted_estimator, 'booster_') or hasattr(self._fitted_estimator, 'estimators_'):
                explainer = shap.TreeExplainer(self._fitted_estimator)
            else:
                explainer = shap.Explainer(self._fitted_estimator, X_sample, feature_names=X.columns)
        except Exception as e:
            logger.warning(f"TreeExplainer failed, using generic Explainer: {e}")
            explainer = shap.Explainer(self._fitted_estimator, X_sample, feature_names=X.columns)
        
        # Compute SHAP values
        try:
            shap_values = explainer(X_sample)
        except Exception as e:
            raise ValueError(f"Error computing SHAP values: {e}")
        
        # Aggregate SHAP values to get feature importances
        if isinstance(shap_values, list):
            # For multi-output models
            shap_abs = [np.abs(shap_value.values).mean(axis=0) for shap_value in shap_values]
            shap_mean = np.mean(shap_abs, axis=0)
        elif hasattr(shap_values, 'values'):
            # For SHAP Explanation objects
            if len(shap_values.values.shape) == 3:
                # Multi-class: (samples, features, classes)
                shap_mean = np.abs(shap_values.values).mean(axis=(0, 2))
            else:
                # Binary/regression: (samples, features)
                shap_mean = np.abs(shap_values.values).mean(axis=0)
        else:
            # Raw numpy array
            shap_mean = np.abs(shap_values).mean(axis=0)
        
        self.feature_importances_ = pd.Series(shap_mean, index=X.columns).sort_values(ascending=False)
        
        # Normalize to 0-1 range
        total = self.feature_importances_.sum()
        if total > 0:
            self.feature_importances_ = self.feature_importances_ / total
        
        # Select top features
        n_select = min(self.n_features, n_features_original)
        self.selected_features_ = list(self.feature_importances_.head(n_select).index)
        
        logger.info(f"DFShapFeatureSelector: Selected {len(self.selected_features_)}/{n_features_original} features")
        logger.debug(f"Top 5 SHAP features: {dict(self.feature_importances_.head(5).round(4))}")
        
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
        if self.selected_features_ is None:
            raise NotFittedError("Feature selector has not been fitted. Call `fit` before `transform`.")
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input `X` must be a pandas DataFrame.")
        
        # Handle missing features
        available = [f for f in self.selected_features_ if f in X.columns]
        if len(available) < len(self.selected_features_):
            missing = set(self.selected_features_) - set(available)
            logger.warning(f"DFShapFeatureSelector: {len(missing)} features missing: {list(missing)[:5]}")
        
        return X[available]
    
    def get_support(self, indices: bool = False):
        """Get a mask or indices of features selected."""
        if self.selected_features_ is None:
            raise NotFittedError("DFShapFeatureSelector has not been fitted.")
        
        mask = np.array([f in self.selected_features_ for f in self.feature_names_in_])
        return np.where(mask)[0] if indices else mask
    
    def get_feature_names_out(self, input_features=None) -> np.ndarray:
        """Get output feature names."""
        if self.selected_features_ is None:
            raise NotFittedError("DFShapFeatureSelector has not been fitted.")
        return np.array(self.selected_features_)

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


class CorrelationFilter(BaseEstimator, TransformerMixin):
    """
    Removes highly correlated features to reduce redundancy before model-based selection.
    
    This is a fast pre-filtering step that removes features with correlation above a threshold,
    keeping the feature with higher variance (more information). Reduces dimensionality before
    more expensive selection methods like RFE or SHAP.
    
    Attributes:
        threshold (float): Correlation threshold above which to remove features (default: 0.95).
        method (str): Correlation method ('pearson', 'spearman', 'kendall').
        feature_names_in_ (list): Feature names from fitting.
        features_to_keep_ (list): Features selected after filtering.
        correlation_matrix_ (pd.DataFrame): Correlation matrix computed during fit.
        dropped_features_ (dict): Mapping of dropped features to the feature they correlate with.
    """
    
    def __init__(self, threshold: float = 0.95, method: str = 'pearson') -> None:
        """
        Initialize the CorrelationFilter.
        
        Args:
            threshold: Correlation threshold (0-1). Features with abs(corr) > threshold are removed.
            method: Correlation method ('pearson', 'spearman', 'kendall').
        """
        if not 0 < threshold <= 1:
            raise ValueError(f"threshold must be between 0 and 1, got {threshold}")
        if method not in ('pearson', 'spearman', 'kendall'):
            raise ValueError(f"method must be 'pearson', 'spearman', or 'kendall', got {method}")
        
        self.threshold = threshold
        self.method = method
        self.feature_names_in_: Optional[List[str]] = None
        self.features_to_keep_: Optional[List[str]] = None
        self.correlation_matrix_: Optional[pd.DataFrame] = None
        self.dropped_features_: Dict[str, str] = {}
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CorrelationFilter':
        """
        Fit the filter by computing correlations and identifying features to drop.
        
        Args:
            X: Input DataFrame with features.
            y: Target variable (unused, for sklearn compatibility).
            
        Returns:
            Self.
        """
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        self.feature_names_in_ = list(X.columns)
        n_features_original = len(self.feature_names_in_)
        
        # Compute correlation matrix
        self.correlation_matrix_ = X.corr(method=self.method).abs()
        
        # Find features to drop
        upper_tri = self.correlation_matrix_.where(
            np.triu(np.ones(self.correlation_matrix_.shape), k=1).astype(bool)
        )
        
        # Track which features to drop and why
        features_to_drop = set()
        self.dropped_features_ = {}
        
        for col in upper_tri.columns:
            # Find features highly correlated with this column
            high_corr = upper_tri[col][upper_tri[col] > self.threshold]
            
            for correlated_feature in high_corr.index:
                # Keep the feature with higher variance (more information)
                if X[col].var() >= X[correlated_feature].var():
                    features_to_drop.add(correlated_feature)
                    self.dropped_features_[correlated_feature] = col
                else:
                    features_to_drop.add(col)
                    self.dropped_features_[col] = correlated_feature
        
        self.features_to_keep_ = [f for f in self.feature_names_in_ if f not in features_to_drop]
        
        n_dropped = n_features_original - len(self.features_to_keep_)
        logger.info(f"CorrelationFilter: Reduced {n_features_original} → {len(self.features_to_keep_)} features "
                   f"(dropped {n_dropped} with corr > {self.threshold})")
        
        if n_dropped > 0 and n_dropped <= 20:
            logger.debug(f"Dropped features: {list(self.dropped_features_.keys())}")
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform by selecting only the uncorrelated features.
        
        Args:
            X: Input DataFrame.
            
        Returns:
            DataFrame with only the selected features.
        """
        if self.features_to_keep_ is None:
            raise NotFittedError("CorrelationFilter has not been fitted. Call fit() first.")
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        # Handle case where some features might be missing (e.g., new data)
        available_features = [f for f in self.features_to_keep_ if f in X.columns]
        
        if len(available_features) < len(self.features_to_keep_):
            missing = set(self.features_to_keep_) - set(available_features)
            logger.warning(f"CorrelationFilter: {len(missing)} features missing in transform: {list(missing)[:5]}")
        
        return X[available_features]
    
    def get_support(self, indices: bool = False):
        """Get a mask or indices of features selected."""
        if self.features_to_keep_ is None:
            raise NotFittedError("CorrelationFilter has not been fitted.")
        
        mask = np.array([f in self.features_to_keep_ for f in self.feature_names_in_])
        return np.where(mask)[0] if indices else mask
    
    def get_feature_names_out(self, input_features=None) -> np.ndarray:
        """Get output feature names."""
        if self.features_to_keep_ is None:
            raise NotFittedError("CorrelationFilter has not been fitted.")
        return np.array(self.features_to_keep_)


class LGBMImportanceSelector(BaseEstimator, TransformerMixin):
    """
    Selects features based on LightGBM's built-in feature importance.
    
    Uses LightGBM's gain-based or split-based importance for fast, model-consistent
    feature selection. This is more efficient than SHAP for large datasets and
    provides importance scores that are directly relevant to the model being used.
    
    Attributes:
        n_features (int): Number of top features to select.
        importance_type (str): Type of importance ('gain', 'split', 'both').
        threshold (float): Minimum importance threshold (alternative to n_features).
        lgbm_params (dict): Parameters passed to LGBMClassifier.
        feature_importances_ (pd.Series): Computed importance scores.
        selected_features_ (list): Names of selected features.
    """
    
    def __init__(
        self,
        n_features: Optional[int] = 50,
        importance_type: str = 'gain',
        threshold: Optional[float] = None,
        lgbm_params: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize the LGBMImportanceSelector.
        
        Args:
            n_features: Number of top features to select. If None, uses threshold.
            importance_type: 'gain' (recommended), 'split', or 'both' (average of both).
            threshold: Minimum importance threshold (0-1, relative). Alternative to n_features.
            lgbm_params: Parameters for the internal LGBMClassifier. Defaults to fast settings.
        """
        if importance_type not in ('gain', 'split', 'both'):
            raise ValueError(f"importance_type must be 'gain', 'split', or 'both', got {importance_type}")
        
        if n_features is None and threshold is None:
            raise ValueError("Either n_features or threshold must be specified")
        
        # Store parameters EXACTLY as received (sklearn clone compatibility)
        self.n_features = n_features
        self.importance_type = importance_type
        self.threshold = threshold
        self.lgbm_params = lgbm_params  # Don't convert None to {} here!
        
        # Fitted attributes (not constructor params, so can initialize here)
        self.feature_importances_: Optional[pd.Series] = None
        self.selected_features_: Optional[List[str]] = None
        self._estimator: Optional[Any] = None
    
    def _create_estimator(self):
        """Create a fast LGBMClassifier for importance calculation."""
        try:
            from lightgbm import LGBMClassifier
        except ImportError:
            raise ImportError("LightGBM is required for LGBMImportanceSelector. Install with: uv add lightgbm")
        
        # Fast default parameters for importance calculation
        default_params = {
            'n_estimators': 100,  # Fewer trees for speed
            'max_depth': 5,       # Shallow trees
            'learning_rate': 0.1,
            'num_leaves': 31,
            'min_child_samples': 20,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'verbosity': -1,
            'n_jobs': -1,
            'random_state': 42,
            'importance_type': self.importance_type if self.importance_type != 'both' else 'gain'
        }
        
        # Override with user params (handle None)
        if self.lgbm_params is not None:
            default_params.update(self.lgbm_params)
        
        return LGBMClassifier(**default_params)
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'LGBMImportanceSelector':
        """
        Fit the selector by training a LightGBM model and extracting importance.
        
        Args:
            X: Input DataFrame with features.
            y: Target variable.
            
        Returns:
            Self.
        """
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        if y is None:
            raise ValueError("Target variable y is required for LGBMImportanceSelector")
        
        self.feature_names_in_ = list(X.columns)
        n_features_original = len(self.feature_names_in_)
        
        # Create and fit the estimator
        self._estimator = self._create_estimator()
        
        logger.debug(f"LGBMImportanceSelector: Fitting on {X.shape[0]} samples, {X.shape[1]} features")
        self._estimator.fit(X, y)
        
        # Get importance scores
        if self.importance_type == 'both':
            # Average of gain and split importance
            gain_importance = self._estimator.booster_.feature_importance(importance_type='gain')
            split_importance = self._estimator.booster_.feature_importance(importance_type='split')
            
            # Normalize and average
            gain_norm = gain_importance / (gain_importance.sum() + 1e-10)
            split_norm = split_importance / (split_importance.sum() + 1e-10)
            importance_values = (gain_norm + split_norm) / 2
        else:
            importance_values = self._estimator.feature_importances_
        
        # Create importance series
        self.feature_importances_ = pd.Series(
            importance_values,
            index=self.feature_names_in_
        ).sort_values(ascending=False)
        
        # Normalize to 0-1 range
        total_importance = self.feature_importances_.sum()
        if total_importance > 0:
            self.feature_importances_ = self.feature_importances_ / total_importance
        
        # Select features
        if self.n_features is not None:
            # Select top N features
            n_select = min(self.n_features, n_features_original)
            self.selected_features_ = list(self.feature_importances_.head(n_select).index)
        else:
            # Select features above threshold
            self.selected_features_ = list(
                self.feature_importances_[self.feature_importances_ >= self.threshold].index
            )
            # Ensure at least some features are selected
            if len(self.selected_features_) == 0:
                self.selected_features_ = list(self.feature_importances_.head(10).index)
                logger.warning(f"No features above threshold {self.threshold}, selecting top 10")
        
        n_selected = len(self.selected_features_)
        logger.info(f"LGBMImportanceSelector: Selected {n_selected}/{n_features_original} features "
                   f"(importance_type={self.importance_type})")
        
        # Log top features
        top_5 = self.feature_importances_.head(5)
        logger.debug(f"Top 5 features: {dict(top_5.round(4))}")
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform by selecting only the important features.
        
        Args:
            X: Input DataFrame.
            
        Returns:
            DataFrame with only the selected features.
        """
        if self.selected_features_ is None:
            raise NotFittedError("LGBMImportanceSelector has not been fitted. Call fit() first.")
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        # Handle missing features
        available = [f for f in self.selected_features_ if f in X.columns]
        if len(available) < len(self.selected_features_):
            missing = set(self.selected_features_) - set(available)
            logger.warning(f"LGBMImportanceSelector: {len(missing)} features missing: {list(missing)[:5]}")
        
        return X[available]
    
    def get_support(self, indices: bool = False):
        """Get a mask or indices of features selected."""
        if self.selected_features_ is None:
            raise NotFittedError("LGBMImportanceSelector has not been fitted.")
        
        mask = np.array([f in self.selected_features_ for f in self.feature_names_in_])
        return np.where(mask)[0] if indices else mask
    
    def get_feature_names_out(self, input_features=None) -> np.ndarray:
        """Get output feature names."""
        if self.selected_features_ is None:
            raise NotFittedError("LGBMImportanceSelector has not been fitted.")
        return np.array(self.selected_features_)
    
    def get_feature_importances(self) -> pd.Series:
        """Get the computed feature importances (sorted descending)."""
        if self.feature_importances_ is None:
            raise NotFittedError("LGBMImportanceSelector has not been fitted.")
        return self.feature_importances_


class MultiStageFeatureSelector(BaseEstimator, TransformerMixin):
    """
    Combines multiple feature selection stages for efficient dimensionality reduction.
    
    Typical usage: CorrelationFilter (fast) -> LGBMImportanceSelector (model-based)
    
    This is more efficient than applying expensive selection methods to all features.
    
    Attributes:
        stages (list): List of (name, selector) tuples.
        selected_features_ (list): Final selected features after all stages.
    """
    
    def __init__(self, stages: Optional[List[Tuple[str, TransformerMixin]]] = None) -> None:
        """
        Initialize MultiStageFeatureSelector.
        
        Args:
            stages: List of (name, selector) tuples. If None, uses default stages.
        """
        # Store parameter EXACTLY as received (sklearn clone compatibility)
        self.stages = stages  # Don't convert None to default here!
        
        # Fitted attributes (not constructor params)
        self.selected_features_: Optional[List[str]] = None
        self.stage_results_: Dict[str, Dict] = {}
        self._fitted_stages: Optional[List[Tuple[str, TransformerMixin]]] = None
    
    def _default_stages(self) -> List[Tuple[str, TransformerMixin]]:
        """Create default selection stages."""
        return [
            ('correlation', CorrelationFilter(threshold=0.95)),
            ('lgbm_importance', LGBMImportanceSelector(n_features=50, importance_type='gain'))
        ]
    
    def _get_stages(self) -> List[Tuple[str, TransformerMixin]]:
        """Get stages, using defaults if None."""
        return self.stages if self.stages is not None else self._default_stages()
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'MultiStageFeatureSelector':
        """
        Fit all selection stages sequentially.
        
        Args:
            X: Input DataFrame.
            y: Target variable.
            
        Returns:
            Self.
        """
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        self.feature_names_in_ = list(X.columns)
        X_current = X.copy()
        
        # Get stages (use defaults if None)
        self._fitted_stages = self._get_stages()
        
        logger.info(f"MultiStageFeatureSelector: Starting with {X_current.shape[1]} features")
        
        for stage_name, selector in self._fitted_stages:
            n_before = X_current.shape[1]
            
            # Fit and transform
            selector.fit(X_current, y)
            X_current = selector.transform(X_current)
            
            n_after = X_current.shape[1]
            
            # Store stage results
            self.stage_results_[stage_name] = {
                'n_features_in': n_before,
                'n_features_out': n_after,
                'reduction_pct': 100 * (1 - n_after / n_before) if n_before > 0 else 0
            }
            
            logger.info(f"  Stage '{stage_name}': {n_before} → {n_after} features "
                       f"({self.stage_results_[stage_name]['reduction_pct']:.1f}% reduction)")
        
        self.selected_features_ = list(X_current.columns)
        
        total_reduction = 100 * (1 - len(self.selected_features_) / len(self.feature_names_in_))
        logger.info(f"MultiStageFeatureSelector: Final {len(self.selected_features_)}/{len(self.feature_names_in_)} "
                   f"features ({total_reduction:.1f}% total reduction)")
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform by applying all selection stages.
        
        Args:
            X: Input DataFrame.
            
        Returns:
            DataFrame with selected features.
        """
        if self._fitted_stages is None or self.selected_features_ is None:
            raise NotFittedError("MultiStageFeatureSelector has not been fitted.")
        
        if not isinstance(X, pd.DataFrame):
            raise ValueError("Input X must be a pandas DataFrame")
        
        # Apply all fitted stages
        X_current = X
        for stage_name, selector in self._fitted_stages:
            X_current = selector.transform(X_current)
        
        return X_current
    
    def get_support(self, indices: bool = False):
        """Get a mask or indices of features selected."""
        if self.selected_features_ is None:
            raise NotFittedError("MultiStageFeatureSelector has not been fitted.")
        
        mask = np.array([f in self.selected_features_ for f in self.feature_names_in_])
        return np.where(mask)[0] if indices else mask
    
    def get_feature_names_out(self, input_features=None) -> np.ndarray:
        """Get output feature names."""
        if self.selected_features_ is None:
            raise NotFittedError("MultiStageFeatureSelector has not been fitted.")
        return np.array(self.selected_features_)
    
    def get_stage_importances(self) -> Dict[str, pd.Series]:
        """Get feature importances from each stage that supports it."""
        if self._fitted_stages is None:
            raise NotFittedError("MultiStageFeatureSelector has not been fitted.")
        
        importances = {}
        for stage_name, selector in self._fitted_stages:
            if hasattr(selector, 'get_feature_importances'):
                try:
                    importances[stage_name] = selector.get_feature_importances()
                except NotFittedError:
                    pass
            elif hasattr(selector, 'feature_importances_') and selector.feature_importances_ is not None:
                importances[stage_name] = selector.feature_importances_
        return importances


class ImbalanceHandler(BaseEstimator, TransformerMixin):
    """
    A custom transformer to handle imbalanced datasets using resampling techniques such as SMOTE and Random Oversampling.

    Attributes:
        technique (str): The resampling technique to use ('smote', 'random', 'smote_tomek', 'smote_enn').
        sampler: The imbalanced-learn sampler object used for resampling.
    """

    def __init__(self, technique: str = 'smote', **kwargs) -> None:
        self.technique = technique
        self.kwargs = kwargs
        self.sampler = None  # Sampler will be initialized in fit_transform

    def _initialize_sampler(self):
        """Initializes the sampler based on the specified technique."""
        technique = self.technique.lower()
        if technique == 'smote':
            return SMOTE(**self.kwargs)
        elif technique == 'random':
            return RandomOverSampler(**self.kwargs)
        elif technique == 'smote_tomek':
            return SMOTETomek(**self.kwargs)
        elif technique == 'smote_enn':
            return SMOTEENN(**self.kwargs)
        else:
            raise ValueError("Invalid technique. Choose from 'smote', 'random', 'smote_tomek', or 'smote_enn'.")

    def fit_transform(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Resamples the data by applying the selected technique to balance classes.

        Args:
            X (pd.DataFrame): Input feature DataFrame.
            y (pd.Series): Target variable Series.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: Resampled features and target.
        """
        if y is None:
            raise ValueError("The target variable 'y' is required for resampling.")
        
        # Initialize the sampler and apply fit_resample
        self.sampler = self._initialize_sampler()
        X_resampled, y_resampled = self.sampler.fit_resample(X, y)

        # Log the resampling results
        logger.debug(f"Resampling technique '{self.technique}' applied.")
        logger.debug(f"Original dataset size: {len(X)}, Resampled dataset size: {len(X_resampled)}")
        
        return pd.DataFrame(X_resampled, columns=X.columns), pd.Series(y_resampled, name=y.name)

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Returns the input features without resampling for inference.

        Args:
            X (pd.DataFrame): Input feature DataFrame.

        Returns:
            pd.DataFrame: Original (unmodified) input features.
        """
        # No resampling during inference, just return X as-is
        return X

    # def transform(self, X: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     The transform method, allowing the ImbalanceHandler to pass data through unmodified during inference or testing.

    #     Args:
    #         X (pd.DataFrame): Input feature DataFrame.

    #     Returns:
    #         pd.DataFrame: Unmodified input DataFrame.
    #     """
    #     return X