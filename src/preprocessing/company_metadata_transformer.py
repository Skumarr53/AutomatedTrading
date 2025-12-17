"""
Company Metadata Transformer

Custom sklearn transformer that joins company metadata with feature data
and transforms categorical metadata into model-ready features.
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from loguru import logger

from src.data.company_metadata import CompanyMetadataFetcher


class CompanyMetadataTransformer(BaseEstimator, TransformerMixin):
    """
    Transformer that enriches feature data with company metadata.
    
    This transformer:
    1. Loads cached company metadata
    2. Joins metadata with feature data based on symbol
    3. Encodes categorical features (sector, industry, etc.)
    4. Creates numerical features from metadata
    5. Handles missing values appropriately
    
    Attributes:
        cache_dir (str): Directory containing the metadata cache
        categorical_features (List[str]): List of categorical features to encode
        numerical_features (List[str]): List of numerical features to include
        encoding_method (str): Method for encoding ('onehot' or 'label')
        handle_missing (str): How to handle missing values ('constant' or 'drop')
    """
    
    ORDINAL_ORDERS = {
    "market_cap_bucket": [
        "micro",
        "small",
        "mid",
        "large",
        "mega",
        "Unknown"  # Always keep 'Unknown' last
    ]
}
    
    ENCODING_MAP = {
      'sector': 'ordinal',
      'industry': 'ordinal',
      'market_cap_bucket': 'ordinal',
      'country': 'ordinal'
  }
    
    CATEGORICAL_FEATURES = [
        'sector', 
        'industry', 
        'market_cap_bucket', 
        'country'
    ]
    
    NUMERICAL_FEATURES = [
        'market_cap',
        'employees',
        'years_since_listing'
    ]
    
    def __init__(
        self,
        cache_dir: str = "./data/cache",
        encoding_method: str = 'label',
        handle_missing: str = 'constant',
        include_categorical: bool = True,
        include_numerical: bool = True,
        # encoding_map: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the CompanyMetadataTransformer.
        Args:
            cache_dir: Directory containing the metadata cache
            encoding_method: Default for encoding if not provided in encoding_map
            handle_missing: 'constant' to fill with default, 'drop' to drop rows
            include_categorical: Whether to include categorical features
            include_numerical: Whether to include numerical features
            encoding_map: dict, feature -> method ('label', 'onehot', 'ordinal')
            ordinal_orders: dict, feature -> value order list (for ordinal encoding)
        """
        self.cache_dir = cache_dir
        self.encoding_method = encoding_method
        self.handle_missing = handle_missing
        self.include_categorical = include_categorical
        self.include_numerical = include_numerical
        # Will be initialized during fit
        self.metadata_fetcher: Optional[CompanyMetadataFetcher] = None
        self.metadata_df: Optional[pd.DataFrame] = None
        self.label_encoders: Dict[str, LabelEncoder] = {}
        self.ordinal_encoders: Dict[str, OrdinalEncoder] = {}
        self.feature_columns: List[str] = []
        self.fitted_ = False
    
    def _initialize_metadata_fetcher(self):
        """Initialize the metadata fetcher if not already initialized."""
        if self.metadata_fetcher is None:
            self.metadata_fetcher = CompanyMetadataFetcher(cache_dir=self.cache_dir)
            self.metadata_df = self.metadata_fetcher.get_cached_metadata()
            logger.info(f"Loaded {len(self.metadata_df)} company metadata entries")
    
    def _compute_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute derived features from raw metadata.
        
        Args:
            df: DataFrame with raw metadata
            
        Returns:
            DataFrame with derived features added
        """
        df = df.copy()
        
        # Calculate years since listing
        if 'listing_date' in df.columns:
            df['listing_date'] = pd.to_datetime(df['listing_date'], errors='coerce')
            current_date = pd.Timestamp.now()
            df['years_since_listing'] = (current_date - df['listing_date']).dt.days / 365.25
            df['years_since_listing'] = df['years_since_listing'].fillna(0)
        else:
            df['years_since_listing'] = 0
        
        # Log transform market cap (helps with scale)
        if 'market_cap' in df.columns:
            df['market_cap_log'] = np.log1p(df['market_cap'].fillna(0))
        
        # Log transform employees
        if 'employees' in df.columns:
            df['employees_log'] = np.log1p(df['employees'].fillna(0))
        
        return df
    
    def _encode_categorical_features(self, df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
        """
        Encode categorical features using the specified method.
        
        Args:
            df: DataFrame with categorical features
            is_training: Whether this is training data (fit encoders) or test data (transform only)
            
        Returns:
            DataFrame with encoded features
        """
        df = df.copy()
        
        if not self.include_categorical:
            return df
        
        for feature in self.CATEGORICAL_FEATURES:
            method = self.ENCODING_MAP.get(feature, self.encoding_method)
            if feature not in df.columns:
                continue
            # Always fill missing with 'Unknown' and cast to string
            df[feature] = df[feature].fillna('Unknown').astype(str)
            
            if method == 'label':
                if is_training:
                    self.label_encoders[feature] = LabelEncoder()
                    unique_vals = pd.Series(df[feature].unique(), dtype=str)
                    if 'Unknown' not in unique_vals.values:
                        unique_vals = pd.concat([unique_vals, pd.Series(['Unknown'])]).unique()
                    self.label_encoders[feature].fit(unique_vals)
                    df[f'{feature}_encoded'] = self.label_encoders[feature].transform(df[feature])
                    logger.debug(f"Label encoded {feature}: {list(self.label_encoders[feature].classes_)}")
                else:
                    if feature in self.label_encoders:
                        known_categories = set(self.label_encoders[feature].classes_)
                        def safe_label(x):
                            if x in known_categories:
                                return x
                            logger.warning(f"Unknown category '{x}' in feature '{feature}'. Setting to 'Unknown'.")
                            return 'Unknown'
                        df[feature] = df[feature].apply(safe_label)
                        df[f'{feature}_encoded'] = self.label_encoders[feature].transform(df[feature])
                    else:
                        logger.warning(f"No encoder for {feature}, filling with 0")
                        df[f'{feature}_encoded'] = 0
                
                # Drop original categorical column
                df = df.drop(columns=[feature])
            
            elif method == 'onehot':
                # One-hot encoding
                if is_training:
                    df = pd.get_dummies(df, columns=[feature], prefix=feature, dummy_na=False, drop_first=True)
                    
                else:
                    dummies = pd.get_dummies(df[feature], prefix=feature, dummy_na=False)
                    for col in dummies.columns:
                        if col not in df.columns:
                            df[col] = dummies[col]
                    for col in [c for c in df.columns if c.startswith(f'{feature}_')]:
                        if col not in dummies.columns:
                            df[col] = 0
                    df = df.drop(columns=[feature])
            elif method == 'ordinal':
                # Ensure values are all string type
                df[feature] = df[feature].astype(str)
                # Get or define order
                order = self.ORDINAL_ORDERS.get(feature)
                if not order:
                    logger.warning(f"No order supplied for {feature} ordinal encoding. Will use sorted unique vals (as string).")
                    uniqs = pd.Series(df[feature].dropna().unique(), dtype=str)
                    if 'Unknown' not in uniqs.values:
                        uniqs = pd.concat([uniqs, pd.Series(['Unknown'])]).unique()
                    order = sorted(uniqs)
                else:
                    # Ensure your order is string list
                    order = [str(v) for v in order]
                if is_training:
                    self.ordinal_encoders[feature] = OrdinalEncoder(categories=[order], handle_unknown='use_encoded_value', unknown_value=-1)
                    df[f'{feature}_encoded'] = self.ordinal_encoders[feature].fit_transform(df[[feature]]).astype(int)
                else:
                    encoder = self.ordinal_encoders.get(feature)
                    if encoder:
                        df[f'{feature}_encoded'] = encoder.transform(df[[feature]]).astype(int)
                    else:
                        logger.warning(f"No ordinal encoder for {feature}, filling with -1")
                        df[f'{feature}_encoded'] = -1
                
                # Drop original categorical column
                df = df.drop(columns=[feature])
        
        return df
    
    def _prepare_numerical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare numerical features with appropriate scaling and missing value handling.
        
        Args:
            df: DataFrame with numerical features
            
        Returns:
            DataFrame with prepared numerical features
        """
        df = df.copy()
        
        if not self.include_numerical:
            return df
        
        # Handle numerical features
        for feature in ['market_cap', 'employees', 'years_since_listing']:
            if feature in df.columns:
                # Fill missing with 0 or median
                if self.handle_missing == 'constant':
                    df[feature] = df[feature].fillna(0)
                else:
                    median_val = df[feature].median()
                    df[feature] = df[feature].fillna(median_val)
        
        # Use log-transformed versions if available
        if 'market_cap_log' in df.columns:
            df = df.drop(columns=['market_cap'], errors='ignore')
            df = df.rename(columns={'market_cap_log': 'market_cap_log'})
        
        if 'employees_log' in df.columns:
            df = df.drop(columns=['employees'], errors='ignore')
            df = df.rename(columns={'employees_log': 'employees_log'})
        
        return df
    
    def drop_symbol_column(self, df): 
        if 'symbol' in df.columns:
            df = df.drop(columns=['symbol'])
        return df
    
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'CompanyMetadataTransformer':
        """
        Fit the transformer on training data.
        
        Args:
            X: Input DataFrame with 'symbol' column
            y: Target variable (not used)
            
        Returns:
            self
        """
        logger.info("Fitting CompanyMetadataTransformer...")
        
        # Initialize metadata fetcher
        self._initialize_metadata_fetcher()
        
        print("Blah Blah Blah")
        if self.metadata_df.empty:
            logger.warning("No metadata available in cache. Transformer will return original data.")
            self.fitted_ = True
            return self
        
        # Check if X has symbol column
        if 'symbol' not in X.columns:
            logger.warning("No 'symbol' column found in input data. Cannot join metadata.")
            self.fitted_ = True
            return self
        
        # Get unique symbols from training data
        unique_symbols = X['symbol'].unique()
        logger.info(f"Found {len(unique_symbols)} unique symbols in training data")
        
        # Prepare metadata for these symbols
        metadata_subset = self.metadata_df[self.metadata_df['symbol'].isin(unique_symbols)].copy()
        
        # Compute derived features
        metadata_subset = self._compute_derived_features(metadata_subset)
        
        # Encode categorical features (fit encoders)
        metadata_subset = self._encode_categorical_features(metadata_subset, is_training=True)
        
        # Prepare numerical features
        metadata_subset = self._prepare_numerical_features(metadata_subset)
        
        # Store feature columns for later
        metadata_features = [
            col for col in metadata_subset.columns 
            if col not in ['symbol', 'company_name', 'listing_date', 'exchange', 
                          'currency', 'website', 'business_summary', 'last_updated', 
                          'data_source', 'sub_industry']
        ]
        self.feature_columns = metadata_features
        
        logger.info(f"Fitted transformer with {len(self.feature_columns)} metadata features: {self.feature_columns}")
        self.fitted_ = True
        
        metadata_subset = self.drop_symbol_column(metadata_subset)
        
        metadata_subset.columns = [col.replace(' ', '_') for col in metadata_subset.columns]
        
        return self
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transform input data by joining with company metadata.
        
        Args:
            X: Input DataFrame with 'symbol' column
            
        Returns:
            DataFrame with metadata features joined
        """
        try:
            if not self.fitted_:
                raise ValueError("Transformer must be fitted before transform")
            
            X_transformed = X.copy()
            
            # If no metadata or no symbol column, return original data
            if self.metadata_df is None or self.metadata_df.empty:
                logger.warning("No metadata available, returning original data")
                return X_transformed
            
            if 'symbol' not in X_transformed.columns:
                logger.warning("No 'symbol' column in input, returning original data")
                return X_transformed
            
            # Prepare metadata
            metadata_for_transform = self.metadata_df.copy()
            metadata_for_transform = self._compute_derived_features(metadata_for_transform)
            metadata_for_transform = self._encode_categorical_features(metadata_for_transform, is_training=False)
            metadata_for_transform = self._prepare_numerical_features(metadata_for_transform)
            
            # Select only relevant columns
            columns_to_join = ['symbol'] + [col for col in self.feature_columns if col in metadata_for_transform.columns]
            metadata_for_join = metadata_for_transform[columns_to_join].copy()
            
            # Store original index
            original_index = X_transformed.index
            
            # Merge with metadata
            X_transformed = X_transformed.merge(
                metadata_for_join,
                on='symbol',
                how='left',
                suffixes=('', '_metadata')
            )
            
            X_transformed = self.drop_symbol_column(X_transformed)
            
            # Fill any missing values that resulted from left join
            for col in self.feature_columns:
                if col in X_transformed.columns:
                    if X_transformed[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                        X_transformed[col] = X_transformed[col].fillna(0)
                    else:
                        X_transformed[col] = X_transformed[col].fillna('Unknown')
            
            # Restore original index
            X_transformed.index = original_index
            
            logger.debug(f"Transformed data with {len(self.feature_columns)} metadata features")
            X_transformed.columns = [col.replace(' ', '_') for col in X_transformed.columns]
            
            return X_transformed
        except Exception as e:
            raise ValueError(f"Error transforming data: {e}\n this failed at CompanyMetadataTransformer transform method")
    
    def get_feature_names(self) -> List[str]:
        """
        Get the list of metadata feature names added by this transformer.
        
        Returns:
            List of feature names
        """
        if not self.fitted_:
            return []
        return self.feature_columns
    
    def get_metadata_summary(self) -> Dict:
        """
        Get a summary of the metadata being used.
        
        Returns:
            Dictionary with metadata statistics
        """
        if self.metadata_fetcher is None:
            return {}
        
        return self.metadata_fetcher.get_cache_stats()

