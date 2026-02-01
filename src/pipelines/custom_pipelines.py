import warnings
import pandas as pd
from typing import Dict, List, Optional
from imblearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from joblib import Memory
from omegaconf import OmegaConf
from loguru import logger

# Suppress warnings in this module
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

from src import config
from src.config import (FeatSelect_mapping,
                        ImbalanceHandler_mapping,
                        ModelType_mapping)
from src.preprocessing.custom_transformers import (
    TargetLabelEncoder,
    DFFeatureUnion,
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    ResamplerTransformer
 # Assuming ImbalanceHandler is defined in custom transformers
)
from src.preprocessing.company_metadata_transformer import CompanyMetadataTransformer
from src.utils.hardware_detector import get_hardware_detector

class CustomModelPipeline():

    def __init__(self, feature_config: Optional[Dict[str, bool]] = None) -> None:
        """
        Initializes the CustomModelPipeline instance.

        Args:
            feature_config (Dict[str, bool], optional): Configuration to specify which feature groups to include.
            feature_selector (str, optional): Feature selector type ('RFE' or 'SHAP').
            model (Any, optional): Model to use in the pipeline (default: RandomForestClassifier).
            use_feature_selection (bool, optional): Whether to include feature selection in the pipeline.
            use_imbalance_handling (bool, optional): Whether to include imbalance handling in the pipeline.
            imbalance_technique (str, optional): Technique to handle imbalanced data ('smote', 'random', etc.).
        """
        super().__init__()
        self.features: List[str] = config.columns.custom_cs_cols if config.model_settings.model_type == 'COMB' else []
        
        # Add 'symbol' to features if using combined model training
        if getattr(config.training, 'combine_all_symbols', False) and 'symbol' not in self.features:
            self.features = ['symbol'] + self.features
            
        self.feature_config = feature_config or {}
        
        # Hardware-aware optimization: adjust max_samples_per_class if auto_detect is enabled
        if hasattr(config, 'hardware_optimization') and config.hardware_optimization.auto_detect:
            hw_detector = get_hardware_detector()
            mode = config.hardware_optimization.mode or 'balanced'
            optimal_max_samples = hw_detector.get_max_samples_per_class(mode=mode)
            
            # Override if not explicitly set in config, or if config value is too high for hardware
            current_max = self.feature_config.get('max_samples_per_class', None)
            if current_max is None or (current_max > optimal_max_samples * 1.5):
                self.feature_config['max_samples_per_class'] = optimal_max_samples
                logger.info(f"Hardware-aware optimization: Set max_samples_per_class={optimal_max_samples} "
                          f"(mode={mode}, RAM={hw_detector.detect()['ram_total_gb']:.1f}GB)")
        
        self.params = {} 
        self.target_encoder = TargetLabelEncoder()  # Initialize TargetLabelEncoder
        self.model = None
        self.steps = None
        self.input_columns_prep()

    def input_columns_prep(self):
        self.short_numeric_cols = [col for col in self.features if col in config.columns.short_num_cols]
        self.long_numeric_cols = [col for col in self.features if col in config.columns.long_num_cols]
        self.cat_cols = [col for col in self.features if col in config.columns.cat_cols]
        
        # Add metadata features if enabled
        if getattr(config.metadata, 'enabled', False):
            # Metadata features will be added dynamically by the transformer
            # We'll store them separately for reference
            self.use_metadata = True
        else:
            self.use_metadata = False

    def empty_pipeline_and_params(self):
        self.steps = []
        self.params = {}

    def get_prepocessed_numeric_features(self):

        # Dynamically add feature groups based on configuration
        numeric_feature_transformers = []
        if self.feature_config.get('std_scale', False):
            # If standard scaling is enabled, extract and normalize numerics
            numeric_feature_transformers.append(('short_numerics', ColumnExtractor(self.short_numeric_cols)))
            numeric_feature_transformers.append(('short_normalize', ShortTermNormalizer()))
            numeric_feature_transformers.append(('long_numerics', ColumnExtractor(self.long_numeric_cols)))
            numeric_feature_transformers.append(('long_normalize', LongTermNormalizer()))
        else:
            # If standard scaling is disabled, just extract numerics without normalization
            numeric_feature_transformers.append(('short_numerics', ColumnExtractor(self.short_numeric_cols)))
            numeric_feature_transformers.append(('long_numerics', ColumnExtractor(self.long_numeric_cols)))

        # self.steps.append(('features', DFFeatureUnion(numeric_feature_transformers)))
        return numeric_feature_transformers

    def get_prepocessed_categorical_features(self):

        cat_feature_transformers = []
        cat_feature_transformers.append(('cat_extract', ColumnExtractor(self.cat_cols)))
        if self.feature_config.get('cat_encode', False):
            cat_feature_transformers.append(('cat_normalize', CategoricalPreprocessor(self.cat_cols)))
        return cat_feature_transformers

    def add_metadata_transformer(self):
        """
        Adds company metadata transformer to the pipeline if enabled.
        This should be called before feature extraction.
        """
        if self.use_metadata:
            metadata_transformer = CompanyMetadataTransformer(
                cache_dir=getattr(config.metadata.cache, 'directory', './data/cache'),
                encoding_method=getattr(config.metadata.transformer, 'encoding_method', 'label'),
                handle_missing=getattr(config.metadata.transformer, 'handle_missing', 'constant'),
                include_categorical=getattr(config.metadata.transformer, 'include_categorical', True),
                include_numerical=getattr(config.metadata.transformer, 'include_numerical', True)
            )
            self.steps.append(('company_metadata', metadata_transformer))
    
    def add_combined_preprocessed_features(self):
        self.steps.append(
            (
                "features",
                DFFeatureUnion(
                    self.get_prepocessed_numeric_features()
                    + self.get_prepocessed_categorical_features()
                ),
            )
        )
    
    def update_resampling_pipeline(self):
        """
        Adds the resampling step to the pipeline if imbalance handling is enabled.
        """
        imbalance_technique = self.feature_config.get('imbalance_technique', None)
        if imbalance_technique:
            sampler = ImbalanceHandler_mapping.get(imbalance_technique, 'smote')
            if sampler is None:
                raise ValueError(f"Imbalance technique '{imbalance_technique}' is not supported.")
            
            # Get max_samples_per_class from config (default: None for no limit)
            max_samples_per_class = self.feature_config.get('max_samples_per_class', None)
            
            self.steps.append(('resample', ResamplerTransformer(
                sampler=sampler(),
                shuffle=True,
                random_state=42,  # You can make this configurable
                max_samples_per_class=max_samples_per_class
            )))

    def update_feature_selection_pipeline(self):
        feature_selector = self.feature_config.get('feature_selector', None)
        if feature_selector:
            # Convert OmegaConf to native Python objects for sklearn compatibility
            pipeline_params = OmegaConf.to_container(config.model.pipeline_params, resolve=True)
            self.params = {**self.params, **pipeline_params}
            feature_selector = FeatSelect_mapping.get(feature_selector, None)
            self.steps.append(('feature_selection', feature_selector()))

    def update_model_pipeline(self):
        model_type = self.feature_config.get('model', None)
        model_class = ModelType_mapping.get(model_type, RandomForestClassifier)
        # Add the model as the final step
        self.steps.append(('model_fit', model_class()))
        # Convert OmegaConf to native Python objects for sklearn compatibility
        model_params = OmegaConf.to_container(
            config.model.model_params.get(model_type, 'RFC'), 
            resolve=True
        )
        self.params = {**self.params, **model_params}

    def define_pipeline(self) -> None:
        """
        Defines the machine learning pipeline with configurable feature extraction, selection, model, 
        and optional imbalance handling.

        The pipeline consists of:
            0. Optional Company Metadata: Enriches features with company metadata if enabled.
            1. Feature Union: Combines configurable feature groups.
            2. Optional Imbalance Handling: Applies a resampling technique if enabled.
            3. Optional Feature Selection: Applies the specified feature selector if enabled.
            4. Model Fit: Uses the specified model.
        """
        if not self.features:
            raise ValueError("Features must be set before defining the pipeline.")

        ## Reset pipeline
        self.empty_pipeline_and_params()

        ## Define pipliene steps
        # self.add_metadata_transformer()  # Add metadata transformer first
        self.add_combined_preprocessed_features()
        self.update_resampling_pipeline()
        self.update_feature_selection_pipeline()
        self.update_model_pipeline()

        # Define the pipeline with the configured steps
        self.pipeline = Pipeline(self.steps)

    def define_model(self, memory: Memory = None, n_splits: int = 5, gap: int = 0, 
                     n_iter: Optional[int] = None, n_jobs: Optional[int] = None) -> RandomizedSearchCV:
        """
        Defines the machine learning model using RandomizedSearchCV for hyperparameter tuning.
        
        IMPORTANT: Uses TimeSeriesSplit for cross-validation to prevent data leakage.
        Standard KFold would allow future data to train the model, which is invalid
        for time series prediction.

        Args:
            memory: Joblib Memory instance for caching.
            n_splits: Number of splits for TimeSeriesSplit cross-validation.
            gap: Number of samples to exclude from the end of each train set before
                 the test set (embargo period to prevent leakage at boundaries).
            n_iter: Number of hyperparameter search iterations. If None, auto-detected from hardware.
            n_jobs: Number of parallel jobs. If None, auto-detected from hardware.

        Returns:
            RandomizedSearchCV: Configured search instance with temporal CV.
        """
        self.define_pipeline()
        if memory:
            self.pipeline.memory = memory

        # Hardware-aware optimization
        hw_detector = get_hardware_detector()
        hw_config = config.hardware_optimization
        
        # Auto-detect n_jobs if not specified
        n_jobs = 10
        if n_jobs is None:
            if hw_config.auto_detect and hw_config.n_jobs is None:
                mode = hw_config.mode or 'balanced'
                reserve = hw_config.reserve_cores or 2
                n_jobs = hw_detector.get_optimal_n_jobs(mode=mode, reserve_cores=reserve)
                logger.info(f"Auto-detected optimal n_jobs={n_jobs} (mode={mode}, reserve_cores={reserve})")
            else:
                n_jobs = hw_config.n_jobs or 4
        
        # Auto-detect n_iter if not specified
        if n_iter is None:
            if hw_config.auto_detect:
                mode = hw_config.mode or 'balanced'
                n_iter = hw_detector.get_optimal_n_iter(mode=mode)
                logger.info(f"Auto-detected optimal n_iter={n_iter} (mode={mode})")
            else:
                n_iter = 20

        # Use TimeSeriesSplit instead of standard KFold
        # This ensures training data always comes BEFORE test data chronologically
        tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
        
        logger.info(f"Using TimeSeriesSplit for CV: n_splits={n_splits}, gap={gap}")

        self.model = RandomizedSearchCV(
            self.pipeline,
            param_distributions=self.params,
            n_iter=n_iter,
            scoring='accuracy',
            n_jobs=n_jobs,
            cv=tscv,  # Use TimeSeriesSplit instead of int
            random_state=42,
            verbose=1,
            return_train_score=True,
            error_score='raise'
        )

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """
        Fits the pipeline to the data, including label encoding for the target variable.

        Args:
            X (pd.DataFrame): Input feature data.
            y (pd.Series): Target variable.
        """
        # Encode the target variable
        y_encoded = self.target_encoder.fit_transform(y)

        # Validate data before fitting (catch sequence issues early)
        import numpy as np
        from loguru import logger
        
        # Check for sequences in X
        sequence_cols = []
        for col in X.columns:
            try:
                sample = X[col].dropna().head(10)
                for val in sample:
                    if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                        sequence_cols.append(col)
                        logger.error(
                            f"Column '{col}' contains sequences before model fit: "
                            f"type={type(val)}, sample_value={val}"
                        )
                        break
            except Exception:
                pass
        
        if sequence_cols:
            raise ValueError(
                f"DataFrame contains columns with sequences (lists/arrays) instead of scalars: {sequence_cols}. "
                f"Shape: {X.shape}. "
                f"This will cause sklearn to fail. Check transformers in pipeline."
            )
        
        # Check shape is reasonable
        if X.shape[0] < 2:
            raise ValueError(f"Not enough samples for training: {X.shape[0]} rows")
        
        if X.shape[1] > 50000:  # Suspiciously large number of columns
            logger.warning(
                f"Very large number of columns: {X.shape[1]}. "
                f"This might indicate a transformer is creating too many features."
            )
        
        # Try to convert to numpy array to catch any issues early
        try:
            test_array = np.asarray(X.head(100))
            logger.debug(f"Data validation passed: shape={X.shape}, array_shape={test_array.shape}")
        except ValueError as e:
            logger.error(f"Failed to convert DataFrame to numpy array: {e}")
            logger.error(f"  DataFrame shape: {X.shape}")
            logger.error(f"  DataFrame dtypes: {X.dtypes.value_counts().to_dict()}")
            
            # Try to identify problematic columns
            problematic = []
            for col in X.columns:
                try:
                    np.asarray(X[col].head(100))
                except (ValueError, TypeError):
                    problematic.append(col)
            
            if problematic:
                logger.error(f"Problematic columns: {problematic[:10]}")
                for col in problematic[:5]:
                    logger.error(f"  Column '{col}': dtype={X[col].dtype}, sample={X[col].head(3).tolist()}")
            
            raise ValueError(
                f"Cannot convert DataFrame to numpy array: {e}. "
                f"Problematic columns: {problematic[:10]}"
            )

        # === CRITICAL: Validate and fix object dtypes before model.fit() ===
        # LightGBM and XGBoost require numeric dtypes (int, float, bool)
        object_cols = X.select_dtypes(include=['object']).columns.tolist()
        if object_cols:
            logger.warning(f"Found {len(object_cols)} object dtype columns before model fit: {object_cols[:10]}...")
            # Force conversion to numeric - this is a safety net, not the primary fix
            for col in object_cols:
                try:
                    X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0.0)
                    logger.debug(f"  Converted '{col}' to numeric dtype")
                except Exception as conv_err:
                    logger.error(f"  Failed to convert '{col}': {conv_err}")
                    # Last resort: drop the column
                    X = X.drop(columns=[col])
                    logger.warning(f"  Dropped column '{col}' due to conversion failure")
            
            # Log final dtype distribution
            dtype_counts = X.dtypes.value_counts().to_dict()
            logger.info(f"After dtype fix: {dtype_counts}")
        
        # Final validation: ensure no object dtypes remain
        remaining_object_cols = X.select_dtypes(include=['object']).columns.tolist()
        if remaining_object_cols:
            raise ValueError(
                f"Cannot proceed with model.fit(): {len(remaining_object_cols)} columns "
                f"still have object dtype after conversion attempt: {remaining_object_cols[:10]}"
            )

        # Fit the pipeline
        self.model.fit(X, y_encoded)

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Predicts the target variable using the fitted pipeline and decodes predictions.

        Args:
            X (pd.DataFrame): Input feature data.

        Returns:
            pd.Series: Decoded predictions.
        """
        # Predict and decode target
        y_pred_encoded = self.model.predict(X)
        return self.target_encoder.inverse_transform(y_pred_encoded)
