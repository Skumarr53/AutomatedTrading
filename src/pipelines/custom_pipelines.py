import warnings
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Union
from imblearn.pipeline import Pipeline
from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from joblib import Memory
from omegaconf import OmegaConf, DictConfig
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
from src.pipelines.family_pipeline import FamilyPipelineBuilder, get_family_for_model

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
        """
        Adds feature selection step to the pipeline.
        
        Supported selectors:
            - 'RFE': Recursive Feature Elimination
            - 'RFECV': RFE with Cross-Validation
            - 'SHAP': SHAP-based selection (uses LGBMClassifier by default)
            - 'LGBM': LightGBM gain-based importance [RECOMMENDED]
            - 'CORR': Correlation filter (removes redundant features)
            - 'MULTI': Multi-stage selection (Correlation + LGBM) [RECOMMENDED]
        """
        feature_selector_name = self.feature_config.get('feature_selector', None)
        if not feature_selector_name:
            return
        
        # Convert OmegaConf to native Python objects for sklearn compatibility
        pipeline_params = OmegaConf.to_container(config.model.pipeline_params, resolve=True)
        self.params = {**self.params, **pipeline_params}
        
        feature_selector_class = FeatSelect_mapping.get(feature_selector_name, None)
        if feature_selector_class is None:
            logger.warning(f"Unknown feature selector '{feature_selector_name}', skipping feature selection")
            return
        
        # Get feature selection parameters from config
        fs_params = self.feature_config.get('feature_selection_params', {})
        if hasattr(fs_params, 'items'):  # OmegaConf/dict
            fs_params = OmegaConf.to_container(fs_params, resolve=True) if hasattr(fs_params, '_iter_ex') else dict(fs_params)
        else:
            fs_params = {}
        
        # Create selector with appropriate parameters
        if feature_selector_name == 'LGBM':
            # LGBMImportanceSelector with configurable parameters
            n_features = fs_params.get('n_features', 50)
            importance_type = fs_params.get('importance_type', 'gain')
            selector = feature_selector_class(
                n_features=n_features,
                importance_type=importance_type
            )
            logger.info(f"Feature selection: LGBM importance (n_features={n_features}, type={importance_type})")
            
        elif feature_selector_name == 'SHAP':
            # DFShapFeatureSelector with configurable parameters
            n_features = fs_params.get('n_features', 50)
            sample_size = fs_params.get('sample_size', 1000)
            selector = feature_selector_class(
                n_features=n_features,
                sample_size=sample_size
            )
            logger.info(f"Feature selection: SHAP (n_features={n_features}, sample_size={sample_size})")
            
        elif feature_selector_name == 'CORR':
            # CorrelationFilter with configurable threshold
            threshold = fs_params.get('threshold', 0.95)
            method = fs_params.get('method', 'pearson')
            selector = feature_selector_class(threshold=threshold, method=method)
            logger.info(f"Feature selection: Correlation filter (threshold={threshold}, method={method})")
            
        elif feature_selector_name == 'MULTI':
            # MultiStageFeatureSelector with configurable stages
            from src.preprocessing.custom_transformers import CorrelationFilter, LGBMImportanceSelector
            
            corr_threshold = fs_params.get('corr_threshold', 0.95)
            n_features = fs_params.get('n_features', 50)
            importance_type = fs_params.get('importance_type', 'gain')
            
            stages = [
                ('correlation', CorrelationFilter(threshold=corr_threshold)),
                ('lgbm_importance', LGBMImportanceSelector(
                    n_features=n_features,
                    importance_type=importance_type
                ))
            ]
            selector = feature_selector_class(stages=stages)
            logger.info(f"Feature selection: Multi-stage (corr_threshold={corr_threshold}, n_features={n_features})")
            
        elif feature_selector_name in ('RFE', 'RFECV'):
            # RFE/RFECV with configurable n_features
            n_features = fs_params.get('n_features', 50)
            selector = feature_selector_class(n_features=n_features)
            logger.info(f"Feature selection: {feature_selector_name} (n_features={n_features})")
            
        else:
            # Default instantiation
            selector = feature_selector_class()
            logger.info(f"Feature selection: {feature_selector_name} (default parameters)")
        
        self.steps.append(('feature_selection', selector))

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


# =============================================================================
# FAMILY-BASED MODEL PIPELINE (New Architecture)
# =============================================================================

class FamilyModelPipeline:
    """
    Family-based ML pipeline that uses model families with auto-determined preprocessing.
    
    This is the new architecture that allows model type to be a hyperparameter
    within a model family. Each family has specific preprocessing requirements
    that are automatically configured.
    
    Model Families:
        - baseline: Fast reference models (LR, DT) - for comparison
        - tree: LGBM, XGB, RFC, GBC, ETC - no scaling needed
        - linear: LR, LSVC, Ridge, SGD - scaling required
        - distance: KNN, SVC - scaling required
        - neural: MLP - scaling + feature reduction
    
    Example:
        # Create pipeline for tree family
        pipeline = FamilyModelPipeline(family='tree')
        pipeline.define_model()
        pipeline.fit(X_train, y_train)
        
        # Compare multiple families
        pipeline = FamilyModelPipeline(families=['tree', 'linear', 'baseline'])
        results = pipeline.compare_families(X_train, y_train)
    """
    
    def __init__(
        self,
        family: Optional[str] = None,
        families: Optional[List[str]] = None,
        feature_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize FamilyModelPipeline.
        
        Args:
            family: Single family name ('tree', 'linear', 'baseline', etc.)
            families: List of families for comparison mode
            feature_config: Additional configuration (imbalance_technique, max_samples_per_class, etc.)
        """
        self.family = family
        self.families = families or ([family] if family else ['tree'])
        self.feature_config = feature_config or {}
        
        # Get features from config
        self.features: List[str] = config.columns.custom_cs_cols if getattr(config.model_settings, 'model_type', None) == 'COMB' else []
        
        # Add 'symbol' to features if using combined model training
        if getattr(config.training, 'combine_all_symbols', False) and 'symbol' not in self.features:
            self.features = ['symbol'] + self.features
        
        # Hardware-aware optimization
        if hasattr(config, 'hardware_optimization') and config.hardware_optimization.auto_detect:
            hw_detector = get_hardware_detector()
            mode = config.hardware_optimization.mode or 'balanced'
            optimal_max_samples = hw_detector.get_max_samples_per_class(mode=mode)
            
            current_max = self.feature_config.get('max_samples_per_class', None)
            if current_max is None or (current_max > optimal_max_samples * 1.5):
                self.feature_config['max_samples_per_class'] = optimal_max_samples
                logger.info(f"Hardware-aware optimization: max_samples_per_class={optimal_max_samples}")
        
        # Initialize components
        self.target_encoder = TargetLabelEncoder()
        self.model = None
        self.pipeline = None
        self.search_cv = None
        self.best_family = None
        self.family_results = {}
        
        # Classify columns
        self._classify_columns()
        
        # Initialize params (for compatibility with base_pipeline.py)
        # Will be populated when define_model() is called
        self.params = {}
        
        logger.info(f"FamilyModelPipeline initialized: families={self.families}")
    
    def _classify_columns(self):
        """Classify columns into numeric and categorical."""
        self.numeric_cols = [
            col for col in self.features 
            if col in config.columns.short_num_cols or col in config.columns.long_num_cols
        ]
        self.categorical_cols = [
            col for col in self.features 
            if col in config.columns.cat_cols
        ]
        
        logger.debug(f"Columns: {len(self.numeric_cols)} numeric, {len(self.categorical_cols)} categorical")
    
    def _get_family_builder(self, family_name: str) -> FamilyPipelineBuilder:
        """Create a FamilyPipelineBuilder for the specified family."""
        return FamilyPipelineBuilder(
            family_name=family_name,
            config=config,
            numeric_cols=self.numeric_cols,
            categorical_cols=self.categorical_cols,
            feature_cols=self.features
        )
    
    def define_pipeline(self, family: Optional[str] = None) -> SklearnPipeline:
        """
        Define the sklearn Pipeline for a specific family.
        
        Args:
            family: Family name. If None, uses self.family or first in self.families.
            
        Returns:
            sklearn Pipeline with preprocessing and ModelSelector.
        """
        family = family or self.family or self.families[0]
        
        builder = self._get_family_builder(family)
        
        # Build pipeline with optional resampler
        imbalance_technique = self.feature_config.get('imbalance_technique')
        self.pipeline = builder.build_pipeline(
            include_resampler=bool(imbalance_technique),
            resampler_type=imbalance_technique
        )
        
        return self.pipeline
    
    def define_model(
        self,
        family: Optional[str] = None,
        n_iter: Optional[int] = None,
        n_splits: int = 5,
        gap: int = 0,
        n_jobs: int = -1,
        scoring: str = 'f1_weighted',
        verbose: int = 1
    ) -> RandomizedSearchCV:
        """
        Define RandomizedSearchCV model with family-based pipeline.
        
        Args:
            family: Family name. If None, uses self.family or first in self.families.
            n_iter: Number of iterations. If None, calculated proportionally.
            n_splits: Number of CV splits.
            gap: Embargo gap for TimeSeriesSplit.
            n_jobs: Number of parallel jobs.
            scoring: Scoring metric.
            verbose: Verbosity level.
            
        Returns:
            Configured RandomizedSearchCV.
        """
        family = family or self.family or self.families[0]
        builder = self._get_family_builder(family)
        
        # Build pipeline
        imbalance_technique = self.feature_config.get('imbalance_technique')
        pipeline = builder.build_pipeline(
            include_resampler=bool(imbalance_technique),
            resampler_type=imbalance_technique
        )
        
        # Get proportional n_iter
        if n_iter is None:
            n_iter = builder.get_n_iter()
        
        # Get param grid and store in self.params for compatibility
        param_grid = builder.get_param_grid()
        self.params = param_grid
        
        # Create TimeSeriesSplit CV
        cv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
        
        # Create RandomizedSearchCV
        self.search_cv = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=param_grid,
            n_iter=n_iter,
            cv=cv,
            scoring=scoring,
            n_jobs=n_jobs,
            verbose=verbose,
            random_state=42,
            return_train_score=True,
            refit=True,
            error_score='raise'
        )
        
        self.model = self.search_cv
        
        logger.info(f"Defined model for '{family}' family: n_iter={n_iter}, "
                   f"cv_splits={n_splits}, scoring={scoring}")
        
        return self.search_cv
    
    def fit(self, X: pd.DataFrame, y: pd.Series) -> 'FamilyModelPipeline':
        """
        Fit the pipeline to the data.
        
        Args:
            X: Input features.
            y: Target variable.
            
        Returns:
            self
        """
        # Encode target
        y_encoded = self.target_encoder.fit_transform(y)
        
        # Validate data
        self._validate_data(X)
        
        # Ensure model is defined
        if self.model is None:
            self.define_model()
        
        # Fit
        logger.info(f"Fitting model: X shape={X.shape}, y shape={y_encoded.shape}")
        self.model.fit(X, y_encoded)
        
        # Log results
        if hasattr(self.model, 'best_params_'):
            logger.info(f"Best params: {self.model.best_params_}")
        if hasattr(self.model, 'best_score_'):
            logger.info(f"Best CV score: {self.model.best_score_:.4f}")
        
        return self
    
    def _validate_data(self, X: pd.DataFrame) -> None:
        """Validate input data before fitting."""
        # Check for sequences
        for col in X.columns:
            sample = X[col].dropna().head(10)
            for val in sample:
                if isinstance(val, (list, tuple, np.ndarray)) and not isinstance(val, str):
                    raise ValueError(f"Column '{col}' contains sequences instead of scalars")
        
        # Check for object dtypes
        object_cols = X.select_dtypes(include=['object']).columns.tolist()
        if object_cols:
            logger.warning(f"Found {len(object_cols)} object dtype columns: {object_cols[:5]}...")
    
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Predict using the fitted model.
        
        Args:
            X: Input features.
            
        Returns:
            Decoded predictions.
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        y_pred_encoded = self.model.predict(X)
        return self.target_encoder.inverse_transform(y_pred_encoded)
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict class probabilities.
        
        Args:
            X: Input features.
            
        Returns:
            Predicted probabilities.
        """
        if self.model is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        return self.model.predict_proba(X)
    
    def compare_families(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        families: Optional[List[str]] = None,
        n_iter: Optional[int] = None,
        n_splits: int = 5,
        scoring: str = 'f1_weighted'
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare multiple model families and find the best one.
        
        Args:
            X: Input features.
            y: Target variable.
            families: List of families to compare. If None, uses self.families.
            n_iter: Iterations per family.
            n_splits: CV splits.
            scoring: Scoring metric.
            
        Returns:
            Dict with results for each family, including best model and score.
        """
        families = families or self.families
        y_encoded = self.target_encoder.fit_transform(y)
        
        results = {}
        best_score = -np.inf
        best_family = None
        best_model = None
        
        for family_name in families:
            logger.info(f"\n{'='*50}")
            logger.info(f"Training family: {family_name}")
            logger.info(f"{'='*50}")
            
            try:
                # Create and fit model for this family
                builder = self._get_family_builder(family_name)
                
                imbalance_technique = self.feature_config.get('imbalance_technique')
                pipeline = builder.build_pipeline(
                    include_resampler=bool(imbalance_technique),
                    resampler_type=imbalance_technique
                )
                
                # Get family-specific n_iter
                family_n_iter = n_iter or builder.get_n_iter()
                param_grid = builder.get_param_grid()
                
                cv = TimeSeriesSplit(n_splits=n_splits)
                
                search = RandomizedSearchCV(
                    estimator=pipeline,
                    param_distributions=param_grid,
                    n_iter=family_n_iter,
                    cv=cv,
                    scoring=scoring,
                    n_jobs=-1,
                    verbose=1,
                    random_state=42,
                    return_train_score=True,
                    refit=True
                )
                
                # Validate data
                self._validate_data(X)
                
                # Fit
                search.fit(X, y_encoded)
                
                # Store results
                results[family_name] = {
                    'best_score': search.best_score_,
                    'best_params': search.best_params_,
                    'best_model_type': search.best_params_.get('model__model_type', 'unknown'),
                    'cv_results': search.cv_results_,
                    'fitted_model': search.best_estimator_,
                    'n_iter': family_n_iter
                }
                
                logger.info(f"Family '{family_name}': best_score={search.best_score_:.4f}, "
                           f"best_model={results[family_name]['best_model_type']}")
                
                # Track overall best
                if search.best_score_ > best_score:
                    best_score = search.best_score_
                    best_family = family_name
                    best_model = search
                    
            except Exception as e:
                logger.error(f"Failed to train family '{family_name}': {e}")
                results[family_name] = {
                    'error': str(e),
                    'best_score': -np.inf
                }
        
        # Set best model
        self.best_family = best_family
        self.model = best_model
        self.family_results = results
        
        logger.info(f"\n{'='*50}")
        logger.info(f"BEST FAMILY: {best_family} (score={best_score:.4f})")
        logger.info(f"{'='*50}")
        
        return results
    
    @property
    def best_params_(self) -> Dict[str, Any]:
        """Get best parameters from fitted model."""
        if self.model is None:
            raise ValueError("Model not fitted.")
        return self.model.best_params_
    
    @property
    def best_score_(self) -> float:
        """Get best CV score from fitted model."""
        if self.model is None:
            raise ValueError("Model not fitted.")
        return self.model.best_score_
    
    @property
    def best_estimator_(self):
        """Get best estimator from fitted model."""
        if self.model is None:
            raise ValueError("Model not fitted.")
        return self.model.best_estimator_
