"""
Family Pipeline Builder - Builds sklearn pipelines based on model family.

This module provides a pipeline builder that automatically determines preprocessing
steps based on the model family (tree, linear, distance, neural, baseline).

Example:
    builder = FamilyPipelineBuilder('tree', config)
    pipeline = builder.build_pipeline()
    param_grid = builder.get_param_grid()
    
    search = RandomizedSearchCV(
        pipeline,
        param_grid,
        n_iter=builder.get_n_iter()
    )
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import warnings
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from imblearn.pipeline import Pipeline as ImbPipeline
from omegaconf import DictConfig, OmegaConf
from loguru import logger

from src.pipelines.model_selector import ModelSelector


class FamilyPipelineBuilder:
    """
    Builds sklearn Pipeline with preprocessing auto-determined by model family.
    
    Each family has specific preprocessing requirements:
    - baseline: Scale + Encode (fast reference models)
    - tree: No scaling/encoding (LGBM/XGB handle natively)
    - linear: Scale + Encode + Feature Selection (coefficients sensitive to scale)
    - distance: Scale + Encode (distance metrics need uniform scale)
    - neural: Scale + Encode + Feature Selection (gradient descent needs normalized inputs)
    
    Attributes:
        family_name (str): Name of the model family.
        family_config (DictConfig): Configuration for this family from model_families.yaml.
        models (list): List of enabled model types for this family.
        numeric_cols (list): Names of numeric columns.
        categorical_cols (list): Names of categorical columns.
    """
    
    def __init__(
        self,
        family_name: str,
        config: DictConfig,
        numeric_cols: Optional[List[str]] = None,
        categorical_cols: Optional[List[str]] = None,
        feature_cols: Optional[List[str]] = None
    ) -> None:
        """
        Initialize FamilyPipelineBuilder.
        
        Args:
            family_name: Name of the model family ('tree', 'linear', 'baseline', etc.)
            config: Full application config containing model_families.
            numeric_cols: List of numeric column names.
            categorical_cols: List of categorical column names.
            feature_cols: List of all feature column names (if not split by type).
        """
        self.family_name = family_name
        self._config = config
        
        # Load family configuration
        if not hasattr(config.model, 'model_families'):
            raise ValueError("Config missing 'model.model_families'. Load model_families.yaml first.")
        
        if family_name not in config.model.model_families:
            available = list(config.model.model_families.keys())
            raise ValueError(f"Unknown family '{family_name}'. Available: {available}")
        
        self.family_config = config.model.model_families[family_name]
        self.models = list(self.family_config.models)  # Convert to list
        
        # Store column info
        self.numeric_cols = numeric_cols or []
        self.categorical_cols = categorical_cols or []
        self.feature_cols = feature_cols or (self.numeric_cols + self.categorical_cols)
        
        # Get resource config (from model_families.yaml)
        self.resource_config = getattr(config.model, 'resource_config', {})
        if isinstance(self.resource_config, DictConfig):
            self.resource_config = OmegaConf.to_container(self.resource_config, resolve=True)
        
        logger.info(f"FamilyPipelineBuilder initialized for '{family_name}' family "
                   f"with {len(self.models)} models: {self.models}")
    
    @property
    def preprocessing_config(self) -> Dict:
        """Get preprocessing configuration for this family."""
        preprocess = self.family_config.preprocessing
        if isinstance(preprocess, DictConfig):
            return OmegaConf.to_container(preprocess, resolve=True)
        return dict(preprocess)
    
    def get_n_iter(self, base_n_iter: Optional[int] = None) -> int:
        """
        Calculate proportional n_iter for RandomizedSearchCV.
        
        Ensures each model gets adequate search iterations.
        
        Args:
            base_n_iter: Base iterations per model. If None, uses config default.
            
        Returns:
            Total iterations (base * num_models).
        """
        if base_n_iter is None:
            base_n_iter = self.resource_config.get('n_iter_base', 15)
        
        num_models = len(self.models)
        total_n_iter = base_n_iter * num_models
        
        logger.debug(f"n_iter calculation: {base_n_iter} base * {num_models} models = {total_n_iter}")
        return total_n_iter
    
    def get_cv_splits(self) -> int:
        """Get number of cross-validation splits."""
        return self.resource_config.get('cv_splits', 5)
    
    def get_scoring(self) -> str:
        """Get scoring metric for optimization."""
        return self.resource_config.get('scoring', 'f1_weighted')
    
    def _build_preprocessor(self) -> Optional[ColumnTransformer]:
        """
        Build the ColumnTransformer for preprocessing.
        
        Returns:
            ColumnTransformer or None if no preprocessing needed.
        """
        preprocess = self.preprocessing_config
        transformers = []
        
        # Numeric preprocessing
        if preprocess.get('scale_numeric', False) and self.numeric_cols:
            transformers.append(
                ('numeric', StandardScaler(), self.numeric_cols)
            )
            logger.debug(f"Adding StandardScaler for {len(self.numeric_cols)} numeric columns")
        elif self.numeric_cols:
            # Passthrough numeric columns
            transformers.append(
                ('numeric', 'passthrough', self.numeric_cols)
            )
        
        # Categorical preprocessing
        if preprocess.get('encode_categorical', False) and self.categorical_cols:
            transformers.append(
                ('categorical', OneHotEncoder(handle_unknown='ignore', sparse_output=False), 
                 self.categorical_cols)
            )
            logger.debug(f"Adding OneHotEncoder for {len(self.categorical_cols)} categorical columns")
        elif self.categorical_cols:
            # Passthrough categorical columns (tree models handle natively)
            transformers.append(
                ('categorical', 'passthrough', self.categorical_cols)
            )
        
        if not transformers:
            return None
        
        return ColumnTransformer(
            transformers=transformers,
            remainder='drop',  # Drop any columns not specified
            verbose_feature_names_out=False
        )
    
    def _build_feature_selector(self) -> Optional[Any]:
        """
        Build the feature selector based on family config.
        
        Returns:
            Feature selector instance or None.
        """
        preprocess = self.preprocessing_config
        fs_type = preprocess.get('feature_selection')
        
        if not fs_type:
            return None
        
        # Import feature selection mapping
        from src.config.model.model_mapping import FeatSelect_mapping
        
        if fs_type not in FeatSelect_mapping:
            logger.warning(f"Unknown feature selector '{fs_type}', skipping")
            return None
        
        # Get feature selection parameters
        fs_params = preprocess.get('feature_selection_params', {})
        if isinstance(fs_params, DictConfig):
            fs_params = OmegaConf.to_container(fs_params, resolve=True)
        
        selector_class = FeatSelect_mapping[fs_type]
        
        try:
            selector = selector_class(**fs_params)
            logger.debug(f"Created feature selector: {fs_type} with params {fs_params}")
            return selector
        except Exception as e:
            logger.warning(f"Failed to create feature selector {fs_type}: {e}")
            return None
    
    def build_pipeline(
        self,
        include_resampler: bool = False,
        resampler_type: Optional[str] = None
    ) -> Pipeline:
        """
        Build the sklearn Pipeline for this family.
        
        Args:
            include_resampler: Whether to include imbalance handling.
            resampler_type: Type of resampler ('smote', 'random', etc.)
            
        Returns:
            sklearn Pipeline with preprocessing and ModelSelector.
        """
        steps = []
        
        # 1. Preprocessing (ColumnTransformer)
        preprocessor = self._build_preprocessor()
        if preprocessor is not None:
            steps.append(('preprocessor', preprocessor))
        
        # 2. Resampling (if requested)
        if include_resampler and resampler_type:
            from src.config.model.model_mapping import ImbalanceHandler_mapping
            from src.preprocessing.custom_transformers import ResamplerTransformer
            
            if resampler_type in ImbalanceHandler_mapping:
                resampler_class = ImbalanceHandler_mapping[resampler_type]
                resampler = ResamplerTransformer(
                    sampler=resampler_class(),
                    max_samples_per_class=self._config.get('max_samples_per_class', 5000)
                    if hasattr(self._config, 'max_samples_per_class') else 5000
                )
                steps.append(('resampler', resampler))
                logger.debug(f"Added resampler: {resampler_type}")
        
        # 3. Feature Selection
        feature_selector = self._build_feature_selector()
        if feature_selector is not None:
            steps.append(('feature_selection', feature_selector))
        
        # 4. Model Selector (allows model_type as hyperparameter)
        model_selector = ModelSelector(
            model_type=self.models[0],  # Default to first model
            models=self.models
        )
        steps.append(('model', model_selector))
        
        # Use imblearn Pipeline if resampler is included, else sklearn Pipeline
        if include_resampler and resampler_type:
            pipeline = ImbPipeline(steps)
        else:
            pipeline = Pipeline(steps)
        
        logger.info(f"Built pipeline for '{self.family_name}' family with steps: "
                   f"{[name for name, _ in steps]}")
        
        return pipeline
    
    def get_param_grid(self) -> Dict[str, List[Any]]:
        """
        Build unified parameter grid for all models in family.
        
        The grid includes:
        - model__model_type: List of model types to try
        - Model-specific hyperparameters for each model type
        
        Returns:
            Dict suitable for RandomizedSearchCV param_distributions.
        """
        grid = {
            'model__model_type': self.models
        }
        
        # Load model hyperparameters from config
        if hasattr(self._config.model, 'model_hyperparams'):
            model_hyperparams = self._config.model.model_hyperparams
            
            for model_name in self.models:
                if model_name in model_hyperparams:
                    model_params = model_hyperparams[model_name]
                    if isinstance(model_params, DictConfig):
                        model_params = OmegaConf.to_container(model_params, resolve=True)
                    
                    # Add model-specific params to grid
                    for param_name, param_values in model_params.items():
                        # Ensure param values are lists
                        if not isinstance(param_values, list):
                            param_values = [param_values]
                        grid[param_name] = param_values
        
        logger.debug(f"Built param grid with {len(grid)} parameters")
        return grid
    
    def create_search_cv(
        self,
        pipeline: Optional[Pipeline] = None,
        n_iter: Optional[int] = None,
        cv: Optional[int] = None,
        scoring: Optional[str] = None,
        n_jobs: int = -1,
        verbose: int = 1,
        random_state: int = 42
    ) -> RandomizedSearchCV:
        """
        Create RandomizedSearchCV with the pipeline and param grid.
        
        Args:
            pipeline: Pipeline to use. If None, builds a new one.
            n_iter: Number of iterations. If None, uses proportional calculation.
            cv: Number of CV splits. If None, uses config default.
            scoring: Scoring metric. If None, uses config default.
            n_jobs: Number of parallel jobs.
            verbose: Verbosity level.
            random_state: Random state for reproducibility.
            
        Returns:
            Configured RandomizedSearchCV instance.
        """
        if pipeline is None:
            pipeline = self.build_pipeline()
        
        if n_iter is None:
            n_iter = self.get_n_iter()
        
        if cv is None:
            cv = self.get_cv_splits()
        
        if scoring is None:
            scoring = self.get_scoring()
        
        param_grid = self.get_param_grid()
        
        # Use TimeSeriesSplit for time-series data
        cv_splitter = TimeSeriesSplit(n_splits=cv)
        
        search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=param_grid,
            n_iter=n_iter,
            cv=cv_splitter,
            scoring=scoring,
            n_jobs=n_jobs,
            verbose=verbose,
            random_state=random_state,
            return_train_score=True,
            refit=True
        )
        
        logger.info(f"Created RandomizedSearchCV: n_iter={n_iter}, cv={cv}, scoring={scoring}")
        
        return search


def get_family_for_model(model_type: str, config: DictConfig) -> Optional[str]:
    """
    Determine which family a model belongs to.
    
    Args:
        model_type: Model type string (e.g., 'LGBM', 'LR').
        config: Config containing model_families.
        
    Returns:
        Family name or None if not found.
    """
    if not hasattr(config.model, 'model_families'):
        return None
    
    for family_name, family_config in config.model.model_families.items():
        if model_type in family_config.models:
            return family_name
    
    return None


def get_preprocessing_for_model(model_type: str, config: DictConfig) -> Dict:
    """
    Get preprocessing requirements for a specific model.
    
    Args:
        model_type: Model type string.
        config: Config containing model_families.
        
    Returns:
        Preprocessing config dict.
    """
    family_name = get_family_for_model(model_type, config)
    
    if family_name is None:
        logger.warning(f"Model '{model_type}' not found in any family, using default preprocessing")
        return {
            'scale_numeric': True,
            'encode_categorical': True,
            'feature_selection': None
        }
    
    family_config = config.model.model_families[family_name]
    preprocess = family_config.preprocessing
    
    if isinstance(preprocess, DictConfig):
        return OmegaConf.to_container(preprocess, resolve=True)
    
    return dict(preprocess)
