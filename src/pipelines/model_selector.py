"""
Model Selector - sklearn-compatible wrapper for model type as hyperparameter.

This module provides a wrapper class that allows model type to be treated as
a hyperparameter in sklearn's RandomizedSearchCV/GridSearchCV.

Example:
    selector = ModelSelector(model_type='LGBM', models=['LGBM', 'XGB', 'RFC'])
    params = {
        'model__model_type': ['LGBM', 'XGB', 'RFC'],
        'model__n_estimators': [100, 300, 500]
    }
    search = RandomizedSearchCV(pipeline, params)
"""

from typing import Any, Dict, List, Optional, Union
import numpy as np
from sklearn.base import BaseEstimator, ClassifierMixin, clone
from sklearn.utils.validation import check_is_fitted
from loguru import logger


class ModelSelector(BaseEstimator, ClassifierMixin):
    """
    Sklearn-compatible wrapper that allows model type as a hyperparameter.
    
    This enables tuning the model type alongside model hyperparameters in
    a single RandomizedSearchCV run.
    
    Attributes:
        model_type (str): The type of model to use (e.g., 'LGBM', 'XGB', 'RFC').
        models (list): List of available model types for this selector.
        
    Note:
        Model-specific parameters are passed as **kwargs and should be prefixed
        appropriately (e.g., n_estimators=100 becomes a direct model param).
    """
    
    def __init__(
        self,
        model_type: str = 'LGBM',
        models: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """
        Initialize ModelSelector.
        
        Args:
            model_type: Which model type to use. Must be in `models` list.
            models: List of available model types. If None, defaults to ['LGBM'].
            **kwargs: Model-specific parameters passed directly to the model.
        """
        # Store parameters EXACTLY as received (sklearn clone compatibility)
        self.model_type = model_type
        self.models = models
        
        # Store any model-specific kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        # Fitted attributes (not constructor params)
        self._model: Optional[BaseEstimator] = None
        self._model_mapping: Optional[Dict] = None
    
    def _get_models_list(self) -> List[str]:
        """Get models list, using defaults if None."""
        return self.models if self.models is not None else ['LGBM']
    
    def _get_model_mapping(self) -> Dict:
        """Lazy load model mapping to avoid circular imports."""
        if self._model_mapping is None:
            from src.config.model.model_mapping import ModelType_mapping
            self._model_mapping = ModelType_mapping
        return self._model_mapping
    
    def _extract_model_params(self) -> Dict[str, Any]:
        """
        Extract model-specific parameters from this instance's attributes.
        
        Returns:
            Dict of parameters to pass to the model constructor.
        """
        # Get all attributes that aren't sklearn internals or our control attributes
        excluded = {'model_type', 'models', '_model', '_model_mapping'}
        params = {}
        
        for key, value in self.__dict__.items():
            if key not in excluded and not key.startswith('_'):
                params[key] = value
        
        return params
    
    def _create_model(self) -> BaseEstimator:
        """
        Create the model instance based on model_type.
        
        Returns:
            Instantiated sklearn-compatible classifier.
            
        Raises:
            ValueError: If model_type is not in the mapping.
        """
        mapping = self._get_model_mapping()
        
        if self.model_type not in mapping:
            available = list(mapping.keys())
            raise ValueError(
                f"Unknown model type '{self.model_type}'. "
                f"Available: {available}"
            )
        
        model_factory = mapping[self.model_type]
        model_params = self._extract_model_params()
        
        # Some models in the mapping are factory functions, others are classes
        if callable(model_factory):
            # Check if it's a factory function or a class
            try:
                # Try to instantiate - if it's a factory, call it first
                if hasattr(model_factory, '__self__') or model_factory.__name__.startswith('_create'):
                    # It's a factory function (like _create_lgbm_with_gpu)
                    base_model = model_factory()
                    if base_model is None:
                        raise ValueError(f"Factory for {self.model_type} returned None")
                    
                    # Set parameters on the created model
                    if model_params:
                        base_model.set_params(**model_params)
                    return base_model
                else:
                    # It's a class, instantiate directly with params
                    return model_factory(**model_params)
            except TypeError:
                # If direct instantiation fails, try without params
                base_model = model_factory()
                if model_params and hasattr(base_model, 'set_params'):
                    base_model.set_params(**model_params)
                return base_model
        else:
            raise ValueError(f"Invalid model factory for {self.model_type}")
    
    def fit(self, X, y, **fit_params):
        """
        Fit the selected model.
        
        Args:
            X: Training features (array-like or DataFrame).
            y: Training labels.
            **fit_params: Additional parameters passed to model.fit().
            
        Returns:
            self
        """
        logger.debug(f"ModelSelector fitting with model_type={self.model_type}")
        
        # Validate model_type is in allowed models
        models_list = self._get_models_list()
        if self.model_type not in models_list:
            logger.warning(
                f"model_type '{self.model_type}' not in models list {models_list}. "
                "Proceeding anyway."
            )
        
        # Create and fit the model
        self._model = self._create_model()
        self._model.fit(X, y, **fit_params)
        
        # Copy over any fitted attributes for sklearn compatibility
        if hasattr(self._model, 'classes_'):
            self.classes_ = self._model.classes_
        if hasattr(self._model, 'n_features_in_'):
            self.n_features_in_ = self._model.n_features_in_
        if hasattr(self._model, 'feature_names_in_'):
            self.feature_names_in_ = self._model.feature_names_in_
        
        return self
    
    def predict(self, X):
        """
        Predict class labels.
        
        Args:
            X: Features to predict on.
            
        Returns:
            Predicted class labels.
        """
        check_is_fitted(self, '_model')
        return self._model.predict(X)
    
    def predict_proba(self, X):
        """
        Predict class probabilities.
        
        Args:
            X: Features to predict on.
            
        Returns:
            Predicted class probabilities.
            
        Raises:
            AttributeError: If the underlying model doesn't support predict_proba.
        """
        check_is_fitted(self, '_model')
        
        if not hasattr(self._model, 'predict_proba'):
            raise AttributeError(
                f"Model type '{self.model_type}' does not support predict_proba. "
                "Use predict() instead or choose a different model."
            )
        
        return self._model.predict_proba(X)
    
    def decision_function(self, X):
        """
        Compute decision function (if supported by underlying model).
        
        Args:
            X: Features.
            
        Returns:
            Decision function values.
        """
        check_is_fitted(self, '_model')
        
        if not hasattr(self._model, 'decision_function'):
            raise AttributeError(
                f"Model type '{self.model_type}' does not support decision_function."
            )
        
        return self._model.decision_function(X)
    
    def score(self, X, y, sample_weight=None):
        """
        Return the mean accuracy on the given test data and labels.
        
        Args:
            X: Test features.
            y: True labels.
            sample_weight: Sample weights.
            
        Returns:
            Mean accuracy score.
        """
        check_is_fitted(self, '_model')
        return self._model.score(X, y, sample_weight=sample_weight)
    
    @property
    def feature_importances_(self):
        """
        Get feature importances from the underlying model.
        
        Returns:
            Feature importance array.
            
        Raises:
            AttributeError: If the model doesn't have feature_importances_.
        """
        check_is_fitted(self, '_model')
        
        if not hasattr(self._model, 'feature_importances_'):
            raise AttributeError(
                f"Model type '{self.model_type}' does not have feature_importances_. "
                "Tree-based models (LGBM, XGB, RFC, GBC) support this."
            )
        
        return self._model.feature_importances_
    
    def get_params(self, deep: bool = True) -> Dict[str, Any]:
        """
        Get parameters for this estimator.
        
        Args:
            deep: If True, return params for nested objects.
            
        Returns:
            Parameter dict.
        """
        params = {'model_type': self.model_type, 'models': self.models}
        
        # Add any model-specific params
        for key in self.__dict__:
            if key not in {'model_type', 'models', '_model', '_model_mapping'}:
                if not key.startswith('_'):
                    params[key] = getattr(self, key)
        
        return params
    
    def set_params(self, **params) -> 'ModelSelector':
        """
        Set parameters for this estimator.
        
        Args:
            **params: Parameters to set.
            
        Returns:
            self
        """
        for key, value in params.items():
            setattr(self, key, value)
        return self
    
    def __repr__(self) -> str:
        """String representation."""
        params = self._extract_model_params()
        if params:
            params_str = ', '.join(f'{k}={v}' for k, v in list(params.items())[:3])
            return f"ModelSelector(model_type='{self.model_type}', {params_str})"
        return f"ModelSelector(model_type='{self.model_type}')"
