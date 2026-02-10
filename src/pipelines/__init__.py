"""
Pipeline modules for the AutomatedTrading system.

This package provides:
- CustomModelPipeline: Legacy pipeline with configurable preprocessing (backward compatible)
- FamilyModelPipeline: New family-based pipeline with model type as hyperparameter
- FamilyPipelineBuilder: Builder class for creating family-aware pipelines
- ModelSelector: sklearn-compatible wrapper for model type as hyperparameter
- MLPipelineBase: Base pipeline class with training and inference logic
"""

from src.pipelines.custom_pipelines import CustomModelPipeline, FamilyModelPipeline
from src.pipelines.family_pipeline import FamilyPipelineBuilder, get_family_for_model
from src.pipelines.model_selector import ModelSelector
from src.pipelines.base_pipeline import MLPipelineBase

__all__ = [
    'CustomModelPipeline',
    'FamilyModelPipeline', 
    'FamilyPipelineBuilder',
    'ModelSelector',
    'MLPipelineBase',
    'get_family_for_model',
]
