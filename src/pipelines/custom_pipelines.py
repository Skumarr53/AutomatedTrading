# src/pipelines/custom_pipelines.py

from src.pipelines.base_pipeline import MLPipelineBase
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from src.config.config import config
from src.preprocessing.custom_transformers import (
    DFFeatureUnion,
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    DFRecursiveFeatureSelector
)
from typing import Any, Dict, List, Optional


class CustomModelPipeline(MLPipelineBase):
    """
    Custom machine learning pipeline for specific trading models.

    This class extends the `MLPipelineBase` to define a tailored pipeline that includes
    feature extraction, normalization, feature selection, and model fitting using a
    Random Forest classifier. It leverages custom transformers for handling different
    types of features.

    Attributes:
        model_id (str): Identifier for the specific model configuration.
        features (List[str]): List of feature names used in the pipeline.
        pipeline (Optional[Pipeline]): Scikit-learn Pipeline object containing the sequence
            of transformations and the estimator.
    """

    def __init__(self, model_id: str) -> None:
        """
        Initializes the CustomModelPipeline instance.

        Sets the model ID, retrieves the corresponding features from the configuration,
        and sets up the pipeline.

        Args:
            model_id (str): Identifier for the specific model configuration.
        """
        super().__init__()
        self.model_id: str = model_id
        self.features: List[str] = config.columns.custom_model_features[self.model_id]
        self.setup()

    def define_pipeline(self) -> None:
        """
        Defines the machine learning pipeline with feature extraction, normalization,
        feature selection, and the Random Forest classifier.

        The pipeline consists of the following steps:
            1. Feature Union: Combines short-term numeric features, long-term numeric features,
               and categorical features.
            2. Feature Selection: Applies recursive feature selection to identify the most
               relevant features.
            3. Model Fit: Trains a Random Forest classifier.

        Raises:
            ValueError: If the `features` attribute is not set.
        """
        if not self.features:
            raise ValueError("Features must be set before defining the pipeline.")

        self.pipeline = Pipeline([
            ('features', DFFeatureUnion([
                ('short_numerics', Pipeline([
                    ('extract', ColumnExtractor(
                        [col for col in self.features if col in config.columns.short_num_cols]
                    )),
                    ('normalize', ShortTermNormalizer())
                ])),
                ('long_numerics', Pipeline([
                    ('extract', ColumnExtractor(
                        [col for col in self.features if col in config.columns.long_num_cols]
                    )),
                    ('normalize', LongTermNormalizer())
                ])),
                ('cat_cols', Pipeline([
                    ('extract', ColumnExtractor(
                        [col for col in self.features if col in config.columns.cat_cols]
                    )),
                    ('normalize', CategoricalPreprocessor(
                        [col for col in self.features if col in config.columns.cat_cols]
                    ))
                ])),
            ])),
            ('feature_selection', DFRecursiveFeatureSelector()),
            ('model_fit', RandomForestClassifier())
        ])