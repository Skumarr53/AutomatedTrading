from typing import Any, Dict, List, Optional
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from src.pipelines.base_pipeline import MLPipelineBase
from src import config
from src.preprocessing.custom_transformers import (
    DFFeatureUnion,
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    DFRecursiveFeatureSelector,
    DFShapFeatureSelector,
    ImbalanceHandler  # Assuming ImbalanceHandler is defined in custom transformers
)

class CustomModelPipeline():

    def __init__(self, feature_config: Optional[Dict[str, bool]] = None,
                 feature_selector: Optional[str] = None, model: Any = None, 
                imbalance_technique: Optional[str] = None) -> None:
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
        self.feature_config = feature_config or {
            'short_numerics': True,
            'long_numerics': True,
            'cat_cols': True
        }
        self.feature_selector = feature_selector
        self.model = model if model else RandomForestClassifier()
        self.imbalance_technique = imbalance_technique
        self.setup()

    def define_pipeline(self) -> None:
        """
        Defines the machine learning pipeline with configurable feature extraction, selection, model, 
        and optional imbalance handling.

        The pipeline consists of:
            1. Feature Union: Combines configurable feature groups.
            2. Optional Imbalance Handling: Applies a resampling technique if enabled.
            3. Optional Feature Selection: Applies the specified feature selector if enabled.
            4. Model Fit: Uses the specified model.
        """
        if not self.features:
            raise ValueError("Features must be set before defining the pipeline.")
        
        feature_union = []

        # Dynamically add feature groups based on configuration
        if self.feature_config.get('short_numerics', False):
            feature_union.append(('short_numerics', Pipeline([
                ('extract', ColumnExtractor(
                    [col for col in self.features if col in config.columns.short_num_cols]
                )),
                ('normalize', ShortTermNormalizer())
            ])))
        
        if self.feature_config.get('long_numerics', False):
            feature_union.append(('long_numerics', Pipeline([
                ('extract', ColumnExtractor(
                    [col for col in self.features if col in config.columns.long_num_cols]
                )),
                ('normalize', LongTermNormalizer())
            ])))
        
        if self.feature_config.get('cat_cols', False):
            feature_union.append(('cat_cols', Pipeline([
                ('extract', ColumnExtractor(
                    [col for col in self.features if col in config.columns.cat_cols]
                )),
                ('normalize', CategoricalPreprocessor(
                    [col for col in self.features if col in config.columns.cat_cols]
                ))
            ])))

        # Build pipeline steps dynamically
        steps = []

        # Optional imbalance handling
        if self.imbalance_technique:
            steps.append(('imbalance_handler', ImbalanceHandler(technique=self.imbalance_technique)))

        # Add feature extraction
        steps.append(('features', DFFeatureUnion(feature_union)))
        
        # Optional feature selection
        if self.feature_selector:
            if self.feature_selector == 'RFE':
                steps.append(('feature_selection', DFRecursiveFeatureSelector()))
            elif self.feature_selector == 'SHAP':
                steps.append(('feature_selection', DFShapFeatureSelector(self.model)))

        # Add the model as the final step
        steps.append(('model_fit', self.model))

        # Define the pipeline with the configured steps
        self.pipeline = Pipeline(steps)
