from typing import Any, Dict, List, Optional
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from src.pipelines.base_pipeline import MLPipelineBase
from src import config
from src.config import (FeatSelect_mapping,
                        ImbalanceHandler_mapping,
                        ModelType_mapping)
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
        self.feature_config = feature_config
        self.params = {} #or {
        #     'short_numerics': True,
        #     'long_numerics': True,
        #     'cat_cols': True
        # }
        # self.feature_selector = feature_selector
        # self.model = model if model else RandomForestClassifier()
        # self.imbalance_technique = imbalance_technique
    

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
        if self.feature_config.get('std_scale', False):
            feature_union.append(('short_numerics', Pipeline([
                ('extract', ColumnExtractor(
                    [col for col in self.features if col in config.columns.short_num_cols]
                )),
                ('normalize', ShortTermNormalizer())
            ])))
        
            feature_union.append(('long_numerics', Pipeline([
                ('extract', ColumnExtractor(
                    [col for col in self.features if col in config.columns.long_num_cols]
                )),
                ('normalize', LongTermNormalizer())
            ])))
        
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
        imb_technique = self.feature_config.get('imbalance_technique', None)
        if imb_technique:
            steps.append(('imbalance_handler', ImbalanceHandler(technique=imb_technique)))

        # Add feature extraction
        steps.append(('features', DFFeatureUnion(feature_union)))
        
        # Optional feature selection
        feature_selector = self.feature_config.get('imbalance_technique', None)

        model_type = self.feature_config.get('model', None)
        model = ModelType_mapping.get(model_type, RandomForestClassifier)

        if feature_selector:
            self.params = {**self.params, **config.model.pipeline_params}
            feature_selector = FeatSelect_mapping.get(feature_selector, None)
                steps.append(('feature_selection', DFRecursiveFeatureSelector()))
            elif feature_selector == 'SHAP':
                steps.append(('feature_selection', DFShapFeatureSelector(model())))

        # Add the model as the final step
        steps.append(('model_fit', model()))
        self.params = {**self.params, **config.model.model_params.get()}

        # Define the pipeline with the configured steps
        self.pipeline = Pipeline(steps)
