import pandas as pd
from typing import Any, Dict, List, Optional
from imblearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
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
        self.params = {} 
        self.target_encoder = TargetLabelEncoder()  # Initialize TargetLabelEncoder
        self.model = None


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
            # Flattened structure without additional Pipeline wrapping
            feature_union.append(('short_numerics', ColumnExtractor(
                [col for col in self.features if col in config.columns.short_num_cols]
            )))
            feature_union.append(('short_normalize', ShortTermNormalizer()))

            feature_union.append(('long_numerics', ColumnExtractor(
                [col for col in self.features if col in config.columns.long_num_cols]
            )))
            feature_union.append(('long_normalize', LongTermNormalizer()))
        
        # Build pipeline steps dynamically
        steps = []

        steps.append(('cat_extract', ColumnExtractor(
            [col for col in self.features if col in config.columns.cat_cols]
        )))
        steps.append(('cat_normalize', CategoricalPreprocessor(
            [col for col in self.features if col in config.columns.cat_cols]
        )))
        # Optional imbalance handling
        imb_technique = self.feature_config.get('imbalance_technique', None)
        if imb_technique:
            steps.append(('imbalance_handler', ImbalanceHandler(technique=imb_technique)))

        # Add feature extraction
        steps.append(('features', DFFeatureUnion(feature_union)))
        
        # Optional feature selection
        feature_selector = self.feature_config.get('feature_selector', None)
        if feature_selector:
            self.params = {**self.params, **config.model.pipeline_params}
            feature_selector = FeatSelect_mapping.get(feature_selector, None)
            steps.append(('feature_selection', DFRecursiveFeatureSelector()))

        model_type = self.feature_config.get('model', None)
        model_class = ModelType_mapping.get(model_type, RandomForestClassifier)
        # Add the model as the final step
        steps.append(('model_fit', model_class()))
        self.params = {**self.params, **config.model.model_params.get(model_type, 'RFC')}

        # Define the pipeline with the configured steps
        self.pipeline = Pipeline(steps)

    def define_model(self) -> GridSearchCV:
        """
        Defines the machine learning model using GridSearchCV for hyperparameter tuning.

        Returns:
            GridSearchCV: An instance of GridSearchCV configured with the pipeline and parameter grid.
        """
        self.define_pipeline()

        self.model = GridSearchCV(
            self.pipeline,
            param_grid=self.params,
            scoring='f1_weighted',
            n_jobs=5,
            cv=5,
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