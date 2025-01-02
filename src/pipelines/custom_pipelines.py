import pandas as pd
from typing import Any, Dict, List, Optional
from imblearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from joblib import Memory  # Add this import

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
        self.steps = None
        self.input_columns_prep()

    def input_columns_prep(self):
        self.short_numeric_cols = [col for col in self.features if col in config.columns.short_num_cols]
        self.long_numeric_cols = [col for col in self.features if col in config.columns.long_num_cols]
        self.cat_cols = [col for col in self.features if col in config.columns.cat_cols]

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
            self.steps.append(('resample', ResamplerTransformer(
                sampler=sampler(),
                shuffle=True,
                random_state=42  # You can make this configurable
            )))

    def update_feature_selection_pipeline(self):
        feature_selector = self.feature_config.get('feature_selector', None)
        if feature_selector:
            self.params = {**self.params, **config.model.pipeline_params}
            feature_selector = FeatSelect_mapping.get(feature_selector, None)
            self.steps.append(('feature_selection', feature_selector()))

    def update_model_pipeline(self):
        model_type = self.feature_config.get('model', None)
        model_class = ModelType_mapping.get(model_type, RandomForestClassifier)
        # Add the model as the final step
        self.steps.append(('model_fit', model_class()))
        self.params = {**self.params, **config.model.model_params.get(model_type, 'RFC')}

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

        ## Reset pipeline
        self.empty_pipeline_and_params()

        ## Define pipliene steps
        self.add_combined_preprocessed_features()
        self.update_resampling_pipeline()
        self.update_feature_selection_pipeline()
        self.update_model_pipeline()

        # Define the pipeline with the configured steps
        self.pipeline = Pipeline(self.steps)

    def define_model(self,memory: Memory = None) -> GridSearchCV:
        """
        Defines the machine learning model using GridSearchCV for hyperparameter tuning.

        Returns:
            GridSearchCV: An instance of GridSearchCV configured with the pipeline and parameter grid.
        """
        self.define_pipeline()
        if memory:
            self.pipeline.memory = memory

        self.model = GridSearchCV(
            self.pipeline,
            param_grid=self.params,
            scoring='accuracy',
            n_jobs=4,
            cv=3,
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
