from pipelines.base_pipeline import MLPipelineBase
from sklearn.pipeline import Pipeline
from config import columns_def
from sklearn.ensemble import RandomForestClassifier
from config.config import MODEL_CONFIG
from preprocessing.custom_transformers import *


class CustomModelPipeline(MLPipelineBase):
    def __init__(self, model_id: str):
        super().__init__()
        self.model_id = model_id
        self.features = MODEL_CONFIG['CUSTOM_MODEL_FEATURES'][self.model_id]
        self.setup()

    def define_pipeline(self):
        self.pipeline = Pipeline([
            ('features', DFFeatureUnion([
                ('short_numerics', Pipeline([
                    ('extract', ColumnExtractor([col for col in self.features if col in columns_def.SHORT_NUM_COLS])),
                    ('normalize', ShortTermNormalizer())
                ])),
                ('long_numerics', Pipeline([
                    ('extract', ColumnExtractor([col for col in self.features if col in columns_def.LONG_NUM_COLS])),
                    ('normalize', LongTermNormalizer())
                ])),
                ('cat_cols', Pipeline([
                    ('normalize', CategoricalPreprocessor([col for col in self.features if col in columns_def.CAT_COLS]))
                ])),
            ])),
            ('feature_selection', DFRecursiveFeatureSelector()),
            ('Model_fit', RandomForestClassifier())
        ])
