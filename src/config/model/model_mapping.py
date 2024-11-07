from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from src.preprocessing.custom_transformers import (
    DFFeatureUnion,
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    DF_RFECV_FeatureSelection,
    DFRecursiveFeatureSelector,
    DFShapFeatureSelector,
    ImbalanceHandler  # Assuming ImbalanceHandler is defined in custom transformers
)

FeatSelect_mapping = {
    'RFE': DFRecursiveFeatureSelector,
    'SHAP': DFShapFeatureSelector,
    'RFECV': DF_RFECV_FeatureSelection
    
}

ImbalanceHandler_mapping = {
    'SMOTE': ImbalanceHandler(technique='smote'),
    'RANDOM': ImbalanceHandler(technique='random'),
    'SMOTE_TOMEK': ImbalanceHandler(technique='smote_tomek'),
    'SMOTE_ENN': ImbalanceHandler(technique='smote_enn')
}


ModelType_mapping = {
    'SVC': SVC,
    'KNN': KNeighborsClassifier,
    'RFC': RandomForestClassifier,
    'GBC': GradientBoostingClassifier,
    'MLP': MLPClassifier,
    
}