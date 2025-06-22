from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from imblearn.over_sampling import SMOTE, RandomOverSampler
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
    'smote': SMOTE,
    'random': RandomOverSampler  
}


ModelType_mapping = {
    'SVC': SVC,
    'KNN': KNeighborsClassifier,
    'RFC': RandomForestClassifier,
    'GBC': GradientBoostingClassifier,
    'MLP': MLPClassifier,
    'LR': LogisticRegression
    
}
