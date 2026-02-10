from sklearn.svm import SVC, LinearSVC
from sklearn.linear_model import LogisticRegression, RidgeClassifier, SGDClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import (
    GradientBoostingClassifier, 
    RandomForestClassifier,
    ExtraTreesClassifier
)
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from imblearn.over_sampling import SMOTE, RandomOverSampler
from loguru import logger

# Optional imports for advanced gradient boosting models
try:
    from lightgbm import LGBMClassifier
    _HAS_LGBM = True
except ImportError:
    LGBMClassifier = None
    _HAS_LGBM = False
    logger.warning("LightGBM not installed. Install with: uv add lightgbm")

try:
    from xgboost import XGBClassifier
    _HAS_XGB = True
except ImportError:
    XGBClassifier = None
    _HAS_XGB = False
    logger.warning("XGBoost not installed. Install with: uv add xgboost")

# GPU detection for LightGBM/XGBoost
try:
    from src.utils.hardware_detector import get_hardware_detector
    _HAS_HW_DETECTOR = True
except ImportError:
    _HAS_HW_DETECTOR = False


def _create_lgbm_with_gpu():
    """Create LightGBM classifier with GPU support if available."""
    if not _HAS_LGBM:
        return None
    
    # Check GPU availability
    use_gpu = False
    if _HAS_HW_DETECTOR:
        hw_detector = get_hardware_detector()
        hw_specs = hw_detector.detect()
        use_gpu = hw_specs.get('has_gpu', False)
        
        # Check config override
        try:
            from src import config
            if hasattr(config, 'hardware_optimization'):
                if config.hardware_optimization.use_gpu is not None:
                    use_gpu = config.hardware_optimization.use_gpu
        except Exception:
            pass
    
    if use_gpu:
        try:
            logger.info("Configuring LightGBM with GPU acceleration")
            return LGBMClassifier(device='gpu', gpu_platform_id=0, gpu_device_id=0, verbosity=-1)
        except Exception as e:
            logger.warning(f"Failed to configure LightGBM with GPU: {e}. Falling back to CPU.")
            return LGBMClassifier(verbosity=-1)
    else:
        return LGBMClassifier(verbosity=-1)


def _create_xgb_with_gpu():
    """Create XGBoost classifier with GPU support if available."""
    if not _HAS_XGB:
        return None
    
    # Check GPU availability
    use_gpu = False
    if _HAS_HW_DETECTOR:
        hw_detector = get_hardware_detector()
        hw_specs = hw_detector.detect()
        use_gpu = hw_specs.get('has_gpu', False)
        
        # Check config override
        try:
            from src import config
            if hasattr(config, 'hardware_optimization'):
                if config.hardware_optimization.use_gpu is not None:
                    use_gpu = config.hardware_optimization.use_gpu
        except Exception:
            pass
    
    if use_gpu:
        try:
            logger.info("Configuring XGBoost with GPU acceleration")
            return XGBClassifier(tree_method='gpu_hist', gpu_id=0, verbosity=0)
        except Exception as e:
            logger.warning(f"Failed to configure XGBoost with GPU: {e}. Falling back to CPU.")
            return XGBClassifier(verbosity=0)
    else:
        return XGBClassifier(verbosity=0)

from src.preprocessing.custom_transformers import (
    DFFeatureUnion,
    ColumnExtractor,
    ShortTermNormalizer,
    LongTermNormalizer,
    CategoricalPreprocessor,
    DF_RFECV_FeatureSelection,
    DFRecursiveFeatureSelector,
    DFShapFeatureSelector,
    CorrelationFilter,
    LGBMImportanceSelector,
    MultiStageFeatureSelector,
    ImbalanceHandler  # Assuming ImbalanceHandler is defined in custom transformers
)

# Feature Selection Mapping
# Available methods:
#   - 'RFE': Recursive Feature Elimination (sklearn-based, uses DecisionTree by default)
#   - 'RFECV': RFE with Cross-Validation (auto-selects optimal feature count)
#   - 'SHAP': SHAP-based selection (model-agnostic, uses LGBMClassifier by default)
#   - 'LGBM': LightGBM gain-based importance (fast, model-consistent) [RECOMMENDED]
#   - 'CORR': Correlation filter (removes redundant features, use as pre-filter)
#   - 'MULTI': Multi-stage selection (Correlation + LGBM importance) [RECOMMENDED FOR PRODUCTION]
FeatSelect_mapping = {
    'RFE': DFRecursiveFeatureSelector,
    'SHAP': DFShapFeatureSelector,
    'RFECV': DF_RFECV_FeatureSelection,
    'LGBM': LGBMImportanceSelector,
    'CORR': CorrelationFilter,
    'MULTI': MultiStageFeatureSelector,
}

ImbalanceHandler_mapping = {
    'smote': SMOTE,
    'random': RandomOverSampler  
}


# =============================================================================
# MODEL TYPE MAPPING
# =============================================================================
# Maps model type strings to sklearn-compatible classifier classes/factories.
# 
# Families:
#   - Baseline: LR, DT (fast reference models)
#   - Tree: LGBM, XGB, RFC, GBC, ETC (no scaling needed)
#   - Linear: LR, LSVC, Ridge, SGD (scaling required)
#   - Distance: KNN, SVC (scaling required)
#   - Neural: MLP (scaling required)

ModelType_mapping = {
    # Baseline models (fast, interpretable)
    'DT': DecisionTreeClassifier,
    
    # Tree-based models (sklearn native)
    'RFC': RandomForestClassifier,
    'GBC': GradientBoostingClassifier,
    'ETC': ExtraTreesClassifier,
    
    # Linear models
    'LR': LogisticRegression,
    'LSVC': LinearSVC,
    'Ridge': RidgeClassifier,
    'SGD': SGDClassifier,
    
    # Distance-based models
    'SVC': SVC,
    'KNN': KNeighborsClassifier,
    
    # Neural models
    'MLP': MLPClassifier,
}

# Add LightGBM if available (recommended for production)
# Uses GPU if available, otherwise CPU
if _HAS_LGBM:
    ModelType_mapping['LGBM'] = _create_lgbm_with_gpu

# Add XGBoost if available
# Uses GPU if available, otherwise CPU
if _HAS_XGB:
    ModelType_mapping['XGB'] = _create_xgb_with_gpu


def get_available_models() -> list:
    """Get list of all available model type strings."""
    return list(ModelType_mapping.keys())


def is_tree_model(model_type: str) -> bool:
    """Check if model type is a tree-based model (no scaling needed)."""
    tree_models = {'LGBM', 'XGB', 'RFC', 'GBC', 'ETC', 'DT'}
    return model_type in tree_models


def is_linear_model(model_type: str) -> bool:
    """Check if model type is a linear model (scaling recommended)."""
    linear_models = {'LR', 'LSVC', 'Ridge', 'SGD'}
    return model_type in linear_models