from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
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


# Base model mapping with sklearn models
ModelType_mapping = {
    'SVC': SVC,
    'KNN': KNeighborsClassifier,
    'RFC': RandomForestClassifier,
    'GBC': GradientBoostingClassifier,
    'MLP': MLPClassifier,
    'LR': LogisticRegression,
}

# Add LightGBM if available (recommended for production)
# Uses GPU if available, otherwise CPU
if _HAS_LGBM:
    ModelType_mapping['LGBM'] = _create_lgbm_with_gpu

# Add XGBoost if available
# Uses GPU if available, otherwise CPU
if _HAS_XGB:
    ModelType_mapping['XGB'] = _create_xgb_with_gpu