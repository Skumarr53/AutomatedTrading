from typing import Any, Dict, Union, List, Optional
import joblib
import os
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
# Assuming this contains configuration details including paths and mode
from config import config
from config.vars import CLOSE
from config.config import MODEL_CONFIG
from utils.utils import categorize_percent_change

class MLPipelineBase:
    def __init__(self):
        self.model_id: Optional[str] = None
        self.features: Optional[List[str]] = None
        self.pipeline: Union[Pipeline, None] = None
        self.best_model_dict: Dict = {} if config.TRADE_MODE == 'BACKTEST' else self._load_models()
        self.run_ids: str = None
        self.model = None
        # self.define_pipeline()
    
    def setup(self):
        if config.TRADE_MODE != 'LIVE':
            self.model = self.define_model()
        self.define_pipeline()
        

    def define_model(self) -> None:
        return GridSearchCV(self.pipeline, 
                            param_grid=MODEL_CONFIG['CUSTOM_MODEL_PARAMS'],
                            scoring='f1_weighted', 
                            n_jobs=5,cv=5, verbose=1, 
                            return_train_score=True)

    def define_pipeline(self) -> None:
        raise NotImplementedError(
            "Subclasses must implement define_pipeline method.")

    def _load_models(self) -> Dict[str, Any]:
        """Load parameters from a file."""
        CUSTOM_MODEL_BEST_PARAM_PATH = '{}_{}_pipeline_params_{}w.joblib'
        params_path = os.path.join(
            config.MODEL_PARAM_FILE, config.CUSTOM_MODEL_BEST_PARAM_PATH.format(self.model_id))
        if os.path.exists(params_path):
            return joblib.load(params_path)
        else:
            raise FileNotFoundError("Parameter file does not exist.")
            
    def run(self, X) -> None:
        """Run the pipeline based on mode."""
        symbol = X.symbol.iloc[0]
        if self.mode == 'BACKTEST':
            for run_id in self.run_ids:
                y_trans = categorize_percent_change(X[CLOSE] , run_id)
                y_filt = (~y_trans.isna())
                X_trans, y_trans = X[y_filt], y_trans[y_filt]
                self.model.fit(X_trans, y_trans)
                self.best_model_dict[symbol][run_id] = self.model.copy()
        elif self.mode == 'LIVE':
            model_fit_dict = self.best_model_dict[symbol]
            for run_id in self.run_ids:
                model_fit_dict[run_id].predict(X)



            # Assume prediction or further processing happens here using loaded parameters
