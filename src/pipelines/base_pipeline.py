# src/pipelines/base_pipeline.py

from typing import Any, Dict, Union, List, Optional
import joblib
import os
import logging
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.base import clone
import pandas as pd
from src.config import config
from src.config.vars import CLOSE
from src.config.config import config
from src.utils.utils import categorize_percent_change


class MLPipelineBase:
    """
    Base class for machine learning pipelines used in trading strategies.

    This class provides a foundational structure for defining and executing machine learning
    pipelines tailored for trading applications. It manages model definition, pipeline
    setup, parameter loading, and execution based on the operational mode (BACKTEST or LIVE).
    
    Attributes:
        model_id (Optional[str]): Identifier for the model. Used for loading model parameters.
        features (Optional[List[str]]): List of feature names used in the pipeline.
        pipeline (Optional[Pipeline]): Scikit-learn Pipeline object containing the sequence of transformations and the estimator.
        best_model_dict (Dict[str, Any]): Dictionary storing the best models per symbol and run ID. Loaded from persisted parameters in LIVE mode.
        run_ids (Optional[List[str]]): List of run identifiers corresponding to different time windows or strategies.
        model (Optional[GridSearchCV]): GridSearchCV object for hyperparameter tuning in BACKTEST mode.
    """

    def __init__(self) -> None:
        """
        Initializes the MLPipelineBase instance.

        Sets up the necessary attributes. In LIVE mode, it attempts to load pre-trained models.
        In BACKTEST mode, models are defined and trained during execution.
        """
        self.model_id: Optional[str] = None
        self.features: Optional[List[str]] = None
        self.pipeline: Optional[Pipeline] = None
        self.run_ids: Optional[List[str]] = None
        self.model: Optional[GridSearchCV] = None
        self.best_model_dict: Dict[str, Any] = (
            {}
            if config.trading_config.trade_mode == 'BACKTEST'
            else self._load_models()
        )
        self.mode: str = config.trading_config.trade_mode
        # self.define_pipeline()

    def setup(self) -> None:
        """
        Sets up the pipeline by defining the model and pipeline components.

        In BACKTEST mode, it defines the model using GridSearchCV.
        In LIVE mode, it relies on pre-loaded models.
        """
        if self.mode != 'LIVE':
            self.model = self.define_model()
        self.define_pipeline()

    def define_model(self) -> GridSearchCV:
        """
        Defines the machine learning model using GridSearchCV for hyperparameter tuning.

        Returns:
            GridSearchCV: An instance of GridSearchCV configured with the pipeline and parameter grid.
        """
        if not self.pipeline:
            raise ValueError("Pipeline must be defined before defining the model.")

        return GridSearchCV(
            self.pipeline,
            # TODO
            param_grid=config.model.model_params,
            scoring='f1_weighted',
            n_jobs=5,
            cv=5,
            verbose=1,
            return_train_score=True
        )

    def define_pipeline(self) -> None:
        """
        Defines the machine learning pipeline.

        This method should be implemented by subclasses to specify the sequence of transformations
        and the estimator.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError(
            "Subclasses must implement the define_pipeline method."
        )

    def _load_models(self) -> Dict[str, Any]:
        """
        Loads pre-trained model parameters from a file based on the model ID.

        This is primarily used in LIVE mode to load existing models.

        Returns:
            Dict[str, Any]: A dictionary mapping symbols to their corresponding models and run IDs.

        Raises:
            FileNotFoundError: If the parameter file does not exist.
        """
        if not self.model_id:
            raise ValueError("model_id must be set before loading models.")

        param_pth = '{}_{}_pipeline_params_{}w.joblib'
        params_filename = param_pth.format(
            self.model_id, config.paths.custom_model_best_param_path
        )
        params_path = os.path.join(config.paths.model_param_path, params_filename)

        if os.path.exists(params_path):
            logging.info(f"Loading models from {params_path}")
            return joblib.load(params_path)
        else:
            raise FileNotFoundError(f"Parameter file does not exist at {params_path}.")

    def run(self, X: pd.DataFrame) -> None:
        """
        Executes the pipeline based on the operational mode (BACKTEST or LIVE).

        In BACKTEST mode, it trains the model on the provided data.
        In LIVE mode, it uses pre-loaded models to make predictions.

        Args:
            X (pd.DataFrame): Input DataFrame containing feature data and a 'symbol' column.
        """
        if 'symbol' not in X.columns:
            raise KeyError("Input DataFrame must contain a 'symbol' column.")

        symbol = X['symbol'].iloc[0]

        if symbol not in self.best_model_dict and self.mode == 'LIVE':
            raise ValueError(f"No models found for symbol '{symbol}' in LIVE mode.")

        if self.mode == 'BACKTEST':
            if not self.run_ids:
                raise ValueError("run_ids must be set for BACKTEST mode.")

            for run_id in self.run_ids:
                y_trans = categorize_percent_change(X[CLOSE], run_id)
                y_filt = ~y_trans.isna()
                X_trans, y_trans = X[y_filt], y_trans[y_filt]

                if self.model is None:
                    raise ValueError("Model has not been defined. Call setup() before running.")

                self.model.fit(X_trans, y_trans)

                # Initialize dictionary for the symbol if not present
                if symbol not in self.best_model_dict:
                    self.best_model_dict[symbol] = {}

                # Store the best estimator
                self.best_model_dict[symbol][run_id] = clone(self.model.best_estimator_)
        elif self.mode == 'LIVE':
            model_fit_dict = self.best_model_dict.get(symbol, {})
            if not model_fit_dict:
                logging.warning(f"No models available for symbol '{symbol}' in LIVE mode.")
                return

            for run_id, model in model_fit_dict.items():
                prediction = model.predict(X)
                # Assume prediction or further processing happens here using loaded parameters
                # For example, storing the prediction:
                X.loc[:, f'prediction_{run_id}'] = prediction

            # Further processing can be implemented as needed
        else:
            raise ValueError(f"Unsupported mode '{self.mode}'. Supported modes are 'BACKTEST' and 'LIVE'.")


# Example of setting up and using the MLPipelineBase class
# Note: The following example is for demonstration purposes and assumes that subclasses are properly implemented.

if __name__ == "__main__":
    # Example subclass implementation
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from sklearn.neural_network import MLPClassifier

    @dataclass
    class ExamplePipeline(MLPipelineBase):
        def define_pipeline(self) -> None:
            """
            Defines a sample pipeline with scaling, PCA, and MLPClassifier.
            """
            self.pipeline = Pipeline([
                ('scaler', StandardScaler()),
                ('pca', PCA(n_components=5)),
                ('mlp', MLPClassifier(hidden_layer_sizes=(100,), activation='relu', solver='adam', max_iter=500))
            ])
            self.features = ['feature1', 'feature2', 'feature3', 'feature4', 'feature5']

    # Example data setup
    example_data = pd.DataFrame({
        'symbol': ['AAPL'] * 10,
        'feature1': np.random.rand(10),
        'feature2': np.random.rand(10),
        'feature3': np.random.rand(10),
        'feature4': np.random.rand(10),
        'feature5': np.random.rand(10),
        CLOSE: np.random.rand(10) * 100
    })

    # Initialize and set up the pipeline
    example_pipeline = ExamplePipeline()
    example_pipeline.setup()

    # Define run_ids for BACKTEST mode
    example_pipeline.run_ids = ['5min', '15min', '30min']

    # Execute the pipeline in BACKTEST mode
    example_pipeline.run(example_data)

    # For LIVE mode, ensure model_id is set and models are loaded appropriately
    # example_pipeline.model_id = 'AAPL_model'
    # example_pipeline.run_ids = ['5min']
    # example_pipeline.run(example_data)
