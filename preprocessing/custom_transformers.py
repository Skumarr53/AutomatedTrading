import json
import config
from sklearn.model_selection import StratifiedKFold
from sklearn.tree import DecisionTreeRegressor
from sklearn.feature_selection import SelectKBest, mutual_info_regression, RFE, RFECV
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from sklearn.base import TransformerMixin, BaseEstimator
from config import config


class ColumnExtractor(BaseEstimator, TransformerMixin):

    def __init__(self, cols):
        self.cols = cols

    def fit(self, X, y=None):
        # stateless transformer
        return self

    def transform(self, X):
        # assumes X is a DataFrame
        Xcols = X[self.cols]
        return Xcols


class DFFeatureUnion(BaseEstimator, TransformerMixin):
    # FeatureUnion but for pandas DataFrames

    def __init__(self, transformer_list):
        self.transformer_list = transformer_list

    def fit(self, X, y=None):
        for (name, t) in self.transformer_list:
            t.fit(X, y)
        return self

    def get_feature_names(self):
        return self.columns

    def transform(self, X):
        # assumes X is a DataFrame
        Xts = [t.transform(X) for _, t in self.transformer_list]
        Xunion = reduce(lambda X1, X2: pd.merge(
            X1, X2, left_index=True, right_index=True), Xts)
        self.columns = Xunion.columns.tolist()
        return Xunion


class ShortTermNormalizer(BaseEstimator, TransformerMixin):
    def __init__(self, look_back_days: int = 5):
        self.look_back_period = look_back_days * config.N_OPERATIONS_HOURS_DAILY
        self.params = {}  # To store mean and std for live mode
        self.columns = []
        self.param_dict = {}

    def fit(self, data: pd.DataFrame):
        self.columns = data.columns
        if config.TRADE_MODE == 'BACKTEST':
            # Assuming data is indexed by datetime
            for column in data.columns:
                rolling_windows = data[column].rolling(
                    window=f'{self.look_back_period}')
                self.params[column] = {
                    'mean': rolling_windows.mean(),  # .iloc[-1]
                    'std': rolling_windows.std()
                }
            # Optionally, store params for later use in LIVE mode
            # self._store_params()
        return self

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        transformed_data = pd.DataFrame(index=data.index)
        if config.TRADE_MODE == 'LIVE':
            # Load parameters if in LIVE mode
            self._load_params()

        for column in self.columns:
            mean = self.params[column]['mean']
            std = self.params[column]['std']
            transformed_data[column] = (data[column] - mean) / std
        return transformed_data

    def _store_params(self):
        for column in data.columns:
            self.param_dict[column] = {'mean': self.params[column]['mean'].iloc[-1],
                                       'std': self.params[column]['std'].iloc[-1]}

        with open('normalization_params.json', 'w') as f:
            json.dump(self.params, f)

    def _load_params(self):
        # TODO think of way to store and load parameters
        with open('normalization_params.json', 'r') as f:
            self.params = json.load(f)

    def get_params(self) -> dict:
        """Return the stored parameters for external use"""
        for column in data.columns:
            self.param_dict[column] = {'mean': self.params[column]['mean'].iloc[-1],
                                       'std': self.params[column]['std'].iloc[-1]}
        return self.params


class LongTermNormalizer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.ss = None
        self.mean_ = None
        self.scale_ = None

    def fit(self, X, y=None):
        self.ss = StandardScaler()
        self.ss.fit(X)
        self.mean_ = pd.Series(self.ss.mean_, index=X.columns)
        self.scale_ = pd.Series(self.ss.scale_, index=X.columns)
        return self

    def transform(self, X):
        # assumes X is a DataFrame
        if config.TRADE_MODE == 'LIVE':
            self._load_params()
            Xscaled = (X - self.mean_) / self.scale_
        else:
            Xss = self.ss.transform(X)
            Xscaled = pd.DataFrame(Xss, index=X.index, columns=X.columns)
        return Xscaled

    def get_params(self) -> dict:
        """Return the stored parameters for external use"""
        return {'mean': self.mean_,
                'std': self.scale_}

    def _load_params(self):
        # TODO think of way to store and load parameters
        with open('normalization_params.json', 'r') as f:
            self.mean_ = json.load(f)['mean']
            self.scale_ = json.load(f)['std']


class DFRecursiveFeatureSelector(BaseEstimator, TransformerMixin):

    def __init__(self, estimator=DecisionTreeRegressor(), n_features=10, step=10):
        self.estimator = estimator
        self.n_features = n_features
        self.step = step

    def fit(self, X, y=None):
        # stateless transformer
        self.RFE = RFE(estimator=self.estimator,
                       n_features_to_select=self.n_features, step=self.step)
        self.RFE.fit(X, y)
        return self

    def transform(self, X):
        return X.loc[:, self.RFE.get_support()]


class DF_RFECV_FeatureSelection(BaseEstimator, TransformerMixin):

    def __init__(self, estimator=DecisionTreeRegressor(), cv=StratifiedKFold(3), step=1, scoring='r2'):
        self.estimator = estimator
        self.scoring = scoring
        self.step = step
        self.cv = cv

    def fit(self, X, y=None):

        self.rfevc = RFECV(estimator=self.estimator,
                           step=self.step, cv=self.cv, scoring=self.scoring)
        self.fit(X, y)
        return self

    def transform(self, X):
        return X.drop(X.columns[np.where(self.rfevc.support_ == False)[0]], axis=1, inplace=True)
