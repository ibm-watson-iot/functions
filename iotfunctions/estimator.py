import logging
import datetime as dt
import numpy as np
from collections import OrderedDict
from sklearn import linear_model, ensemble, metrics, neural_network
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from iotfunctions.base import BaseTransformer
from .db import Database
from .pipeline import CalcPipeline, PipelineExpression
from .base import BaseRegressor, BaseEstimatorFunction, BaseClassifier
from .bif import IoTAlertHighValue
from .metadata import Model

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'


class SimpleAnomaly(BaseRegressor):
    '''
    Sample function uses a regression model to predict the value of one or more output
    variables. It compares the actual value to the prediction and generates an alert 
    when the difference between the actual and predicted value is outside of a threshold.
    '''
    #class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3
    def __init__(self, features, targets, threshold,
                 predictions=None, alerts = None):
        super().__init__(features=features, targets = targets, predictions=predictions)
        if alerts is None:
            alerts = ['%s_alert' %x for x in self.targets]
        self.alerts = alerts
        self.threshold = threshold
        #registration
        self.inputs = ['features','target']
        self.outputs = ['predictions','alerts']
        self.contsants = ['threshold']
        
    def execute(self,df):
        
        df = super().execute(df)
        for i,t in enumerate(self.targets):
            prediction = self.predictions[i]
            df['_diff_'] = (df[t] - df[prediction]).abs()
            alert = IoTAlertHighValue(input_item = '_diff_',
                                      upper_threshold = self.threshold,
                                      alert_name = self.alerts[i])
        df = alert.execute(df)
        
        return df

class SimpleRegressor(BaseRegressor):
    '''
    Sample function that predicts the value of a continuous target variable using the selected list of features.
    This function is intended to demonstrate the basic workflow of training, evaluating, deploying
    using a model. 
    '''
    #class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3
    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets = targets, predictions=predictions)
        #registration
        self.inputs = ['features','target']
        self.outputs = ['predictions']
        

class SimpleClassifier(BaseClassifier):
    '''
    Sample function that predicts the value of a discrete target variable using the selected list of features.
    This function is intended to demonstrate the basic workflow of training, evaluating, deploying
    using a model. 
    '''
    eval_metric = staticmethod(metrics.accuracy_score)
    #class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3
    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets = targets, predictions=predictions)
        #registration
        self.inputs = ['features','target']
        self.outputs = ['predictions']
        
class SimpleBinaryClassifier(BaseClassifier):
    '''
    Sample function that predicts the value of a discrete target variable using the selected list of features.
    This function is intended to demonstrate the basic workflow of training, evaluating, deploying
    using a model. 
    '''
    eval_metric = staticmethod(metrics.f1_score)
    #class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3
    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets = targets, predictions=predictions)
        #registration
        self.inputs = ['features','target']
        self.outputs = ['predictions']
        for t in self.targets:
            self.add_training_expression(t,'df[%s]=df[%s].astype(bool)' %(t,t))




