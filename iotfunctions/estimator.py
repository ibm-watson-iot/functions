import logging
import numpy as np
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from .util import cosLoad, cosSave
import ibm_botocore
from iotfunctions.preprocessor import BaseTransformer

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class BaseRegressor(BaseTransformer):

    _bucket = None
    _expiration_days = None


    def __init__(self, features, targets, predictions):
        self.features = features
        self.targets = targets

        #Name predictions based on targets if predictions is None
        if predictions is None:
            predictions = ['predicted_%s'%x for x in self.targets]

        self.predictions = predictions
        super().__init__()


    def set_bucket_name(self, bucket):
        self._bucket = bucket

    def get_bucket_name(self):
        return self._bucket

    def get_model_name(self):
        return 'abc.ml'

class SampleAnomalySGDRegressor(BaseRegressor):
    '''
    Simple regression function - Stochastic Gradient Descent
    Full documentation: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDRegressor.html#sklearn.linear_model.SGDRegressor
    '''


    #Attributes defined by instance
    _bucket = 'models-bucket'

    model_created = False
    train_if_no_model = True
    needs_score = True

    config={
        'max_iter': 1000,
        'tol': 1e-3,
        'sample_weight': None,
        'training_score_threshold': 0.0000005,
        'real_score_threshold': 0.0000005
    }

    def __init__(self, credentials, features, targets, predictions=None):
        self.credentials = credentials
        self.estimator = linear_model.SGDRegressor(max_iter=self.config['max_iter'], tol=self.config['tol'])
        super().__init__(features=features, targets=targets, predictions=predictions)

    def fit(self, X_train, y_train):
        logger.info('fitting a model with X and y')
        self.estimator = self.estimator.fit(X_train, y_train)
        return self.estimator


    def predict(self, X_test):
        result = self.estimator.predict(X_test)
        logger.info('Model predicted with test X')
        return result


    def score(self, X_test, y_test):
        result = self.estimator.score(X_test, y_test, self.config['sample_weight'])
        logger.info('Model scored with test X and y')
        return result


    def execute(self, df):

        df = df.copy()

        persisted_model = None

        pipeline = cosLoad(bucket=self.get_bucket_name(), filename=self.get_model_name(),
                           credentials=self.credentials)

        if pipeline is not None:
            self.model_created = True
        else:
            logger.info('No model available.')

        if not self.model_created:
            if self.train_if_no_model:
                X_train, X_test, y_train, y_test = train_test_split(df[self.features].values,
                                                                    df[self.targets].values.ravel(), test_size=0.2)

                self.estimator = linear_model.SGDRegressor(max_iter=self.config['max_iter'], tol=self.config['tol'])


                '''
                
                ml-pipeline -
                            |- model 
                            |- 
                
                '''

                logger.info('Prepare to train a model.')
                persisted_model = self.fit(X_train, y_train)

                logger.info('Preparing prediction')
                predictions = self.predict(X_test)
                logger.debug('Predictions: %s' % predictions)

                if self.needs_score:
                    logger.info('Preparing scoring')
                    scores = self.score(X_test, y_test)
                    logger.debug('Scores: %s' % scores)

                threshold = self.config['training_score_threshold']
                if scores > threshold:
                    logger.info('Training score %s is greater than threshold %s : GOOD' % (scores, threshold))
                    cosSave(persisted_model, bucket=self.get_bucket_name(), filename=self.get_model_name(),
                        credentials=self.credentials)

                else:
                    logger.info('Training score %s is smaller than threshold %s : BAD' % (scores, threshold))


        logger.info('Model is already created. No need to create a new one.')
        predictions = pipeline.predict(df[self.features].values)

        df[self.predictions[0]] = predictions

        scores = pipeline.score(df[self.features].values,
                            df[self.targets].values.ravel())
        logger.debug('Scores: %s' % scores)


        #write score to the model!

        threshold = self.config['real_score_threshold']
        if scores > threshold:
            logger.info('Read score %s is greater than threshold %s : STILL GOOD' % (scores, threshold))
        else:
            logger.info('Real score %s is smaller than threshold %s : REALLY BAD' % (scores, threshold))
            #need to delete the old model
            #cosDelete is not available yet


        return df



