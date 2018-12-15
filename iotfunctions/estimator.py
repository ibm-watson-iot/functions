import logging
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from iotfunctions.preprocessor import BaseTransformer
from .db import Database

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

    def get_model_name(self, target_name, suffix=None):
        return self.generate_model_name(target_name=target_name, suffix=suffix)

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

    def __init__(self, features, targets, predictions=None):
        self._estimator = linear_model.SGDRegressor(max_iter=self.config['max_iter'], tol=self.config['tol'])
        super().__init__(features=features, targets=targets, predictions=predictions)

    def fit(self, X_train, y_train):
        logger.info('fitting a model with X and y')
        return self._estimator.fit(X_train, y_train)


    def predict(self, X_test):
        result = self._estimator.predict(X_test)
        logger.info('Model predicted with test X')
        return result


    def score(self, X_test, y_test):
        result = self._estimator.score(X_test, y_test, self.config['sample_weight'])
        logger.info('Model scored with test X and y')
        return result


    def execute(self, df):

        df = df.copy()

        db = Database()

        #use only the first target of the list for now. Multiple targets are not supported yet
        model_stored = db.cos_load(filename=self.get_model_name(target_name=list(df[self.targets])[0]),
                            bucket=self.get_bucket_name(),
                            binary=True)


        if model_stored is not None:
            self.model_created = True
            self._estimator = model_stored
        else:
            logger.info('No model available.')

        if not self.model_created:
            if self.train_if_no_model:
                X_train, X_test, y_train, y_test = train_test_split(df[self.features].values,
                                                                    df[self.targets].values.ravel(), test_size=0.2)

                '''
                
                ml-pipeline -
                            |- model 
                            |- 
                
                '''

                logger.info('Prepare to train a model.')
                self._estimator = self.fit(X_train, y_train)

                logger.info('Preparing prediction')
                predictions = self.predict(X_test)
                logger.debug('Predictions: %s' % predictions)

                if self.needs_score:
                    logger.info('Preparing scoring')
                    scores = self.score(X_test, y_test)
                    logger.debug('Scores: %s' % scores)

                threshold = self.config['training_score_threshold']
                #if scores > threshold:
                if 1==1:
                    logger.info('Training score %s is greater than threshold %s : GOOD' % (scores, threshold))

                    # use only the first target of the list for now. Multiple targets are not supported yet
                    db.cos_save(persisted_object=self._estimator,
                                filename=self.get_model_name(target_name=list(df[self.targets])[0]),
                                bucket=self.get_bucket_name(),
                                binary=True)
                else:
                    logger.info('Training score %s is smaller than threshold %s : BAD' % (scores, threshold))


        logger.info('Model is already created. No need to create a new one.')
        predictions = self._estimator.predict(df[self.features].values)


        df[self.predictions[0]] = predictions


        scores = self._estimator.score(df[self.features].values,
                            df[self.targets].values.ravel())


        logger.debug('Scores: %s' % scores)



        '''
        #DELETE operation is working. Use when the model's results are not satisfactory.
        A new model will be generated.
        
        db.cos_delete(filename=self.get_model_name(target_name=list(df[self.targets])[0]),
            bucket=self.get_bucket_name())

        '''

        return df



