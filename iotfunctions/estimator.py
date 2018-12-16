import logging
import datetime as dt
import numpy as np
from collections import OrderedDict
from sklearn import linear_model, ensemble, metrics
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from iotfunctions.preprocessor import BaseTransformer
from .db import Database
from .pipeline import CalcPipeline
from .metadata import Model

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class BaseEstimatorFunction(BaseTransformer):
    '''
    Base class for functions that train, evaluate and predict using sklearn 
    compatible estimators.
    '''
    _bucket = None
    expiration_days = None
    # Train automatically
    auto_train = True
    experiments_per_execution = 1
    parameter_tuning_iterations = 3
    #cross_validation
    cv = None #(default)
    eval_metric = None
    # Test Train split
    test_size = 0.2
    # Model evaluation
    stop_auto_improve_at = 0.85
    acceptable_score_for_model_acceptance = -1
    greater_is_better = True
    def __init__(self, features, targets, predictions):
        self.features = features
        self.targets = targets
        #Name predictions based on targets if predictions is None
        if predictions is None:
            predictions = ['predicted_%s'%x for x in self.targets]
        self.predictions = predictions
        super().__init__()
        self._preprocessors = OrderedDict()
        self.estimators = OrderedDict()
        
    def add_preprocessor(self,stage):
        self._preprocessors[stage.name] = stage
        
    def decide_training_required(self,model):
        if self.auto_train:
            if model is None:
                msg = 'Training required because there is no existing model'
                logger.debug(msg)
                return True
            elif model.expiry_date <= dt.datetime.utc_now():
                msg = 'Training required model expired on %s' %model.expiry_date
                logger.debug(msg)                
                return True
            elif self.greater_is_better and model.eval_metric_test < self.stop_auto_improve_at:
                msg = 'Training required because eval metric of %s is lower than threshold %s ' %(model.eval_metric_test,self.stop_auto_improve_at)
                logger.debug(msg)                                
                return True
            elif not self.greater_is_better and model.eval_metric_test > self.stop_auto_improve_at:
                msg = 'Training required because eval metric of %s is higher than threshold %s ' %(model.eval_metric_test,self.stop_auto_improve_at)
                logger.debug(msg)                                
                return True            
        else:
            return False
        
    def execute(self,df):    
        raise NotImplementedError('You must implement an execute method')    
        return df
    
    def find_best_model(self,df_train, df_test, target, features, existing_model):
        scorer = self.make_scorer()
        metric_name = scorer.__class__.__name__
        estimators = self.make_estimators(names=None, count = self.experiments_per_execution)
        if existing_model is None:
            trained_models = []
            best_test_metric = None
            best_model = None
        else:
            trained_models = [existing_model]
            best_test_metric = model.eval_metric_test
            best_model = existing_model
        for (name,estimator,params) in estimators:
            estimator = self.fit_with_search_cv(estimator = estimator,
                                                params = params,
                                                df_train = df_train,
                                                target = target,
                                                features = features)
            eval_metric_train = estimator.score(df_train[features],df_train[target])
            msg = 'Trained estimator %s with an %s score of %s' %(self.__class__.__name__, metric_name, eval_metric_train)
            logger.debug(msg)
            model = Model(name = self.get_model_name(target_name = target),
                          target = target,
                          features = features,
                          params = estimator.best_params_,
                          eval_metric_type = scorer,
                          eval_metric_train = eval_metric_train,
                          estimator = estimator,
                          estimator_name = name)
            eval_metric_test = model.score(df_test)
            trained_models.append(model)
            if best_test_metric is None:
                best_model = model
                best_test_metric = eval_metric_test
                msg = 'No prior model, first created is best'
                logger.debug(msg)
            elif self.greater_is_better and eval_metric_test > best_test_metric:
                msg = 'Higher than previous best of %s. New metric is %s' %(best_test_metric,eval_metric_test)
                best_model = model
                best_test_metric = eval_metric_test        
                logger.debug(msg)
            elif not self.greater_is_better and eval_metric_test < best_test_metric:
                msg = 'Lower than previous best of %s. New metric is %s' %(best_test_metric,eval_metric_test)
                best_model = model
                best_test_metric = eval_metric_test
                logger.debug(msg)
        
        return best_model
                
                
    def fit_with_search_cv(self, estimator, params, df_train, target, features):
        
        scorer = self.make_scorer()
        
        search = RandomizedSearchCV(estimator = estimator,
                                    param_distributions = params,
                                    n_iter= self.parameter_tuning_iterations,
                                    scoring=scorer, refit=True, 
                                    cv= self.cv, return_train_score = False)
        estimator = search.fit(X=df_train[features], y = df_train[target])
        msg = 'Used randomize search cross validation to find best hyper parameters for estimator %s' %estimator.__class__.__name__
        logger.debug(msg)
        
        return estimator
        
        
    def set_bucket_name(self, bucket):
        self._bucket = bucket

    def set_estimators(self):
        '''
        Set the list of candidate estimators and associated parameters
        '''
        # populate the estimators dict with a list of tuples containing instance of an estimator and parameters for estimator
        raise NotImplementedError('You must implement a set estimator method')
        
    def set_preprocessors(self):
        '''
        Add the preprocessing stages that will transform data prior to training, evaluation or making prediction
        '''
        #self.add_preprocessor(ClassName(args))
        
    def train_test_split(self,df):
        '''
        Split dataframe into test and training sets
        '''
        
        df_train, df_test = train_test_split(df,test_size=self.test_size)
        self.log_df_info(df_train,msg='training set',include_data=False)
        self.log_df_info(df_test,msg='test set',include_data=False)        
        return (df_train,df_test)

    def get_bucket_name(self):
        return self._bucket

    def get_model_name(self, target_name, suffix=None):
        return self.generate_model_name(target_name=target_name, suffix=suffix)
    
    def make_estimators(self, names = None, count = None):
        '''
        Make a list of candidate estimators based on available estimator classes
        '''
        self.set_estimators()
        if names is None:
            estimators = list(self.estimators.keys())
            if len(estimators) == 0:
                msg = 'No estimators defined. Implement the set_estimators method to define estimators'
                raise ValueError(msg)
            if count is not None:
                names = list(np.random.choice(estimators,count))
            else:
                names = estimators

        msg = 'Selected estimators %s' %names
        logger.debug(msg)
        
        out = []
        for e in names:
            (e_cls, parameters) = self.estimators[e]
            out.append((e,e_cls(),parameters))
            
        return out
            
    def make_scorer(self):
        '''
        Make a scorer
        '''
        return metrics.make_scorer(self.eval_metric,greater_is_better = self.greater_is_better)
    
    def make_feature_list(self,df,features,unprocessed_targets):
        '''
        Simple feature selector. Includes all candidate features that to not
        involve targets that have not yet been processed. Use a custom implementation
        of this method to do more advanced feature selection.
        '''
        features = [x for x in features if x not in unprocessed_targets]
        return features            
    
    
class BaseRegressor(BaseEstimatorFunction):
    '''
    Base class for building regression models
    '''
    eval_metric = staticmethod(metrics.r2_score)
    def set_estimators(self):

        params = {'n_estimators': [100,250,500,1000],
                   'max_depth': [2,4,10], 
                   'min_samples_split': [2,5,9],
                   'learning_rate': [0.01,0.02,0.05],
                   'loss': ['ls']}
        self.estimators['gradient_boosted_regressor'] = (ensemble.GradientBoostingRegressor,params)
        params = {'max_iter': [250,1000,5000,10000],
                  'tol' : [0.001, 0.002, 0.005] }
        self.estimators['gradient_boosted_regressor'] = (linear_model.SGDRegressor,params)
                
    

class SimpleRegressor(BaseRegressor):
    '''
    Sample function that predicts the value of a target variable using the selected list of features.
    This function is intended to demonstrate the basic workflow of training, evaluating, deploying
    using a model. This simplified sample will not produce a robust regression model.
    '''
    #class variables
    _bucket = 'models-bucket'
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3
    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets = targets, predictions=predictions)
        #registration
        self.inputs = ['features','target']
        self.outputs = ['prediction_name']
        
    def execute(self, df):
        df  = df.copy()
        db = self._entity_type.db
        bucket = self.get_bucket_name()
        # transform incoming data using any preprocessors
        # include whatever preprocessing stages are required by implementing a set_preprocessors method
        self.set_preprocessors()
        preprocessors = list(self._preprocessors.values())
        pl = CalcPipeline(stages = preprocessors , entity_type = self._entity_type)
        df = pl.execute(df)
        msg = 'Completed preprocessing'
        logger.debug(msg)
        # do test train split
        df_train,df_test = self.train_test_split(df)
        unprocessed_targets = []
        unprocessed_targets.extend(self.targets)
        for i,target in enumerate(self.targets):
            logger.debug('processing target %s' %target)
            features = self.make_feature_list(features=self.features,
                                              df = df,
                                              unprocessed_targets = unprocessed_targets)
            model_name = self.get_model_name(target)
            model = db.cos_load(filename= model_name,
                                bucket=bucket,
                                binary=True)
            if self.decide_training_required(model):
                msg = 'Prepare to train model %s' %model_name
                logger.info(msg) 
                best_model = self.find_best_model(df_train=df_train,
                                             df_test=df_test,
                                             target = target,
                                             features = features,
                                             existing_model=model)
                best_model.test(df_test)
                if model != best_model:
                    model = best_model
                    if self.greater_is_better and model.eval_metric_test > self.acceptable_score_for_model_acceptance:
                        db.cos_save(persisted_object=model, filename=model_name, bucket=bucket, binary=True)
                        logger.debug('wrote new model')
                    elif not self.greater_is_better and model.eval_metric_test < self.acceptable_score_for_model_acceptance:
                        db.cos_save(persisted_object=model, filename=model_name, bucket=bucket, binary=True)                    
                        logger.debug('wrote new model')
                    else:
                        msg = 'Training process did not  manage to create that passed the model acceptance critera. Model evaluaton result was %s' %model.eval_metric_test
                        logger.debug(msg)
                        model = None
            if model is not None:        
                df[self.predictions[i]] = model.predict(df)
                self.log_df_info(df,'After adding predictions for target %s' %target)
            else:
                df[self.predictions[i]] = None
                logger.debug('No suitable model found. Created null predictions')
            unprocessed_targets.remove(target)
            
        return df


