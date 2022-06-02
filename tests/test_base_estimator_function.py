# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
import unittest
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sklearn import ensemble, linear_model
from sqlalchemy import Column, Float
from iotfunctions.base import BaseEstimatorFunction
from iotfunctions.anomaly import GBMRegressor
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
#from nose.tools import assert_true, nottest

# constants
Temperature = 'TEMP_AIR'
Humidity = 'HUMIDITY'
KW = 'KW'
MyShop = 'MyShop'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'

logger = logging.getLogger('Test Regressor')

#@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('./data')
    def _init(self):
        return


#@nottest
class TestRegressor(BaseEstimatorFunction):

    eval_metric = staticmethod(r2_score)

    train_if_no_model = True
    num_rounds_per_estimator = 3

    def set_estimators(self):
    # gradient_boosted
        params = {'n_estimators': [100, 250, 500], 'max_depth': [2, 4, 10], 'min_samples_split': [2, 5, 9],
                  'learning_rate': [0.01, 0.02, 0.05], 'loss': ['ls']}
        self.estimators['gradient_boosted_regressor'] = (ensemble.GradientBoostingRegressor, params)
        # sgd
        params = {'max_iter': [250, 1000, 5000, 10000], 'tol': [0.001, 0.002, 0.005]}
        self.estimators['sgd_regressor'] = (linear_model.SGDRegressor, params)


    def __init__(self, features, targets, predictions):
        super().__init__(features=features, targets=targets, predictions=predictions, keep_current_models=True)
        self.correlation_threshold = 0

    def execute(self, df):

        try:
            df_new = super().execute(df)
            df = df_new
        except Exception as e:
            logger.info('Test Regressor failed with: ' + str(e))

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        outputs = []
        return (inputs, outputs)



def test_base_estimator_function():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    # Run on the good pump first
    # Get stuff in
    print('Read Regressor Sample data in')
    df_i = pd.read_csv('./data/RegressionTestData.csv', index_col=False, parse_dates=['DATETIME'])
    df_i = df_i.rename(columns={'DATETIME': 'timestamp'})

    df_i['entity'] = 'MyShop'

    # and sort it by timestamp
    df_i = df_i.sort_values(by='timestamp')
    df_i = df_i.set_index(['entity', 'timestamp']).dropna()

    for i in range(0, df_i.index.nlevels):
        print(str(df_i.index.get_level_values(i)))

    EngineLogging.configure_console_logging(logging.DEBUG)

    #####
    print('Create dummy database')
    db_schema=None
    db = DatabaseDummy()
    print (db.model_store)

    #####

    print('Base regressor - testing model generated with sklearn 0.21.3')

    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = TestRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW)  # no suffix, works on entity type level

    df_i = brgi.execute(df=df_i)
    print('Base regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.4)

    print('Base regressor - testing training pipeline done ')


    db.model_store = FileModelStore('/tmp')

    print('Base regressor - testing training pipeline with recent sklearn')

    print('Base regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = TestRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW)

    df_i = brgi.execute(df=df_i)
    print('Base regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.35)

    print('Base regressor - testing training pipeline done ')


    #####

    print('Base regressor - inference')

    print('Base regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = TestRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW)

    df_i = brgi.execute(df=df_i)
    print('Base regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.35)

    print('Base regressor - inference done')

    #####

    print('Base regressor - enforce retraining')

    print('Base regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = TestRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = mtrc + 2  # force retrain as r2 metric is considered bad now

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW)

    df_i = brgi.execute(df=df_i)
    print('Base regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.35)

    print('Base regressor - enforce retraining done')

    pass


if __name__ == '__main__':
    test_base_estimator_function()

