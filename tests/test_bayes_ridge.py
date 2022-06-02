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
from iotfunctions.anomaly import BayesRidgeRegressor, BayesRidgeRegressorExt
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
#from nose.tools import assert_true, nottest

# constants
Temperature = 'TEMP_AIR'
Humidity = 'HUMIDITY'
KW = 'KW'
MyShop = 'MyShop'

logger = logging.getLogger('Test Regressor')

#@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('./data')
    def _init(self):
        return


def test_bayes_ridge():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    # Run on the good pump first
    # Get stuff in
    print('Read Regressor Sample data in')
    df_i = pd.read_csv('./data/RegressionTestData.csv', index_col=False, parse_dates=['DATETIME'])
    df_i = df_i.rename(columns={'DATETIME': 'timestamp'})

    df_i['entity'] = 'MyShop'
    df_i[Temperature] = pd.to_numeric(df_i[Temperature], errors='coerce')
    df_i[Humidity] = pd.to_numeric(df_i[Humidity], errors='coerce')

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
    print('Bayes ridge - testing training pipeline with sklearn 0.21.3')

    jobsettings = { 'db': db, '_db_schema': 'public'}

    brgi = BayesRidgeRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    df_i = brgi.execute(df=df_i)

    print('Bayes ridge - testing training pipeline with recent sklearn')
    db.model_store = FileModelStore('/tmp')

    print('Bayes ridge - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'}

    brgi = BayesRidgeRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW, suffix=MyShop)

    df_i = brgi.execute(df=df_i)
    print('Bayes regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.4)

    print('Bayes regressor - testing training pipeline done ')


    #####

    print('Bayes regressor - inference')

    print('Bayes regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = BayesRidgeRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)

    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW, suffix=MyShop)

    df_i = brgi.execute(df=df_i)
    print('Bayes regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.4)

    print('Bayes regressor - inference done')

    #####

    print('Bayes regressor - enforce retraining')

    print('Bayes regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = BayesRidgeRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = mtrc + 2  # force retrain as r2 metric is considered bad now
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW, suffix=MyShop)

    df_i = brgi.execute(df=df_i)
    print('Bayes regressor done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.4)

    print('Bayes regressor - enforce retraining done')

    #####

    print('Bayes regressor ext - training with degree 3')

    print('Bayes regressor ext - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgei = BayesRidgeRegressorExt(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'], degree=3)
    brgei.stop_auto_improve_at = 0.4
    brgei.active_models = dict()

    et = brgei._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgei._entity_type = et
    brgi._entity_type.name = MyShop

    model_name = brgi.generate_model_name([Temperature, Humidity], KW, suffix=MyShop)

    df_i = brgei.execute(df=df_i)
    print('Bayes regressor ext done')

    mtrc = brgi.active_models[model_name][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert (mtrc > 0.4)

    print('Bayes regressor ext - training done')

    pass


if __name__ == '__main__':
    test_bayes_ridge()
