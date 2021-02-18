import logging
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sklearn import ensemble, linear_model
from sqlalchemy import Column, Float
from iotfunctions.anomaly import GBMRegressor
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
from nose.tools import assert_true, nottest

# constants
Temperature = 'TEMP_AIR'
Humidity = 'HUMIDITY'
KW = 'KW'

logger = logging.getLogger('Test Regressor')

@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('/tmp')
    def _init(self):
        return


def test_light_gbm():

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

    print('lightGBM regressor - testing training pipeline')

    print('lightGBM regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'}

    brgi = GBMRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'],
                        n_estimators=500, num_leaves=40, learning_rate=0.2, max_depth=-1)

    brgi.stop_auto_improve_at = 0.4
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et

    df_i = brgi.execute(df=df_i)
    print('lightGBM regressor done')

    mtrc = brgi.active_models['model.TEST_ENTITY_FOR_GBMREGRESSOR.GBMRegressor.KW.MyShop'][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert_true(mtrc > 0.4)

    print('lightGBM regressor - testing training pipeline done ')


    #####

    print('lightGBM regressor - inference')

    print('lightGBM regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = GBMRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = 0.4
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)

    brgi._entity_type = et
    df_i = brgi.execute(df=df_i)
    print('lightGBM regressor done')

    mtrc = brgi.active_models['model.TEST_ENTITY_FOR_GBMREGRESSOR.GBMRegressor.KW.MyShop'][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert_true(mtrc > 0.4)

    print('lightGBM regressor - inference done')

    #####

    print('lightGBM regressor - enforce retraining')

    print('lightGBM regressor - first time training')
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    brgi = GBMRegressor(features=[Temperature, Humidity], targets=[KW], predictions=['KW_pred'])
    brgi.stop_auto_improve_at = mtrc + 2  # force retrain as r2 metric is considered bad now
    brgi.active_models = dict()

    et = brgi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    brgi._entity_type = et
    df_i = brgi.execute(df=df_i)
    print('lightGBM regressor done')

    mtrc = brgi.active_models['model.TEST_ENTITY_FOR_GBMREGRESSOR.GBMRegressor.KW.MyShop'][0].eval_metric_test
    print ('Trained model r2 ', mtrc)
    assert_true(mtrc > 0.4)

    print('lightGBM regressor - enforce retraining done')

    pass


# uncomment to run from the command line
# test_light_gbm()

