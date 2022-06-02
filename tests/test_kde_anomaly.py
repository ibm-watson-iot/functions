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
from iotfunctions.anomaly import (FeatureBuilder, GBMForecaster, KDEAnomalyScore, VIAnomalyScore)
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
#from nose.tools import assert_true, nottest

# constants
Temperature = 'TEMP_AIR'
Humidity = 'HUMIDITY'
KW = 'KW'

logger = logging.getLogger('Test Regressor')

#@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('/tmp')
    def _init(self):
        return


def test_kde_anomaly_score():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    # Run on the good pump first
    # Get stuff in
    print('Read KDE Anomaly sample data in')
    df_i = pd.read_csv('./data/PumpTestData.csv', index_col=False, parse_dates=['evt_timestamp'])
    df_i = df_i.rename(columns={'evt_timestamp': 'timestamp', 'deviceid': 'entity'})

    # and sort it by timestamp
    df_i = df_i.sort_values(by='timestamp')
    #for entity in np.unique(df_i['entity']):
    #    print(entity, df_i[df_i['entity'] == entity].values.shape)
    df_i = df_i[df_i['entity'] == '04714B60553A']  # single entity with few entries to reduce test time
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
    print('Train KDE Anomaly model for ' + df_i.index.levels[0].values)
    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    # Now run the anomaly functions as if they were executed in a pipeline
    kdei = KDEAnomalyScore(0.000001, ['speed'], ['rms_x'])
    #spsi.epochs = 1  # only for testing model storage
    kdei.epochs = 300

    kdei.auto_train = True
    kdei.delete_model = True
    et = kdei._build_entity_type(columns = [Column('MinTemp',Float())], **jobsettings)
    et.name = 'IOT_SHADOW_PUMP_DE_GEN5'

    kdei._entity_type = et
    df_i = kdei.execute(df=df_i)
    #####

    print('KDE Anomaly score - inference')

    kdei = KDEAnomalyScore(0.000001, ['speed'], ['rms_x'])
    kdei.epochs = 300
    kdei.auto_train = True

    kdei.delete_model = False

    et = kdei._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)

    kdei._entity_type = et
    df_i = kdei.execute(df=df_i)
    print('KDE Anomaly inferencing done')

    pass


if __name__ == '__main__':
    test_kde_anomaly_score()
