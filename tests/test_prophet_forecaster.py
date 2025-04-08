# *****************************************************************************
# © Copyright IBM Corp. 2018, 2025  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sqlalchemy import Column, Float

from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
from iotfunctions.aggregate import (Aggregation, add_simple_aggregator_execute)
from iotfunctions.bif import AggregateWithExpression

from iotfunctions.experimental import ProphetForecaster

#from nose.tools import (assert_true, nottest)

logger = logging.getLogger('Test Regressor')

# constants
Temperature = 'Temperature'


#@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('/tmp')
    def _init(self):
        return


def test_prophet_forecaster():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    # Run on the good pump first
    # Get stuff in
    print('Read occupancy data in')
    df=pd.read_excel('./data/OccuFi_SpaceDetail.xlsx')  


    print('Data cleansing')
    print(df['PROPERTY_ID'].value_counts())                # checking all properties   
    df=df[df['PROPERTY_ID']=='TWNAKANG']                   # selecting one property
    df.drop(columns=['PROPERTY_ID'],inplace=True)          # dropping property_id column since we selected one property
    df.reset_index(drop=True,inplace=True)
    df.sort_values(by=['DATE'],ignore_index=True,inplace=True)
    df.drop(df.loc[df['WiFi Occupants']==0].index,inplace=True)           # dropping all records with 0 occupancy
    df.reset_index(inplace=True,drop=True)
    #print('\n',df['Workpoint Capacity'].value_counts())          # checking all the available workpoints

    df=df[df['Workpoint Capacity']==219]                    # Selecting data of workpoint 219
    df.drop(columns=['Workpoint Capacity'],inplace=True)    # dropping the workpoint column
    df.reset_index(inplace=True,drop=True)
    df.rename(columns={'DATE':'time','WiFi Occupants':'occupancy'},inplace=True)    # renaming columns

    # turn into Monitor compatible data source
    dfk = df.copy()
    dfk['entity'] = 'MyRoom'
    dfk = dfk.sort_values(by='time')
    dfk = dfk.set_index(['entity','time'])

    # build mini pipeline
    EngineLogging.configure_console_logging(logging.DEBUG)
    db_schema=None
    db = DatabaseDummy()
    print (db.model_store)

    jobsettings = { 'db': db, '_db_schema': 'public', 'save_trace_to_file' : True}
    gstd = ProphetForecaster(['occupancy'], 'TW', 'pred', 'pred_lower', 'pred_upper', 'pred_date')

    et = gstd._build_entity_type(columns = [Column('MinTemp',Float())], **jobsettings)

    gstd.name = 'ProphetForecaster'

    gstd._entity_type = et

    # Run mini pipeline
    dfk = gstd.execute(df=dfk)

    EngineLogging.configure_console_logging(logging.INFO)


    assert ('pred' in dfk.columns)

    print("Executed Prophet forecaster", dfk.empty)

    my_model_file = Path("/tmp/KPI_MODEL_STOREProphet.TEST_ENTITY_FOR_PROPHETFORECASTER.ProphetForecaster.a426dfbaa8dd42e211238cb4d58a9ae95fb1644520949eba53f56fa399adc6ac:pred.MyRoom")
    assert (my_model_file.is_file())

    pass


if __name__ == '__main__':
    test_prophet_forecaster()

