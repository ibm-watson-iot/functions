import logging
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sklearn import ensemble, linear_model
from sqlalchemy import Column, Float

# helper class and function to run aggregators
from iotfunctions.aggregate import (Aggregation, add_simple_aggregator_execute)

from iotfunctions.bif import AggregateWithExpression
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
from nose.tools import assert_true, nottest

# constants
Temperature = 'TEMP_AIR'
Humidity = 'HUMIDITY'
KW = 'KW'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'

logger = logging.getLogger('Test Regressor')

@nottest
class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore('./data')
    def _init(self):
        return


def test_aggregation():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    # Run on the good pump first
    # Get stuff in
    print('Read Regressor Sample data in')
    df_i = pd.read_csv('./data/RegressionTestData.csv', index_col=False, parse_dates=['DATETIME'])
    df_i = df_i.rename(columns={'DATETIME': 'timestamp'})

    df_i['entity'] = 'MyShop'

    #print(type(df_i['timestamp'][0]))
    df_i = df_i.dropna()

    # make sure timestamp is a datetime (aggregations are very picky about datetime indices)
    df_i['timestamp'] = pd.to_datetime(df_i['timestamp']) #pd.to_datetime(df_rst.index, format="%Y-%m-%d-%H.%M.%S.%f")
    df_i['TEMP_AIR'] = df_i['TEMP_AIR'].astype(float)

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

    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}

    # build closure from aggregation class
    func = AggregateWithExpression

    # prepare parameter list for closure
    params_dict = {}
    params_dict['source'] = 'TEMP_AIR'
    params_dict['name'] = 'Temp_diff'
    params_dict['expression'] = 'x.max()-x.min()'

    # replace aggregate call with 'execute_AggregateWithExpression'
    func_name = 'execute_AggregateTimeInState'
    add_simple_aggregator_execute(func, func_name)

    # finally set up closure
    func_clos = getattr(func(**params_dict), func_name)


    # set up an Aggregation thingy with the entity index, timestamp index,
    # desired granularity and a (short) chain of aggregators
    # granularity = frequency, dimension(s), include entity, entity id
    aggobj = Aggregation(None, ids=['entity'], timestamp='timestamp', granularity=('D', None, True, 0),
                    simple_aggregators=[(['TEMP_AIR'], func_clos, 'x.max() - x.min()')])

    print(aggobj)


    et = aggobj._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)

    df_agg = aggobj.execute(df=df_i)
    df_agg_comp = pd.read_csv('./data/aggregated.csv', index_col=False, parse_dates=['timestamp'])

    assert_true(np.allclose(df_agg['x.max() - x.min()'].values, df_agg_comp['x.max() - x.min()'].values))

    print('Aggregation done', df_agg)

    pass


# uncomment to run from the command line
# test_base_estimator_function()

