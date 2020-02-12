
import numpy as np
import pandas as pd
from sqlalchemy import Column, Float
from iotfunctions.bif import Coalesce
from nose.tools import assert_true

# constants
Temperature = 'Temperature'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'


def test_base_functions():

    # Run on the good pump first
    # Get stuff in
    print('Read Anomaly Sample data in')
    df_i = pd.read_csv('./AzureAnomalysample.csv', index_col=False, parse_dates=['timestamp'])

    df_i['entity'] = 'MyRoom'
    df_i[Temperature] = df_i['value'] + 20
    df_i = df_i.drop(columns=['value'])

    # and sort it by timestamp
    df_i = df_i.sort_values(by='timestamp')
    df_i = df_i.set_index(['entity', 'timestamp']).dropna()

    print('Add columns with NaNs')
    addl = np.arange(0, 5, 0.00125)
    df_i['Test1'] = df_i[Temperature] + addl
    df_i['Test2'] = df_i[Temperature] + addl
    df_i['Test3'] = df_i[Temperature] + addl
    df_i['Test4'] = df_i[Temperature] + addl
    df_i['Test1'][3] = None
    df_i['Test2'][2] = None
    df_i['Test2'][3] = None
    df_i['Test3'][1] = None
    df_i['Test4'][1] = 10000.0
    df_i['Test4'][3] = 20000.0

    #####

    print('Run Coalesce')
    coal = Coalesce(['Test1', 'Test2', 'Test3', 'Test4'], 'Results1')
    et = coal._build_entity_type(columns=[Column('Test1', Float()), Column('Test2', Float()),
                                          Column('Test3', Float()), Column('Test4', Float())])
    coal._entity_type = et
    df_i = coal.execute(df=df_i)

    print('Run Coalesce 2nd time')
    coal = Coalesce(['Test4', 'Test1', 'Test2', 'Test3'], 'Results2')
    et = coal._build_entity_type(columns=[Column('Test4', Float()), Column('Test1', Float()),
                                          Column('Test2', Float()), Column('Test3', Float())])
    coal._entity_type = et
    df_i = coal.execute(df=df_i)

    print('Compare Scores')
    results1 = df_i['Results1'].values[0:5]
    results2 = df_i['Results2'].values[0:5]
    origins1 = np.asarray([23.0, 23.00125, 23.0025, 23.00375, 23.005])
    origins2 = np.asarray([23.0, 10000.0, 23.0025, 20000.0, 23.005])

    comp = (np.all(results1 == origins1), np.all(results2 == origins2))


    print (results1)
    print (results2)
    print (origins1)
    print (origins2)
    print(comp)

    assert_true(comp[0])
    assert_true(comp[1])

    pass

# test_base_scores()
