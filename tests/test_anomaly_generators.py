from collections import OrderedDict
import datetime as dt
import numpy as np
import pandas as pd
from sqlalchemy import Column, Float, DateTime
from iotfunctions.bif import AnomalyGeneratorExtremeValue, DateDifference
from nose.tools import assert_true

# constants
Temperature = 'Temperature'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'


def test_anomaly_generators():

    # Run on the good pump first
    # Get stuff in
    print('Read Anomaly Sample data in')
    df_i = pd.read_csv('./AzureAnomalysample.csv', index_col=False, parse_dates=['timestamp'])

    df_i['id'] = 'MyRoom'
    df_i[Temperature] = df_i['value'] + 20
    df_i = df_i.drop(columns=['value'])
    df_i['evt_timestamp'] = df_i['timestamp']

    # and sort it by timestamp
    df_i = df_i.sort_values(by='timestamp')
    df_i = df_i.set_index(['id', 'timestamp']).dropna()

    print('Add columns with NaNs')
    addl = np.arange(0, 5, 0.00125)
    df_i['Test1'] = df_i[Temperature] + addl
    df_i['Test2'] = df_i[Temperature] + addl
    df_i['Test3'] = df_i[Temperature] + addl
    df_i['Test4'] = df_i[Temperature] + addl

    #####

    print('Run Extreme Value Generator')
    evg = AnomalyGeneratorExtremeValue('Test1', 3, 4, 'Results1')
    et = evg._build_entity_type(columns=[Column('Test1', Float())])
    evg._entity_type = et
    df_i = evg.execute(df=df_i)

    print('Run Extreme Value Generator 2nd time with count 3')
    evg = AnomalyGeneratorExtremeValue('Test2', 5, 7, 'Results2')
    evg.count = {'MyRoom': 3}
    et = evg._build_entity_type(columns=[Column('Test2', Float())])
    evg._entity_type = et
    df_i = evg.execute(df=df_i)

    print(df_i.head(15))

    print('Compare Scores')

    #print(results1)
    #print(results2)
    #print(origins1)
    #print(origins2)

    # assert_true(comp[0])
    # assert_true(comp[1])
    # assert_true(comp[2])

    pass

# test_anomaly_generators()
