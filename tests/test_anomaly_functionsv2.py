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
from sqlalchemy import Column, Float
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
from iotfunctions.anomaly import (SaliencybasedGeneralizedAnomalyScoreV2, SpectralAnomalyScoreExt,
                                  FFTbasedGeneralizedAnomalyScoreV2, KMeansAnomalyScoreV2)
#from nose.tools import assert_true, nottest

# constants
Temperature = 'Temperature'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
spectralinv = 'TemperatureSpectralScoreInv'
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



def test_anomaly_scores():

    numba_logger = logging.getLogger('numba')
    numba_logger.setLevel(logging.ERROR)

    ####
    print('Create dummy database')
    db_schema=None
    db = DatabaseDummy()
    print (db.model_store)

    #####

    jobsettings = { 'db': db, '_db_schema': 'public'}
    EngineLogging.configure_console_logging(logging.DEBUG)

    # Run on the good pump first
    # Get stuff in
    print('Read Anomaly Sample data in')
    df_i = pd.read_csv('./data/AzureAnomalysample.csv', index_col=False, parse_dates=['timestamp'])

    df_i['entity'] = 'MyRoom'
    df_i[Temperature] = df_i['value'] + 20
    df_i = df_i.drop(columns=['value'])

    # and sort it by timestamp
    df_i = df_i.sort_values(by='timestamp')
    df_i = df_i.set_index(['entity', 'timestamp']).dropna()

    for i in range(0, df_i.index.nlevels):
        print(str(df_i.index.get_level_values(i)))

    #####
    print('Use scaling model generated with sklearn 0.21.3')

    print('Compute Saliency Anomaly Score')
    sali = SaliencybasedGeneralizedAnomalyScoreV2(Temperature, 12, True, sal)
    et = sali._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    sali._entity_type = et
    df_i = sali.execute(df=df_i)

    print('Compute FFT Anomaly Score')
    ffti = FFTbasedGeneralizedAnomalyScoreV2(Temperature, 12, True, fft)
    et = ffti._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    ffti._entity_type = et
    df_i = ffti.execute(df=df_i)

    print('Compute K-Means Anomaly Score')
    kmi = KMeansAnomalyScoreV2(Temperature, 12, True, kmeans)
    et = kmi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    kmi._entity_type = et
    df_comp = kmi.execute(df=df_i)

    #####
    print('Test with data frame with 0 rows')

    db.model_store = FileModelStore('/tmp')   # generate new scalings
    df_0rows= df_i[0:0]   # new data frame with same index and columns, but 0 rows

    print('Compute Scores of empty frame - integrity check')
    spsi = SpectralAnomalyScoreExt(Temperature, 12, spectral, spectralinv)
    et = spsi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    spsi._entity_type = et
    df_0rows = spsi.execute(df=df_0rows)

    sali = SaliencybasedGeneralizedAnomalyScoreV2(Temperature, 12, True, sal)
    et = sali._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    sali._entity_type = et
    df_0rows = sali.execute(df=df_0rows)

    ffti = FFTbasedGeneralizedAnomalyScoreV2(Temperature, 12, True, fft)
    et = ffti._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    ffti._entity_type = et
    df_0rows = ffti.execute(df=df_0rows)

    kmi = KMeansAnomalyScoreV2(Temperature, 12, True, kmeans)
    et = kmi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    kmi._entity_type = et
    df_0rows = kmi.execute(df=df_0rows)

    assert (len(df_0rows.index.names) > 0)

    return

    ####
    print("Executed Anomaly functions on sklearn 0.24.1")

    print("Now generate new scalings with recent sklearn")
    db.model_store = FileModelStore('/tmp')

    print('Compute Spectral Anomaly Score')
    spsi = SpectralAnomalyScoreExt(Temperature, 12, spectral, spectralinv)
    et = spsi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    spsi._entity_type = et
    df_i = spsi.execute(df=df_i)

    print('Compute Saliency Anomaly Score')
    sali = SaliencybasedGeneralizedAnomalyScoreV2(Temperature, 12, True, sal)
    et = sali._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    sali._entity_type = et
    df_i = sali.execute(df=df_i)

    print('Compute FFT Anomaly Score')
    ffti = FFTbasedGeneralizedAnomalyScoreV2(Temperature, 12, True, fft)
    et = ffti._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    ffti._entity_type = et
    df_i = ffti.execute(df=df_i)

    print('Compute K-Means Anomaly Score')
    kmi = KMeansAnomalyScoreV2(Temperature, 12, True, kmeans)
    et = kmi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    kmi._entity_type = et
    df_comp = kmi.execute(df=df_i)

    print("Executed Anomaly functions")

    # df_comp.to_csv('./data/AzureAnomalysampleOutputV2.csv')
    df_o = pd.read_csv('./data/AzureAnomalysampleOutputV2.csv')

    # print('Compare Scores - Linf')

    print('Compare Scores R2-score')

    comp2 = {spectral: r2_score(df_o[spectralinv].values, df_comp[spectralinv].values),
             fft: r2_score(df_o[fft].values, df_comp[fft].values),
             sal: r2_score(df_o[sal].values, df_comp[sal].values),
             kmeans: r2_score(df_o[kmeans].values, df_comp[kmeans].values)}

    print(comp2)

    # assert_true(comp2[spectral] > 0.9)
    assert (comp2[fft] > 0.9)
    assert (comp2[sal] > 0.9)
    # assert_true(comp2[kmeans] > 0.9)

    df_agg = df_i.copy()

    # add frequency to time
    df_agg = df_agg.reset_index().set_index(['timestamp']).asfreq(freq='T')
    df_agg['site'] = 'Munich'
    df_agg = df_agg.reset_index().set_index(['entity', 'timestamp', 'site']).dropna()

    print('Compute Spectral Anomaly Score - aggr')
    spsi = SpectralAnomalyScoreExt(Temperature, 12, spectral, spectralinv)
    et = spsi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    spsi._entity_type = et
    df_agg = spsi.execute(df=df_agg)

    print('Compute K-Means Anomaly Score - aggr')
    kmi = KMeansAnomalyScoreV2(Temperature, 12, True, kmeans)
    et = kmi._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    kmi._entity_type = et
    df_agg = kmi.execute(df=df_agg)

    print('Compute Saliency Anomaly Score - aggr')
    sali = SaliencybasedGeneralizedAnomalyScoreV2(Temperature, 12, True, sal)
    et = sali._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    sali._entity_type = et
    df_agg = sali.execute(df=df_agg)

    print('Compute FFT Anomaly Score - aggr')
    ffti = FFTbasedGeneralizedAnomalyScoreV2(Temperature, 12, True, fft)
    et = ffti._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    ffti._entity_type = et
    df_agg = ffti.execute(df=df_agg)

    print(df_agg.describe())

    comp3 = {spectral: r2_score(df_o[spectralinv].values, df_agg[spectralinv].values),
             fft: r2_score(df_o[fft].values, df_agg[fft].values),
             sal: r2_score(df_o[sal].values, df_agg[sal].values),
             kmeans: r2_score(df_o[kmeans].values, df_agg[kmeans].values)}

    print(comp3)

    print("Executed Anomaly functions on aggregation data")

    pass


if __name__ == '__main__':
    test_anomaly_scores()
