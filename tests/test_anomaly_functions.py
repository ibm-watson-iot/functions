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
from iotfunctions.aggregate import (Aggregation, add_simple_aggregator_execute)
from iotfunctions.bif import AggregateWithExpression

from iotfunctions.anomaly import (SaliencybasedGeneralizedAnomalyScore, SpectralAnomalyScore, NoDataAnomalyScore,
                                 FFTbasedGeneralizedAnomalyScore, KMeansAnomalyScore, MatrixProfileAnomalyScore)
#from nose.tools import (assert_true, nottest)

logger = logging.getLogger('Test Regressor')

# constants
Temperature = 'Temperature'
TempDiff = 'Temp_Diff'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'
mat = 'TemperatureMatrixProfileScore'
nod = 'TemperatureNoDataScore'


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
    print('Compute Spectral Anomaly Score')
    spsi = SpectralAnomalyScore(Temperature, 12, spectral)
    et = spsi._build_entity_type(columns=[Column(Temperature, Float())])
    spsi._entity_type = et
    df_i = spsi.execute(df=df_i)

    print('Compute Saliency Anomaly Score')
    sali = SaliencybasedGeneralizedAnomalyScore(Temperature, 12, sal)
    et = sali._build_entity_type(columns=[Column(Temperature, Float())])
    sali._entity_type = et
    df_i = sali.execute(df=df_i)

    print('Compute FFT Anomaly Score')
    ffti = FFTbasedGeneralizedAnomalyScore(Temperature, 12, fft)
    et = ffti._build_entity_type(columns=[Column(Temperature, Float())])
    ffti._entity_type = et
    df_i = ffti.execute(df=df_i)

    print('Compute K-Means Anomaly Score')
    kmi = KMeansAnomalyScore(Temperature, 12, kmeans)
    et = kmi._build_entity_type(columns=[Column(Temperature, Float())])
    kmi._entity_type = et
    df_i = kmi.execute(df=df_i)

    print('Compute Matrix Profile Anomaly Score')
    mati = MatrixProfileAnomalyScore(Temperature, 12, mat)
    et = mati._build_entity_type(columns=[Column(Temperature, Float())])
    mati._entity_type = et
    df_i = mati.execute(df=df_i)

    print('Compute NoData Anomaly Score')
    nodi = NoDataAnomalyScore(Temperature, 12, nod)
    et = nodi._build_entity_type(columns=[Column(Temperature, Float())])
    nodi._entity_type = et
    df_comp = nodi.execute(df=df_i)

    print("Executed Anomaly functions")

    #df_comp.to_csv('./data/AzureAnomalysampleOutput.csv')
    df_o = pd.read_csv('./data/AzureAnomalysampleOutput.csv')

    # print('Compare Scores - L(inf)')

    print('Compare Scores R2-score')

    comp2 = {spectral: r2_score(df_o[spectral].values, df_comp[spectral].values),
             fft: r2_score(df_o[fft].values, df_comp[fft].values),
             sal: r2_score(df_o[sal].values, df_comp[sal].values),
             kmeans: r2_score(df_o[kmeans].values, df_comp[kmeans].values),
             mat: r2_score(df_o[mat].values, df_comp[mat].values),
             nod: r2_score(df_o[nod].values, df_comp[nod].values)}

    print(comp2)

    # assert_true(comp2[spectral] > 0.9)
    assert (comp2[fft] > 0.9)
    assert (comp2[sal] > 0.9)

    # assert_true(comp2[kmeans] > 0.9)
    df_agg = df_i.copy()

    # build closure from aggregation class
    func = AggregateWithExpression

    # prepare parameter list for closure
    params_dict = {}
    params_dict['source'] = Temperature
    params_dict['name'] = TempDiff
    params_dict['expression'] = 'x.max()-x.min()'

    # replace aggregate call with 'execute_AggregateWithExpression'
    func_name = 'execute_AggregateTimeInState'
    add_simple_aggregator_execute(func, func_name)

    # finally set up closure
    func_clos = getattr(func(**params_dict), func_name)

    #####
    print('Create dummy database')
    db_schema=None
    db = DatabaseDummy()
    print (db.model_store)


    jobsettings = { 'db': db, '_db_schema': 'public'} #, 'save_trace_to_file' : True}
    #EngineLogging.configure_console_logging(logging.DEBUG)

    #aggobj = Aggregation(None, ids=['entity'], timestamp='timestamp', granularity=('5T', (Temperature,), True, 0),
    #                simple_aggregators=[([Temperature], func_clos, [TempDiff])])
    aggobj = Aggregation(None, ids=['entity'], timestamp='timestamp', granularity=('5T', None, True, 0),
                    simple_aggregators=[(func_clos, [Temperature], [TempDiff])])
    et = aggobj._build_entity_type(columns=[Column(Temperature, Float())], **jobsettings)
    df_agg = aggobj.execute(df=df_agg)

    df_agg['site'] = 'Munich'
    df_agg = df_agg.set_index('site', append=True)
    print("----->", df_agg.index.names, df_agg.columns)


    # add frequency to time
    #df_agg = df_agg.reset_index().set_index(['timestamp']).asfreq(freq='T')
    #df_agg = df_agg.reset_index().set_index(['entity', 'timestamp', 'site']).dropna()

    print('Compute Spectral Anomaly Score')
    spsi = SpectralAnomalyScore(TempDiff, 12, spectral)
    et = spsi._build_entity_type(columns=[Column(TempDiff, Float())])
    spsi._entity_type = et
    df_agg = spsi.execute(df=df_agg)

    print('Compute K-Means Anomaly Score')
    kmi = KMeansAnomalyScore(TempDiff, 12, kmeans)
    et = kmi._build_entity_type(columns=[Column(TempDiff, Float())])
    kmi._entity_type = et
    df_agg = kmi.execute(df=df_agg)

    print('Compute Saliency Anomaly Score')
    sali = SaliencybasedGeneralizedAnomalyScore(TempDiff, 12, sal)
    et = sali._build_entity_type(columns=[Column(TempDiff, Float())])
    sali._entity_type = et
    df_agg = sali.execute(df=df_agg)

    print('Compute FFT Anomaly Score')
    ffti = FFTbasedGeneralizedAnomalyScore(TempDiff, 12, fft)
    et = ffti._build_entity_type(columns=[Column(TempDiff, Float())])
    ffti._entity_type = et
    df_agg = ffti.execute(df=df_agg)

    print('Compute Matrix Profile Anomaly Score')
    mati = MatrixProfileAnomalyScore(TempDiff, 12, fft)
    et = mati._build_entity_type(columns=[Column(TempDiff, Float())])
    mati._entity_type = et
    df_agg = mati.execute(df=df_agg)

    print('Compute NoData Anomaly Score')
    nodi = NoDataAnomalyScore(TempDiff, 12, nod)
    et = nodi._build_entity_type(columns=[Column(TempDiff, Float())])
    nodi._entity_type = et
    df_agg = nodi.execute(df=df_agg)

    print(df_agg.index, df_agg.describe())
    print(df_agg.index)

    '''
    # useless because df_o contains the non aggregated scores
    comp3 = {spectral: r2_score(df_o[spectral].values, df_agg[spectral].values),
             fft: r2_score(df_o[fft].values, df_agg[fft].values),
             sal: r2_score(df_o[sal].values, df_agg[sal].values),
             kmeans: r2_score(df_o[kmeans].values, df_agg[kmeans].values),
             mat: r2_score(df_o[mat].values, df_agg[mat].values)}

    print(comp3)
    '''

    print("Executed Anomaly functions on aggregation data")

    df_i = df_i[0:10]   # cut off data
    spsi = SpectralAnomalyScore(Temperature, 12, spectral)
    et = spsi._build_entity_type(columns=[Column(Temperature, Float())])
    spsi._entity_type = et
    df_i = spsi.execute(df=df_i)

    assert (not df_i.empty)

    print("Executed Anomaly functions on minimal data ", df_i.empty)

    pass


if __name__ == '__main__':
    test_anomaly_scores()

