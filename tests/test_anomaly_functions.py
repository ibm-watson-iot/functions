
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sqlalchemy import Column, Float
from iotfunctions.anomaly import SaliencybasedGeneralizedAnomalyScore, SpectralAnomalyScore, \
                                 FFTbasedGeneralizedAnomalyScore, KMeansAnomalyScore
from nose.tools import assert_true

# constants
Temperature = 'Temperature'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'


def test_anomaly_scores():

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
    df_comp = kmi.execute(df=df_i)

    print("Executed Anomaly functions")

    # df_comp.to_csv('AzureAnomalysampleOutput.csv')
    df_o = pd.read_csv('AzureAnomalysampleOutput.csv')

    #print('Compare Scores - Linf')

    comp = {spectral: np.max(abs(df_comp[spectral].values - df_o[spectral].values)),
            fft: np.max(abs(df_comp[fft].values - df_o[fft].values)),
            sal: np.max(abs(df_comp[sal].values - df_o[sal].values)),
            kmeans: np.max(abs(df_comp[kmeans].values - df_o[kmeans].values))}

    #print(comp)

    print('Compare Scores R2-score')

    comp2 = {spectral: r2_score(df_o[spectral].values, df_comp[spectral].values),
             fft: r2_score(df_o[fft].values, df_comp[fft].values),
             sal: r2_score(df_o[sal].values, df_comp[sal].values),
             kmeans: r2_score(df_o[kmeans].values, df_comp[kmeans].values)}

    print(comp2)

    # assert_true(comp2[spectral] > 0.9)
    assert_true(comp2[fft] > 0.9)
    assert_true(comp2[sal] > 0.9)
    # assert_true(comp2[kmeans] > 0.9)

    pass

# uncomment to run from the command line
# test_anomaly_scores()
