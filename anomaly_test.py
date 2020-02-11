
import numpy as np
import pandas as pd
from sqlalchemy import Column, Float
from iotfunctions.anomaly import SaliencybasedGeneralizedAnomalyScore, SpectralAnomalyScore, \
                                 FFTbasedGeneralizedAnomalyScore, KMeansAnomalyScore

# constants
Temperature = 'Temperature'
kmeans = 'TemperatureKmeansScore'
fft = 'TemperatureFFTScore'
spectral = 'TemperatureSpectralScore'
sal = 'SaliencyAnomalyScore'
gen = 'TemperatureGeneralizedScore'

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
df_i = kmi.execute(df=df_i)

df_o = pd.read_csv('AzureAnomalysampleOutput.csv')

df_comp = df_i.copy()
df_comp[spectral+'O'] = df_o[spectral]
df_comp[fft+'O'] = df_o[fft]
df_comp[sal+'O'] = df_o[sal]
df_comp[kmeans+'O'] = df_o[kmeans]

print('Compare Scores')
comp = (np.all(np.where(df_comp[spectral] != df_comp[spectral+'O'], True, False)),
        np.all(np.where(df_comp[sal] != df_comp[sal+'O'], True, False)),
        np.all(np.where(df_comp[fft] != df_comp[fft+'O'], True, False)),
        np.all(np.where(df_comp[kmeans] != df_comp[kmeans+'O'], True, False)))

print(comp)

exit(np.all(comp))
