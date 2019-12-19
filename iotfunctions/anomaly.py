# *****************************************************************************
# © Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

'''
The Built In Functions module contains preinstalled functions
'''

import datetime as dt
import time
from collections import OrderedDict
import numpy as np
import scipy as sp

# for Spectral Analysis
from scipy import signal
from scipy.stats import energy_distance
from sklearn import metrics
from sklearn.covariance import EllipticEnvelope

# for KMeans
import skimage as ski
from skimage import util as skiutil # for nifty windowing
from pyod.models.cblof import CBLOF

import re
import pandas as pd
import logging
import warnings
import json
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from .base import (BaseTransformer, BaseEvent, BaseSCDLookup, BaseMetadataProvider, BasePreload, BaseDatabaseLookup,
                   BaseDataSource, BaseDBActivityMerge, BaseSimpleAggregator, BaseRegressor, BaseClassifier)

from .bif import (AlertHighValue)
from .ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti, UIExpression,
                 UIText, UIStatusFlag, UIParameters)

from .util import adjust_probabilities, reset_df_index

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True

def custom_resampler(array_like):
    # initialize
    if not 'gap' in dir():
        gap = 0
    
    if (array_like.values.size > 0):
        gap = 0
        return 0
    else:
        gap += 1
        return gap

def minDelta(df):
    # minimal time delta for merging
            
    try: 
        mindelta = dfe_orig.index.to_series().diff().min()
    except: 
        mindelta = pd.Timedelta('5 seconds')

    if mindelta == dt.timedelta(seconds = 0) or pd.isnull(mindelta):
        mindelta = pd.Timedelta('5 seconds')
    return mindelta
    

class NoDataAnomalyScore(BaseTransformer):
    '''
    Employs spectral analysis to extract features from the gaps in time series data and to compute the elliptic envelope from it
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.info(input_item)
        self.input_item = input_item

        # use 24 by default - must be larger than 12
        self.windowsize = np.maximum(windowsize,1)

        # overlap 
        if self.windowsize == 1:
            self.windowoverlap = 0
        else:
            self.windowoverlap = self.windowsize - np.maximum(self.windowsize // 12, 1)

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df.index.levels[0])
        logger.info(str(entities))


        df_copy[self.output_item] = 0
        #df_copy.sort_index(level=1)
        df_copy.sort_index()

        for entity in entities:
            # per entity
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            dfe = dfe.reset_index(level=[0]).sort_index()
            dfe_orig = dfe_orig.reset_index(level=[0]).sort_index()

            # minimal time delta for merging
            mindelta = minDelta(dfe_orig)

            logger.info('Timedelta:' + str(mindelta))

            # count the timedelta in seconds between two events
            timeSeq = dfe.index.values - dfe.index[0].to_datetime64()
            temperature = np.gradient(timeSeq)  # we look at the gradient for anomaly detection

            # interpolate gaps - data imputation
            dfe[[self.input_item]] = temperature
            Size = temperature.size
            dfe = dfe.interpolate(method='time')

            # one dimensional time series - named temperature for catchyness
            #temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            logger.info('Spectral: ' + str(entity) + ', ' + str(self.input_item) + ', ' + str(self.windowsize) + ', ' +
                         str(self.output_item) + ', ' + str(self.windowoverlap) + ', ' + str(temperature.size))

            if temperature.size <= self.windowsize:
                logger.info(str(temperature.size) + ' <= ' + str(self.windowsize))
                df_copy.loc[[entity]] = 0.0001
            else:
                logger.info(str(temperature.size) + str(self.windowsize))
                # Fourier transform:
                #   frequency, time, spectral density
                freqsTS, timesTS, SxTS = signal.spectrogram(temperature, fs = self.frame_rate, window = 'hanning',
                                                        nperseg = self.windowsize, noverlap = self.windowoverlap,
                                                        detrend = False, scaling='spectrum')

                # cut off freqencies too low to fit into the window
                freqsTSb = (freqsTS > 2/self.windowsize).astype(int)
                freqsTS = freqsTS * freqsTSb
                freqsTS[freqsTS == 0] = 1 / self.windowsize

                highfreqsTS = freqsTS.copy()
                lowfreqsTS = freqsTS.copy()
                highfreqsTS[highfreqsTS <= 0.3] = 0
                lowfreqsTS[lowfreqsTS > 0.3] = 0

                # Compute energy = frequency * spectral density over time in decibel
                try:
                    lowETS = np.log10(np.dot(SxTS.T, lowfreqsTS))
                    highETS = np.log10(np.dot(SxTS.T, highfreqsTS))
                    ETS = np.log10(np.dot(SxTS.T, freqsTS))

                    # compute the elliptic envelope to exploit Minimum Covariance Determinant estimates
                    #    standardizing
                    lowETS = (lowETS - lowETS.mean())/lowETS.std(ddof=0)
                    highETS = (highETS - highETS.mean())/highETS.std(ddof=0)

                    twoDimETS = np.vstack((lowETS, highETS)).T
                    logger.info('lowETS: ' + str(lowETS) + ', highETS:' + str(highETS) + 'input' + str(twoDimETS))

                    # inliers have a score of 1, outliers -1, and 0 indicates an issue with the data
                    dfe[self.output_item] = 0.0002
                    ellEnv = EllipticEnvelope(random_state=0)

                    dfe[self.output_item] = 0.0003
                    ellEnv.fit(twoDimETS)
                    #ets_zscore = ellEnv.predict(twoDimETS)
                    dfe[self.output_item] = 0.0004
                    ets_zscore = ellEnv.decision_function(twoDimETS, raw_values=True).copy()

                    # compute zscore over the energy
                    #ets_zscore = np.abs((ETS - ETS.mean())/ETS.std(ddof=0))
                    logger.info('Spectral z-score max: ' + str(ets_zscore.max()))

                    # length of timesTS, ETS and ets_zscore is smaller than half the original
                    #   extend it to cover the full original length 
                    dfe[self.output_item] = 0.0005
                    linear_interpolate = sp.interpolate.interp1d(timesTS, ets_zscore, kind='linear', fill_value='extrapolate')

                    dfe[self.output_item] = 0.0006
                    zscoreI = linear_interpolate(np.arange(0, temperature.size, 1))

                    dfe[self.output_item] = zscoreI

                except Exception as e:
                    logger.error('Spectral failed with ' + str(e))



                # absolute zscore > 3 ---> anomaly
                #df_copy.loc[(entity,), self.output_item] = zscoreI

                dfe_orig = pd.merge_asof(dfe_orig, dfe[self.output_item],
                         left_index = True, right_index = True, direction='nearest', tolerance = mindelta)

                if self.output_item+'_y' in dfe_orig:
                    zScoreII = dfe_orig[self.output_item+'_y'].to_numpy()
                elif self.output_item in dfe_orig:
                    zScoreII = dfe_orig[self.output_item].to_numpy()
                else:
                    print (dfe_orig.head(2))
                    zScoreII = dfe_orig[self.input_item].to_numpy()

                #df_copy.loc[(entity,) :, self.output_item] = zScoreII
                idx = pd.IndexSlice
                df_copy.loc[idx[entity,:], self.output_item] = zScoreII

        msg = 'NoDataAnomalyScore'
        self.trace_append(msg)

        #print(df_copy.head(20))

        return (df_copy)

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name = 'input_item',
                datatype=float,
                description = 'Column for feature extraction'
                                              ))

        inputs.append(UISingle(
                name = 'windowsize',
                datatype=int,
                description = 'Window size for spectral analysis - default 12'
                                              ))

        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name = 'output_item',
                datatype=float,
                description='Anomaly gap score'
                ))
        return (inputs,outputs)


class SpectralAnomalyScore(BaseTransformer):
    '''
    Employs spectral analysis to extract features from the time series data and to compute zscore from it
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.info(input_item)
        self.input_item = input_item

        # use 24 by default - must be larger than 12
        self.windowsize = np.maximum(windowsize,1)

        # overlap 
        if self.windowsize == 1:
            self.windowoverlap = 0
        else:
            self.windowoverlap = self.windowsize - np.maximum(self.windowsize // 12, 1)

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df.index.levels[0])
        logger.info(str(entities))

        df_copy[self.output_item] = 0
        #df_copy.sort_index(level=1)
        df_copy.sort_index()

        for entity in entities:
            # per entity
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            dfe = dfe.reset_index(level=[0]).sort_index()
            dfe_orig = dfe_orig.reset_index(level=[0]).sort_index()

            # minimal time delta for merging
            mindelta = minDelta(dfe_orig)

            logger.info('Timedelta:' + str(mindelta))

            # interpolate gaps - data imputation
            Size = dfe[[self.input_item]].fillna(0).to_numpy().size
            dfe = dfe.interpolate(method='time')

            # one dimensional time series - named temperature for catchyness
            temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            logger.info('Spectral: ' + str(entity) + ', ' + str(self.input_item) + ', ' + str(self.windowsize) + ', ' +
                         str(self.output_item) + ', ' + str(self.windowoverlap) + ', ' + str(temperature.size))

            if temperature.size <= self.windowsize:
                logger.info(str(temperature.size) + ' <= ' + str(self.windowsize))
                df_copy.loc[[entity]] = 0.0001
            else:
                logger.info(str(temperature.size) + str(self.windowsize))
                # Fourier transform:
                #   frequency, time, spectral density
                freqsTS, timesTS, SxTS = signal.spectrogram(temperature, fs = self.frame_rate, window = 'hanning',
                                                        nperseg = self.windowsize, noverlap = self.windowoverlap,
                                                        detrend = False, scaling='spectrum')

                # cut off freqencies too low to fit into the window
                freqsTSb = (freqsTS > 2/self.windowsize).astype(int)
                freqsTS = freqsTS * freqsTSb
                freqsTS[freqsTS == 0] = 1 / self.windowsize

                highfreqsTS = freqsTS.copy()
                lowfreqsTS = freqsTS.copy()
                highfreqsTS[highfreqsTS <= 0.3] = 0
                lowfreqsTS[lowfreqsTS > 0.3] = 0

                # Compute energy = frequency * spectral density over time in decibel
                try:
                    lowETS = np.log10(np.dot(SxTS.T, lowfreqsTS))
                    highETS = np.log10(np.dot(SxTS.T, highfreqsTS))
                    ETS = np.log10(np.dot(SxTS.T, freqsTS))

                    # compute the elliptic envelope to exploit Minimum Covariance Determinant estimates
                    #    standardizing
                    lowETS = (lowETS - lowETS.mean())/lowETS.std(ddof=0)
                    highETS = (highETS - highETS.mean())/highETS.std(ddof=0)

                    twoDimETS = np.vstack((lowETS, highETS)).T
                    logger.info('lowETS: ' + str(lowETS) + ', highETS:' + str(highETS) + 'input' + str(twoDimETS))

                    # inliers have a score of 1, outliers -1, and 0 indicates an issue with the data
                    dfe[self.output_item] = 0.0002
                    ellEnv = EllipticEnvelope(random_state=0)

                    dfe[self.output_item] = 0.0003
                    ellEnv.fit(twoDimETS)
                     
                    #ets_zscore = ellEnv.predict(twoDimETS)
                    dfe[self.output_item] = 0.0004
                    ets_zscore = ellEnv.decision_function(twoDimETS, raw_values=True).copy()

                    # compute zscore over the energy
                    #ets_zscore = np.abs((ETS - ETS.mean())/ETS.std(ddof=0))
                    logger.info('Spectral z-score max: ' + str(ets_zscore.max()))

                    # length of timesTS, ETS and ets_zscore is smaller than half the original
                    #   extend it to cover the full original length 
                    dfe[self.output_item] = 0.0005
                    linear_interpolate = sp.interpolate.interp1d(timesTS, ets_zscore, kind='linear', fill_value='extrapolate')

                    dfe[self.output_item] = 0.0006
                    zscoreI = linear_interpolate(np.arange(0, temperature.size, 1))

                    dfe[self.output_item] = zscoreI

                except Exception as e:
                    logger.error('Spectral failed with ' + str(e))

                    

                # absolute zscore > 3 ---> anomaly
                #df_copy.loc[(entity,), self.output_item] = zscoreI

                dfe_orig = pd.merge_asof(dfe_orig, dfe[self.output_item],
                         left_index = True, right_index = True, direction='nearest', tolerance = mindelta)

                if self.output_item+'_y' in dfe_orig:
                    zScoreII = dfe_orig[self.output_item+'_y'].to_numpy()
                elif self.output_item in dfe_orig:
                    zScoreII = dfe_orig[self.output_item].to_numpy()
                else:
                    print (dfe_orig.head(2))
                    zScoreII = dfe_orig[self.input_item].to_numpy()

                #print (dfe_orig.head(2))

                idx = pd.IndexSlice
                df_copy.loc[idx[entity,:], self.output_item] = zScoreII

        msg = 'SpectralAnomalyScore'
        self.trace_append(msg)
        #print(df_copy.head(30))

        return (df_copy)

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name = 'input_item',
                datatype=float,
                description = 'Column for feature extraction'
                                              ))

        inputs.append(UISingle(
                name = 'windowsize',
                datatype=int,
                description = 'Window size for spectral analysis - default 12'
                                              ))

        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name = 'output_item',
                datatype=float,
                description='Anomaly score (zScore)'
                ))
        return (inputs,outputs)


class KMeansAnomalyScore(BaseTransformer):
    '''
    Employs kmeans on windowed time series data and to compute an anomaly score from proximity to centroid's center points
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.info(input_item)
        self.input_item = input_item

        # use 24 by default - must be larger than 12
        self.windowsize = np.maximum(windowsize,1)

        # step 
        self.step = 1

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.info(str(entities))

        df_copy[self.output_item] = 0
        df_copy.sort_index()

        for entity in entities:
            # per entity
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            dfe = dfe.reset_index(level=[0]).sort_index()
            dfe_orig = dfe_orig.reset_index(level=[0]).sort_index()

            # minimal time delta for merging
            mindelta = minDelta(dfe_orig)

            logger.info('Timedelta:' + str(mindelta))

            # interpolate gaps - data imputation
            Size = dfe[[self.input_item]].fillna(0).to_numpy().size
            dfe = dfe.interpolate(method='time')


            # one dimensional time series - named temperature for catchyness
            temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            logger.info('KMeans: ' + str(entity) + ', ' + str(self.input_item) + ', ' + str(self.windowsize) + ', ' +
                         str(self.output_item) + ', ' + str(self.step) + ', ' + str(temperature.size))

            if temperature.size > self.windowsize:
                logger.info(str(temperature.size) + ',' + str(self.windowsize))

                # Chop into overlapping windows
                slices = skiutil.view_as_windows(temperature, window_shape=(self.windowsize,), step=self.step)
                print (slices.shape)

                if self.windowsize > 1:
                   n_clus = 40
                else:
                   n_clus = 20

                cblofwin = CBLOF(n_clusters=n_clus, n_jobs=-1)
                try:
                    cblofwin.fit(slices)
                except:
                    self.trace_append('KMeans failed')
                    continue
                    
                pred_score = cblofwin.decision_scores_.copy()

                # length of timesTS, ETS and ets_zscore is smaller than half the original
                #   extend it to cover the full original length 
                timesTS = np.linspace(self.windowsize//2, temperature.size - self.windowsize//2 + 1, temperature.size - self.windowsize + 1)

                print (timesTS.shape, pred_score.shape)

                #timesI = np.linspace(0, Size - 1, Size)
                linear_interpolateK = sp.interpolate.interp1d(timesTS, pred_score, kind='linear', fill_value='extrapolate')

                #kmeans_scoreI = np.interp(timesI, timesTS, pred_score)
                kmeans_scoreI = linear_interpolateK(np.arange(0, temperature.size, 1))

                dfe[self.output_item] = kmeans_scoreI

                # absolute kmeans_score > 1000 ---> anomaly
                #df_copy.loc[(entity,), self.output_item] = kmeans_scoreI
                dfe_orig = pd.merge_asof(dfe_orig, dfe[self.output_item],
                         left_index = True, right_index = True, direction='nearest', tolerance = mindelta)

                if self.output_item+'_y' in dfe_orig:
                    zScoreII = dfe_orig[self.output_item+'_y'].to_numpy()
                elif self.output_item in dfe_orig:
                    zScoreII = dfe_orig[self.output_item].to_numpy()
                else:
                    print (dfe_orig.head(2))
                    zScoreII = dfe_orig[self.input_item].to_numpy()

                #df_copy.loc[(entity,) :, self.output_item] = zScoreII
                idx = pd.IndexSlice
                df_copy.loc[idx[entity,:], self.output_item] = zScoreII


        msg = 'KMeansAnomalyScore'
        self.trace_append(msg)
        return (df_copy)

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name = 'input_item',
                datatype=float,
                description = 'Column for feature extraction'
                                              ))

        inputs.append(UISingle(
                name = 'windowsize',
                datatype=int,
                description = 'Window size for spectral analysis - default 12'
                                              ))

        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name = 'output_item',
                datatype=float,
                description='Anomaly score (kmeans)'
                ))
        return (inputs,outputs)


class SimpleAnomaly(BaseRegressor):
    '''
    Sample function uses a regression model to predict the value of one or more output
    variables. It compares the actual value to the prediction and generates an alert 
    when the difference between the actual and predicted value is outside of a threshold.
    '''
    # class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3

    def __init__(self, features, targets, threshold, predictions=None, alerts=None):
        super().__init__(features=features, targets=targets, predictions=predictions)
        if alerts is None:
            alerts = ['%s_alert' % x for x in self.targets]
        self.alerts = alerts
        self.threshold = threshold

    def execute(self, df):

        df = super().execute(df)
        for i, t in enumerate(self.targets):
            prediction = self.predictions[i]
            df['_diff_'] = (df[t] - df[prediction]).abs()
            alert = AlertHighValue(input_item='_diff_', upper_threshold=self.threshold, alert_name=self.alerts[i])
            alert.set_entity_type(self.get_entity_type())
            df = alert.execute(df)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(UISingle(name='threshold', datatype=float, description=('Threshold for firing an alert. '
                                                                              'Expressed as absolute value not percent.')))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='alerts', datatype=bool, cardinality_from='targets', is_datatype_derived=False, ))

        return (inputs, outputs)


class SimpleRegressor(BaseRegressor):
    '''
    Sample function that predicts the value of a continuous target variable using the selected list of features.
    This function is intended to demonstrate the basic workflow of training, evaluating, deploying
    using a model. 
    '''
    # class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3

    def __init__(self, features, targets, predictions=None):
        super().__init__(features=features, targets=targets, predictions=predictions)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        return (inputs, [])

