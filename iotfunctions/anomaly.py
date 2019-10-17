# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
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
                   BaseDataSource, BaseDBActivityMerge, BaseSimpleAggregator)

from .ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti, UIExpression,
                 UIText, UIStatusFlag, UIParameters)

from .util import adjust_probabilities, reset_df_index

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True


class SpectralAnomalyScore(BaseTransformer):
    '''
    Employs spectral analysis to extract features from the time series data and to compute zscore from it
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.debug(input_item)
        self.input_item = input_item

        # use 24 by default - must be larger than 12
        self.windowsize = windowsize

        # overlap 
        self.windowoverlap = self.windowsize - self.windowsize // 12

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

    def execute(self, df):

        entities = np.unique(df.index.levels[0])
        logger.debug(str(entities))

        df[self.output_item] = 0

        for entity in entities:
            # per entity
            dfe = df.loc[[entity]].dropna(how='all')
            dfe_orig = df.loc[[entity]].copy()

            # get rid of entityid part of the index
            dfe = dfe.reset_index(level=[0])
            dfe_orig = dfe_orig.reset_index(level=[0])

            # minimal time delta for merging
            mindelta = dfe_orig.index.to_series().diff().min()
            if mindelta == 0 or pd.isnull(mindelta):
                mindelta = pd.Timedelta.min

            # interpolate gaps - data imputation
            Size = dfe[[self.input_item]].fillna(0).to_numpy().size
            dfe = dfe.interpolate(method='time')

            # one dimensional time series - named temperature for catchyness
            temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            logger.debug(str(entity) + str(self.input_item) + str(self.windowsize) +
                         str(self.output_item) + str(self.windowoverlap) + str(temperature.size))

            if temperature.size > self.windowsize:
                logger.debug(str(temperature.size) + str(self.windowsize))
                # Fourier transform:
                #   frequency, time, spectral density
                freqsTS, timesTS, SxTS = signal.spectrogram(temperature, fs = self.frame_rate, window = 'hanning',
                                                        nperseg = self.windowsize, noverlap = self.windowoverlap,
                                                        detrend = False, scaling='spectrum')

                # cut off freqencies too low to fit into the window
                freqsTSb = (freqsTS > 2/self.windowsize).astype(int)
                freqsTS = freqsTS * freqsTSb
                freqsTS[freqsTS == 0] = 1 / self.windowsize

                # Compute energy = frequency * spectral density over time in decibel
                ETS = np.log10(np.dot(SxTS.T, freqsTS))

                # compute zscore over the energy
                ets_zscore = (ETS - ETS.mean())/ETS.std(ddof=0)

                # length of timesTS, ETS and ets_zscore is smaller than half the original
                #   extend it to cover the full original length 
                Linear = sp.interpolate.interp1d(timesTS, ets_zscore, kind='linear', fill_value='extrapolate')
                zscoreI = Linear(np.arange(0, temperature.size, 1))

                dfe[self.output_item] = zscoreI

                # absolute zscore > 3 ---> anomaly
                #df.loc[(entity,), self.output_item] = zscoreI

                dfe_orig = pd.merge_asof(dfe_orig, dfe[self.output_item],
                         left_index = True, right_index = True, direction='nearest', tolerance = mindelta)

                zScoreII = dfe_orig[self.output_item+'_y'].to_numpy()

                df.loc[(entity,) :, self.output_item] = zScoreII

        msg = 'SpectralAnomalyScore'
        self.trace_append(msg)
        return (df)

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
        logger.debug(input_item)
        self.input_item = input_item

        # use 24 by default - must be larger than 12
        self.windowsize = windowsize

        # overlap 
        self.windowoverlap = self.windowsize - self.windowsize // 12

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

    def execute(self, df):

        entities = np.unique(df.index.levels[0])
        logger.debug(str(entities))

        df[self.output_item] = 0

        for entity in entities:
            # per entity
            dfe = df.loc[[entity]].dropna(how='all')
            dfe_orig = df.loc[[entity]].copy()

            # get rid of entityid part of the index
            dfe = dfe.reset_index(level=[0])
            dfe_orig = dfe_orig.reset_index(level=[0])

            # interpolate gaps - data imputation
            Size = dfe[[self.input_item]].fillna(0).to_numpy().size
            dfe = dfe.interpolate(method='time')


            # one dimensional time series - named temperature for catchyness
            temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            logger.debug(str(entity) + str(self.input_item) + str(self.windowsize) +
                         str(self.output_item) + str(self.windowoverlap) + str(temperature.size))

            if temperature.size > self.windowsize:
                logger.debug(str(temperature.size) + str(self.windowsize))

                # Chop into overlapping windows
                slices = skiutil.view_as_windows(temperature, window_shape=(self.windowsize,), step=self.windowoverlap)

                if self.windowsize > 1:
                   n_clus = 40
                else:
                   n_clus = 20

                cblofwin = CBLOF(n_clusters=n_clus, n_jobs=-1)
                cblofwin.fit(slices)

                pred_score = cblofwin.decision_scores_.copy()

                # length of timesTS, ETS and ets_zscore is smaller than half the original
                #   extend it to cover the full original length 
                timesTS = np.linspace(self.windowsize//2, temperature.size - self.windowsize//2 + 1, temperature.size - self.windowsize + 1)
                #timesI = np.linspace(0, Size - 1, Size)
                Linear = sp.interpolate.interp1d(timesTS, pred_score, kind='linear', fill_value='extrapolate')

                #kmeans_scoreI = np.interp(timesI, timesTS, pred_score)
                kmeans_scoreI = Linear(np.arange(0, temperature.size, 1))

                dfe[self.output_item] = kmeans_scoreI

                # absolute kmeans_score > 1000 ---> anomaly
                #df.loc[(entity,), self.output_item] = kmeans_scoreI
                dfe_orig = pd.merge_asof(dfe_orig, dfe[self.output_item],
                         left_index = True, right_index = True, direction='nearest', tolerance = mindelta)

                zScoreII = dfe_orig[self.output_item+'_y'].to_numpy()

                df.loc[(entity,) :, self.output_item] = zScoreII


        msg = 'KMeansAnomalyScore'
        self.trace_append(msg)
        return (df)

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


