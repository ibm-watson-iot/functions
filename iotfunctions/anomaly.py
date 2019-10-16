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
from scipy import signal
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

            # interpolate gaps - data imputation
            dfe = dfe.reset_index(level=[0])
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
                timesI = np.linspace(0, Size - 1, Size)
                zscoreI = np.interp(timesI, timesTS, ets_zscore)

                # absolute zscore > 3 ---> anomaly
                df.loc[(entity,), self.output_item] = zscoreI

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


