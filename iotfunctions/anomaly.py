# *****************************************************************************
# © Copyright IBM Corp. 2018-2020.  All Rights Reserved.
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
import numpy as np
import scipy as sp

#  for Spectral Analysis
from scipy import signal, fftpack
# from scipy.stats import energy_distance
from sklearn.utils import check_X_y
from sklearn import metrics
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.covariance import MinCovDet
from sklearn import ensemble
from sklearn import linear_model

#   for KMeans
#  import skimage as ski
from skimage import util as skiutil  # for nifty windowing
from pyod.models.cblof import CBLOF

# for gradient boosting
import lightgbm

import pandas as pd
import logging
# import warnings
# import json
# from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from .base import (BaseTransformer, BaseRegressor, BaseEstimatorFunction)
from .bif import (AlertHighValue)
from .ui import (UISingle, UIMulti, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti)

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True

FrequencySplit = 0.3
DefaultWindowSize = 12
SmallEnergy = 1e-20

# KMeans_normalizer = 100 / 1.3
KMeans_normalizer = 1
Spectral_normalizer = 100 / 2.8
FFT_normalizer = 1
Saliency_normalizer = 1
Generalized_normalizer = 1 / 300


def custom_resampler(array_like):
    # initialize
    if 'gap' not in dir():
        gap = 0

    if (array_like.values.size > 0):
        gap = 0
        return 0
    else:
        gap += 1
        return gap


def min_delta(df):
    # minimal time delta for merging

    if len(df.index.names) > 1:
        df2 = df.copy()
        df2.index = df2.index.droplevel(list(range(1, df.index.nlevels)))
    else:
        df2 = df

    try:
        mindelta = df2.index.to_series().diff().min()
    except Exception as e:
        logger.debug('Min Delta error: ' + str(e))
        mindelta = pd.Timedelta('5 seconds')

    if mindelta == dt.timedelta(seconds=0) or pd.isnull(mindelta):
        mindelta = pd.Timedelta('5 seconds')

    return mindelta, df2


def set_window_size_and_overlap(windowsize, trim_value=2*DefaultWindowSize):
    # make sure it is positive and not too large
    trimmed_ws = np.minimum(np.maximum(windowsize, 1), trim_value)

    # overlap
    if trimmed_ws == 1:
        ws_overlap = 0
    else:
        ws_overlap = trimmed_ws - np.maximum(trimmed_ws // DefaultWindowSize, 1)

    return trimmed_ws, ws_overlap


def dampen_anomaly_score(array, dampening):

    if dampening is None:
        dampening = 0.9  # gradient dampening

    if dampening >= 1:
        return array

    if dampening < 0.01:
        return array

    # ToDo error testing for arrays of size <= 1
    if array.size <= 1:
        return array

    gradient = np.gradient(array)

    # dampened
    grad_damp = np.float_power(abs(gradient), dampening) * np.sign(gradient)

    # reconstruct (dampened) anomaly score by discrete integration
    integral = []
    x = array[0]
    for x_el in np.nditer(grad_damp):
        x = x + x_el
        integral.append(x)

    # shift array slightly to the right to position anomaly score
    array_damp = np.roll(np.asarray(integral), 1)
    array_damp[0] = array_damp[1]

    # normalize
    return array_damp / dampening / 2


# Saliency helper functions
# copied from https://github.com/y-bar/ml-based-anomaly-detection
#   remove the boring part from an image resp. time series
def series_filter(values, kernel_size=3):
    """
    Filter a time series. Practically, calculated mean value inside kernel size.
    As math formula, see https://docs.opencv.org/2.4/modules/imgproc/doc/filtering.html.
    :param values:
    :param kernel_size:
    :return: The list of filtered average
    """
    filter_values = np.cumsum(values, dtype=float)

    filter_values[kernel_size:] = filter_values[kernel_size:] - filter_values[:-kernel_size]
    filter_values[kernel_size:] = filter_values[kernel_size:] / kernel_size

    for i in range(1, kernel_size):
        filter_values[i] /= i + 1

    return filter_values


# Saliency class
#  see https://www.inf.uni-hamburg.de/en/inst/ab/cv/research/research1-visual-attention.html
class Saliency(object):
    def __init__(self, amp_window_size, series_window_size, score_window_size):
        self.amp_window_size = amp_window_size
        self.series_window_size = series_window_size
        self.score_window_size = score_window_size

    def transform_saliency_map(self, values):
        """
        Transform a time-series into spectral residual, which is method in computer vision.
        For example, See https://docs.opencv.org/master/d8/d65/group__saliency.html
        :param values: a list or numpy array of float values.
        :return: silency map and spectral residual
        """

        freq = np.fft.fft(values)
        mag = np.sqrt(freq.real ** 2 + freq.imag ** 2)

        # remove the boring part of a timeseries
        spectral_residual = np.exp(np.log(mag) - series_filter(np.log(mag), self.amp_window_size))

        freq.real = freq.real * spectral_residual / mag
        freq.imag = freq.imag * spectral_residual / mag

        # and apply inverse fourier transform
        saliency_map = np.fft.ifft(freq)
        return saliency_map

    def transform_spectral_residual(self, values):
        saliency_map = self.transform_saliency_map(values)
        spectral_residual = np.sqrt(saliency_map.real ** 2 + saliency_map.imag ** 2)
        return spectral_residual


def merge_score(dfEntity, dfEntityOrig, column_name, score, mindelta):
    '''
    Fit interpolated score to original entity slice of the full dataframe
    '''

    # equip score with time values, make sure it's positive
    score[score < 0] = 0
    dfEntity[column_name] = score

    # merge
    dfEntityOrig = pd.merge_asof(dfEntityOrig, dfEntity[column_name],
                                 left_index=True, right_index=True, direction='nearest', tolerance=mindelta)

    if column_name + '_y' in dfEntityOrig:
        merged_score = dfEntityOrig[column_name + '_y'].to_numpy()
    else:
        merged_score = dfEntityOrig[column_name].to_numpy()

    return merged_score


class SpectralAnomalyScore(BaseTransformer):
    '''
    An unsupervised anomaly detection function.
     Applies a spectral analysis clustering techniqueto extract features from time series data and to create z scores.
     Moves a sliding window across the data signal and applies the anomalymodelto each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.debug(input_item)
        self.input_item = input_item

        # use 12 by default
        self.windowsize, self.windowoverlap = set_window_size_and_overlap(windowsize)

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

        self.inv_zscore = None

        self.whoami = 'Spectral'

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index().set_index(index_names[0])
        else:
            index_names = None
            dfe = dfEntity

        # interpolate gaps - data imputation
        try:
            dfe = dfe.interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

        return dfe, temperature

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df.index.levels[0])
        logger.debug(str(entities))

        df_copy[self.output_item] = 0
        # df_copy.sort_index()   # NoOp

        for entity in entities:
            # per entity - copy for later inplace operations
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            # do it inplace as we copied the data before
            dfe.reset_index(level=[0], inplace=True)
            dfe.sort_index(inplace=True)
            dfe_orig.reset_index(level=[0], inplace=True)
            dfe_orig.sort_index(inplace=True)

            # minimal time delta for merging
            mindelta, dfe_orig = min_delta(dfe_orig)

            logger.debug('Timedelta:' + str(mindelta) + ' Index: ' + str(dfe_orig.index))

            # one dimensional time series - named temperature for catchyness
            temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

            # interpolate gaps - data imputation by default
            #   for missing data detection we look at the timestamp gradient instead
            dfe, temperature = self.prepare_data(dfe)

            logger.debug('Module Spectral, Entity: ' + str(entity) + ', Input: ' + str(self.input_item) +
                         ', Windowsize: ' + str(self.windowsize) + ', Output: ' + str(self.output_item) +
                         ', Overlap: ' + str(self.windowoverlap) + ', Inputsize: ' + str(temperature.size))

            if temperature.size <= self.windowsize:
                logger.debug(str(temperature.size) + ' <= ' + str(self.windowsize))
                dfe[self.output_item] = 0.0001
            else:
                logger.debug(str(temperature.size) + str(self.windowsize))

                dfe[self.output_item] = 0.00001
                if self.inv_zscore is not None:
                    dfe[self.inv_zscore] = 0.00001

                zScoreII = None
                inv_zScoreII = None
                try:
                    # Fourier transform:
                    #   frequency, time, spectral density
                    frequency_temperature, time_series_temperature, spectral_density_temperature = signal.spectrogram(
                        temperature, fs=self.frame_rate, window='hanning',
                        nperseg=self.windowsize, noverlap=self.windowoverlap,
                        detrend='l', scaling='spectrum')

                    # cut off freqencies too low to fit into the window
                    frequency_temperatureb = (frequency_temperature > 2/self.windowsize).astype(int)
                    frequency_temperature = frequency_temperature * frequency_temperatureb
                    frequency_temperature[frequency_temperature == 0] = 1 / self.windowsize

                    signal_energy = np.dot(spectral_density_temperature.T, frequency_temperature)

                    # np.linalg.norm(spectral_density_temperature, axis=0)

                    signal_energy[signal_energy < SmallEnergy] = SmallEnergy
                    inv_signal_energy = np.divide(np.ones(signal_energy.size), signal_energy)

                    dfe[self.output_item] = 0.0002

                    ets_zscore = abs(sp.stats.zscore(signal_energy)) * Spectral_normalizer
                    inv_zscore = abs(sp.stats.zscore(inv_signal_energy))

                    logger.debug('Spectral z-score max: ' + str(ets_zscore.max()) +
                                 ',   Spectral inv z-score max: ' + str(inv_zscore.max()))

                    # length of time_series_temperature, signal_energy and ets_zscore is smaller than half the original
                    #   extend it to cover the full original length
                    dfe[self.output_item] = 0.0005
                    linear_interpolate = sp.interpolate.interp1d(
                        time_series_temperature, ets_zscore, kind='linear', fill_value='extrapolate')

                    zScoreII = merge_score(dfe, dfe_orig, self.output_item,
                                           abs(linear_interpolate(np.arange(0, temperature.size, 1))), mindelta)

                    if self.inv_zscore is not None:
                        linear_interpol_inv_zscore = sp.interpolate.interp1d(
                            time_series_temperature, inv_zscore, kind='linear', fill_value='extrapolate')

                        inv_zScoreII = merge_score(dfe, dfe_orig, self.inv_zscore,
                                                   abs(linear_interpol_inv_zscore(np.arange(0, temperature.size, 1))), mindelta)

                except Exception as e:
                    logger.error('Spectral failed with ' + str(e))

                idx = pd.IndexSlice
                df_copy.loc[idx[entity, :], self.output_item] = zScoreII

                if self.inv_zscore is not None:
                    df_copy.loc[idx[entity, :], self.inv_zscore] = inv_zScoreII

        if self.inv_zscore is not None:
            msg = 'SpectralAnomalyScoreExt'
        else:
            msg = 'SpectralAnomalyScore'
        self.trace_append(msg)

        return (df_copy)

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Data item to analyze'
                                              ))

        inputs.append(UISingle(
                name='windowsize',
                datatype=int,
                description='Size of each sliding window in data points. Typically set to 12.'
                                              ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Spectral anomaly score (z-score)'
                ))
        return (inputs, outputs)


class SpectralAnomalyScoreExt(SpectralAnomalyScore):
    '''
    An unsupervised anomaly detection function.
     Applies a spectral analysis clustering techniqueto extract features from time series data and to create z scores.
     Moves a sliding window across the data signal and applies the anomalymodelto each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    '''
    def __init__(self, input_item, windowsize, output_item, inv_zscore):
        super().__init__(input_item, windowsize, output_item)
        logger.debug(input_item)

        self.inv_zscore = inv_zscore

    def execute(self, df):

        return super().execute(df)

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Data item to analyze'
                                              ))

        inputs.append(UISingle(
                name='windowsize',
                datatype=int,
                description='Size of each sliding window in data points. Typically set to 12.'
                                              ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Spectral anomaly score (z-score)'
                ))
        outputs.append(UIFunctionOutSingle(
                name='inv_zscore',
                datatype=float,
                description='z-score of inverted signal energy - detects unusually low activity'
                ))
        return (inputs, outputs)


class KMeansAnomalyScore(BaseTransformer):
    '''
    An unsupervised anomaly detection function.
     Applies a k-means analysis clustering technique to time series data.
     Moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
     Try several anomaly models on your data and use the one that fits your databest.
    '''
    def __init__(self, entity_list, input_item, windowsize, output_item):

        #adding entity filters
        if entity_list:
            kwargs = {'_entity_filter_list': entity_list}
            self._metadata_params = kwargs

        super().__init__()
        logger.debug(input_item)
        self.input_item = input_item

        # use 12 by default
        self.windowsize, _ = set_window_size_and_overlap(windowsize)

        # step
        self.step = 1

        # assume 1 per sec for now
        self.frame_rate = 1

        self.output_item = output_item

        self.whoami = 'KMeans'

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index().set_index(index_names[0])
        else:
            index_names = None
            dfe = dfEntity

        # interpolate gaps - data imputation
        try:
            dfe = dfe.interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

        return dfe, temperature

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        df_copy[self.output_item] = 0
        # df_copy.sort_index() - NoOP

        for entity in entities:
            # per entity - copy for later inplace operations
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            # do it inplace as we copied the data before
            dfe.reset_index(level=[0], inplace=True)
            dfe.sort_index(inplace=True)
            dfe_orig.reset_index(level=[0], inplace=True)
            dfe_orig.sort_index(inplace=True)

            # minimal time delta for merging
            mindelta, dfe_orig = min_delta(dfe_orig)

            logger.debug('Timedelta:' + str(mindelta))

            # interpolate gaps - data imputation by default
            #   for missing data detection we look at the timestamp gradient instead
            dfe, temperature = self.prepare_data(dfe)

            logger.debug('Module KMeans, Entity: ' + str(entity) + ', Input: ' + str(self.input_item) +
                         ', Windowsize: ' + str(self.windowsize) + ', Output: ' + str(self.output_item) +
                         ', Overlap: ' + str(self.step) + ', Inputsize: ' + str(temperature.size))

            if temperature.size > self.windowsize:
                logger.debug(str(temperature.size) + ',' + str(self.windowsize))

                # Chop into overlapping windows
                slices = skiutil.view_as_windows(temperature, window_shape=(self.windowsize,), step=self.step)

                if self.windowsize > 1:
                    n_cluster = 40
                else:
                    n_cluster = 20

                n_cluster = np.minimum(n_cluster, slices.shape[0] // 2)

                logger.debug('KMeans parms, Clusters: ' + str(n_cluster) + ', Slices: ' + str(slices.shape))

                cblofwin = CBLOF(n_clusters=n_cluster, n_jobs=-1)
                try:
                    cblofwin.fit(slices)
                except Exception as e:
                    logger.info('KMeans failed with ' + str(e))
                    self.trace_append('KMeans failed with' + str(e))
                    continue

                pred_score = cblofwin.decision_scores_.copy() * KMeans_normalizer

                # length of time_series_temperature, signal_energy and ets_zscore is smaller than half the original
                #   extend it to cover the full original length
                time_series_temperature = np.linspace(
                     self.windowsize//2, temperature.size - self.windowsize//2 + 1,
                     temperature.size - self.windowsize + 1)

                linear_interpolateK = sp.interpolate.interp1d(
                    time_series_temperature, pred_score, kind='linear', fill_value='extrapolate')

                zScoreII = merge_score(dfe, dfe_orig, self.output_item,
                                       linear_interpolateK(np.arange(0, temperature.size, 1)), mindelta)

                idx = pd.IndexSlice
                df_copy.loc[idx[entity, :], self.output_item] = zScoreII

        msg = 'KMeansAnomalyScore'
        self.trace_append(msg)
        return (df_copy)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(
                name='entity_list',
                datatype=str,
                description='comma separated list of entity ids'
                                              ))
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Data item to analyze'
                                              ))
        inputs.append(UISingle(
                name='windowsize',
                datatype=int,
                description='Size of each sliding window in data points. Typically set to 12.'
                                              ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Anomaly score (kmeans)'
                ))
        return (inputs, outputs)


class GeneralizedAnomalyScore(BaseTransformer):
    """
    An unsupervised anomaly detection function.
     Applies the Minimum Covariance Determinant (FastMCD) technique to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """
    def __init__(self, input_item, windowsize, output_item):
        super().__init__()
        logger.debug(input_item)

        self.whoami = 'GAM'

        self.input_item = input_item

        # use 12 by default
        self.windowsize, self.windowoverlap = set_window_size_and_overlap(windowsize)

        # step
        self.step = 1

        # assume 1 per sec for now
        self.frame_rate = 1

        self.dampening = 1  # dampening - dampen anomaly score

        self.output_item = output_item

        self.normalizer = Generalized_normalizer

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index().set_index(index_names[0])
        else:
            index_names = None
            dfe = dfEntity

        # interpolate gaps - data imputation
        try:
            dfe = dfe.interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        temperature = dfe[[self.input_item]].fillna(0).to_numpy().reshape(-1,)

        return dfe, temperature

    def feature_extract(self, temperature):

        logger.debug(self.whoami + ': feature extract')

        slices = skiutil.view_as_windows(
            temperature, window_shape=(self.windowsize,), step=self.step
        )
        return slices

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        df_copy[self.output_item] = 0
        # df_copy.sort_index()

        for entity in entities:
            # per entity - copy for later inplace operations
            dfe = df_copy.loc[[entity]].dropna(how='all')
            dfe_orig = df_copy.loc[[entity]].copy()

            # get rid of entityid part of the index
            # do it inplace as we copied the data before
            dfe.reset_index(level=[0], inplace=True)
            dfe.sort_index(inplace=True)
            dfe_orig.reset_index(level=[0], inplace=True)
            dfe_orig.sort_index(inplace=True)

            # minimal time delta for merging
            mindelta, dfe_orig = min_delta(dfe_orig)

            # interpolate gaps - data imputation by default
            #   for missing data detection we look at the timestamp gradient instead
            dfe, temperature = self.prepare_data(dfe)

            logger.debug('Module GeneralizedAnomaly, Entity: ' + str(entity) + ', Input: ' + str(self.input_item) +
                         ', Windowsize: ' + str(self.windowsize) + ', Output: ' + str(self.output_item) +
                         ', Overlap: ' + str(self.step) + ', Inputsize: ' + str(temperature.size))

            if temperature.size > self.windowsize:
                logger.debug(str(temperature.size) + "," + str(self.windowsize))

                # NN = GeneralizedAnomalyModel( base_learner=MinCovDet(), fit_function="fit",
                #        predict_function="mahalanobis", score_sign=1,)
                temperature -= np.mean(temperature, axis=0)
                mcd = MinCovDet()

                # Chop into overlapping windows (default) or run through FFT first
                slices = self.feature_extract(temperature)

                pred_score = None

                try:
                    # NN.fit(slices)
                    mcd.fit(slices)

                    # pred_score = NN.decision_function(slices).copy()
                    pred_score = mcd.mahalanobis(slices).copy() * self.normalizer

                except ValueError as ve:

                    logger.info(
                        self.whoami + " GeneralizedAnomalyScore: Entity: "
                        + str(entity) + ", Input: " + str(self.input_item) + ", WindowSize: "
                        + str(self.windowsize) + ", Output: " + str(self.output_item) + ", Step: "
                        + str(self.step) + ", InputSize: " + str(slices.shape)
                        + " failed in the fitting step with \"" + str(ve) + "\" - scoring zero")

                    pred_score = np.zeros(slices.shape[0])
                    pass

                except Exception as e:

                    dfe[self.output_item] = 0
                    logger.error(
                        self.whoami + " GeneralizedAnomalyScore: Entity: "
                        + str(entity) + ", Input: " + str(self.input_item) + ", WindowSize: "
                        + str(self.windowsize) + ", Output: " + str(self.output_item) + ", Step: "
                        + str(self.step) + ", InputSize: " + str(slices.shape)
                        + " failed in the fitting step with " + str(e))
                    continue

                # will break if pred_score is None
                # length of timesTS, ETS and ets_zscore is smaller than half the original
                #   extend it to cover the full original length
                timesTS = np.linspace(
                    self.windowsize // 2,
                    temperature.size - self.windowsize // 2 + 1,
                    temperature.size - self.windowsize + 1,
                )

                logger.debug(self.whoami + '   Entity: ' + str(entity) + ', result shape: ' + str(timesTS.shape) +
                             ' score shape: ' + str(pred_score.shape))

                # timesI = np.linspace(0, Size - 1, Size)
                linear_interpolateK = sp.interpolate.interp1d(
                    timesTS, pred_score, kind="linear", fill_value="extrapolate"
                )

                # kmeans_scoreI = np.interp(timesI, timesTS, pred_score)
                gam_scoreI = linear_interpolateK(np.arange(0, temperature.size, 1))

                dampen_anomaly_score(gam_scoreI, self.dampening)

                zScoreII = merge_score(dfe, dfe_orig, self.output_item, gam_scoreI, mindelta)

                idx = pd.IndexSlice
                df_copy.loc[idx[entity, :], self.output_item] = zScoreII

        msg = "GeneralizedAnomalyScore"
        self.trace_append(msg)
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingleItem(
                name="input_item",
                datatype=float,
                description="Data item to analyze",
            )
        )

        inputs.append(
            UISingle(
                name="windowsize",
                datatype=int,
                description="Size of each sliding window in data points. Typically set to 12."
            )
        )

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                name="output_item",
                datatype=float,
                description="Anomaly score (GeneralizedAnomaly)",
            )
        )
        return (inputs, outputs)


class NoDataAnomalyScore(GeneralizedAnomalyScore):
    '''
    An unsupervised anomaly detection function.
     Uses FastMCD to find gaps in data.
     The function moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
    '''
    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'NoData'
        self.normalizer = 1

        logger.debug('NoData')

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index().set_index(index_names[0])
        else:
            index_names = None
            dfe = dfEntity

        # count the timedelta in seconds between two events
        timeSeq = (dfEntity.index.values - dfEntity.index[0].to_datetime64()) / np.timedelta64(1, 's')

        dfe = dfEntity.copy()

        # one dimensional time series - named temperature for catchyness
        #   we look at the gradient of the time series timestamps for anomaly detection
        #   might throw an exception - we catch it in the super class !!
        try:
            temperature = np.gradient(timeSeq)
            dfe[[self.input_item]] = temperature
        except Exception as pe:
            logger.info("NoData Gradient failed with " + str(pe))
            dfe[[self.input_item]] = 0
            temperature = dfe[[self.input_item]].values
            temperature[0] = 10**10
            pass

        return dfe, temperature

    def execute(self, df):
        df_copy = super().execute(df)

        msg = "NoDataAnomalyScore"
        self.trace_append(msg)
        return df_copy

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Data item to analyze'
                                              ))

        inputs.append(UISingle(
                name='windowsize',
                datatype=int,
                description='Size of each sliding window in data points. Typically set to 12.'
                                              ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='No data anomaly score'
                ))
        return (inputs, outputs)


class FFTbasedGeneralizedAnomalyScore(GeneralizedAnomalyScore):
    """
    An unsupervised and robust anomaly detection function.
     Extracts temporal features from time series data using Fast Fourier Transforms.
     Applies the GeneralizedAnomalyScore to the features to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly models to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that best fits your data.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'FFT'
        self.normalizer = FFT_normalizer

        logger.debug('FFT')

    def feature_extract(self, temperature):

        logger.debug(self.whoami + ': feature extract')

        slices_ = skiutil.view_as_windows(
            temperature, window_shape=(self.windowsize,), step=self.step
        )
        slicelist = []
        for slice in slices_:
            slicelist.append(fftpack.rfft(slice))

        # return np.array(slicelist)
        return np.stack(slicelist, axis=0)

    def execute(self, df):
        df_copy = super().execute(df)

        msg = "FFTbasedGeneralizedAnomalyScore"
        self.trace_append(msg)
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingleItem(
                name="input_item",
                datatype=float,
                description="Data item to analyze",
            )
        )

        inputs.append(
            UISingle(
                name="windowsize",
                datatype=int,
                description="Size of each sliding window in data points. Typically set to 12."
            )
        )

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                name="output_item",
                datatype=float,
                description="Anomaly score (FFTbasedGeneralizedAnomalyScore)",
            )
        )
        return (inputs, outputs)


class FFTbasedGeneralizedAnomalyScore2(GeneralizedAnomalyScore):
    """
    An unsupervised and robust anomaly detection function.
     Extracts temporal features from time series data using Fast Fourier Transforms.
     Applies the GeneralizedAnomalyScore to the features to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly models to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that best fits your data.
    """

    def __init__(self, input_item, windowsize, dampening, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'FFT dampen'
        self.dampening = dampening
        self.normalizer = FFT_normalizer / dampening

        logger.debug('FFT')

    def feature_extract(self, temperature):

        logger.debug(self.whoami + ': feature extract')

        slices_ = skiutil.view_as_windows(
            temperature, window_shape=(self.windowsize,), step=self.step
        )
        slicelist = []
        for slice in slices_:
            slicelist.append(fftpack.rfft(slice))

        # return np.array(slicelist)
        return np.stack(slicelist, axis=0)

    def execute(self, df):
        df_copy = super().execute(df)

        msg = "FFTbasedGeneralizedAnomalyScore"
        self.trace_append(msg)
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingleItem(
                name="input_item",
                datatype=float,
                description="Data item to analyze",
            )
        )

        inputs.append(
            UISingle(
                name="windowsize",
                datatype=int,
                description="Size of each sliding window in data points. Typically set to 12."
            )
        )

        inputs.append(
            UISingle(
                name="dampening",
                datatype=float,
                description="Moderate the anomaly score. Use a value <=1. Typically set to 1."
            )
        )

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                name="output_item",
                datatype=float,
                description="Anomaly score (FFTbasedGeneralizedAnomalyScore)",
            )
        )
        return (inputs, outputs)


class SaliencybasedGeneralizedAnomalyScore(GeneralizedAnomalyScore):
    """
    An unsupervised anomaly detection function.
     Based on salient region detection models,
         it uses fast fourier transform to reconstruct a signal using the salient features of a the signal.
     It applies GeneralizedAnomalyScore to the reconstructed signal.
     The function moves a sliding window across the data signal and applies its analysis to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'Saliency'
        self.saliency = Saliency(windowsize, 0, 0)
        self.normalizer = Saliency_normalizer

        logger.debug('Saliency')

    def feature_extract(self, temperature):

        logger.debug(self.whoami + ': feature extract')

        temperature_saliency = self.saliency.transform_spectral_residual(temperature)

        slices = skiutil.view_as_windows(
            temperature_saliency, window_shape=(self.windowsize,), step=self.step
        )
        return slices

    def execute(self, df):
        df_copy = super().execute(df)

        msg = "SaliencybasedGeneralizedAnomalyScore"
        self.trace_append(msg)
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingleItem(
                name="input_item",
                datatype=float,
                description="Data item to analyze"
            )
        )

        inputs.append(
            UISingle(
                name="windowsize",
                datatype=int,
                description="Size of each sliding window in data points. Typically set to 12.",
            )
        )

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                name="output_item",
                datatype=float,
                description="Anomaly score (SaliencybasedGeneralizedAnomalyScore)",
            )
        )
        return (inputs, outputs)

#######################################################################################


class GBMRegressor(BaseEstimatorFunction):

    '''
    Regressor based on gradient boosting method as provided by lightGBM
    '''
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    estimators_per_execution = 1
    num_rounds_per_estimator = 1

    def GBMPipeline(self):
        steps = [('scaler', StandardScaler()), ('gbm', lightgbm.LGBMRegressor())]
        return Pipeline(steps)

    def set_estimators(self):
        # gradient_boosted
        # self.estimators['light_gradient_boosted_regressor'] = (lightgbm.LGBMRegressor, self.params)
        self.estimators['light_gradient_boosted_regressor'] = (self.GBMPipeline, self.params)
        logger.info('GBMRegressor start searching for best model')

    def __init__(self, features, targets, predictions=None,
                 n_estimators=None, num_leaves=None, learning_rate=None, max_depth=None):
        super().__init__(features=features, targets=targets, predictions=predictions)
        self.experiments_per_execution = 1
        self.correlation_threshold = 0
        self.auto_train = True

        # if n_estimators is not None or num_leaves is not None or learning_rate is not None or max_depth is not None:
        if n_estimators is not None or num_leaves is not None or learning_rate is not None:
            self.params = {'gbm__n_estimators': [n_estimators],
                           'gbm__num_leaves': [num_leaves],
                           'gbm__learning_rate': [learning_rate],
                           'gbm__max_depth': [max_depth],
                           'gbm__verbosity': [2]}
        else:
            self.params = {'gbm__n_estimators': [500],
                           'gbm__num_leaves': [50],
                           'gbm__learning_rate': [0.001],
                           'gbm__verbosity': [2]}

        self.stop_auto_improve_at = -2

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            # per entity - copy for later inplace operations
            # dfe = df_copy.loc[[entity]].dropna(how='all')
            # dfe = df_copy.loc[[entity]].copy()
            # try:
                dfe = super()._execute(df_copy.loc[[entity]], entity)
                print(df_copy.columns)
                # for c in self.predictions:
                df_copy.loc[entity, self.predictions] = dfe[self.predictions]
                # df_copy = df_copy.loc[[entity]] = dfe
                print(df_copy.columns)
            # except Exception as e:
                # logger.info('GBMRegressor for entity ' + str(entity) + ' failed with: ' + str(e))
                # continue
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(UISingle(name='n_estimators', datatype=int, required=False,
                               description=('Max rounds of boosting')))
        inputs.append(UISingle(name='num_leaves', datatype=int, required=False,
                               description=('Max leaves in a boosting tree')))
        inputs.append(UISingle(name='learning_rate', datatype=float, required=False,
                               description=('Learning rate')))
        inputs.append(UISingle(name='max_depth', datatype=int, required=False,
                               description=('Cut tree to prevent overfitting')))
        # define arguments that behave as function outputs
        outputs = []
        return (inputs, outputs)

    # @classmethod
    # def get_input_items(cls):
    #     return {'features', 'targets'}


class SimpleRegressor(BaseEstimatorFunction):

    '''
    Regressor based on stochastic gradient descent and gradient boosting method as provided by sklearn
    '''
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    estimators_per_execution = 3
    num_rounds_per_estimator = 3

    def GBRPipeline(self):
        steps = [('scaler', StandardScaler()), ('gbr', ensemble.GradientBoostingRegressor)]
        # steps = [('scaler', StandardScaler()), ('gbm', lightgbm.LGBMRegressor())]
        return Pipeline(steps)

    def SGDPipeline(self):
        steps = [('scaler', StandardScaler()), ('sgd', linear_model.SGDRegressor)]
        return Pipeline(steps)

    def set_estimators(self):
        # gradient_boosted
        # params = {'gbr__n_estimators': [100, 250, 500, 1000],
        #           'gbr__max_depth': [2, 4, 10],
        #           'gbr__min_samples_split': [2, 5, 9],
        #           'gbr__learning_rate': [0.01, 0.02, 0.05],
        #           'gbr__loss': ['ls']}
        params = {'n_estimators': [100, 250, 500, 1000],
                  'max_depth': [2, 4, 10],
                  'min_samples_split': [2, 5, 9],
                  'learning_rate': [0.01, 0.02, 0.05],
                  'loss': ['ls']}
        self.estimators['gradient_boosted_regressor'] = (ensemble.GradientBoostingRegressor, params)
        # self.estimators['gradient_boosted_regressor'] = (self.GBRPipeline, params)

        # sgd
        # params = {'sgd__max_iter': [250, 1000, 5000, 10000],
        #           'sgd__tol': [0.001, 0.002, 0.005]}
        # params = {'sgd__max_iter': [250, 1000, 5000, 10000],
        #           'sgd__tol': [0.001, 0.002, 0.005]}
        # self.estimators['sgd_regressor'] = (linear_model.SGDRegressor, params)
        # self.estimators['sgd_regressor'] = (self.SGDPipeline, params)
        logger.info('SimpleRegressor start searching for best model')

    def __init__(self, features, targets, predictions=None,
                 n_estimators=None, num_leaves=None, learning_rate=None, max_depth=None):
        super().__init__(features=features, targets=targets, predictions=predictions)

        self.experiments_per_execution = 1
        self.auto_train = True
        self.correlation_threshold = 0

        # self.stop_auto_improve_at = -2

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            try:
                check_X_y(df_copy.loc[[entity]][self.features].values)
                dfe = super()._execute(df_copy.loc[[entity]], entity)
                print(df_copy.columns)
                # for c in self.predictions:
                df_copy.loc[entity, self.predictions] = dfe[self.predictions]
                # df_copy = df_copy.loc[[entity]] = dfe
                print(df_copy.columns)
            except Exception as e:
                logger.info('GBMRegressor for entity ' + str(entity) + ' failed with: ' + str(e))
                df_copy.loc[entity, self.predictions] = 0
                pass
        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        return (inputs, [])

        # define arguments that behave as function outputs
        outputs = []
        return (inputs, outputs)

    # @classmethod
    # def get_input_items(cls):
    #     return ['features', 'targets']


class SimpleAnomaly(BaseRegressor):
    '''
    A supervised anomaly detection function.
     Uses a regression model to predict the value of target data items based on dependent data items or features.
     Then, it compares the actual value to the predicted valueand generates an alert when the difference falls outside of a threshold.
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

        try:
            df_new = super().execute(df)
            df = df_new
            for i, t in enumerate(self.targets):
                prediction = self.predictions[i]
                df['_diff_'] = (df[t] - df[prediction]).abs()
                alert = AlertHighValue(input_item='_diff_', upper_threshold=self.threshold, alert_name=self.alerts[i])
                alert.set_entity_type(self.get_entity_type())
                df = alert.execute(df)
        except Exception as e:
            logger.info('Simple Anomaly failed with: ' + str(e))
            pass

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(UISingle(name='threshold', datatype=float,
                               description=('Threshold for firing an alert. Expressed as absolute value not percent.')))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='alerts', datatype=bool, cardinality_from='targets', is_datatype_derived=False, ))

        return (inputs, outputs)
