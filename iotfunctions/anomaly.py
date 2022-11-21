# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

"""
The Built In Functions module contains preinstalled functions
"""

import itertools as it
import datetime as dt
import importlib
import logging
import time
import hashlib # encode feature names

import numpy as np
import pandas as pd
import scipy as sp
from pyod.models.cblof import CBLOF

import numpy as np
import pandas as pd
import scipy as sp
from pyod.models.cblof import CBLOF
import ruptures as rpt

# for Spectral Analysis
from scipy import signal, fftpack

import skimage as ski
from skimage import util as skiutil # for nifty windowing

# for KMeans
from sklearn import ensemble
from sklearn import linear_model
from sklearn import metrics
from sklearn.covariance import MinCovDet
from sklearn.neighbors import (KernelDensity, LocalOutlierFactor)
from sklearn.pipeline import Pipeline, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.mixture import BayesianGaussianMixture
from sklearn.preprocessing import (StandardScaler, RobustScaler, MinMaxScaler,
                                   minmax_scale, PolynomialFeatures)
from sklearn.utils import check_array


# for Matrix Profile
import stumpy

# for KDEAnomalyScorer
import statsmodels.api as sm
from statsmodels.nonparametric.kernel_density import KDEMultivariate
from statsmodels.tsa.arima.model import ARIMA
# EXCLUDED until we upgrade to statsmodels 0.12
#from statsmodels.tsa.forecasting.stl import STLForecast

from .base import (BaseTransformer, BaseRegressor, BaseEstimatorFunction, BaseSimpleAggregator)
from .bif import (AlertHighValue)
from .ui import (UISingle, UIMulti, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti)
from .db import (Database, DatabaseFacade)
from .dbtables import (FileModelStore, DBModelStore)

# VAE
import torch
import torch.autograd
import torch.nn as nn

logger = logging.getLogger(__name__)

try:
    # for gradient boosting
    import lightgbm
except (AttributeError, ImportError):
    logger.exception('')
    logger.debug(f'Could not import lightgm package. Might have issues when using GBMRegressor catalog function')

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True

Error_SmallWindowsize = 0.0001
Error_Generic = 0.0002

FrequencySplit = 0.3
DefaultWindowSize = 12
SmallEnergy = 1e-20

KMeans_normalizer = 1
Spectral_normalizer = 100 / 2.8
FFT_normalizer = 1
Saliency_normalizer = 1
Generalized_normalizer = 1 / 300

# from
# https://stackoverflow.com/questions/44790072/sliding-window-on-time-series-data
def view_as_windows1(temperature, length, step):
    logger.info('VIEW ' + str(temperature.shape) + ' ' + str(length) + ' ' + str(step))

    def moving_window(x, length, _step=1):
        if type(step) != 'int' or _step < 1:
            logger.info('MOVE ' + str(_step))
            _step = 1
        streams = it.tee(x, length)
        return zip(*[it.islice(stream, i, None, _step) for stream, i in zip(streams, it.count(step=1))])

    x_ = list(moving_window(temperature, length, step))
    return np.asarray(x_)


def view_as_windows(temperature, length, step):
    return skiutil.view_as_windows(temperature, window_shape=(length,), step=step)


def custom_resampler(array_like):
    # initialize
    if 'gap' not in dir():
        gap = 0

    if array_like.values.size > 0:
        gap = 0
        return 0
    else:
        gap += 1
        return gap


def min_delta(df):
    # minimal time delta for merging

    if df is None:
        return pd.Timedelta('5 seconds'), df
    elif len(df.index.names) > 1:
        df2 = df.reset_index(level=df.index.names[1:], drop=True)
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


def set_window_size_and_overlap(windowsize, trim_value=2 * DefaultWindowSize):
    # make sure it exists
    if windowsize is None:
        windowsize = DefaultWindowSize

    # make sure it is positive and not too large
    trimmed_ws = np.minimum(np.maximum(windowsize, 1), trim_value)

    # overlap
    if trimmed_ws == 1:
        ws_overlap = 0
    else:
        # larger overlap - half the window
        ws_overlap = trimmed_ws // 2

    return trimmed_ws, ws_overlap


def dampen_anomaly_score(array, dampening):
    if dampening is None:
        dampening = 0.9  # gradient dampening

    if dampening >= 1:
        return array

    if dampening < 0.01:
        return array

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
    logger.info('SERIES_FILTER: ' + str(values.shape) + ',' + str(filter_values.shape) + ',' + str(kernel_size))

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
    """
    Fit interpolated score to original entity slice of the full dataframe
    """

    # equip score with time values, make sure it's positive
    score[score < 0] = 0
    dfEntity[column_name] = score

    # merge
    dfEntityOrig = pd.merge_asof(dfEntityOrig, dfEntity[column_name], left_index=True, right_index=True,
                                 direction='nearest', tolerance=mindelta)

    if column_name + '_y' in dfEntityOrig:
        merged_score = dfEntityOrig[column_name + '_y'].to_numpy()
    else:
        merged_score = dfEntityOrig[column_name].to_numpy()

    return merged_score


#######################################################################################
# Scalers
#######################################################################################

class Standard_Scaler(BaseEstimatorFunction):
    """
    Learns and applies standard scaling
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True

    def set_estimators(self):
        self.estimators['standard_scaler'] = (StandardScaler, self.params)
        logger.info('Standard Scaler initialized')

    def __init__(self, features=None, targets=None, predictions=None):
        super().__init__(features=features, targets=targets, predictions=predictions, keep_current_models=True)

        # do not run score and call transform instead of predict
        self.is_scaler = True
        self.experiments_per_execution = 1
        self.normalize = True  # support for optional scaling in subclasses
        self.prediction = self.predictions[0]  # support for subclasses with univariate focus

        self.params = {}
        self.whoami = 'Standard_Scaler'

    # used by all the anomaly scorers based on it
    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data for ' + self.prediction + ' column')

        # operate on simple timestamp index
        #  needed for aggregated data with 3 or more indices
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index(index_names[1:])
        else:
            dfe = dfEntity

        # interpolate gaps - data imputation
        try:
            dfe = dfe.interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        temperature = dfe[self.prediction].fillna(0).to_numpy(dtype=np.float64)

        return dfe, temperature

    # dummy function for scaler, can be replaced with anomaly functions
    def kexecute(self, entity, df_copy):
        return df_copy

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:

            normalize_entity = self.normalize

            try:
                check_array(df_copy.loc[[entity]][self.features].values, allow_nd=True)
            except Exception as e:
                normalize_entity = False
                logger.error(
                    'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))

            # support for optional scaling in subclasses
            if normalize_entity:
                dfe = super()._execute(df_copy.loc[[entity]], entity)
                df_copy.loc[entity, self.predictions] = dfe[self.predictions]
            else:
                self.prediction = self.features[0]

            df_copy = self.kexecute(entity, df_copy)
            self.prediction = self.predictions[0]

        logger.info('Standard_Scaler: Found columns ' + str(df_copy.columns))

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class Robust_Scaler(BaseEstimatorFunction):
    """
    Learns and applies robust scaling, scaling after outlier removal
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True

    def set_estimators(self):
        self.estimators['robust_scaler'] = (RobustScaler, self.params)
        logger.info('Robust Scaler initialized')

    def __init__(self, features=None, targets=None, predictions=None):
        super().__init__(features=features, targets=targets, predictions=predictions, keep_current_models=True)

        # do not run score and call transform instead of predict
        self.is_scaler = True
        self.experiments_per_execution = 1

        self.params = {}

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            # per entity - copy for later inplace operations
            try:
                check_array(df_copy.loc[[entity]][self.features].values, allow_nd=True)
            except Exception as e:
                logger.error(
                    'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))
                continue

            dfe = super()._execute(df_copy.loc[[entity]], entity)
            df_copy.loc[entity, self.predictions] = dfe[self.predictions]

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class MinMax_Scaler(BaseEstimatorFunction):
    """
    Learns and applies minmax scaling
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True

    def set_estimators(self):
        self.estimators['minmax_scaler'] = (MinMaxScaler, self.params)
        logger.info('MinMax Scaler initialized')

    def __init__(self, features=None, targets=None, predictions=None):
        super().__init__(features=features, targets=targets, predictions=predictions, keep_current_models=True)

        # do not run score and call transform instead of predict
        self.is_scaler = True
        self.experiments_per_execution = 1

        self.params = {}

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            try:
                check_array(df_copy.loc[[entity]][self.features].values, allow_nd=True)
            except Exception as e:
                logger.error(
                    'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))
                continue

            dfe = super()._execute(df_copy.loc[[entity]], entity)
            df_copy.loc[entity, self.predictions] = dfe[self.predictions]

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


#######################################################################################
# Anomaly Scorers
#######################################################################################

class AnomalyScorer(BaseTransformer):
    """
    Superclass of all unsupervised anomaly detection functions.
    """
    def __init__(self, input_item, windowsize, output_items):
        super().__init__()
        logger.debug(input_item)
        self.input_item = input_item

        # use 12 by default
        self.windowsize, self.windowoverlap = set_window_size_and_overlap(windowsize)

        # assume 1 per sec for now
        self.frame_rate = 1

        # step
        self.step = self.windowsize - self.windowoverlap

        self.output_items = output_items

        self.normalize = False

        self.whoami = 'Anomaly'

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms

    def get_model_name(self, prefix='model', suffix=None):

        name = []
        if prefix is not None:
            name.append(prefix)

        name.extend([self._entity_type.name, self.whoami])
        name.append(self.output_items[0])
        if suffix is not None:
            name.append(suffix)
        name = '.'.join(name)

        return name

    # make sure data is evenly spaced
    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index(index_names[1:])
        else:
            dfe = dfEntity

        # interpolate gaps - data imputation
        try:
            dfe = dfe.dropna(subset=[self.input_item]).interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        temperature = dfe[self.input_item].fillna(0).to_numpy(dtype=np.float64)

        return dfe, temperature

    def execute(self, df):

        logger.debug('Execute ' + self.whoami)
        df_copy = df # no copy

        # check data type
        if not pd.api.types.is_numeric_dtype(df_copy[self.input_item].dtype):
            logger.error('Anomaly scoring on non-numeric feature:' + str(self.input_item))
            return df_copy

        # set output columns to zero
        for output_item in self.output_items:
            df_copy[output_item] = 0

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        if not df_copy.empty:
            df_copy = df_copy.groupby(group_base).apply(self._calc)

        logger.debug('Scoring done')
        return df_copy

    def _calc(self, df):

        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # get rid of entity id as part of the index
        df = df.droplevel(0)

        # Get new data frame with sorted index
        dfe_orig = df.sort_index()

        # remove all rows with only null entries
        dfe = dfe_orig.dropna(how='all')

        # minimal time delta for merging
        mindelta, dfe_orig = min_delta(dfe_orig)

        logger.debug('Timedelta:' + str(mindelta) + ' Index: ' + str(dfe_orig.index))

        # one dimensional time series - named temperature for catchyness
        # interpolate gaps - data imputation by default
        #   for missing data detection we look at the timestamp gradient instead
        dfe, temperature = self.prepare_data(dfe)

        logger.debug(
                self.whoami + ', Entity: ' + str(entity) + ', Input: ' + str(self.input_item) + ', Windowsize: ' + str(
                    self.windowsize) + ', Output: ' + str(self.output_items) + ', Overlap: ' + str(
                    self.windowoverlap) + ', Inputsize: ' + str(temperature.size))

        if temperature.size <= self.windowsize:
            logger.debug(str(temperature.size) + ' <= ' + str(self.windowsize))
            for output_item in self.output_items:
                dfe[output_item] = Error_SmallWindowsize
        else:
            logger.debug(str(temperature.size) + ", " + str(self.windowsize))

            for output_item in self.output_items:
                dfe[output_item] = Error_Generic

            temperature = self.scale(temperature, entity)

            scores = self.score(temperature)

            # length of time_series_temperature, signal_energy and ets_zscore is smaller than half the original
            #   extend it to cover the full original length
            logger.debug('->')
            try:
                for i,output_item in enumerate(self.output_items):

                    # check for fast path, no interpolation required
                    diff = temperature.size - scores[i].size

                    # slow path - interpolate result score to stretch it to the size of the input data
                    if diff > 0:
                        dfe[output_item] = 0.0006
                        time_series_temperature = np.linspace(self.windowsize // 2, temperature.size - self.windowsize // 2 + 1,
                                                              temperature.size - diff)
                        linear_interpolate = sp.interpolate.interp1d(time_series_temperature, scores[i], kind='linear',
                                                                     fill_value='extrapolate')

                        zScoreII = merge_score(dfe, dfe_orig, output_item,
                                               abs(linear_interpolate(np.arange(0, temperature.size, 1))), mindelta)
                    # fast path - either cut off or just copy
                    elif diff < 0:
                        zScoreII = scores[i][0:temperature.size]
                    else:
                        zScoreII = scores[i]

                    # make sure shape is correct
                    try:
                        df[output_item] = zScoreII
                    except Exception as e2:                    
                        df[output_item] = zScoreII.reshape(-1,1)
                        pass

            except Exception as e:
                logger.error(self.whoami + ' score integration failed with ' + str(e))

            logger.debug('--->')

        return df

    def score(self, temperature):

        #scores = np.zeros((len(self.output_items), ) + temperature.shape)
        scores = []
        for output_item in self.output_items:
            scores.append(np.zeros(temperature.shape))

        try:
            # super simple 1-dimensional z-score
            ets_zscore = abs(sp.stats.zscore(temperature))

            scores[0] = ets_zscore

            # 2nd argument to return the modified input argument (for no data)
            if len(self.output_items) > 1:
                scores[1] = temperature

        except Exception as e:
            logger.error(self.whoami + ' failed with ' + str(e))

        return scores


    def scale(self, temperature, entity):

        normalize_entity = self.normalize
        if not normalize_entity:
            return temperature

        temp = temperature.reshape(-1, 1)
        logger.info(self.whoami + ' scaling ' + str(temperature.shape))
        try:
            check_array(temp, allow_nd=True)
        except Exception as e:
            logger.error('Found Nan or infinite value in input data,  error: ' + str(e))
            return temperature

        # obtain db handler
        db = self.get_db()

        scaler_model = None
        # per entity - copy for later inplace operations
        model_name = self.get_model_name(suffix=entity)
        try:
            scaler_model = db.model_store.retrieve_model(model_name)
            logger.info('load model %s' % str(scaler_model))
        except Exception as e:
            logger.error('Model retrieval failed with ' + str(e))

        # failed to load a model, so train it
        if scaler_model is None:
            # all variables should be continuous
            scaler_model = StandardScaler().fit(temp)
            logger.debug('Created Scaler ' + str(scaler_model))

            try:
                db.model_store.store_model(model_name, scaler_model)
            except Exception as e:
                logger.error('Model store failed with ' + str(e))

        if scaler_model is not None:
            temp = scaler_model.transform(temp)
            return temp.reshape(temperature.shape)

        return temperature


#####
#  experimental function to interpolate over larger gaps
####
class Interpolator(AnomalyScorer):
    """
    Interpolates NaN and data to be interpreted as NaN (for example 0 as invalid sensor reading)
    The window size is typically set large enough to allow for "bridging" gaps
    Missing indicates sensor readings to be interpreted as invalid.
    """

    def __init__(self, input_item, windowsize, missing, output_item):
        super().__init__(input_item, windowsize, [output_item])
        logger.debug(input_item)

        self.missing = missing

        self.whoami = 'Interpolator'

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index(index_names[1:])
        else:
            dfe = dfEntity

        # remove Nan
        dfe = dfe[dfe[self.input_item].notna()]

        # remove self.missing
        dfe = dfe[dfe[self.input_item] != self.missing]

        # interpolate gaps - data imputation
        try:
            dfe = dfe.interpolate(method="time")
        except Exception as e:
            logger.error('Prepare data error: ' + str(e))

        # one dimensional time series - named temperature for catchyness
        # replace NaN with self.missing
        temperature = dfe[self.input_item].fillna(0).to_numpy(dtype=np.float64)

        return dfe, temperature

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to interpolate'))

        inputs.append(
            UISingle(name='windowsize', datatype=int, description='Minimal size of the window for interpolating data.'))
        inputs.append(UISingle(name='missing', datatype=int, description='Data to be interpreted as not-a-number.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Interpolated data'))
        return (inputs, outputs)


class NoDataAnomalyScoreExt(AnomalyScorer):
    """
    An unsupervised anomaly detection function.
     Uses z-score AnomalyScorer to find gaps in data.
     The function moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
    """
    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, [output_item])

        self.whoami = 'NoDataExt'
        self.normalizer = 1

        logger.debug('NoDataExt')

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names
            dfe = dfEntity.reset_index(index_names[1:])
        else:
            dfe = dfEntity

        # count the timedelta in seconds between two events
        timeSeq = (dfe.index.values - dfe.index[0].to_datetime64()) / np.timedelta64(1, 's')

        #dfe = dfEntity.copy()

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
            temperature[0] = 10 ** 10

        temperature = temperature.astype('float64').reshape(-1)

        return dfe, temperature

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='No data anomaly score'))
        return inputs, outputs


class ChangePointDetector(AnomalyScorer):
    '''
    An unsupervised anomaly detection function.
     Applies a spectral analysis clustering techniqueto extract features from time series data and to create z scores.
     Moves a sliding window across the data signal and applies the anomalymodelto each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    '''
    def __init__(self, input_item, windowsize, chg_pts):
        super().__init__(input_item, windowsize, [chg_pts])

        logger.debug(input_item)

        self.whoami = 'ChangePointDetector'

    def score(self, temperature):

        scores = []

        sc = np.zeros(temperature.shape)

        try:
            algo = rpt.BottomUp(model="l2", jump=2).fit(temperature)
            chg_pts = algo.predict(n_bkps=15)

            for j in chg_pts:
                x = np.arange(0, temperature.shape[0], 1)
                Gaussian = sp.stats.norm(j-1, temperature.shape[0]/20) # high precision
                y = Gaussian.pdf(x) * temperature.shape[0]/8  # max is ~1

                sc += y

        except Exception as e:
            logger.error(self.whoami + ' failed with ' + str(e))

        scores.append(sc)
        return scores

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='chg_pts', datatype=float, description='Change points'))
        return inputs, outputs


ENSEMBLE = '_ensemble_'
SPECTRALEXT = 'SpectralAnomalyScoreExt'

class EnsembleAnomalyScore(BaseTransformer):
    '''
    Call a set of anomaly detectors and return an joint vote along with the individual results
    '''
    def __init__(self, input_item, windowsize, scorers, thresholds, output_item):
        super().__init__()

        self.input_item = input_item
        self.windowsize = windowsize
        self.output_item = output_item

        logger.debug(input_item)

        self.whoami = 'EnsembleAnomalyScore'

        self.list_of_scorers = scorers.split(',')
        self.thresholds = list(map(int, thresholds.split(',')))

        self.klasses = []
        self.instances = []
        self.output_items = []

        module = importlib.import_module('mmfunctions.anomaly')

        for m in self.list_of_scorers:
            klass = getattr(module, m)
            self.klasses.append(klass)
            print(klass.__name__)
            if klass.__name__ == SPECTRALEXT:
                inst = klass(input_item, windowsize, output_item + ENSEMBLE + klass.__name__,
                             output_item + ENSEMBLE + klass.__name__ + '_inv')
            else:
                inst = klass(input_item, windowsize, output_item + ENSEMBLE + klass.__name__)
            self.output_items.append(output_item + ENSEMBLE + klass.__name__)
            self.instances.append(inst)

    def execute(self, df):
        logger.debug('Execute ' + self.whoami)
        df_copy = df # no copy

        binned_indices_list = []
        for inst, output, threshold in zip(self.instances, self.output_items, self.thresholds):
            logger.info('Execute anomaly scorer ' + str(inst.__class__.__name__) + ' with threshold ' + str(threshold))
            tic = time.perf_counter_ns()
            df_copy = inst.execute(df_copy)
            toc = time.perf_counter_ns()
            logger.info('Executed anomaly scorer ' + str(inst.__class__.__name__) + ' in ' +\
                         str((toc-tic)//1000000) + ' milliseconds')

            arr = df_copy[output]

            # sort results into bins that depend on the thresholds
            #   0 - below 3/4 threshold, 1 - up to the threshold, 2 - crossed the threshold,
            #     3 - very high, 4 - extreme
            if inst.__class__.__name__ == SPECTRALEXT and isinstance(threshold, int):
                # hard coded threshold for inverted values
                threshold_ = 5

            bins = [threshold * 0.75, threshold, threshold * 1.5, threshold * 2]
            binned_indices_list.append(np.searchsorted(bins, arr, side='left'))

            if inst.__class__.__name__ == SPECTRALEXT:
                bins = [threshold_ * 0.75, threshold_, threshold_ * 1.5, threshold_ * 2]
                arr = df_copy[output + '_inv']
                binned_indices_list.append(np.searchsorted(bins, arr, side='left'))

        binned_indices = np.vstack(binned_indices_list).mean(axis=0)

        # should we explicitly drop the columns generated by the ensemble members
        #df[self.output_item] = binned_indices
        df_copy[self.output_item] = binned_indices

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=float, description='Spectral anomaly score (z-score)'))
        return inputs, outputs



class SpectralAnomalyScore(AnomalyScorer):
    '''
    An unsupervised anomaly detection function.
     Applies a spectral analysis clustering techniqueto extract features from time series data and to create z scores.
     Moves a sliding window across the data signal and applies the anomalymodelto each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    '''
    def __init__(self, input_item, windowsize, output_item):
        if isinstance(output_item, list):
            super().__init__(input_item, windowsize, output_item)
        else:
            super().__init__(input_item, windowsize, [output_item])

        logger.debug(input_item)

        self.whoami = 'SpectralAnomalyScore'

    def score(self, temperature):

        scores = []
        for output_item in self.output_items:
            scores.append(np.zeros(temperature.shape))

        try:
            # Fourier transform:
            #   frequency, time, spectral density
            frequency_temperature, time_series_temperature, spectral_density_temperature = signal.spectrogram(
                temperature, fs=self.frame_rate, window='hanning', nperseg=self.windowsize,
                noverlap=self.windowoverlap, detrend='l', scaling='spectrum')

            # cut off freqencies too low to fit into the window
            frequency_temperatureb = (frequency_temperature > 2 / self.windowsize).astype(int)
            frequency_temperature = frequency_temperature * frequency_temperatureb
            frequency_temperature[frequency_temperature == 0] = 1 / self.windowsize

            signal_energy = np.dot(spectral_density_temperature.T, frequency_temperature)

            signal_energy[signal_energy < SmallEnergy] = SmallEnergy
            inv_signal_energy = np.divide(np.ones(signal_energy.size), signal_energy)

            ets_zscore = abs(sp.stats.zscore(signal_energy)) * Spectral_normalizer
            inv_zscore = abs(sp.stats.zscore(inv_signal_energy))

            scores[0] = ets_zscore
            if len(self.output_items) > 1:
                scores[1] = inv_zscore

            # 3rd argument to return the raw windowed signal energy
            if len(self.output_items) > 2:
                scores[2] = signal_energy

            # 4th argument to return the modified input argument (for no data)
            if len(self.output_items) > 3:
                scores[3] = temperature.copy()

            logger.debug(
                'Spectral z-score max: ' + str(ets_zscore.max()) + ',   Spectral inv z-score max: ' + str(
                    inv_zscore.max()))

        except Exception as e:
            logger.error(self.whoami + ' failed with ' + str(e))

        return scores

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=float, description='Spectral anomaly score (z-score)'))
        return inputs, outputs


class SpectralAnomalyScoreExt(SpectralAnomalyScore):
    '''
    An unsupervised anomaly detection function.
     Applies a spectral analysis clustering techniqueto extract features from time series data and to create z scores.
     Moves a sliding window across the data signal and applies the anomalymodelto each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    '''
    def __init__(self, input_item, windowsize, output_item, inv_zscore, signal_energy=None):
        if signal_energy is None:
            super().__init__(input_item, windowsize, [output_item, inv_zscore])
        else:
            super().__init__(input_item, windowsize, [output_item, inv_zscore, signal_energy])

        logger.debug(input_item)

        self.whoami = 'SpectralAnomalyScoreExt'

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=float, description='Spectral anomaly score (z-score)'))
        outputs.append(UIFunctionOutSingle(name='inv_zscore', datatype=float,
                                           description='z-score of inverted signal energy - detects unusually low activity'))
        outputs.append(UIFunctionOutSingle(name='signal_enerty', datatype=float,
                                           description='signal energy'))
        return inputs, outputs



class KMeansAnomalyScore(AnomalyScorer):
    """
    An unsupervised anomaly detection function.
     Applies a k-means analysis clustering technique to time series data.
     Moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
     Try several anomaly models on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, output_item, expr=None):
        super().__init__(input_item, windowsize, [output_item])

        logger.debug(input_item)

        self.whoami = 'KMeans'


    def score(self, temperature):

        scores = []
        for output_item in self.output_items:
            scores.append(np.zeros(temperature.shape))

        try:
            # Chop into overlapping windows
            slices = view_as_windows(temperature, self.windowsize, self.step)

            if self.windowsize > 1:
                n_cluster = 40
            else:
                n_cluster = 20

            n_cluster = 15

            n_cluster = np.minimum(n_cluster, slices.shape[0] // 2)

            logger.debug(self.whoami + 'params, Clusters: ' + str(n_cluster) + ', Slices: ' + str(slices.shape))

            cblofwin = CBLOF(n_clusters=n_cluster, n_jobs=-1)
            try:
                cblofwin.fit(slices)
            except Exception as e:
                logger.info('KMeans failed with ' + str(e))
                self.trace_append('KMeans failed with' + str(e))
                return scores

            pred_score = cblofwin.decision_scores_.copy() * KMeans_normalizer

            scores[0] = pred_score

            logger.debug('KMeans score max: ' + str(pred_score.max()))

        except Exception as e:
            logger.error(self.whoami + ' failed with ' + str(e))

        return scores

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Anomaly score (kmeans)'))
        return inputs, outputs


class GeneralizedAnomalyScore(AnomalyScorer):
    """
    An unsupervised anomaly detection function.
     Applies the Minimum Covariance Determinant (FastMCD) technique to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, [output_item])
        logger.debug(input_item)

        self.whoami = 'GAM'

        self.normalizer = Generalized_normalizer

    def feature_extract(self, temperature):

        logger.debug(self.whoami + ': feature extract')

        slices = view_as_windows(temperature, self.windowsize, self.step)

        return slices

    def score(self, temperature):

        scores = []
        for output_item in self.output_items:
            scores.append(np.zeros(temperature.shape))

        logger.debug(str(temperature.size) + ", " + str(self.windowsize))

        temperature -= np.mean(temperature.astype(np.float64), axis=0)
        mcd = MinCovDet()

        # Chop into overlapping windows (default) or run through FFT first
        slices = self.feature_extract(temperature)

        try:
            mcd.fit(slices)
            pred_score = mcd.mahalanobis(slices).copy() * self.normalizer

        except ValueError as ve:

            pred_score = np.zeros(temperature.shape)
            logger.info(self.whoami + ", Input: " + str(
                    self.input_item) + ", WindowSize: " + str(self.windowsize) + ", Output: " + str(
                    self.output_items[0]) + ", Step: " + str(self.step) + ", InputSize: " + str(
                    slices.shape) + " failed in the fitting step with \"" + str(ve) + "\" - scoring zero")

        except Exception as e:

            pred_score = np.zeros(temperature.shape)
            logger.error(self.whoami + ", Input: " + str(
                    self.input_item) + ", WindowSize: " + str(self.windowsize) + ", Output: " + str(
                    self.output_items[0]) + ", Step: " + str(self.step) + ", InputSize: " + str(
                    slices.shape) + " failed in the fitting step with " + str(e))

        scores[0] = pred_score

        logger.debug(self.whoami + ' score max: ' + str(pred_score.max()))

        return scores

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze", ))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12."))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name="output_item", datatype=float, description="Anomaly score (GeneralizedAnomaly)", ))
        return inputs, outputs


class NoDataAnomalyScore(GeneralizedAnomalyScore):
    """
    An unsupervised anomaly detection function.
     Uses FastMCD to find gaps in data.
     The function moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'NoData'
        self.normalizer = 1

        logger.debug('NoData')

    def prepare_data(self, dfEntity):

        logger.debug(self.whoami + ': prepare Data')

        # operate on simple timestamp index
        if len(dfEntity.index.names) > 1:
            index_names = dfEntity.index.names[1:]
            dfe = dfEntity.reset_index(index_names)
        else:
            dfe = dfEntity

        # count the timedelta in seconds between two events
        logger.debug('type of index[0] is ' + str(type(dfEntity.index[0])))

        try:
            timeSeq = (dfe.index.values - dfe.index[0].to_datetime64()) / np.timedelta64(1, 's')
        except Exception:
            try:
                time_to_numpy = np.array(dfe.index[0], dtype='datetime64')
                print('5. ', type(time_to_numpy), dfe.index[0][0])
                timeSeq = (time_to_numpy - dfe.index[0][0].to_datetime64()) / np.timedelta64(1, 's')
            except Exception:
                print('Nochens')
                timeSeq = 1.0

        #dfe = dfEntity.copy()

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
            temperature[0] = 10 ** 10

        temperature = temperature.astype('float64').reshape(-1)

        return dfe, temperature

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='No data anomaly score'))
        return inputs, outputs


class FFTbasedGeneralizedAnomalyScore(GeneralizedAnomalyScore):
    """
    An unsupervised and robust anomaly detection function.
     Extracts temporal features from time series data using Fast Fourier Transforms.
     Applies the GeneralizedAnomalyScore to the features to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly models to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'FFT'
        self.normalizer = FFT_normalizer

        logger.debug('FFT')

    def feature_extract(self, temperature):
        logger.debug(self.whoami + ': feature extract')

        slices_ = view_as_windows(temperature, self.windowsize, self.step)

        slicelist = []
        for slice in slices_:
            slicelist.append(fftpack.rfft(slice))

        return np.stack(slicelist, axis=0)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze", ))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12."))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name="output_item", datatype=float,
                                           description="Anomaly score (FFTbasedGeneralizedAnomalyScore)", ))
        return inputs, outputs


class MatrixProfileAnomalyScore(AnomalyScorer):
    """
    An unsupervised anomaly detection function.
     Applies matrix profile analysis on time series data.
     Moves a sliding window across the data signal to calculate the euclidean distance from one window to all others to build a distance profile.
     The window size is typically set to 12 data points.
     Try several anomaly models on your data and use the one that fits your data best.
    """
    DATAPOINTS_AFTER_LAST_WINDOW = 1e-15
    INIT_SCORES = 1e-20
    ERROR_SCORES = 1e-16

    def __init__(self, input_item, window_size, output_item):
        super().__init__(input_item, window_size, [output_item])
        logger.debug(f'Input item: {input_item}')

        self.whoami = 'MatrixProfile'


    def score(self, temperature):

        scores = []
        for output_item in self.output_items:
            scores.append(np.zeros(temperature.shape))

        try:  # calculate scores
            # replaced aamp with stump for stumpy 1.8.0 and above
            #matrix_profile = stumpy.aamp(temperature, m=self.windowsize)[:, 0]
            matrix_profile = stumpy.stump(temperature, m=self.windowsize, normalize=False)[:, 0]

            # fill in a small value for newer data points outside the last possible window
            fillers = np.array([self.DATAPOINTS_AFTER_LAST_WINDOW] * (self.windowsize - 1))

            matrix_profile = np.append(matrix_profile, fillers)
        except Exception as er:
            logger.warning(f' Error in calculating Matrix Profile Scores. {er}')
            matrix_profile = np.array([self.ERROR_SCORES] * temperature.shape[0])

        scores[0] = matrix_profile

        logger.debug('Matrix Profile score max: ' + str(matrix_profile.max()))

        return scores

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = [UISingleItem(name="input_item", datatype=float, description="Time series data item to analyze", ),
                  UISingle(name="window_size", datatype=int,
                           description="Size of each sliding window in data points. Typically set to 12.")]

        # define arguments that behave as function outputs
        outputs = [UIFunctionOutSingle(name="output_item", datatype=float,
                                       description="Anomaly score (MatrixProfileAnomalyScore)", )]
        return inputs, outputs


class SaliencybasedGeneralizedAnomalyScore(GeneralizedAnomalyScore):
    """
    An unsupervised anomaly detection function.
     Based on salient region detection models,
         it uses fast fourier transform to reconstruct a signal using the salient features of a the signal.
     It applies GeneralizedAnomalyScore to the reconstructed signal.
     The function moves a sliding window across the data signal and applies its analysis to each window.
     The window size is typically set to 12 data points.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'Saliency'
        self.saliency = Saliency(windowsize, 0, 0)
        self.normalizer = Saliency_normalizer

        logger.debug('Saliency')

    def feature_extract(self, temperature):
        logger.debug(self.whoami + ': feature extract')

        slices = view_as_windows(temperature, self.windowsize, self.step)

        return slices

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze"))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12.", ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name="output_item", datatype=float,
                                           description="Anomaly score (SaliencybasedGeneralizedAnomalyScore)", ))
        return (inputs, outputs)


#######################################################################################
# Anomaly detectors with scaling
#######################################################################################
class KMeansAnomalyScoreV2(KMeansAnomalyScore):

    def __init__(self, input_item, windowsize, normalize, output_item, expr=None):
        super().__init__(input_item, windowsize, output_item)

        logger.debug(input_item)
        self.normalize = normalize

        self.whoami = 'KMeansV2'

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to analyze'))

        inputs.append(UISingle(name='windowsize', datatype=int,
                               description='Size of each sliding window in data points. Typically set to 12.'))

        inputs.append(UISingle(name='normalize', datatype=bool, description='Flag for normalizing data.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Anomaly score (kmeans)'))
        return (inputs, outputs)


class GeneralizedAnomalyScoreV2(GeneralizedAnomalyScore):
    """
    An unsupervised anomaly detection function.
     Applies the Minimum Covariance Determinant (FastMCD) technique to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly model to each window.
     The window size is typically set to 12 data points.
     The normalize switch allows to learn and apply a standard scaler prior to computing the anomaly score.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, normalize, output_item, expr=None):
        super().__init__(input_item, windowsize, output_item)

        logger.debug(input_item)
        # do not run score and call transform instead of predict

        self.normalize = normalize

        self.whoami = 'GAMV2'

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze", ))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12."))

        inputs.append(UISingle(name='normalize', datatype=bool, description='Flag for normalizing data.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name="output_item", datatype=float, description="Anomaly score (GeneralizedAnomaly)", ))
        return inputs, outputs


class FFTbasedGeneralizedAnomalyScoreV2(GeneralizedAnomalyScoreV2):
    """
    An unsupervised and robust anomaly detection function.
     Extracts temporal features from time series data using Fast Fourier Transforms.
     Applies the GeneralizedAnomalyScore to the features to detect outliers.
     Moves a sliding window across the data signal and applies the anomaly models to each window.
     The window size is typically set to 12 data points.
     The normalize switch allows to learn and apply a standard scaler prior to computing the anomaly score.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, normalize, output_item):
        super().__init__(input_item, windowsize, normalize, output_item)

        self.normalize = normalize

        self.whoami = 'FFTV2'
        self.normalizer = FFT_normalizer

        logger.debug('FFT')

    def feature_extract(self, temperature):
        logger.debug(self.whoami + ': feature extract')

        slices_ = view_as_windows(temperature, self.windowsize, self.step)

        slicelist = []
        for slice in slices_:
            slicelist.append(fftpack.rfft(slice))

        return np.stack(slicelist, axis=0)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze", ))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12."))

        inputs.append(UISingle(name='normalize', datatype=bool, description='Flag for normalizing data.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name="output_item", datatype=float,
                                           description="Anomaly score (FFTbasedGeneralizedAnomalyScore)", ))
        return inputs, outputs


class SaliencybasedGeneralizedAnomalyScoreV2(SaliencybasedGeneralizedAnomalyScore):
    """
    An unsupervised anomaly detection function.
     Based on salient region detection models,
         it uses fast fourier transform to reconstruct a signal using the salient features of a the signal.
     It applies GeneralizedAnomalyScore to the reconstructed signal.
     The function moves a sliding window across the data signal and applies its analysis to each window.
     The window size is typically set to 12 data points.
     The normalize switch allows to learn and apply a standard scaler prior to computing the anomaly score.
     Try several anomaly detectors on your data and use the one that fits your data best.
    """

    def __init__(self, input_item, windowsize, normalize, output_item):
        super().__init__(input_item, windowsize, output_item)

        self.whoami = 'SaliencyV2'

        self.normalize = normalize

        logger.debug('SaliencyV2')

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze"))

        inputs.append(UISingle(name="windowsize", datatype=int,
                               description="Size of each sliding window in data points. Typically set to 12.", ))

        inputs.append(UISingle(name='normalize', datatype=bool, description='Flag for normalizing data.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name="output_item", datatype=float,
                                           description="Anomaly score (SaliencybasedGeneralizedAnomalyScore)", ))
        return inputs, outputs


KMeansAnomalyScorev2 = KMeansAnomalyScoreV2
FFTbasedGeneralizedAnomalyScorev2 = FFTbasedGeneralizedAnomalyScoreV2
SaliencybasedGeneralizedAnomalyScorev2 = SaliencybasedGeneralizedAnomalyScoreV2
GeneralizedAnomalyScorev2 = GeneralizedAnomalyScoreV2


#######################################################################################
# Base class to handle models
#######################################################################################

class SupervisedLearningTransformer(BaseTransformer):

    name = 'SupervisedLearningTransformer'

    """
    Base class for anomaly scorers that can be trained with historic data in a notebook
    and automatically store a trained model in the tenant database
    Inferencing is run in the pipeline
    """
    def __init__(self, features, targets, predictions=None):
        super().__init__()

        logging.debug("__init__" + self.name)

        # do NOT automatically train if no model is found (subclasses)
        self.auto_train = False
        self.delete_model = False

        self.features = features
        self.targets = targets
        self.predictions = predictions

        parms = []
        if features is not None:
            parms.extend(features)
        if targets is not None:
            parms.extend(targets)
        parms = '.'.join(parms)
        logging.debug("__init__ done with parameters: " + parms)
        self.whoami = 'SupervisedLearningTransformer'


    def load_model(self, suffix=None):
        # TODO: Lift assumption there is only a single target
        model_name = self.generate_model_name([], self.targets[0], prefix='model', suffix=suffix)
        my_model = None
        db = self.get_db()
        try:
            my_model = db.model_store.retrieve_model(model_name)
            logger.info('load model %s' % str(my_model))
        except Exception as e:
            logger.error('Model retrieval failed with ' + str(e))
            pass

        # ditch old model
        version = 1
        if self.delete_model:
            if my_model is not None:
                if hasattr(my_model, 'version'):
                    version = my_model.version + 1
                logger.debug('Deleting robust model ' + str(version-1) + ' for entity: ' + str(suffix))
                my_model = None

        return model_name, my_model, version


    def execute(self, df):
        logger.debug('Execute ' + self.whoami)

        # obtain db handler
        db = self.get_db()

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.features:
            if not pd.api.types.is_numeric_dtype(df[feature].dtype):
                logger.error('Regression on non-numeric feature:' + str(feature))
                return (df)

        # Create missing columns before doing group-apply
        df_copy = df.copy()
        missing_cols = [x for x in self.targets + self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        df_copy = df_copy.groupby(group_base).apply(self._calc)

        logger.debug('Scoring done')

        return df_copy


#######################################################################################
# Outlier removal in pipeline
#######################################################################################

class LocalOutlierFactor:
    def __init__(self):
        self.lof = LocalOutlierFactor() #**kwargs)
        self.version = 1

    def fit(self, X):
        self.lof.fit(X.reshape(-1,1))

    def predict(self, X, threshold):
        #return (X >= self.MinMax[0]) & (X <= self.MinMax[1])
        return self.lof.negative_outlier_factor_ < threshold

class KDEMaxMin:
    def __init__(self, version=1):
        self.version = version
        self.kde = KernelDensity(kernel='gaussian')
        self.Min = None
        self.Max = None

    def fit(self, X, alpha):

        self.kde.fit(X.reshape(-1,1))

        kde_X = self.kde.score_samples(X.reshape(-1,1))

        # find outliers of the kde score
        tau_kde = sp.stats.mstats.mquantiles(kde_X, 1. - alpha)  # alpha = 0.995

        # determine outliers
        X_outliers = X[np.argwhere(kde_X < tau_kde).flatten()]
        X_valid = X[np.argwhere(kde_X >= tau_kde).flatten()]

        # determine max of all sample that are not outliers
        self.Min = X_valid.min()
        self.Max = X_valid.max()
        if len(X_outliers) > 0:
            X_min = X_outliers[X_outliers < self.Min]
            X_max = X_outliers[X_outliers > self.Max]
            if len(X_min) > 0:
                self.Min = max(X_min.max(), self.Min)
            if len(X_max) > 0:
                self.Max = min(X_max.min(), self.Max)
        #    self.Min = max(X_outliers[X_outliers < self.Min].max(), self.Min)
        #    self.Max = min(X_outliers[X_outliers > self.Max].min(), self.Max)

        logger.info('KDEMaxMin - Min: ' + str(self.Min) + ', ' + str(self.Max))

        return kde_X

    def predict(self, X, threshold=None):
        return (X >= self.Min) & (X <= self.Max)

class RobustThreshold(SupervisedLearningTransformer):

    def __init__(self, input_item, threshold, output_item):
        super().__init__(features=[input_item], targets=[output_item])

        self.input_item = input_item
        self.threshold = threshold
        self.output_item = output_item
        self.auto_train = True
        self.Min = dict()
        self.Max = dict()

        self.whoami = 'RobustThreshold'

        logger.info(self.whoami + ' from ' + self.input_item + ' quantile threshold ' +  str(self.threshold) +
                    ' exceeding boolean ' + self.output_item)


    def execute(self, df):
        # set output columns to zero
        logger.debug('Called ' + self.whoami + ' with columns: ' + str(df.columns))
        df[self.output_item] = 0
        return super().execute(df)


    def _calc(self, df):
        # per entity - copy for later inplace operations
        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        model_name, robust_model, version = self.load_model(suffix=entity)

        feature = df[self.input_item].values

        if robust_model is None and self.auto_train:
            robust_model = KDEMaxMin(version=version)
            try:
                robust_model.fit(feature, self.threshold)
                db.model_store.store_model(model_name, robust_model)
            except Exception as e:
                logger.error('Model store failed with ' + str(e))
                robust_model = None

        if robust_model is not None:
            self.Min[entity] = robust_model.Min
            self.Max[entity] = robust_model.Max

            df[self.output_item] = robust_model.predict(feature, self.threshold)
        else:
            df[self.output_item] = 0

        return df.droplevel(0)


    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name="input_item", datatype=float, description="Data item to analyze"))

        inputs.append(UISingle(name="threshold", datatype=int,
                               description="Threshold to determine outliers by quantile. Typically set to 0.95", ))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name="output_item", datatype=bool,
                                           description="Boolean outlier condition"))
        return (inputs, outputs)



#######################################################################################
# Regressors
#######################################################################################

class BayesRidgeRegressor(BaseEstimatorFunction):
    """
    Linear regressor based on a probabilistic model as provided by sklearn
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    num_rounds_per_estimator = 3

    def BRidgePipeline(self):
        steps = [('scaler', StandardScaler()), ('bridge', linear_model.BayesianRidge(compute_score=True))]
        return Pipeline(steps)

    def set_estimators(self):
        params = {}
        self.estimators['bayesianridge'] = (self.BRidgePipeline, params)

        logger.info('Bayesian Ridge Regressor start searching for best model')

    def __init__(self, features, targets, predictions=None, deviations=None):
        super().__init__(features=features, targets=targets, predictions=predictions, stddev=True, keep_current_models=True)

        if deviations is not None:
            self.pred_stddev = deviations

        self.experiments_per_execution = 1
        self.auto_train = True
        self.correlation_threshold = 0
        self.stop_auto_improve_at = -2

        self.whoami = 'BayesianRidgeRegressor'


    def execute(self, df):

        logger.debug('Execute ' + self.whoami)

        df_copy = df.copy()
        # Create missing columns before doing group-apply
        missing_cols = [x for x in self.predictions + self.pred_stddev if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.features:
            if not pd.api.types.is_numeric_dtype(df_copy[feature].dtype):
                logger.error('Regression on non-numeric feature:' + str(feature))
                return (df_copy)

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        df_copy = df_copy.groupby(group_base).apply(self._calc)

        logger.debug('Scoring done')
        return df_copy


    def _calc(self, df):

        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # obtain db handle
        db = self.get_db()

        logger.debug('BayesRidgeRegressor execute: ' + str(type(df)) + ' for entity ' + str(entity) +
                     ' predicting ' + str(self.targets) + ' from ' + str(self.features) +
                     ' to appear in ' + str(self.predictions) + ' with confidence interval ' + str(self.pred_stddev))

        try:
            dfe = super()._execute(df, entity)

            logger.debug('BayesianRidge: Entity ' + str(entity) + ' Type of pred, stddev arrays ' +
                         str(type(dfe[self.predictions])) + str(type(dfe[self.pred_stddev].values)))

            dfe.fillna(0, inplace=True)

            df[self.predictions] = dfe[self.predictions]
            df[self.pred_stddev] = dfe[self.pred_stddev]

        except Exception as e:
            logger.info('Bayesian Ridge regressor for entity ' + str(entity) + ' failed with: ' + str(e))
            df[self.predictions] = 0
            df[self.pred_stddev] = 0

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True, output_item='deviations',
                                  is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))

        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class BayesRidgeRegressorExt(BaseEstimatorFunction):
    """
    Linear regressor based on a probabilistic model as provided by sklearn
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    num_rounds_per_estimator = 3

    def BRidgePipelineDeg(self):
        steps = [('scaler', StandardScaler()),
                 ('poly', PolynomialFeatures(degree=self.degree)),
                 ('bridge', linear_model.BayesianRidge(compute_score=True))]
        return Pipeline(steps)

    def set_estimators(self):
        params = {}
        self.estimators['bayesianridge'] = (self.BRidgePipelineDeg, params)

        logger.info('Bayesian Ridge Regressor start searching for best polynomial model of degree ' + str(self.degree))

    def __init__(self, features, targets, predictions=None, deviations=None, degree=3):
        super().__init__(features=features, targets=targets, predictions=predictions, stddev=True, keep_current_models=True)

        if deviations is not None:
            self.pred_stddev = deviations

        self.experiments_per_execution = 1
        self.auto_train = True
        self.correlation_threshold = 0
        self.stop_auto_improve_at = -2
        self.degree = degree

        self.whoami = 'BayesianRidgeRegressorExt'


    def execute(self, df):

        logger.debug('Execute ' + self.whoami)

        df_copy = df.copy()
        # Create missing columns before doing group-apply
        missing_cols = [x for x in self.predictions + self.pred_stddev if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.features:
            if not pd.api.types.is_numeric_dtype(df_copy[feature].dtype):
                logger.error('Regression on non-numeric feature:' + str(feature))
                return (df_copy)

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        df_copy = df_copy.groupby(group_base).apply(self._calc)

        logger.debug('Scoring done')
        return df_copy


    def _calc(self, df):

        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        logger.debug('BayesRidgeRegressor execute: ' + str(type(df)) + ' for entity ' + str(entity) +
                     ' predicting ' + str(self.targets) + ' from ' + str(self.features) +
                     ' to appear in ' + str(self.predictions) + ' with confidence interval ' + str(self.pred_stddev))

        try:
            logger.debug('check passed')

            dfe = super()._execute(df, entity)

            logger.debug('BayesianRidge: Entity ' + str(entity) + ' Type of pred, stddev arrays ' +
                         str(type(dfe[self.predictions])) + str(type(dfe[self.pred_stddev].values)))

            dfe.fillna(0, inplace=True)

            df[self.predictions] = dfe[self.predictions]
            df[self.pred_stddev] = dfe[self.pred_stddev]

        except Exception as e:
            logger.info('Bayesian Ridge regressor for entity ' + str(entity) + ' failed with: ' + str(e))
            df[self.predictions] = 0
            df[self.pred_stddev] = 0

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True, output_item='deviations',
                                  is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(
            UISingle(name='degree', datatype=int, required=False, description='Degree of polynomial'))

        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class GBMRegressor(BaseEstimatorFunction):
    """
    Regressor based on gradient boosting method as provided by lightGBM
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True

    def GBMPipeline(self):
        steps = [('scaler', StandardScaler()), ('gbm', lightgbm.LGBMRegressor())]
        return Pipeline(steps=steps)

    def set_estimators(self):
        # gradient_boosted
        self.estimators['light_gradient_boosted_regressor'] = (self.GBMPipeline, self.params)
        logger.info('GBMRegressor start searching for best model')

    def __init__(self, features, targets, predictions=None, n_estimators=500, num_leaves=40, learning_rate=0.2,
                 max_depth=-1, lags=None):
        #
        # from https://github.com/ashitole/Time-Series-Project/blob/main/Auto-Arima%20and%20LGBM.ipynb
        #   as taken from https://www.kaggle.com/rohanrao/ashrae-half-and-half
        #
        self.n_estimators = n_estimators  # 500
        self.num_leaves = num_leaves    # 40
        self.learning_rate = learning_rate #0.2   # default 0.001
        feature_fraction = 0.85  # default 1.0
        reg_lambda = 2  # default 0
        self.max_depth = max_depth # -1
        self.lagged_features = features
        self.lags = lags

        self.forecast = None
        if lags is not None:
            self.forecast = min(lags)  # forecast = number to shift features back is the negative minimum lag
            newfeatures, _ = self.lag_features()

            super().__init__(features=newfeatures, targets=targets, predictions=predictions, keep_current_models=True)
        else:
            super().__init__(features=features, targets=targets, predictions=predictions, keep_current_models=True)

        self.experiments_per_execution = 1
        self.correlation_threshold = 0
        self.auto_train = True

        self.num_rounds_per_estimator = 1
        self.parameter_tuning_iterations = 1
        self.cv = 1

        self.set_parameters()

        self.stop_auto_improve_at = -2
        self.whoami = 'GBMRegressor'


    def set_parameters(self):
        self.params = {'gbm__n_estimators': [self.n_estimators], 'gbm__num_leaves': [self.num_leaves],
                       'gbm__learning_rate': [self.learning_rate], 'gbm__max_depth': [self.max_depth], 'gbm__verbosity': [-1]}

    #
    # forecasting support
    #   return list of new columns for the lagged features and dataframe extended with these new columns
    #
    def lag_features(self, df=None, Train=True):
        logger.debug('lags ' + str(self.lags) + '  lagged_features ' + str(self.lagged_features) + ' Train mode: '
                     + str(Train))
        create_feature_triplets = []
        new_features = []

        if self.lags is None or self.lagged_features is None:
            return new_features, None

        for lagged_feature in self.lagged_features:
            for lag in self.lags:
                # collect triple of new column, original column and lag
                if Train:
                    create_feature_triplets.append((lagged_feature + '_' + str(lag), lagged_feature, lag))
                else:
                    create_feature_triplets.append((lagged_feature + '_' + str(lag), lagged_feature, lag - self.forecast))

                new_features.append(lagged_feature + '_' + str(lag))

        # find out proper timescale
        mindelta, df_copy = min_delta(df)

        # add day of week and month of year as two feature pairs for at least hourly timescales
        include_day_of_week = False
        include_hour_of_day = False
        if mindelta >= pd.Timedelta('1h'):
            logger.info(self.whoami + ' adding day_of_week feature')
            include_day_of_week = True
        elif mindelta >= pd.Timedelta('1m'):
            logger.info(self.whoami + ' adding hour_of_day feature')
            include_hour_of_day = True

        # add day of week or hour of day if appropriate

        if df is not None:
            df_copy = df.copy()
            missing_cols = [x[0] for x in create_feature_triplets if x not in df_copy.columns]
            for m in missing_cols:
                df_copy[m] = None

            # I hope I can do that for all entities in one fell swoop
            for new_feature in create_feature_triplets:
                df_copy[new_feature[0]] = df[new_feature[1]].shift(new_feature[2])

            # get rid of NaN as result of shifting columns
            df_copy.dropna(inplace=True)

            # add day of week and month of year as two feature pairs
            # operate on simple timestamp index
            if include_day_of_week:
                new_features = np.concatenate((new_features, ['_DayOfWeekCos_', '_DayOfWeekSin_', '_DayOfYearCos_', '_DayOfYearSin_']))
                df_copy['_DayOfWeekCos_'] = np.cos(df_copy.index.get_level_values(1).dayofweek / 7)
                df_copy['_DayOfWeekSin_'] = np.sin(df_copy.index.get_level_values(1).dayofweek / 7)
                df_copy['_DayOfYearCos_'] = np.cos(df_copy.index.get_level_values(1).dayofyear / 365)
                df_copy['_DayOfYearSin_'] = np.sin(df_copy.index.get_level_values(1).dayofyear / 365)
            elif include_hour_of_day:
                new_features = np.concatenate((new_features, ['_HourOfDayCos_', '_HourOfDaySin_']))
                df_copy['_HourOfDayCos_'] = np.cos(df_copy.index.get_level_values(1).hour / 24)
                df_copy['_HourOfDaySin_'] = np.sin(df_copy.index.get_level_values(1).hour / 24)

        else:
            df_copy = df

        return new_features, df_copy

    def execute(self, df):

        logger.debug('Execute ' + self.whoami)

        # forecasting support
        if self.lags is not None:
            _, df_copy = self.lag_features(df=df, Train=True)
        else:
            df_copy = df.copy()

        # Create missing columns before doing group-apply
        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.features:
            if not pd.api.types.is_numeric_dtype(df_copy[feature].dtype):
                logger.error('Regression on non-numeric feature:' + str(feature))
                return (df_copy)

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        # first round - training
        df_copy = df_copy.groupby(group_base).apply(self._calc)


        # strip off lagged features
        if self.lags is not None:
            strip_features, df_copy = self.lag_features(df=df, Train=False)

            # second round - inferencing
            df_copy = df_copy.groupby(group_base).apply(self._calc)

            logger.debug('Drop artificial features ' + str(strip_features))
            df_copy.drop(columns = strip_features, inplace=True)

        logger.debug('Scoring done')
        return df_copy


    def _calc(self, df):

        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        logger.debug('GBMRegressor execute: ' + str(type(df)) + ' for entity ' + str(entity) +
                     ' predicting ' + str(self.targets) + ' from ' + str(self.features) +
                     ' to appear in ' + str(self.predictions))

        try:
            check_array(df[self.features].values, allow_nd=True)
        except Exception as e:
            logger.error(
                'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))
            return df

        try:
            dfe = super()._execute(df, entity)

            logger.debug('GBMRegressor: Entity ' + str(entity) + ' Type of pred ' +
                         str(type(dfe[self.predictions])))

            dfe.fillna(0, inplace=True)

            df[self.predictions] = dfe[self.predictions]

        except Exception as e:
            logger.info('GBMRegressor for entity ' + str(entity) + ' failed with: ' + str(e))
            df[self.predictions] = 0

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(
            UISingle(name='n_estimators', datatype=int, required=False, description='Max rounds of boosting'))
        inputs.append(
            UISingle(name='num_leaves', datatype=int, required=False, description='Max leaves in a boosting tree'))
        inputs.append(UISingle(name='learning_rate', datatype=float, required=False, description='Learning rate'))
        inputs.append(
            UISingle(name='max_depth', datatype=int, required=False, description='Cut tree to prevent overfitting'))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class SimpleRegressor(BaseEstimatorFunction):
    """
    Regressor based on stochastic gradient descent and gradient boosting method as provided by sklearn
    """
    eval_metric = staticmethod(metrics.r2_score)

    # class variables
    train_if_no_model = True
    num_rounds_per_estimator = 3

    def GBRPipeline(self):
        steps = [('scaler', StandardScaler()), ('gbr', ensemble.GradientBoostingRegressor)]
        return Pipeline(steps)

    def SGDPipeline(self):
        steps = [('scaler', StandardScaler()), ('sgd', linear_model.SGDRegressor)]
        return Pipeline(steps)

    def set_estimators(self):
        # gradient_boosted
        params = {'n_estimators': [100, 250, 500, 1000], 'max_depth': [2, 4, 10], 'min_samples_split': [2, 5, 9],
                  'learning_rate': [0.01, 0.02, 0.05], 'loss': ['ls']}
        self.estimators['gradient_boosted_regressor'] = (ensemble.GradientBoostingRegressor, params)
        logger.info('SimpleRegressor start searching for best model')

    def __init__(self, features, targets, predictions=None, n_estimators=None, num_leaves=None, learning_rate=None,
                 max_depth=None):
        super().__init__(features=features, targets=targets, predictions=predictions)

        self.experiments_per_execution = 1
        self.auto_train = True
        self.correlation_threshold = 0

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            try:
                check_array(df_copy.loc[[entity]][self.features].values)
                dfe = super()._execute(df_copy.loc[[entity]], entity)
                df_copy.loc[entity, self.predictions] = dfe[self.predictions]

            except Exception as e:
                logger.info('GBMRegressor for entity ' + str(entity) + ' failed with: ' + str(e))
                df_copy.loc[entity, self.predictions] = 0

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class SimpleAnomaly(BaseRegressor):
    """
    A supervised anomaly detection function.
     Uses a regression model to predict the value of target data items based on dependent data items or features.
     Then, it compares the actual value to the predicted valueand generates an alert when the difference falls outside of a threshold.
    """

    # class variables
    train_if_no_model = True
    num_rounds_per_estimator = 3

    def __init__(self, features, targets, threshold, predictions=None, alerts=None):
        super().__init__(features=features, targets=targets, predictions=predictions)
        if alerts is None:
            alerts = ['%s_alert' % x for x in self.targets]
        self.alerts = alerts
        self.threshold = threshold
        self.correlation_threshold = 0

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

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(UISingle(name='threshold', datatype=float,
                               description='Threshold for firing an alert. Expressed as absolute value not percent.'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='alerts', datatype=bool, cardinality_from='targets', is_datatype_derived=False, ))

        return inputs, outputs


#######################################################################################
# Forecasting
#######################################################################################

class FeatureBuilder(BaseTransformer):

    def __init__(self, features, lag, method, lagged_features):
        super().__init__()

        self.features = features
        self.lagged_features = lagged_features

        self.lag = lag   # list of integers (days) to define lags

        self.method = method   #

        self.whoami = 'FeatureBuilder'

        logger.debug(self.whoami, self.features, self.lagged_features, self.lag, self.method)

    def execute(self, df):

        df_copy = df.copy()
        entities = np.unique(df_copy.index.levels[0])
        logger.debug(str(entities))

        missing_cols = [x for x in self.lagged_features if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        for entity in entities:
            # per entity - copy for later inplace operations
            try:
                check_array(df_copy.loc[[entity]][self.features].values, allow_nd=True)
                dfe = df_copy.loc[[entity]]
            except Exception as e:
                logger.error(
                    'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))
                continue

            dfroll = dfe[self.features].rolling(window=self.lag, min_periods=0)
            if self.method == 'mean':
                dfe[self.lagged_features] = dfroll.mean().shift(1)
            elif self.method == 'stddev':
                dfe[self.lagged_features] = dfroll.std().shift(1)
            else:
                dfe[self.lagged_features] = dfe[self.features].shift(1)

            df_copy.loc[entity, self.lagged_features] = dfe[self.lagged_features]

        return df_copy

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True, output_item='lagged_features',
                                  is_output_datatype_derived=True))
        inputs.append(UISingle(name='lag', datatype=int, description='Lag for each input_item'))
        inputs.append(UISingle(name='method', datatype=str, description='Method: Plain, Mean, Stddev'))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


class GBMForecaster(GBMRegressor):
    """
    Forecasting regressor based on gradient boosting method as provided by lightGBM
    """
    def __init__(self, features, targets, predictions=None, lags=None):
        #
        # from https://github.com/ashitole/Time-Series-Project/blob/main/Auto-Arima%20and%20LGBM.ipynb
        #   as taken from https://www.kaggle.com/rohanrao/ashrae-half-and-half
        #
        super().__init__(features=features, targets=targets, predictions=predictions, n_estimators=500,
                        num_leaves=40, learning_rate=0.2, max_depth=-1, lags=lags)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        inputs.append(UIMulti(name='lags', datatype=int, description='Comma separated list of lags'))

        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


#######################################################################################
# ARIMA
#######################################################################################

# self.model_class = STLForecast(np.arange(0,1), ARIMA, model_kwargs=dict(order=(1,1,1), trend="c"), period=7*24)
class ARIMAForecaster(SupervisedLearningTransformer):
    """
    Provides a forecast for 'n_forecast' data points for from endogenous data in input_item
    Data is returned as input_item shifted by n_forecast positions with the forecast appended
    """

    def __init__(self, input_item, n_forecast, output_item):
        super().__init__(features=[input_item], targets=[output_item])

        self.input_item = input_item
        self.n_forecast = n_forecast
        self.output_item = output_item

        self.power = None    # used to store box cox lambda
        self.active_models = dict()

        self.name = 'ARIMAForecaster'


    def execute(self, df):
        # set output columns to zero
        df[self.output_item] = 0

        # check data type
        if df[self.input_item].dtype != np.float64:
            logger.error('ARIMA forecasting on non-numeric feature:' + str(self.input_item))
            return df

        return super().execute(df)


    # EXCLUDED until we upgrade to statsmodels 0.12
    '''
    def _calc(self, df):
        # per entity - copy for later inplace operations
        #entity = df.index.levels[0][0]
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        df = df.droplevel(0)

        model_name, arima_model, version = self.load_model(suffix=entity)

        logger.debug('Module ARIMA Forecaster, Entity: ' + str(entity) + ', Input: ' + str(
                     self.input_item) + ', Forecasting: ' + str(self.n_forecast) + ', Output: ' + str(
                     self.output_item))

        feature = df[self.input_item].values

        if arima_model is None and self.auto_train:

            # all variables should be continuous
            stlf = STLForecast(temperature, ARIMA, model_kwargs=dict(order=(1, 0, 1), trend="n"), period=7*24)

            arima_model = stlf.fit()

            logger.debug('Created STL + ARIMA' + str(arima_model))

            try:
                db.model_store.store_model(model_name, arima_model)
            except Exception as e:
                logger.error('Model store failed with ' + str(e))
                pass

        # remove n_forecast elements and append the forecast of length n_forecast
        predictions_ = arima_model.forecast(self.n_forecast)
        logger.debug(predictions_.shape, temperature.shape)
        predictions = np.hstack([temperature[self.n_forecast:].reshape(-1,), predictions_])

        self.active_models[entity] = arima_model

        logger.debug(arima_model.summary())

        df[self.output_item] = predictions

        return df
    '''

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Data item to interpolate'))

        inputs.append(
            UISingle(name='n_forecast', datatype=int, description='Forecasting n_forecast data points.'))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Interpolated data'))
        return inputs, outputs


#
# following Jake Vanderplas Data Science Handbook
#   https://jakevdp.github.io/PythonDataScienceHandbook/05.13-kernel-density-estimation.html
#

class KDEAnomalyScore(SupervisedLearningTransformer):
    """
    A supervised anomaly detection function.
     Uses kernel density estimate to assign an anomaly score
    """
    def __init__(self, threshold, features, targets, predictions=None):
        logger.debug("init KDE Estimator")
        self.name = 'KDEAnomalyScore'
        self.whoami= 'KDEAnomalyScore'
        super().__init__(features, targets)

        self.threshold = threshold
        self.active_models = dict()
        if predictions is None:
            predictions = ['predicted_%s' % x for x in self.targets]
        self.predictions = predictions

    def execute(self, df):

        # Create missing columns before doing group-apply
        df = df.copy()
        missing_cols = [x for x in self.predictions if x not in df.columns]
        for m in missing_cols:
            df[m] = None

        return super().execute(df)


    def _calc(self, df):

        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        logger.debug('KDEAnomalyScore execute: ' + str(type(df)) + ' for entity ' + str(entity))

        # check data okay
        try:
            logger.debug(self.features)
            check_array(df[self.features].values, allow_nd=True)
        except Exception as e:
            logger.error(
                'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))

        # per entity - copy for later inplace operations
        model_name, kde_model, version = self.load_model(suffix=entity)

        xy = np.hstack([df[self.features].values, df[self.targets].values])

        # train new model
        if kde_model is None:

            logger.debug('Running KDE with ' + str(xy.shape))
            # all variables should be continuous
            kde_model = KDEMultivariate(xy, var_type="c" * (len(self.features) + len(self.targets)))
            logger.debug('Created KDE ' + str(kde_model))

            try:
                db.model_store.store_model(model_name, kde_model)
            except Exception as e:
                logger.error('Model store failed with ' + str(e))

        self.active_models[entity] = kde_model

        predictions = kde_model.pdf(xy).reshape(-1,1)
        print(predictions.shape, df[self.predictions].values.shape)
        df[self.predictions] = predictions

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name="threshold", datatype=float,
                               description="Probability threshold for outliers. Typically set to 10e-6.", required=True))
        inputs.append(UIMultiItem(name='features', datatype=float, required=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True, output_item='predictions',
                                  is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs


'''
    def fit(self, X, y):
        xy = np.vstack(X, y).T
        self.kde = KDEMultivariate(xy, var_type='cc')
        return self

    def predict_proba(self, X):
        logprobs = np.vstack([model.score_samples(X)
                              for model in self.models_]).T
        result = np.exp(logprobs + self.logpriors_)
        return result / result.sum(1, keepdims=True)

    def predict(self, X):
        return self.classes_[np.argmax(self.predict_proba(X), 1)]
'''


#######################################################################################
# Variational Autoencoder
#   to approximate probability distribution of targets with respect to features
#######################################################################################
# from https://www.ritchievink.com/blog/2019/09/16/variational-inference-from-scratch/
#   usual ELBO with standard prior N(0,1), standard reparametrization

# helper function
def ll_gaussian(y, mu, log_var):
    sigma = torch.exp(0.5 * log_var)
    return -0.5 * torch.log(2 * np.pi * sigma**2) - (1 / (2 * sigma**2)) * (y-mu)**2


def l_gaussian(y, mu, log_var):
    sigma = torch.exp(0.5 * log_var)
    return 1/torch.sqrt(2 * np.pi * sigma**2) / torch.exp((1 / (2 * sigma**2)) * (y-mu)**2)


# not used - 100% buggy
def kl_div(mu1, mu2, lg_sigma1, lg_sigma2):
    return 0.5 * torch.sum(2 * lg_sigma2 - 2 * lg_sigma1 + (lg_sigma1.exp() ** 2 + (mu1 - mu2)**2)/lg_sigma2.exp()**2 - 1)


class VI(nn.Module):
    def __init__(self, scaler, prior_mu=0.0, prior_sigma=1.0, beta=1.0, adjust_mean=0.0, version=None):
        self.prior_mu = prior_mu
        self.prior_sigma = prior_sigma
        self.beta = beta
        self.onnx_session = None
        self.version = version
        self.build_time = pd.Timestamp.now()
        self.scaler = scaler
        self.show_once = True
        self.adjust_mean = adjust_mean
        super().__init__()

        self.q_mu = nn.Sequential(
            nn.Linear(1, 20),
            nn.ReLU(),
            nn.Linear(20, 10),
            nn.ReLU(),
            nn.Linear(10, 1)
        )

        self.q_log_var = nn.Sequential(
            nn.Linear(1, 50),    # more parameters for sigma
            nn.ReLU(),
            nn.Linear(50, 35),
            nn.ReLU(),
            nn.Linear(35, 10),
            nn.ReLU(),
            nn.Linear(10, 1)
        )

    # draw from N(mu, sigma)
    def reparameterize(self, mu, log_var):
        # std can not be negative, thats why we use log variance
        sigma = torch.add(torch.exp(0.5 * log_var), 1e-7)
        eps = torch.randn_like(sigma)
        return mu + sigma * eps

    # sample from the one-dimensional normal distribution N(mu, exp(log_var))
    def forward(self, x):
        mu = self.q_mu(x)
        log_var = self.q_log_var(x)
        return self.reparameterize(mu, log_var), mu, log_var

    # see 2.3 in https://arxiv.org/pdf/1312.6114.pdf
    #
    def elbo(self, y_pred, y, mu, log_var):
        # likelihood of observing y given Variational mu and sigma - reconstruction error
        loglikelihood = ll_gaussian(y, mu, log_var)

        # Sample from p(x|z) by sampling from q(z|x), passing through decoder (y_pred)
        # likelihood of observing y given Variational decoder mu and sigma - reconstruction error
        log_qzCx = ll_gaussian(y, mu, log_var)

        # KL - probability of sample y_pred w.r.t. prior
        log_pz = ll_gaussian(y_pred, self.prior_mu, torch.log(torch.tensor(self.prior_sigma)))

        # KL - probability of sample y_pred w.r.t the variational likelihood
        log_pxCz = ll_gaussian(y_pred, mu, log_var)

        ## Alternatively we could compute the KL div to the gaussian prior with something like
        # KL = -0.5 * torch.sum(1 + log_var - torch.square(mu) - torch.exp(log_var)))

        if self.show_once:
            self.show_once = False
            logger.info('Cardinalities: Mu: ' + str(mu.shape) + ' Sigma: ' + str(log_var.shape) +
                        ' loglikelihood: ' + str(log_qzCx.shape) + ' KL value: ' +
                        str((log_pz - log_pxCz).mean()))

        # by taking the mean we approximate the expectation according to the law of large numbers
        return (log_qzCx + self.beta * (log_pz - log_pxCz)).mean()


    # from https://arxiv.org/pdf/1509.00519.pdf
    #  and https://justin-tan.github.io/blog/2020/06/20/Intuitive-Importance-Weighted-ELBO-Bounds
    def iwae(self, x, y, k_samples):

        log_iw = None
        for _ in range(k_samples):

            # Encode - sample from the encoder
            #  Latent variables mean,variance: mu_enc, log_var_enc
            # y_pred: Sample from q(z|x) by passing data through encoder and reparametrizing
            y_pred, mu_enc, log_var_enc = self.forward(x)

            # there is not much of a decoder - hence we use the identity below as decoder 'stub'
            dec_mu = mu_enc
            dec_log_var = log_var_enc

            # Sample from p(x|z) by sampling from q(z|x), passing through decoder (y_pred)
            # likelihood of observing y given Variational decoder mu and sigma - reconstruction error
            log_qzCx = ll_gaussian(y, dec_mu, dec_log_var)

            # KL (well, not true for IWAE) - prior probability of y_pred w.r.t. N(0,1)
            log_pz = ll_gaussian(y_pred, self.prior_mu, torch.log(torch.tensor(self.prior_sigma)))

            # KL (well, not true for IWAE) - probability of y_pred w.r.t the decoded variational likelihood
            log_pxCz = ll_gaussian(y_pred, dec_mu, dec_log_var)

            i_sum = log_qzCx + log_pz - log_pxCz
            if log_iw is None:
                log_iw = i_sum
            else:
                log_iw = torch.cat([log_iw, i_sum], 1)

        # loss calculation
        log_iw = log_iw.reshape(-1, k_samples)

        iwelbo = torch.logsumexp(log_iw, dim=1) - np.log(k_samples)

        return iwelbo.mean()


class VIAnomalyScore(SupervisedLearningTransformer):
    """
    A supervised anomaly detection function.
     Uses VAE based density approximation to assign an anomaly score
    """
    # set self.auto_train and self.delete_model
    def __init__(self, features, targets, predictions=None, pred_stddev=None):

        self.name = "VIAnomalyScore"
        self.whoami = "VIAnomalyScore"
        super().__init__(features, targets)

        self.epochs = 80  # turns out to be sufficient for IWAE
        self.learning_rate = 0.005
        self.quantile = 0.99

        self.active_models = dict()
        self.Input = {}
        self.Output = {}
        self.mu = {}
        self.quantile099 = {}

        if predictions is None:
            predictions = ['predicted_%s' % x for x in self.targets]
        if pred_stddev is None:
            pred_stddev = ['pred_dev_%s' % x for x in self.targets]

        self.predictions = predictions
        self.pred_stddev = pred_stddev

        # make sure mean is > 0
        self.prior_mu = 0.0
        self.prior_sigma = 1.0
        self.beta = 1.0
        self.iwae_samples = 10

    def execute(self, df):

        # Create missing columns before doing group-apply
        df = df.copy()
        missing_cols = [x for x in (self.predictions + self.pred_stddev) if x not in df.columns]
        for m in missing_cols:
            df[m] = None

        return super().execute(df)


    def _calc(self, df):

        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        logger.debug('VIAnomalyScore execute: ' + str(type(df)) + ' for entity ' + str(entity))

        # check data okay
        try:
            logger.debug(self.features)
            check_array(df[self.features].values, allow_nd=True)
        except Exception as e:
            logger.error(
                'Found Nan or infinite value in feature columns for entity ' + str(entity) + ' error: ' + str(e))

        # per entity - copy for later inplace operations
        model_name, vi_model, version = self.load_model(suffix=entity)

        # learn to scale features
        if vi_model is not None:
            scaler = vi_model.scaler
        else:
            scaler = StandardScaler().fit(df[self.features].values)

        features = scaler.transform(df[self.features].values)
        targets = df[self.targets].values

        # deal with negative means - ReLU doesn't like negative values
        #  adjust targets wth the (positive) prior mean to make sure it's > 0
        if vi_model is None:
            adjust_mean = targets.mean() - self.prior_mu
        else:
            adjust_mean = vi_model.adjust_mean
        logger.info('Adjusting target mean with ' + str(adjust_mean))
        targets -= adjust_mean

        xy = np.hstack([features, targets])

        # TODO: assumption is cardinality of One for features and targets !!!
        ind = np.lexsort((xy[:, 1], xy[:, 0]))
        ind_r = np.argsort(ind)

        self.Input[entity] = xy[ind][:, 0]

        X = torch.tensor(xy[ind][:, 0].reshape(-1, 1), dtype=torch.float)
        Y = torch.tensor(xy[ind][:, 1].reshape(-1, 1), dtype=torch.float)

        # train new model if there is none and autotrain is set
        if vi_model is None and self.auto_train:

            # equip prior with a 'plausible' deviation
            self.prior_sigma = targets.std()

            vi_model = VI(scaler, prior_mu=self.prior_mu, prior_sigma=self.prior_sigma,
                          beta=self.beta, adjust_mean=adjust_mean, version=version)

            logger.debug('Training VI model ' + str(vi_model.version) + ' for entity: ' + str(entity) +
                         'Prior mean: ' + str(self.prior_mu) + ', sigma: ' + str(self.prior_sigma))

            optim = torch.optim.Adam(vi_model.parameters(), lr=self.learning_rate)

            for epoch in range(self.epochs):
                optim.zero_grad()
                y_pred, mu, log_var = vi_model(X)
                loss = -vi_model.elbo(y_pred, Y, mu, log_var)
                iwae = -vi_model.iwae(X, Y, self.iwae_samples)  # default is to try with 10 samples
                if epoch % 10 == 0:
                    logger.debug('Epoch: ' + str(epoch) + ', neg ELBO: ' + str(loss.item()) + ', IWAE ELBO: ' + str(iwae.item()))

                #loss.backward()
                iwae.backward()
                optim.step()

            logger.debug('Created VAE ' + str(vi_model))

            try:
                db.model_store.store_model(model_name, vi_model)
            except Exception as e:
                logger.error('Model store failed with ' + str(e))

        # check if training was not allowed or failed
        if vi_model is not None:
            self.active_models[entity] = vi_model

            with torch.no_grad():
                mu_and_log_sigma = vi_model(X)
                mue = mu_and_log_sigma[1]
                sigma = torch.exp(0.5 * mu_and_log_sigma[2]) + 1e-5
                mu = sp.stats.norm.ppf(0.5, loc=mue, scale=sigma).reshape(-1,)
                q1 = sp.stats.norm.ppf(self.quantile, loc=mue, scale=sigma).reshape(-1,)
                self.mu[entity] = mu
                self.quantile099[entity] = q1

            df[self.predictions] = (mu[ind_r] + vi_model.adjust_mean).reshape(-1,1)
            df[self.pred_stddev] = (q1[ind_r]).reshape(-1,1)
        else:
            logger.debug('No VI model for entity: ' + str(entity))

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='features', datatype=float, required=True,
                                  output_item='pred_stddev', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='targets', datatype=float, required=True,
                                  output_item='predictions', is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        return inputs, outputs

#######################################################################################
# Crude change point detection
#######################################################################################

def make_histogram(t, bins):
    rv = ''
    if t is None:
        logger.warning('make_histogram encountered None')
        return rv
    logger.info('make_histogram ' + str(type(t)) + ' ' + str(t.shape))
    if np.isnan(t).any():
        logger.warning('make_histogram encountered NaN')
        return rv
    try:
        tv = minmax_scale(t.values)
        hist = np.histogram(tv, bins=bins, density=True)
        logger.info('make_histogram returns ' + str(hist))
        rv = str(hist[0])
    except Exception as e:
        logger.warning('make_histogram np.hist failed with ' + str(e))
    return rv


class HistogramAggregator(BaseSimpleAggregator):
    """
    The docstring of the function will show as the function description in the UI.
    """

    def __init__(self, source=None, bins=None):

        super().__init__()

        self.input_item = source
        if bins is None:
            self.bins = 15
        else:
            self.bins = int(bins)

    def execute(self, group):
        #
        # group is a series
        #   when calling agg(<aggregator functions>) for each element of the group dictionary
        #   df_input.groupby([pd.Grouper(freq='1H', level='timestamp'), pd.Grouper(level='deviceid')])
        #
        return make_histogram(group, self.bins)

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='source', datatype=float,
                                   description='Choose the data items that you would like to aggregate'))
        # output_item='name', is_output_datatype_derived=True))
        inputs.append(UISingle(name='bins', datatype=int, description='Histogram bins - 15 by default'))

        outputs = []
        outputs.append(UIFunctionOutSingle(name='name', datatype=str, description='Histogram encoded as string'))
        return inputs, outputs


# SROM Org Function Scorer
class AutoRegScore(SupervisedLearningTransformer):
    """_summary_
    """
    category = 'PreTrainedPipeline'
    tags = None

    #def __init__(self, features, output, model=None):
    def __init__(self, features, target, prediction, model=None):
        if isinstance(features, set): features = list(features)
        super().__init__(features, [target], [prediction])
        self.auto_train = True
        self.model = model
        self.whoami = 'AutoRegScore'
        #self.category = 'PreTrainedPipeline'
        #self.tag = None

    def _calc(self, df):

        entity = df.index[0][0]

        try:
            df[self.predictions] = self.model.predict(df[self.features])
        except Exception as e:
            logger.info('AutoReg for entity failed with: ' + str(e))

        return df

    def execute(self, df):

        df_copy = df.copy()
        logger.info('AutoReg ' + ' Inference, Features: ' + str(self.features) + ' Targets: ' + str(self.targets))

        missing_cols = [x for x in self.targets + self.predictions if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = None

        db = self.get_db()

        # model singleton first
        #model_name, autoreg_model, version = self.load_model(suffix=entity)
        if self.model is None:
            model_name, self.model, version = self.load_model()

        if self.model is None and self.auto_train:

            logger.info('AutoReg ' + ' Train ' + model_name)
            steps = [('scaler', StandardScaler()),
                     ('lin', linear_model.BayesianRidge(compute_score=True))]
                     #('gbm', GradientBoostingRegressor(n_estimators = 500, learning_rate=0.2))]
            self.model = Pipeline(steps)

            df_train, df_test = train_test_split(df_copy, test_size=0.2)

            try:
                # do some interesting stuff
                self.model.fit(df_train[self.features], df_train[self.targets])
                db.model_store.store_model(model_name, self.model)
            except Exception as e:
                logger.error('Training failed with ' + str(e))

        if self.model is None:
            # go away if training failed (ignore failures to save the model)
            df[self.targets[0]] = 0
            return df

        # evaluate on a per entity basis
        return super().execute(df)

