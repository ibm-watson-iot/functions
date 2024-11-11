# *****************************************************************************
# © Copyright IBM Corp. 2024  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

"""
The experimental functions module contains (no surprise here) experimental functions
"""
import datetime as dt
import logging
import re
import time
import warnings
import ast
import os
import subprocess
import importlib
from collections import OrderedDict
from datetime import date, timedelta, datetime

import numpy as np
import scipy as sp
import pandas as pd
from sqlalchemy import String

import torch
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

from .base import (BaseTransformer, BaseEvent, BaseSCDLookup, BaseSCDLookupWithDefault, BaseMetadataProvider,
                   BasePreload, BaseDatabaseLookup, BaseDataSource, BaseDBActivityMerge, BaseSimpleAggregator,
                   DataExpanderTransformer)
from .bif import (InvokeWMLModel)
from .loader import _generate_metadata
from .ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti, UIExpression,
                 UIText, UIParameters)
from .util import adjust_probabilities, reset_df_index, asList, UNIQUE_EXTENSION_LABEL

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

# Do away with numba and onnxscript logs
numba_logger = logging.getLogger('numba')
numba_logger.setLevel(logging.INFO)
onnx_logger = logging.getLogger('onnxscript')
onnx_logger.setLevel(logging.ERROR)


def install_and_activate_granite_tsfm():
    #db.install_package("git+https://github.com/sedgewickmm18/granite-tsfm")
    #db.import_target("tsfm_public.models","tinytimemixer", "TinyTimeMixerForPrediction")

    url = "git+https://github.com/sedgewickmm18/granite-tsfm"
    try:
        sequence = ['pip', 'install', '--no-cache-dir', '--upgrade', url]
        completedProcess = subprocess.run(sequence, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                                                  universal_newlines=True)
    except Exception as e:
        #raise ImportError('pip install for url %s failed: \n%s', url, str(e))
        logger.error('Could not download from ' + url)
        return False

    #if completedProcess.returncode == 0:

    if True:
        importlib.invalidate_caches()
        logger.debug('pip install for url %s was successful: \n %s', url, completedProcess.stdout)

    else:
        #raise ImportError('pip install for url %s failed: \n %s.', url, completedProcess.stdout)
        logger.error('Could not install ' + url + ', ' + str(completedProcess.stdout))
        return False
    
    try:
        exec('from tsfm_public.models.tinytimemixer import TinyTimeMixerForPrediction')
    except Exception as e:
        logger.error('Could not import TinyTimeMixers')
        return False

    return True

class TSFMZeroShotScorer(InvokeWMLModel):
    """
    Call time series foundation model
    """
    def __init__(self, input_items, output_items=None, context=512, horizon=96, watsonx_auth=None):
        logger.debug(str(input_items) + ', ' + str(output_items) + ', ' + str(context) + ', ' + str(horizon))

        super().__init__(input_items, watsonx_auth, output_items)

        self.context = context
        if context <= 0:
            self.context = 512
        self.horizon = horizon
        if horizon <= 0:
            self.horizon = 96
        self.whoami = 'TSFMZeroShot'

        # allow for expansion of the dataframe
        self.allowed_to_expand = True
    
        self.init_local_model = install_and_activate_granite_tsfm()
        self.model = None              # cache model for multiple calls


    # ask for more data if we do not have enough data for context and horizon
    def check_size(self, size_df):
        return min(size_df) < self.context + self.horizon

    # TODO implement local model lookup and initialization later
    # initialize local model is a NoOp for superclass
    def initialize_local_model(self):
        logger.info('initialize local model')
        try:
            from tsfm_public.models.tinytimemixer import TinyTimeMixerForPrediction
            TTM_MODEL_REVISION = "main"
            # Forecasting parameters
            #context_length = 512
            #forecast_length = 96
            #install_and_activate_granite_tsfm()
            self.model = TinyTimeMixerForPrediction.from_pretrained("ibm/TTM", cache_dir='/tmp', revision=TTM_MODEL_REVISION)
        except Exception as e:
            logger.error("Failed to load local model with error " + str(e))
            return False
        logger.info('local model ready')
        return True

    # inference on local model 
    def call_local_model(self, df):
        logger.info('call local model')

        logger.debug('df columns  ' + str(df.columns))
        logger.debug('df index ' + str(df.index.names))

        # size of the df should be fine
        len = self.context + self.horizon

        if self.model is not None:

            logger.debug('Forecast ' + str(df.shape[0]/self.horizon) + ' times')
            df[self.output_items] = 0

            #for i in range(self.context, df.shape[0], self.horizon):
            for i in range(df.shape[0] - len, 0 , -self.horizon):
                #inputtensor_ = torch.from_numpy(df[i-self.context:i][self.input_items].values).to(torch.float32)
                inputtensor_ = torch.from_numpy(df[i:i+self.context][self.input_items].values).to(torch.float32)

                #logger.debug('shape   input ' + str(inputtensor_.shape))
                # add dimension
                #inputtensor = inputtensor_[None,:self.context,:]              # only the historic context
                inputtensor = inputtensor_[None,:,:]              # only the historic context
                #logger.debug('shape   input ' + str(inputtensor.shape))
                outputtensor = self.model(inputtensor)['prediction_outputs']  # get the forecasting horizon back
                #logger.debug('shapes   input ' + str(inputtensor.shape) + ' , output ' + str(outputtensor.shape))
                #   and update the dataframe with it
                #df.loc[df.tail(self.horizon).index, self.output_items] = outputtensor[0].detach().numpy()
                try:
                    df.loc[df[i:i + self.horizon].index, self.output_items] = outputtensor[0].detach().numpy().astype(float)
                except:
                    logger.debug('Issue with ' + str(i) + ':' + str(i+self.horizon))
                    pass

        return df

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []

        inputs.append(UIMultiItem(name='input_items', datatype=float, required=True, output_item='output_items',
                                  is_output_datatype_derived=True))
        inputs.append(
            UISingle(name='context', datatype=int, required=False, description='Context - past data'))
        inputs.append(
            UISingle(name='horizon', datatype=int, required=False, description='Forecasting horizon'))
        inputs.append(UISingle(name='watsonx_auth', datatype=str,
                               description='Endpoint to the WatsonX service where model is hosted', tags=['TEXT'], required=True))

        # define arguments that behave as function outputs
        outputs=[]
        #outputs.append(UISingle(name='output_items', datatype=float))
        return inputs, outputs


class ProphetForecaster(DataExpanderTransformer):

    def __init__(self, input_items, y_hat=None, y_date=None):
        self.input_items = input_items
        self.y_hat= y_hat
        self.y_date= y_date
        '''
        self.horizon = horizon
        if horizon <= 0:
            self.horizon = 10
        '''
        self.can_train = True
        self.whoami = 'ProphetForecaster'


    def execute(self, df):
        logger.debug('Execute ' + self.whoami)

        # obtain db handler
        db = self.get_db()

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.input_items:
            if not pd.api.types.is_numeric_dtype(df[feature].dtype):
                logger.error('Training forecaster on non-numeric feature:' + str(feature))
                self.can_train = False

        # Create missing columns before doing group-apply
        df_copy = df.copy()

        column_list = [self.y_hat]  # list will get longer
        missing_cols = [x for x in column_list if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = 0

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # group over entities
        group_base = [pd.Grouper(axis=0, level=0)]

        df_copy = df_copy.groupby(group_base).apply(self._calc)

        logger.debug('Scoring done')

        return df_copy


    def _calc(self, df):
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        # get rid of entity id as part of the index
        df = df.droplevel(0)

        # get model
        model_name = self.generate_model_name([], self.y_hat, prefix='Prophet', suffix=entity)
        prophet_model_json = None
        prophet_model = None

        try:
            prophet_model_json = db.model_store.retrieve_model(model_name, deserialize=False)
            logger.debug('load model %s' % str(prophet_model_json))
        except Exception as e:
            logger.error('Model retrieval for %s failed with %s', model_name, str(e))
            return df

        try:
            prophet_model = model_from_json(prophet_model_json)
        except Exception as e:
            logger.error('Deserializing prophet model failed with ' + str(e)) 
            return df

        # pass input features - only needed for inline training

        # for now just take the number of rows - assume daily frequency for now
        # future_dates column name 'ds' as Prophet expects it
        future_dates = pd.date_range(start=df.tail(1).index[0], periods=df.shape[0], freq='D').to_frame(index=False, name='ds')
        #logger.debug('Future values start/end/length ' + str(future_dates[0]) + ', ' + str(future_dates[-1]) + ', ' + str(future_dates.shape[0]))
        logger.debug('Future values ' + str(future_dates.describe))

        prediction=prophet_model.predict(future_dates)

        df[self.y_hat] = prediction['yhat'].values
        df[self.y_date] = future_dates.values
        return df

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs, output contains the time shifted forecasts
        inputs = []

        inputs.append(UIMultiItem(name='input_items', datatype=float, required=True))
        #inputs.append(
        #    UISingle(name='history', datatype=int, required=False, description='History length for training'))
        #inputs.append(
        #    UISingle(name='horizon', datatype=int, required=False, description='Forecasting horizon length'))

        # define arguments that behave as function outputs
        outputs=[]
        # we might need more like 'yhat', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper' ...
        outputs.append(UISingle(name='y_hat', datatype=float, description='Forecasted occupancy'))
        outputs.append(UISingle(name='y_date', datatype=float, description='Date for forecasted occupancy'))

        return inputs, outputs
