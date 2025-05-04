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
import json 
import warnings
import ast
import os
import subprocess
import importlib
from collections import OrderedDict

import numpy as np
import scipy as sp
import pandas as pd
from sqlalchemy import String

import torch
from torch.nn import functional as f

import holidays
import datetime as dt
from datetime import timedelta
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json

from iotfunctions.base import (BaseTransformer, BaseEvent, BaseSCDLookup, BaseSCDLookupWithDefault, BaseMetadataProvider,
                   BasePreload, BaseDatabaseLookup, BaseDataSource, BaseDBActivityMerge, BaseSimpleAggregator,
                   DataExpanderTransformer)
from iotfunctions.bif import (InvokeWMLModel)
from iotfunctions.loader import _generate_metadata
from iotfunctions.ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti, UIExpression,
                 UIText, UIParameters)
from iotfunctions.util import adjust_probabilities, reset_df_index, asList, UNIQUE_EXTENSION_LABEL

from tsfm_public.models.tinytimemixer import TinyTimeMixerForPrediction

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

# Do away with numba and onnxscript logs
numba_logger = logging.getLogger('numba')
numba_logger.setLevel(logging.INFO)
onnx_logger = logging.getLogger('onnxscript')
onnx_logger.setLevel(logging.ERROR)

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

        # only use the first half of the forecast
        self.use_horizon = self.horizon // 2

        # allow for expansion of the dataframe
        self.allowed_to_expand = True
    
        self.init_local_model = True # install_and_activate_granite_tsfm()
        self.model = None              # cache model for multiple calls


    # ask for more data if we do not have enough data for context and horizon
    def check_size(self, size_df):
        return min(size_df) < self.context + self.horizon

    # TODO implement local model lookup and initialization later
    # initialize local model is a NoOp for superclass
    def initialize_local_model(self):
        logger.info('initialize local model')
        try:
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
        logger.info('call local model ----- ')

        logger.debug('df columns  ' + str(df.columns))
        logger.debug('df index ' + str(df.index.names))

        # size of the df should be fine
        full_length = self.context + self.horizon
        df[self.output_items] = 0

        if self.model is not None:

            logger.debug('Forecast ' + str(df.shape[0]/self.use_horizon) + ' times')

            # for default overlap (use_horizon) we better use unfold

            # so we get all relevant features in a tensor with dimensions
            #     features x time
            inputtensor_ = torch.from_numpy(df[self.input_items].values).to(torch.float32)

            if len(list(inputtensor_.shape)) < 2:
                inputtensor_ = inputtensor_[None, :]

            print("we got", inputtensor_.T.shape)
            # and slice it up along the time dimension (potentially ditching last elements)
            #   dimensions: features x slices x time
            inputtensor = inputtensor_.T.unfold(1, self.context, self.use_horizon)
            print("we unfolded it into", inputtensor.shape)

            #print(inputtensor[0,14,:])

            # TTM expects the dimensions permuted: features x time x slices
            # and we get a result tensor back with dimensions features x forecast_time x slices
            outputtensor = self.model(inputtensor.permute(0,2,1))['prediction_outputs']
            print("the model produced", outputtensor.shape)

            #print(outputtensor[0,:,14])

            # we fold the sequence of forecasts back into its original shape: features x time
            fold_back = f.fold(outputtensor, (1, self.use_horizon * (outputtensor.shape[2] + 1)) ,
                          kernel_size=(1, outputtensor.shape[1]), stride=(1,self.use_horizon)).squeeze()
            print(fold_back[512:1024])
            #fold_back2 = f.fold(outputtensor, (1, 48 * (157+1)), kernel_size=(1, 96), stride=(1,48))
            print("we folded it", fold_back.shape)

            try:
                nr_features = df[self.output_items].shape[1]
                if len(list(fold_back.shape)) < 2:
                    #fold_back = fold_back[None, :]
                    nr_features = 1
                    forecast_tensor = fold_back.detach().resize_(len(df.index) - self.context)
                else:
                    forecast_tensor = fold_back.detach().resize_(fold_back.shape[0], len(df.index) - self.context).permute(1,0)
                forecast = forecast_tensor.numpy().astype(float)
                #if nr_features > 1:
                #    forecast = forecast.reshape(-1, nr_features)
                print(nr_features, forecast.shape)
                #forecast = fold_back.detach().numpy().astype(float)
                print("finally", forecast.shape, df.loc[df[self.context:].index, self.output_items].shape)
                df.loc[df[self.context:].index, self.output_items] = forecast
            except Exception as e:
                # indexing issue shouldn't happen
                logger.debug('Issue with:' + str(self.use_horizon) + '    ' + str(e))
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

    def __init__(self, input_items, country_code=None, y_hat=None, yhat_lower=None, yhat_upper=None, y_date=None):
        super().__init__(input_items)
        self.input_items = input_items
        self.country_code = country_code  # TODO make sure it's a country code
        self.y_hat= y_hat
        self.yhat_lower = yhat_lower
        self.yhat_upper = yhat_upper
        self.y_date = y_date
        self.output_items = [y_hat, yhat_lower, yhat_upper, y_date]  # for DataExpander

        try:
            holidays.country_holidays(country_code, years=2024)
        except Exception as e:
            self.country_code = None
            logger.info('Invalid country code')
            pass

        # allow for expansion of the dataframe
        self.has_access_to_db = True
        self.allowed_to_expand = True
        self.can_train = True

        self.whoami = 'ProphetForecaster'
        self.name = 'ProphetForecaster'


    def execute(self, df):
        logger.debug('Execute ' + self.whoami)

        # obtain db handler
        db = self.get_db()

        if not hasattr(self, 'dms'): 
            # indicate that we must not attempt to load more data
            self.has_access_to_db = False
            logger.warning('Started without database access')

        # check data type
        #if df[self.input_item].dtype != np.float64:
        for feature in self.input_items:
            if not pd.api.types.is_numeric_dtype(df[feature].dtype):
                logger.error('Training forecaster on non-numeric feature:' + str(feature))
                self.can_train = False

        # Create missing columns before doing group-apply
        df_copy = df.copy()

        column_list = self.output_items
        missing_cols = [x for x in column_list if x not in df_copy.columns]
        for m in missing_cols:
            df_copy[m] = 0

        # delegate to _calc
        logger.debug('Execute ' + self.whoami + ' enter per entity execution')

        # check whether we have models available or need to train first
        entities = np.unique(df_copy.index.get_level_values(0).to_numpy())
        must_train = False
        logger.debug('Do we need data expansion for training ?')
        for entity in entities:
            model_name = self.generate_model_name([], self.y_hat, prefix='Prophet', suffix=entity)
            try:
                prophet_model_bytes = db.model_store.retrieve_model(model_name, deserialize=False)
                prophet_model_json = prophet_model_bytes.decode('utf-8')
            except Exception as e:
                logger.info('Must train first')
                must_train = True
                break

        # get more data if we must train, haven't loaded data yet and ..
        # we have access to our database and are allowed to go to it
        logger.debug('must_train: ' + str(must_train) + ', original_frame: ' + str(self.original_frame) + \
                ', has_access_to_db: ' + str(self.has_access_to_db) + ', allowed_to_expand: ' + str(self.allowed_to_expand))
        if must_train:
            logger.debug('Must train first')
            df_new = None
            if self.original_frame is None and self.has_access_to_db and self.allowed_to_expand:
                logger.info('Expand dataset')

                # TODO compute the lookback parameter, 6 months of data
                df_new = self.expand_dataset(df_copy,
                     (np.unique(df_copy.index.get_level_values(0).values).shape[0] + 1) * 180)

                # drop NaN for input items and create output items
                #df_new[self.input_items] = df_new[self.input_items].fillna(0)

                column_list = [self.y_hat, self.y_date]  # list will get longer
                column_list = self.output_items
                missing_cols = [x for x in column_list if x not in df_copy.columns]
                for m in missing_cols:
                    df_new[m] = None

            else: # use present data as training data
                df_new = df_copy

            # drive by-entity training with the expanded dataset
            if df_new is not None:
                group_base = [pd.Grouper(axis=0, level=0)]
                df_new = df_new.groupby(group_base, group_keys=False).apply(self._train)

        # now we're in a position to score
        group_base = [pd.Grouper(axis=0, level=0)]

        df_copy = df_copy.groupby(group_base, group_keys=False).apply(self._calc)

        logger.debug('Scoring done')

        return df_copy

    def _train(self, df):
        logger.info('_train')
        entity = df.index[0][0]

        # obtain db handler
        db = self.get_db()

        # get model
        model_name = self.generate_model_name([], self.y_hat, prefix='Prophet', suffix=entity)
        prophet_model_json = None
        prophet_model = None

        try:
            prophet_model_bytes = db.model_store.retrieve_model(model_name, deserialize=False)
            prophet_model_json = prophet_model_bytes.decode('utf-8')
            logger.debug('load model %s' % str(prophet_model_json)[0:40])
        except Exception as e:
            logger.debug('could not load model %s' % str(model_name))
            # ToDo exception handling
            #logger.error('Model retrieval for %s failed with %s', model_name, str(e))
            #return df

        if prophet_model_json is None:
            prophet_model_json = self.train_model(df, model_name)
            # get back the original frame

        '''
        try:
            prophet_model = model_from_json(prophet_model_json)
        except Exception as e:
            logger.error('Deserializing prophet model failed with ' + str(e)) 
            return df
        '''
        return df  # we don't really care

    def _calc(self, df):
        logger.info('_calc')
        entity = df.index[0][0]
        time_var = df.index.names[1]

        # obtain db handler
        db = self.get_db()

        # get model
        model_name = self.generate_model_name([], self.y_hat, prefix='Prophet', suffix=entity)
        prophet_model_json = None
        prophet_model = None

        try:
            prophet_model_bytes = db.model_store.retrieve_model(model_name, deserialize=False)
            prophet_model_json = prophet_model_bytes.decode('utf-8')
            logger.debug('load model %s' % str(prophet_model_json)[0:40])
        except Exception as e:
            logger.info('could not load model %s' % str(model_name))
            # ToDo exception handling
            #logger.error('Model retrieval for %s failed with %s', model_name, str(e))
            #return df

        if prophet_model_json is None:
            #prophet_model_json = self.train_model(df, model_name)
            # get back the original frame
            logger.error('No model available despite training')
            return df

        try:
            prophet_model = model_from_json(prophet_model_json)
        except Exception as e:
            logger.error('Deserializing prophet model failed with ' + str(e)) 
            return df

        holiday_mean = 0
        holiday_std = 0
        holiday_days = 0
        try:
            model_js = json.loads(prophet_model_json)
            holiday_mean = model_js['holiday_mean']
            holiday_std = model_js['holiday_std']
            holiday_days = model_js['holiday_days']
        except Exception as e:
            pass

        # pass input features - only needed for inline training

        # for now just take the number of rows - assume daily frequency for now
        # future_dates column name 'ds' as Prophet expects it
        #future_dates = pd.date_range(start=df.tail(1).index[0][1], periods=df.shape[0], freq='D').to_frame(index=False, name='ds')
        future_dates = df.reset_index()[[time_var]]
        #logger.debug('Future values start/end/length ' + str(future_dates[0]) + ', ' + str(future_dates[-1]) + ', ' + str(future_dates.shape[0]))
        logger.debug('Future values ' + str(future_dates.describe()))

        future_dates = df.droplevel(0).reset_index().rename(columns={time_var: "ds", self.input_items[0]: "y"})
        prediction=prophet_model.predict(future_dates)

        # Take holidays into account
        if self.country_code is not None:    
            holiday = pd.DataFrame([])

            for date, name in sorted(holidays.country_holidays(self.country_code, years=[2023,2024,2025]).items()):
                holiday = pd.concat([holiday,pd.DataFrame.from_records([{'ds': date, 'holiday': name}])])
                holiday['ds'] = pd.to_datetime(holiday['ds'], format='%Y-%m-%d', errors='ignore')

            # replace holiday forecasts with the mean
            holiday_index = future_dates['ds'].isin(holiday['ds']).values

            # adjust holiday mean
            #holiday_mean = (holiday_mean * holiday_days + future_dates[holiday_index]['y'].sum())/(holiday_days + len(holiday_index))
            yhat = np.where(holiday_index, holiday_mean, prediction['yhat'].values)
            yhat_lower = np.where(holiday_index, holiday_mean - 3*holiday_std, prediction['yhat_lower'].values)
            yhat_upper = np.where(holiday_index, holiday_mean + 3*holiday_std, prediction['yhat_upper'].values)

        #df[self.y_hat] = prediction['yhat'].values
        df[self.y_hat] = yhat
        df[self.yhat_lower] = yhat_lower
        df[self.yhat_upper] = yhat_upper
        df[self.y_date] = None #future_dates.values

        return df

    def train_model(self, df, model_name):

        logger.info('Train model')

        # obtain db handler
        db = self.get_db()

        #daysforTraining = round(len(df)*0.95)  # take always everything
        daysforTraining = len(df)               # take always everything
        time_var = df.index.names[1]
        df_train = df.iloc[:daysforTraining].droplevel(0).reset_index().rename(columns={time_var: "ds", self.input_items[0]: "y"})
        #df_test = df.iloc[daysforTraining:].droplevel(0).reset_index().rename(columns={time_var: "ds", self.input_items[0]: "y"})

        prophet_model = None
        holiday = None
        holiday_mean = 0
        holiday_std = 0
        holiday_days = 0

        # Take holidays into account
        if self.country_code is not None:    
            logger.debug('Train with holidays from ' + str(self.country_code))
            holiday = pd.DataFrame([])

            for date, name in sorted(holidays.country_holidays(self.country_code, years=[2023,2024,2025]).items()):
                holiday = pd.concat([holiday,pd.DataFrame.from_records([{'ds': date, 'holiday': name}])])
                holiday['ds'] = pd.to_datetime(holiday['ds'], format='%Y-%m-%d', errors='ignore')

            prophet_model = Prophet(holidays=holiday)
            prophet_model.add_country_holidays(country_name=self.country_code)
        else:
            prophet_model = Prophet()

        prophet_model.fit(df_train)
        
        if holiday is not None:
            holiday_index = df_train['ds'].isin(list(holiday['ds']))
            holiday_mean = df_train[holiday_index]['y'].mean()
            holiday_std = df_train[holiday_index]['y'].std()

        forecast_holidays = prophet_model.predict(df_train)  # sanity test

        # serialize model
        model_json = model_to_json(prophet_model)

        # turn model json string into json to add element
        logger.debug('add holiday mean,std %s, %s', holiday_mean, holiday_std)
        model_js = json.loads(model_json)
        model_js['holiday_mean'] = holiday_mean
        model_js['holiday_std'] = holiday_std
        model_js['holiday_days'] = len(holiday_index)
        model_json = json.dumps(model_js)

        model_bytes = model_json.encode('utf-8')
        db.model_store.store_model(model_name, model_bytes, serialize=False)

        return model_json

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs, output contains the time shifted forecasts
        inputs = []

        inputs.append(UIMultiItem(name='input_items', datatype=float, required=True, 
                        description='Data to forecast'))
        inputs.append(UISingle(name='country_code', datatype=str, required=False,
                        description='Country code to determine holiday'))
        #inputs.append(
        #    UISingle(name='history', datatype=int, required=False, description='History length for training'))
        #inputs.append(
        #    UISingle(name='horizon', datatype=int, required=False, description='Forecasting horizon length'))

        # define arguments that behave as function outputs
        outputs=[]
        # we might need more like 'yhat', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper' ...
        outputs.append(UISingle(name='y_hat', datatype=float, description='Forecasted occupancy'))
        outputs.append(UISingle(name='yhat_lower', datatype=float, description='Forecasted occupancy lower bound'))
        outputs.append(UISingle(name='yhat_upper', datatype=float, description='Forecasted occupancy upper bound'))
        outputs.append(UISingle(name='y_date', datatype=dt.datetime, description='Date for forecasted occupancy'))

        return inputs, outputs

