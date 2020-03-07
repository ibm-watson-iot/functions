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


class ActivityDuration(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities. An activity table
    must have a deviceid, activity_code, start_date and end_date. The
    function returns an activity duration for each selected activity code.
    '''

    _is_instance_level_logged = False

    def __init__(self, table_name, activity_codes, activity_duration=None, additional_items=None,
                 additional_output_names=None):
        super().__init__(input_activities=activity_codes, activity_duration=activity_duration,
                         additional_items=additional_items, additional_output_names=additional_output_names)

        self.table_name = table_name
        self.activity_codes = activity_codes
        self.activities_metadata[table_name] = activity_codes
        self.activities_custom_query_metadata = {}

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='table_name', datatype=str, description='Source table name', ))
        inputs.append(UIMulti(name='activity_codes', datatype=str, description='Comma separated list of activity codes',
                              output_item='activity_duration', is_output_datatype_derived=False, output_datatype=float))
        inputs.append(UIMulti(name='additional_items', datatype=str, required=False,
                              description='Comma separated list of additional column names to retrieve',
                              output_item='additional_output_names', is_output_datatype_derived=True,
                              output_datatype=None))
        outputs = []

        return (inputs, outputs)


class AggregateWithExpression(BaseSimpleAggregator):
    '''
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.

    Example:

    x.max() - x.min()

    '''

    def __init__(self, input_items, expression, output_items):
        super().__init__()

        self.input_items = input_items
        self.expression = expression
        self.output_items = output_items

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UIMultiItem(name='input_items', datatype=None, description=('Choose the data items'
                                                                                  ' that you would like to'
                                                                                  ' aggregate'),
                                  output_item='output_items', is_output_datatype_derived=True))

        inputs.append(UIExpression(name='expression', description='Paste in or type an AS expression'))

        return (inputs, [])

    def aggregate(self, x):
        return eval(self.expression)


class AlertExpression(BaseEvent):
    '''
    Create alerts that are triggered when data values the expression is True
    '''

    def __init__(self, expression, alert_name, **kwargs):
        self.expression = expression
        self.alert_name = alert_name
        super().__init__()

    def _calc(self, df):
        '''
        unused
        '''
        return df

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
            msg = 'Expression converted to %s. ' % expr
        else:
            expr = self.expression
            msg = 'Expression (%s). ' % expr
        self.trace_append(msg)
        df[self.alert_name] = np.where(eval(expr), True, None)
        return df

    def get_input_items(self):
        items = self.get_expression_items(self.expression)
        return items

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='expression',
                                   description="Define alert expression using pandas systax. Example: df['inlet_temperature']>50"))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)


class AlertOutOfRange(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least one threshold.
    """

    def __init__(self, input_item, lower_threshold=None, upper_threshold=None, output_alert_upper='output_alert_upper',
                 output_alert_lower='output_alert_lower', **kwargs):

        self.input_item = input_item
        if not lower_threshold is None:
            lower_threshold = float(lower_threshold)
        self.lower_threshold = lower_threshold
        if not upper_threshold is None:
            upper_threshold = float(upper_threshold)
        self.upper_threshold = upper_threshold
        self.output_alert_lower = output_alert_lower
        self.output_alert_upper = output_alert_upper
        super().__init__()

    def _calc(self, df):
        '''
        unused
        '''

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        df[self.output_alert_upper] = False
        df[self.output_alert_lower] = False

        if not self.lower_threshold is None:
            df[self.output_alert_lower] = np.where(df[self.input_item] <= self.lower_threshold, True, None)
        if not self.upper_threshold is None:
            df[self.output_alert_upper] = np.where(df[self.input_item] >= self.upper_threshold, True, None)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='lower_threshold', datatype=float,
                               description='Alert when item value is lower than this value', required=False, ))
        inputs.append(UISingle(name='upper_threshold', datatype=float,
                               description='Alert when item value is higher than this value', required=False, ))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_alert_lower', datatype=bool, description='Output of alert function'))
        outputs.append(
            UIFunctionOutSingle(name='output_alert_upper', datatype=bool, description='Output of alert function'))

        return (inputs, outputs)


class AlertHighValue(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold'.
    """

    def __init__(self, input_item, upper_threshold=None, alert_name='alert_name', **kwargs):
        self.input_item = input_item
        self.upper_threshold = float(upper_threshold)
        self.alert_name = alert_name
        super().__init__()

    def _calc(self, df):
        '''
        unused
        '''

    def execute(self, df):
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item] >= self.upper_threshold, True, None)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='upper_threshold', datatype=float,
                               description='Alert when item value is higher than this value'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)

    def _getMetadata(self, df=None, new_df=None, inputs=None, outputs=None, constants=None):
        return self.build_ui()


class AlertLowValue(BaseEvent):
    """
    Fire alert when metric goes below a threshold'.
    """

    def __init__(self, input_item, lower_threshold=None, alert_name='alert_name', **kwargs):
        self.input_item = input_item
        self.lower_threshold = float(lower_threshold)
        self.alert_name = alert_name
        super().__init__()

    def _calc(self, df):
        '''
        unused
        '''
        return df

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item] <= self.lower_threshold, True, None)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='lower_threshold', datatype=float,
                               description='Alert when item value is lower than this value'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)


class AutoTest(BaseTransformer):
    '''
    Test the results of pipeline execution against a known test dataset.
    The test will compare calculated values with values in the test dataset.
    Discepancies will the written to a test output file.

    Note: This function is experimental
    '''

    def __init__(self, test_datset_name, columns_to_test, result_col='test_result'):
        super().__init__()

        self.test_datset_name = test_datset_name
        self.columns_to_test = columns_to_test
        self.result_col = result_col

    def execute(self, df):
        db = self.get_db()
        bucket = self.get_bucket_name()

        file = db.model_store.retrieve_model(self.test_datset_name)

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['test_datset_name'] = UISingle(name='test_datset_name', datatype=str,
                                              description=('Name of cos object containing'
                                                           ' test data. Object is a pickled '
                                                           ' dataframe. Object must be placed '
                                                           ' in the bos_runtime_bucket'))
        inputs['columns_to_test'] = UIMultiItem(name='input_items', datatype=None,
                                                description=('Choose the data items that'
                                                             ' you would like to compare'))
        outputs = OrderedDict()

        return (inputs, outputs)


class Coalesce(BaseTransformer):
    """
    Return first non-null value from a list of data items.
    """

    def __init__(self, data_items, output_item='output_item'):
        super().__init__()
        self.data_items = data_items
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = df[self.data_items].bfill(axis=1).iloc[:, 0]

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('data_items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=float))

        return (inputs, outputs)


class CoalesceDimension(BaseTransformer):
    """
    Return first non-null value from a list of data items.
    """

    def __init__(self, data_items, output_item='output_item'):
        super().__init__()
        self.data_items = data_items
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = df[self.data_items].bfill(axis=1).iloc[:, 0]

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('data_items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=str, tags=['DIMENSION']))

        return (inputs, outputs)


class ConditionalItems(BaseTransformer):
    """
    Return null unless a condition is met.
    eg. if df["sensor_is_valid"]==True then deliver the value of df["temperature"] else deliver Null
    """

    def __init__(self, conditional_expression, conditional_items, output_items=None):

        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.conditional_items = conditional_items
        if output_items is None:
            output_items = ['conditional_%s' % x for x in conditional_items]
        self.output_items = output_items

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        result = eval(self.conditional_expression)
        for i, o in enumerate(self.conditional_items):
            df[self.output_items[i]] = np.where(result, df[o], None)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='conditional_expression',
                                   description="expression that returns a True/False value, eg. if df['sensor_is_valid']==True"))
        inputs.append(UIMultiItem(name='conditional_items', datatype=None,
                                  description='Data items that have conditional values, e.g. temp and pressure'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='output_items', cardinality_from='conditional_items', is_datatype_derived=False,
                               description='Function output items'))

        return (inputs, outputs)

    def get_input_items(self):
        items = self.get_expression_items(self.conditional_expression)
        return items


class DateDifference(BaseTransformer):
    """
    Calculate the difference between two date data items in days,ie: ie date_2 - date_1
    """

    def __init__(self, date_1, date_2, num_days='num_days'):

        super().__init__()
        self.date_1 = date_1
        self.date_2 = date_2
        self.num_days = num_days

    def execute(self, df):

        if self.date_1 is None or self.date_1 == self._entity_type._timestamp:
            ds_1 = self.get_timestamp_series(df)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]

        if self.date_2 is None or self.date_2 == self._entity_type._timestamp:
            ds_2 = self.get_timestamp_series(df)
            ds_2 = pd.to_datetime(ds_2)
        else:
            ds_2 = df[self.date_2]

        df[self.num_days] = (ds_2 - ds_1).dt.total_seconds() / (60 * 60 * 24)

        return df

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='date_1', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        inputs.append(UISingleItem(name='date_2', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='num_days', datatype=float, description='Number of days'))

        return (inputs, outputs)


class DateDifferenceConstant(BaseTransformer):
    """
    Calculate the difference between a data item and a constant_date,
    ie: ie constant_date - date_1
    """

    def __init__(self, date_1, date_constant, num_days='num_days'):

        super().__init__()
        self.date_1 = date_1
        self.date_constant = date_constant
        self.num_days = num_days

    def execute(self, df):

        if self.date_1 is None or self.date_1 == self._entity_type._timestamp:
            ds_1 = self.get_timestamp_series(df)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]

        c = self._entity_type.get_attributes_dict()
        constant_value = c[self.date_constant]
        ds_2 = pd.Series(data=constant_value, index=df.index)
        ds_2 = pd.to_datetime(ds_2)
        df[self.num_days] = (ds_2 - ds_1).dt.total_seconds() / (60 * 60 * 24)

        return df

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='date_1', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        inputs.append(UISingle(name='date_constant', datatype=str, description='Name of datetime constant'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='num_days', datatype=float, description='Number of days'))

        return (inputs, outputs)


class DatabaseLookup(BaseDatabaseLookup):
    """
    Lookup columns from a database table. The lookup is time invariant. Lookup key column names
    must match data items names. Example: Lookup EmployeeCount and Country from a Company lookup
    table that is keyed on country_code.
    """

    # create the table and populate it using the data dict
    _auto_create_lookup_table = False

    def __init__(self, lookup_table_name, lookup_keys, lookup_items, parse_dates=None, output_items=None):
        super().__init__(lookup_table_name=lookup_table_name, lookup_keys=lookup_keys, lookup_items=lookup_items,
                         parse_dates=parse_dates, output_items=output_items)

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingle(name='lookup_table_name', datatype=str, description='Table name to perform lookup against')),
        inputs.append(UIMulti(name='lookup_keys', datatype=str, description='Data items to use as a key to the lookup'))
        inputs.append(UIMulti(name='lookup_items', datatype=str, description='columns to return from the lookup')),
        inputs.append(UIMulti(name='parse_dates', datatype=str, description='columns that should be converted to dates',
                              required=False))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='output_items', cardinality_from='lookup_items', is_datatype_derived=False,
                               description='Function output items', tags=['DIMENSION']))
        return (inputs, outputs)

    def get_item_values(self, arg, db):
        raise NotImplementedError(
            'No items values available for generic database lookup function. Implement a specific one for each table to define item values. ')


class DeleteInputData(BasePreload):
    '''
    Delete data from time series input table for entity type
    '''

    def __init__(self, dummy_items, older_than_days, output_item='output_item'):
        super().__init__(dummy_items=dummy_items)
        self.older_than_days = older_than_days
        self.output_item = output_item

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):
        entity_type = self.get_entity_type()
        self.get_db().delete_data(table_name=entity_type.name, schema=entity_type._db_schema,
                                  timestamp=entity_type._timestamp, older_than_days=self.older_than_days)
        msg = 'Deleted data for %s' % (self._entity_type.name)
        logger.debug(msg)
        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, description='Dummy data items'))
        inputs.append(
            UISingle(name='older_than_days', datatype=float, description='Delete data older than this many days'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class DropNull(BaseMetadataProvider):
    '''
    Drop any row that has all null metrics
    '''

    def __init__(self, exclude_items, drop_all_null_rows=True, output_item='drop_nulls'):
        kw = {'_custom_exclude_col_from_auto_drop_nulls': exclude_items, '_drop_all_null_rows': drop_all_null_rows}
        super().__init__(dummy_items=exclude_items, output_item=output_item, **kw)
        self.exclude_items = exclude_items
        self.drop_all_null_rows = drop_all_null_rows

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='exclude_items', datatype=None,
                                  description='Ignore non-null values in these columns when dropping rows'))
        inputs.append(
            UISingle(name='drop_all_null_rows', datatype=bool, description='Enable or disable drop of all null rows'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class EntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    Time series columns defined on the entity data table will be populated
    with random data.

    Optional parameters:

    freq: pandas frequency string. Time series frequency.
    scd_frequency: pandas frequency string.  Dimension change frequency.
    activity_frequency: pandas frequency string. Activity frequency.
    activities = dict keyed on activity name containing list of activities codes
    scds = dict keyes on scd property name containing list of string domain items
    data_item_mean: dict keyed by data item name. Mean value.
    data_item_sd: dict keyed by data item name. Standard deviation.
    data_item_domain: dictionary keyed by data item name. List of values.
    drop_existing: bool. Drop existing input tables and generate new for each run.

    """

    is_data_generator = True
    freq = '5min'
    scd_frequency = '1D'
    activity_frequency = '3D'
    start_entity_id = 73000  # used to build entity ids
    auto_entity_count = 5  # default number of entities to generate data for
    data_item_mean = None
    data_item_sd = None
    data_item_domain = None
    activities = None
    scds = None
    drop_existing = False

    # ids of entities to generate. Change the value of the range() function to change the number of entities

    def __init__(self, ids=None, output_item='entity_data_generator', parameters=None, **kw):

        if parameters is None:
            parameters = {}
        parameters = {**kw, **parameters}
        self.parameters = parameters
        self.set_params(**parameters)
        super().__init__(dummy_items=[], output_item=output_item)
        if ids is None:
            ids = self.get_entity_ids()
        self.ids = ids

        if self.data_item_mean is None:
            self.data_item_mean = {}
        if self.data_item_sd is None:
            self.data_item_sd = {}
        if self.data_item_domain is None:
            self.data_item_domain = {}
        if self.activities is None:
            self.activities = {}
        if self.scds is None:
            self.scds = {}

    def execute(self, df, start_ts=None, end_ts=None, entities=None):

        # Define simulation related metadata on the entity type

        if entities is None:
            entities = self.ids

        # Add scds
        for key, values in list(self.scds.items()):
            self._entity_type.add_slowly_changing_dimension(key, String(255))
            self.data_item_domain[key] = values

        # Add activities metadata to entity type
        for key, codes in list(self.activities.items()):
            name = '%s_%s' % (self._entity_type.name, key)
            self._entity_type.add_activity_table(name, codes)

        # Generate data

        if not start_ts is None:
            seconds = (dt.datetime.utcnow() - start_ts).total_seconds()
        else:
            seconds = pd.to_timedelta(self.freq).total_seconds()

        df = self._entity_type.generate_data(entities=entities, days=0, seconds=seconds, freq=self.freq,
                                             scd_freq=self.scd_frequency, write=True,
                                             data_item_mean=self.data_item_mean, data_item_sd=self.data_item_sd,
                                             data_item_domain=self.data_item_domain, drop_existing=self.drop_existing)

        kw = {'rows_generated': len(df.index), 'start_ts': start_ts, 'seconds': seconds}

        trace = self.get_trace()
        trace.update_last_entry(df=df, **kw)

        self.usage_ = len(df.index)

        return True

    def get_entity_ids(self):
        '''
        Generate a list of entity ids
        '''
        ids = [str(self.start_entity_id + x) for x in list(range(self.auto_entity_count))]
        return (ids)

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMulti(name='ids', datatype=str, description='Comma separate list of entity ids, e.g: X902-A01,X902-A03'))
        inputs.append(UIParameters())
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class AnomalyGeneratorExtremeValue(BaseTransformer):
    '''
    This function generates extreme anomaly.
    '''

    def __init__(self, input_item, factor, size, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.factor = int(factor)
        self.size = int(size)
        super().__init__()

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        entity_type = self.get_entity_type()
        derived_metric_table_name = 'DM_'+ entity_type.logical_name
        schema = entity_type._db_schema

        #Store and initialize the counts by entity id
        # db = self.get_db()
        db = self._entity_type.db
        query, table = db.query(derived_metric_table_name,schema,column_names='KEY',filters={'KEY':self.output_item})
        raw_dataframe = db.get_query_data(query)
        logger.debug('Check for key {} in derived metric table {}'.format(self.output_item,raw_dataframe.shape))
        key = '_'.join([derived_metric_table_name, self.output_item])

        if raw_dataframe is not None and raw_dataframe.empty:
            #Delete old counts if present
            db.model_store.delete_model(key)
            logger.debug('Intialize count for first run')

        counts_by_entity_id = db.model_store.retrieve_model(key)
        if counts_by_entity_id is None:
            counts_by_entity_id = {}
        logger.debug('Initial Grp Counts {}'.format(counts_by_entity_id))

        #Mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby=timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]
            logger.debug('Group {} Indexes {}'.format(grp[0],df_entity_grp.index))

            count = 0
            local_std = df_entity_grp.iloc[:10][self.input_item].std()
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id]

            mark_anomaly = False
            for grp_row_index in df_entity_grp.index:
                count += 1
                if count%self.factor == 0:
                    #Mark anomaly point
                    timeseries[self.output_item].iloc[grp_row_index] = np.random.choice([-1, 1]) * self.size * local_std
                    logger.debug('Anomaly Index Value{}'.format(grp_row_index))

            counts_by_entity_id[entity_grp_id] = count

        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        #Save the group counts to db
        db.model_store.store_model(key, counts_by_entity_id)

        timeseries.set_index(df.index.names,inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Item to base anomaly on'
                                              ))

        inputs.append(UISingle(
                name='factor',
                datatype=int,
                description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                default=5
                                              ))

        inputs.append(UISingle(
                name='size',
                datatype=int,
                description='Size of extreme anomalies to be created. e.g. 10 will create 10x size extreme anomaly compared to the normal variance',
                default=10
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With Extreme anomalies'
                ))
        return (inputs, outputs)


class AnomalyGeneratorNoData(BaseTransformer):
    '''
    This function generates nodata anomaly.
    '''

    def __init__(self, input_item, width, factor, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor)
        super().__init__()

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        entity_type = self.get_entity_type()
        derived_metric_table_name = 'DM_'+entity_type.logical_name
        schema = entity_type._db_schema

        #Store and initialize the counts by entity id
        # db = self.get_db()
        db = self._entity_type.db
        query, table = db.query(derived_metric_table_name,schema,column_names='KEY',filters={'KEY':self.output_item})
        raw_dataframe = db.get_query_data(query)
        logger.debug('Check for key {} in derived metric table {}'.format(self.output_item,raw_dataframe.shape))
        key = '_'.join([derived_metric_table_name, self.output_item])

        if raw_dataframe is not None and raw_dataframe.empty:
            #Delete old counts if present
            db.model_store.delete_model(key)
            logger.debug('Intialize count for first run')

        counts_by_entity_id = db.model_store.retrieve_model(key)
        if counts_by_entity_id is None:
            counts_by_entity_id = {}
        logger.debug('Initial Grp Counts {}'.format(counts_by_entity_id))

        #Mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby=timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]
            logger.debug('Group {} Indexes {}'.format(grp[0],df_entity_grp.index))

            count = 0
            width = self.width
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id][0]
                width = counts_by_entity_id[entity_grp_id][1]

            mark_anomaly = False
            for grp_row_index in df_entity_grp.index:
                count += 1

                if width!=self.width or count%self.factor == 0:
                    #Start marking points
                    mark_anomaly = True

                if mark_anomaly:
                    timeseries[self.output_item].iloc[grp_row_index] = np.NaN
                    width -= 1
                    logger.debug('Anomaly Index Value{}'.format(grp_row_index))

                if width==0:
                    #End marking points
                    mark_anomaly = False
                    #Update values
                    width = self.width
                    count = 0

            counts_by_entity_id[entity_grp_id] = (count,width)

        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        #Save the group counts to db
        db.model_store.store_model(key, counts_by_entity_id)

        timeseries.set_index(df.index.names,inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Item to base anomaly on'
                                              ))

        inputs.append(UISingle(
                name='factor',
                datatype=int,
                description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                default=10
                                              ))

        inputs.append(UISingle(
                name='width',
                datatype=int,
                description='Width of the anomaly created',
                default=5
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With NoData anomalies'
                ))
        return (inputs, outputs)


class AnomalyGeneratorFlatline(BaseTransformer):
    '''
    This function generates flatline anomaly.
    '''

    def __init__(self, input_item, width, factor, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor)
        super().__init__()

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        entity_type = self.get_entity_type()
        derived_metric_table_name = 'DM_'+entity_type.logical_name
        schema = entity_type._db_schema

        #Store and initialize the counts by entity id
        # db = self.get_db()
        db = self._entity_type.db
        query, table = db.query(derived_metric_table_name,schema,column_names='KEY',filters={'KEY':self.output_item})
        raw_dataframe = db.get_query_data(query)
        logger.debug('Check for key column {} in derived metric table {}'.format(self.output_item,raw_dataframe.shape))
        key = '_'.join([derived_metric_table_name, self.output_item])

        if raw_dataframe is not None and raw_dataframe.empty:
            #Delete old counts if present
            db.model_store.delete_model(key)
            logger.debug('Intialize count for first run')

        counts_by_entity_id = db.model_store.retrieve_model(key)
        if counts_by_entity_id is None:
            counts_by_entity_id = {}
        logger.debug('Initial Grp Counts {}'.format(counts_by_entity_id))

        #Mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby=timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]
            logger.debug('Group {} Indexes {}'.format(grp[0],df_entity_grp.index))

            count = 0
            width = self.width
            local_mean = df_entity_grp.iloc[:10][self.input_item].mean()
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id][0]
                width = counts_by_entity_id[entity_grp_id][1]
                if count != 0:
                    local_mean = counts_by_entity_id[entity_grp_id][2]

            mark_anomaly = False
            for grp_row_index in df_entity_grp.index:
                count += 1

                if width!=self.width or count%self.factor == 0:
                    #Start marking points
                    mark_anomaly = True

                if mark_anomaly:
                    timeseries[self.output_item].iloc[grp_row_index] = local_mean
                    width -= 1
                    logger.debug('Anomaly Index Value{}'.format(grp_row_index))

                if width==0:
                    #End marking points
                    mark_anomaly = False
                    #Update values
                    width = self.width
                    count = 0
                    local_mean = df_entity_grp.iloc[:10][self.input_item].mean()

            counts_by_entity_id[entity_grp_id] = (count,width,local_mean)


        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        #Save the group counts to db
        db.model_store.store_model(key, counts_by_entity_id)

        timeseries.set_index(df.index.names,inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Item to base anomaly on'
                                              ))

        inputs.append(UISingle(
                name='factor',
                datatype=int,
                description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                default=10
                                              ))

        inputs.append(UISingle(
                name='width',
                datatype=int,
                description='Width of the anomaly created',
                default=5
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With Flatline anomalies'
                ))
        return (inputs, outputs)


class EntityFilter(BaseMetadataProvider):
    '''
    Filter data retrieval queries to retrieve only data for the entity ids
    included in the filter
    '''

    def __init__(self, entity_list, output_item='is_filter_set'):
        dummy_items = ['deviceid']
        kwargs = {'_entity_filter_list': entity_list}
        super().__init__(dummy_items, output_item=output_item, **kwargs)
        self.entity_list = entity_list

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name='entity_list', datatype=str, description='comma separated list of entity ids'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class PythonExpression(BaseTransformer):
    '''
    Create a new item from an expression involving other items
    '''

    def __init__(self, expression, output_name):
        self.output_name = output_name
        super().__init__()
        # convert single quotes to double
        self.expression = self.parse_expression(expression)
        # registration
        self.constants = ['expression']
        self.outputs = ['output_name']

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        requested = list(self.get_input_items())
        msg = self.expression + ' .'
        self.trace_append(msg)
        msg = 'Function requested items: %s . ' % ','.join(requested)
        self.trace_append(msg)
        df[self.output_name] = eval(self.expression)
        return df

    def get_input_items(self):
        items = self.get_expression_items(self.expression)
        return items

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='expression',
                                   description="Define alert expression using pandas systax. Example: df['inlet_temperature']>50"))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_name', datatype=float, description='Output of expression'))

        return (inputs, outputs)


class GetEntityData(BaseDataSource):
    """
    Get time series data from an entity type. Provide the table name for the entity type and
    specify the key column to use for mapping the source entity type to the destination.
    e.g. Add temperature sensor data to a location entity type by selecting a location_id
    as the mapping key on the source entity type

    Note: This function is experimental
    """

    merge_method = 'outer'
    allow_projection_list_trim = False

    def __init__(self, source_entity_type_name, key_map_column, input_items, output_items=None):
        self.source_entity_type_name = source_entity_type_name
        self.key_map_column = key_map_column
        super().__init__(input_items=input_items, output_items=output_items)

    def get_data(self, start_ts=None, end_ts=None, entities=None):
        db = self.get_db()
        target = self.get_entity_type()
        # get entity type metadata from the AS API
        source = db.get_entity_type(self.source_entity_type_name)
        source._checkpoint_by_entity = False
        source._pre_aggregate_time_grain = target._pre_aggregate_time_grain
        source._pre_agg_rules = target._pre_agg_rules
        source._pre_agg_outputs = target._pre_agg_outputs
        cols = [self.key_map_column, source._timestamp]
        cols.extend(self.input_items)
        renamed_cols = [target._entity_id, target._timestamp]
        renamed_cols.extend(self.output_items)
        df = source.get_data(start_ts=start_ts, end_ts=end_ts, entities=entities, columns=cols)
        df = self.rename_cols(df, cols, renamed_cols)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='source_entity_type_name', datatype=str,
                               description="Enter the name of the entity type that you would like to retrieve data from"))
        inputs.append(UISingle(name='key_map_column', datatype=str,
                               description="Enter the name of the column on the source entity type that represents the map to the device id of this entity type"))
        inputs.append(UIMulti(name='input_items', datatype=str,
                              description="Comma separated list of data item names to retrieve from the source entity type",
                              output_item='output_items', is_output_datatype_derived=True))
        outputs = []

        return (inputs, outputs)


class EntityId(BaseTransformer):
    """
    Deliver a data item containing the id of each entity. Optionally only return the entity
    id when one or more data items are populated, else deliver a null value.
    """

    def __init__(self, data_items=None, output_item='entity_id'):

        super().__init__()
        self.data_items = data_items
        self.output_item = output_item

    def execute(self, df):

        df = df.copy()
        if self.data_items is None:
            df[self.output_item] = df[self.get_entity_type()._entity_id]
        else:
            df[self.output_item] = np.where(df[self.data_items].notna().max(axis=1),
                                            df[self.get_entity_type()._entity_id], None)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='data_items', datatype=None, required=False,
                                  description='Choose one or more data items. If data items are defined, entity id will only be shown if these data items are not null'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class IfThenElse(BaseTransformer):
    """
    Set the value of the output_item based on a conditional expression.
    When the conditional expression returns a True value, return the value of the true_expression.

    Example:
    conditional_expression: df['x1'] > 5 * df['x2']
    true expression: df['x2'] * 5
    false expression: 0
    """

    def __init__(self, conditional_expression, true_expression, false_expression, output_item='output_item'):
        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.true_expression = self.parse_expression(true_expression)
        self.false_expression = self.parse_expression(false_expression)
        self.output_item = output_item

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        df[self.output_item] = np.where(eval(self.conditional_expression), eval(self.true_expression),
                                        eval(self.false_expression))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='conditional_expression',
                                   description="expression that returns a True/False value, eg. if df['temp']>50 then df['temp'] else None"))
        inputs.append(UIExpression(name='true_expression', description="expression when true, eg. df['temp']"))
        inputs.append(UIExpression(name='false_expression', description='expression when false, eg. None'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)

    def get_input_items(self):
        items = self.get_expression_items([self.conditional_expression, self.true_expression, self.false_expression])
        return items


class PackageInfo(BaseTransformer):
    """
    Show the version of a list of installed packages. Optionally install packages that are not installed.
    """

    def __init__(self, package_names, add_to_trace=True, install_missing=True, version_output=None):

        self.package_names = package_names
        self.add_to_trace = add_to_trace
        self.install_missing = install_missing
        if version_output is None:
            version_output = ['%s_version' % x for x in package_names]
        self.version_output = version_output
        super().__init__()

    def execute(self, df):
        import importlib
        entity_type = self.get_entity_type()
        df = df.copy()
        for i, p in enumerate(self.package_names):
            ver = ''
            try:
                installed_package = importlib.import_module(p)
            except (BaseException):
                if self.install_missing:
                    entity_type.db.install_package(p)
                    try:
                        installed_package = importlib.import_module(p)
                    except (BaseException):
                        ver = 'Package could not be installed'
                    else:
                        try:
                            ver = 'installed %s' % installed_package.__version__
                        except AttributeError:
                            ver = 'Package has no __version__ attribute'
            else:
                try:
                    ver = installed_package.__version__
                except AttributeError:
                    ver = 'Package has no __version__ attribute'
            df[self.version_output[i]] = ver
            if self.add_to_trace:
                msg = '( %s : %s)' % (p, ver)
                self.trace_append(msg)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMulti(name='package_names', datatype=str, description='Comma separate list of python package names',
                    output_item='version_output', is_output_datatype_derived=False, output_datatype=str))
        inputs.append(UISingle(name='install_missing', datatype=bool))
        inputs.append(UISingle(name='add_to_trace', datatype=bool))
        # define arguments that behave as function outputs
        outputs = []

        return (inputs, outputs)


class PythonFunction(BaseTransformer):
    """
    Execute a paste-in function. A paste-in function is python function declaration
    code block. The function must be called 'f' and accept two inputs:
    df (a pandas DataFrame) and parameters (a dict that you can use
    to externalize the configuration of the function).

    The function can return a DataFrame,Series,NumpyArray or scalar value.

    Example:
    def f(df,parameters):
        #  generate an 2-D array of random numbers
        output = np.random.normal(1,0.1,len(df.index))
        return output

    Function source may be pasted in or retrieved from Cloud Object Storage.

    PythonFunction is currently experimental.
    """

    function_name = 'f'

    def __init__(self, function_code, input_items, output_item, parameters=None):

        self.function_code = function_code
        self.input_items = input_items
        self.output_item = output_item
        super().__init__()
        if parameters is None:
            parameters = {}

        function_name = parameters.get('function_name', None)
        if function_name is not None:
            self.function_name = function_name

        self.parameters = parameters

    def execute(self, df):

        # function may have already been serialized to cos

        kw = {}

        if not self.function_code.startswith('def '):
            bucket = self.get_bucket_name()
            fn = self._entity_type.db.model_store.retrieve_model(self.function_code)
            kw['source'] = 'cos'
            kw['filename'] = self.function_code
            if fn is None:
                msg = (' Function text does not start with "def ". '
                       ' Function is assumed to located in COS'
                       ' Cant locate function %s in cos. Make sure this '
                       ' function exists in the %s bucket' % (self.function_code, bucket))
                raise RuntimeError(msg)

        else:
            fn = self._entity_type.db.make_function(function_name=self.function_name, function_code=self.function_code)
            kw['source'] = 'paste-in code'
            kw['filename'] = None

        kw['input_items'] = self.input_items
        kw['output_item'] = self.output_item
        kw['entity_type'] = self._entity_type
        kw['db'] = self._entity_type.db
        kw['c'] = self._entity_type.get_attributes_dict()
        kw['logger'] = logger
        self.trace_append(msg=self.function_code, log_method=logger.debug, **kw)

        result = fn(df=df, parameters={**kw, **self.parameters})

        df[self.output_item] = result

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        inputs.append(UIText(name='function_code', description='Paste in your function definition'))
        inputs.append(UISingle(name='parameters', datatype=dict, required=False,
                               description='optional parameters specified in json format'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=float))

        return (inputs, outputs)


class RaiseError(BaseTransformer):
    """
    Halt execution of the pipeline raising an error that will be shown. This function is
    useful for testing a pipeline that is running to completion but not delivering the expected results.
    By halting execution of the pipeline you can view useful diagnostic information in an error
    message displayed in the UI.
    """

    def __init__(self, halt_after, abort_execution=True, output_item='pipeline_exception'):
        super().__init__()
        self.halt_after = halt_after
        self.abort_execution = abort_execution
        self.output_item = output_item

    def execute(self, df):
        msg = self.log_df_info(df, 'Prior to raising error')
        self.trace_append(msg)
        msg = 'The calculation was halted deliberately by the IoTRaiseError function. Remove the IoTRaiseError function or disable "abort_execution" in the function configuration. '
        if self.abort_execution:
            raise RuntimeError(msg)

        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='halt_after', datatype=None, description='Raise error after calculating items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class RandomNoise(BaseTransformer):
    '''
    Add random noise to one or more data items
    '''

    def __init__(self, input_items, standard_deviation, output_items):
        super().__init__()
        self.input_items = input_items
        self.standard_deviation = standard_deviation
        self.output_items = output_items

    def execute(self, df):
        for i, item in enumerate(self.input_items):
            output = self.output_items[i]
            random_noise = np.random.normal(0, self.standard_deviation, len(df.index))
            df[output] = df[item] + random_noise
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='standard_deviation', datatype=float, description="Standard deviation of noise"))
        inputs.append(
            UIMultiItem(name='input_items', description="Chose data items to add noise to", output_item='output_items',
                        is_output_datatype_derived=True))
        outputs = []

        return (inputs, outputs)


class RandomUniform(BaseTransformer):
    """
    Generate a uniformally distributed random number.
    """

    def __init__(self, min_value, max_value, output_item='output_item'):
        super().__init__()
        self.min_value = min_value
        self.max_value = max_value
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.uniform(self.min_value, self.max_value, len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='min_value', datatype=float))
        inputs.append(UISingle(name='max_value', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class RandomNormal(BaseTransformer):
    """
    Generate a normally distributed random number.
    """

    def __init__(self, mean, standard_deviation, output_item='output_item'):
        super().__init__()
        self.mean = mean
        self.standard_deviation = standard_deviation
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.normal(self.mean, self.standard_deviation, len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='mean', datatype=float))
        inputs.append(UISingle(name='standard_deviation', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class RandomNull(BaseTransformer):
    """
    Occassionally replace random values with null values for selected items.
    """

    def __init__(self, input_items, output_items):
        super().__init__()
        self.input_items = input_items
        self.output_items = output_items

    def execute(self, df):
        for counter, item in enumerate(self.input_items):
            choice = np.random.choice([True, False], len(df.index))
            df[self.output_items[counter]] = np.where(choice, None, df[item])

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMultiItem(name='input_items', datatype=None, description='Select items to apply null replacement to',
                        output_item='output_items', is_output_datatype_derived=True, output_datatype=None))
        outputs = []
        return (inputs, outputs)


class RandomChoiceString(BaseTransformer):
    """
    Generate random categorical values.
    """

    def __init__(self, domain_of_values, probabilities=None, output_item='output_item'):
        super().__init__()
        self.domain_of_values = domain_of_values
        self.probabilities = adjust_probabilities(probabilities)
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.choice(a=self.domain_of_values, p=self.probabilities, size=len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        #  define arguments that behave as function inputs

        inputs = []
        inputs.append(UIMulti(name='domain_of_values', datatype=str, required=True))
        inputs.append(UIMulti(name='probabilities', datatype=float, required=False))
        #  define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=str, description='Random output', tags=['DIMENSION']))

        return (inputs, outputs)


class RandomDiscreteNumeric(BaseTransformer):
    """
    Generate random discrete numeric values.
    """

    def __init__(self, discrete_values, probabilities=None, output_item='output_item'):
        super().__init__()
        self.discrete_values = discrete_values
        self.probabilities = adjust_probabilities(probabilities)
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.choice(a=self.discrete_values, p=self.probabilities, size=len(df.index))

        return df

    @classmethod
    def build_ui(cls):
        #  define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name='discrete_values', datatype=float))
        inputs.append(UIMulti(name='probabilities', datatype=float, required=False))
        #  define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class SaveCosDataFrame(BaseTransformer):
    """
    Serialize dataframe to COS
    """

    def __init__(self, filename='job_output_df', columns=None, output_item='save_df_result'):

        super().__init__()
        self.filename = filename
        self.columns = columns
        self.output_item = output_item

    def execute(self, df):

        if self.columns is not None:
            sf = df[self.columns]
        else:
            sf = df
        db = self.get_db()
        bucket = self.get_bucket_name()
        db.cos_save(persisted_object=sf, filename=self.filename, bucket=bucket, binary=True)
        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='filename', datatype=str))
        inputs.append(UIMultiItem(name='columns'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=str, description='Result of save operation'))

        return (inputs, outputs)


class SCDLookup(BaseSCDLookup):
    '''
    Lookup an slowly changing dimension property from a scd lookup table containing:
    Start_date, end_date, device_id and property. End dates are not currently used.
    Previous lookup value is assumed to be valid until the next.
    '''

    def __init__(self, table_name, output_item=None):
        self.table_name = table_name
        super().__init__(table_name=table_name, output_item=output_item)


class ShiftCalendar(BaseTransformer):
    '''
    Generate data for a shift calendar using a shift_definition in the form of a dict keyed on shift_id
    Dict contains a tuple with the start and end hours of the shift expressed as numbers. Example:
          {
               "1": [5.5, 14],
               "2": [14, 21],
               "3": [21, 29.5]
           },
    '''

    is_custom_calendar = True
    auto_conform_index = True

    def __init__(self, shift_definition=None, period_start_date='shift_start_date', period_end_date='shift_end_date',
                 shift_day='shift_day', shift_id='shift_id'):
        if shift_definition is None:
            shift_definition = {"1": [5.5, 14], "2": [14, 21], "3": [21, 29.5]}
        self.shift_definition = shift_definition
        self.period_start_date = period_start_date
        self.period_end_date = period_end_date
        self.shift_day = shift_day
        self.shift_id = shift_id
        super().__init__()

    def get_data(self, start_date, end_date):
        if start_date is None:
            raise ValueError('Start date is required when building data for a shift calendar')
        if end_date is None:
            raise ValueError('End date is required when building data for a shift calendar')

        # Subtract a day from start_date and add a day to end_date to provide shift information for the full
        # calendar days at left and right boundary.
        # Example: shift1 = [22:00,10:00], shift2 = [10:00, 22:00], data point = '2019-11-22 23:01:00' ==> data point
        # falls into shift_day '2019-11-23', not '2019-11-22'
        one_day = pd.DateOffset(days=1)
        start_date = start_date.date() - one_day
        end_date = end_date.date() + one_day
        dates = pd.date_range(start=start_date, end=end_date, freq='1D').tolist()
        dfs = []
        for shift_id, start_end in list(self.shift_definition.items()):
            data = {}
            data[self.shift_day] = dates
            data[self.shift_id] = shift_id
            data[self.period_start_date] = [x + dt.timedelta(hours=start_end[0]) for x in dates]
            data[self.period_end_date] = [x + dt.timedelta(hours=start_end[1]) for x in dates]
            dfs.append(pd.DataFrame(data))
        df = pd.concat(dfs)
        df[self.period_start_date] = pd.to_datetime(df[self.period_start_date])
        df[self.period_end_date] = pd.to_datetime(df[self.period_end_date])
        df.sort_values([self.period_start_date], inplace=True)
        return df

    def get_empty_data(self):
        cols = [self.shift_day, self.shift_id, self.period_start_date, self.period_end_date]
        df = pd.DataFrame(columns=cols)

        df[self.period_start_date] = df[self.period_start_date].astype('datetime64[ms]')
        df[self.period_end_date] = df[self.period_end_date].astype('datetime64[ms]')
        df[self.shift_day] = df[self.shift_day].astype('datetime64[ms]')
        df[self.shift_id] = df[self.shift_id].astype('float64')

        return df

    def execute(self, df):

        df = reset_df_index(df, auto_index_name=self.auto_index_name)
        entity_type = self.get_entity_type()
        (df, ts_col) = entity_type.df_sort_timestamp(df)
        start_date = df[ts_col].min()
        end_date = df[ts_col].max()

        if len(df.index) > 0:
            calendar_df = self.get_data(start_date=start_date, end_date=end_date)
            df = pd.merge_asof(left=df, right=calendar_df, left_on=ts_col, right_on=self.period_start_date,
                               direction='backward')

            df = self._entity_type.index_df(df)

        return df

    def get_period_end(self, date):

        df = self.get_data(date, date)
        result = df[self.period_end_date].max()

        return result

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='shift_definition', datatype=dict, description=''))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='period_start_date', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='period_end_date', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='shift_day', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='shift_id', datatype=int, tags=['DIMENSION']))

        return (inputs, outputs)


class Sleep(BaseTransformer):
    """
    Wait for the designated number of seconds
    """

    def __init__(self, sleep_after, sleep_duration_seconds=30, output_item='sleep_status'):
        super().__init__()
        self.sleep_after = sleep_after
        self.sleep_duration_seconds = sleep_duration_seconds
        self.output_item = output_item

    def execute(self, df):
        msg = 'Sleep duration: %s. ' % self.sleep_duration_seconds
        self.trace_append(msg)
        time.sleep(self.sleep_duration_seconds)
        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMultiItem(name='sleep_after', datatype=None, required=False, description='Sleep after calculating items'))
        inputs.append(UISingle(name='sleep_duration_seconds', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class TraceConstants(BaseTransformer):
    """
    Write the values of available constants to the trace
    """

    def __init__(self, dummy_items, output_item='trace_written'):
        super().__init__()

        self.dummy_items = dummy_items
        self.output_item = output_item

    def execute(self, df):
        c = self._entity_type.get_attributes_dict()
        msg = 'entity constants retrieved'
        self.trace_append(msg, **c)

        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, required=False, description='Not required'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class TimestampCol(BaseTransformer):
    """
    Deliver a data item containing the timestamp
    """

    def __init__(self, dummy_items=None, output_item='timestamp_col'):
        super().__init__()
        self.dummy_items = dummy_items
        self.output_item = output_item

    def execute(self, df):
        ds_1 = self.get_timestamp_series(df)
        ds_1 = pd.to_datetime(ds_1)
        df[self.output_item] = ds_1

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, required=False, description='Not required'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=dt.datetime, description='Timestamp column name'))

        return (inputs, outputs)


# Renamed functions

IoTExpression = PythonExpression
IoTRandomChoice = RandomChoiceString
IoTRandonNormal = RandomNormal
IoTActivityDuration = ActivityDuration
IoTSCDLookup = SCDLookup
IoTShiftCalendar = ShiftCalendar
IoTAlertHighValue = AlertHighValue
IoTAlertLow = AlertLowValue
IoTAlertExpression = AlertExpression
IoTAlertOutOfRange = AlertOutOfRange
IoTAutoTest = AutoTest
IoTConditionalItems = ConditionalItems
IoTDatabaseLookup = DatabaseLookup
IoTDeleteInputData = DeleteInputData
IoTDropNull = DropNull
IoTEntityFilter = EntityFilter
IoTGetEntityId = EntityId
IoTIfThenElse = IfThenElse
IoTPackageInfo = PackageInfo
IoTRaiseError = RaiseError
IoTSaveCosDataFrame = SaveCosDataFrame
IoTSleep = Sleep
IoTTraceConstants = TraceConstants


# Deprecated functions

class IoTEntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    Time series columns defined on the entity data table will be populated
    with random data.
    """

    is_deprecated = True

    def __init__(self, ids=None, output_item='entity_data_generator'):
        self.ids = ids
        self.output_item = output_item

    def get_replacement(self):
        new = EntityDataGenerator(ids=self.ids, output_item=self.output_item)

        return new


class IoTCalcSettings(BaseMetadataProvider):
    """
    Overide default calculation settings for the entity type
    """

    warnings.warn(('IoTCalcSettings is deprecated. Use entity type constants'
                   ' instead of a metadata provider to set entity type properties'))

    is_deprecated = True

    def __init__(self, checkpoint_by_entity=False, pre_aggregate_time_grain=None, auto_read_from_ts_table=True,
                 sum_items=None, mean_items=None, min_items=None, max_items=None, count_items=None, sum_outputs=None,
                 mean_outputs=None, min_outputs=None, max_outputs=None, count_outputs=None, output_item='output_item'):

        # metadata for pre-aggregation:
        # pandas aggregate dict containing a list of aggregates for each item
        self._pre_agg_rules = {}
        # dict containing names of aggregate items produced for each item
        self._pre_agg_outputs = {}
        # assemble these metadata structures
        self._apply_pre_agg_metadata('sum', items=sum_items, outputs=sum_outputs)
        self._apply_pre_agg_metadata('mean', items=mean_items, outputs=mean_outputs)
        self._apply_pre_agg_metadata('min', items=min_items, outputs=min_outputs)
        self._apply_pre_agg_metadata('max', items=max_items, outputs=max_outputs)
        self._apply_pre_agg_metadata('count', items=count_items, outputs=count_outputs)
        # pass metadata to the entity type
        kwargs = {'_checkpoint_by_entity': checkpoint_by_entity, '_pre_aggregate_time_grain': pre_aggregate_time_grain,
                  '_auto_read_from_ts_table': auto_read_from_ts_table, '_pre_agg_rules': self._pre_agg_rules,
                  '_pre_agg_outputs': self._pre_agg_outputs}
        super().__init__(dummy_items=[], output_item=output_item, **kwargs)

    def _apply_pre_agg_metadata(self, aggregate, items, outputs):
        '''
        convert UI inputs into a pandas aggregate dictioonary and a separate dictionary containing names of aggregate items
        '''
        if items is not None:
            if outputs is None:
                outputs = ['%s_%s' % (x, aggregate) for x in items]
            for i, item in enumerate(items):
                try:
                    self._pre_agg_rules[item].append(aggregate)
                    self._pre_agg_outputs[item].append(outputs[i])
                except KeyError:
                    self._pre_agg_rules[item] = [aggregate]
                    self._pre_agg_outputs[item] = [outputs[i]]
                except IndexError:
                    msg = 'Metadata for aggregate %s is not defined correctly. Outputs array should match length of items array.' % aggregate
                    raise ValueError(msg)

        return None

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='auto_read_from_ts_table', datatype=bool, required=False,
                               description='By default, data retrieved is from the designated input table. Use this setting to disable.', ))
        inputs.append(
            UISingle(name='checkpoint_by_entity', datatype=bool, required=False, description='By default a single '))
        inputs.append(UISingle(name='pre_aggregate_time_grain', datatype=str, required=False,
                               description='By default, data is retrieved at the input grain. Use this setting to preaggregate data and reduce the volumne of data retrieved',
                               values=['1min', '5min', '15min', '30min', '1H', '2H', '4H', '8H', '12H', 'day', 'week',
                                       'month', 'year']))
        inputs.append(UIMultiItem(name='sum_items', datatype=float, required=False,
                                  description='Choose items that should be added when aggregating',
                                  output_item='sum_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='mean_items', datatype=float, required=False,
                                  description='Choose items that should be averaged when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='min_items', datatype=float, required=False,
                                  description='Choose items that the system should choose the smallest value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='max_items', datatype=float, required=False,
                                  description='Choose items that the system should choose the smallest value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='count_items', datatype=float, required=False,
                                  description='Choose items that the system should choose the smallest value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class IoTCosFunction(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage.
    Function returns a single output.

    Function is replaced by PythonFunction

    """

    is_deprecated = True

    def __init__(self, function_name, input_items, output_item='output_item', parameters=None):

        # the function name may be passed as a function object or function name (string)
        # if a string is provided, it is assumed that the function object has already been serialized to COS
        # if a function onbject is supplied, it will be serialized to cos
        self.input_items = input_items
        self.output_item = output_item
        super().__init__()
        # get the cos bucket
        # if function object, serialize and get name
        self.function_name = function_name
        # The function called during execution accepts a single dictionary as input
        # add all instance variables to the parameters dict in case the function needs them
        if parameters is None:
            parameters = {}
        parameters = {**parameters, **self.__dict__}
        self.parameters = parameters

        warnings.warn('IoTCosFunction is deprecated. Use PythonFunction.', DeprecationWarning)

    def execute(self, df):
        db = self.get_db()
        bucket = self.get_bucket_name()
        # first test execution could include a fnction object
        # serialize it
        if callable(self.function_name):
            db.cos_save(persisted_object=self.function_name, filename=self.function_name.__name__, bucket=bucket,
                        binary=True)
            self.function_name = self.function_name.__name__
        # retrieve
        function = db.cos_load(filename=self.function_name, bucket=bucket, binary=True)
        # execute
        df = df.copy()
        rf = function(df, self.parameters)
        # rf will contain the orginal columns along with a single new output column.
        return rf

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        inputs.append(UISingle(name='function_name', datatype=float,
                               description='Name of function object. Function object must be serialized to COS before you can use it'))
        inputs.append(UISingle(name='parameters', datatype=dict, required=False,
                               description='Parameters required by the function are provides as json.'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item'))
