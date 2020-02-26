# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import datetime as dt
import logging
import warnings
import numpy as np
import pandas as pd
import json
from sqlalchemy import (Table, Column, Integer, SmallInteger, String, DateTime)

from pandas.api.types import (is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like)

from iotfunctions.db import SystemLogTable
from iotfunctions.metadata import EntityType
from iotfunctions.automation import TimeSeriesGenerator
from iotfunctions.base import (BaseTransformer, BaseDataSource, BaseEvent, BaseFilter, BaseAggregator,
                               BaseDatabaseLookup, BaseDBActivityMerge, BaseSCDLookup, BaseMetadataProvider,
                               BasePreload)
from iotfunctions import ui

'''
This module contains a number of sample functions. 
'''

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'


class CompanyFilter(BaseFilter):
    '''
    Demonstration function that filters data on a particular company code. 
    '''

    def __init__(self, company_code, company, status_flag=None):
        super().__init__(dependent_items=company_code, output_item=status_flag)
        self.company_code = company_code
        self.company = company
        self.status_flag = status_flag

    def filter(self, df):
        df = df[df[self.company_code] == self.company]
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingleItem(name='company_code', datatype=bool))
        inputs.append(ui.UIMulti(name='company', datatype=str, values=['AMCE', 'ABC', 'JDI']))
        return (inputs, [ui.UIStatusFlag('status_flag')])


class DateDifferenceReference(BaseTransformer):
    """
    Calculate the difference between a data item and a reference value,
    ie: ie ref_date - date_1
    """

    def __init__(self, date_1, ref_date, num_days='num_days'):

        super().__init__()
        self.date_1 = date_1

        if isinstance(ref_date, int):
            ref_date = dt.datetime.fromtimestamp(ref_date)

        self.ref_date = ref_date
        self.num_days = num_days

    def execute(self, df):

        if self.date_1 is None or self.date_1 == self._entity_type._timestamp:
            ds_1 = self.get_timestamp_series(df)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]

        df[self.num_days] = (self.ref_date - ds_1).dt.total_seconds() / (60 * 60 * 24)

        return df

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingleItem(name='date_1', datatype=dt.datetime, required=False,
                                      description=('Date data item. Use timestamp'
                                                   ' if no date specified')))
        inputs.append(ui.UISingle(name='ref_date', datatype=dt.datetime, description='Date value'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name='num_days', datatype=float, description='Number of days'))

        return (inputs, outputs)


class HTTPPreload(BasePreload):
    '''
    Do a HTTP request as a preload activity. Load results of the get into the Entity Type time series table.
    HTTP request is experimental
    '''

    out_table_name = None

    def __init__(self, request, url, headers=None, body=None, column_map=None, output_item='http_preload_done'):

        if body is None:
            body = {}

        if headers is None:
            headers = {}

        if column_map is None:
            column_map = {}

        super().__init__(dummy_items=[], output_item=output_item)

        # create an instance variable with the same name as each arg

        self.url = url
        self.request = request
        self.headers = headers
        self.body = body
        self.column_map = column_map

        # do not do any processing in the init() method. Processing will be done in the execute() method.

    def execute(self, df, start_ts=None, end_ts=None, entities=None):

        entity_type = self.get_entity_type()
        db = entity_type.db
        encoded_body = json.dumps(self.body).encode('utf-8')
        encoded_headers = json.dumps(self.headers).encode('utf-8')

        # This class is setup to write to the entity time series table
        # To route data to a different table in a custom function,
        # you can assign the table name to the out_table_name class variable
        # or create a new instance variable with the same name

        if self.out_table_name is None:
            table = entity_type.name
        else:
            table = self.out_table_name

        schema = entity_type._db_schema

        # There is a a special test "url" called internal_test
        # Create a dict containing random data when using this
        if self.url == 'internal_test':
            rows = 3
            response_data = {}
            (metrics, dates, categoricals, others) = db.get_column_lists_by_type(table=table, schema=schema,
                                                                                 exclude_cols=[])
            for m in metrics:
                response_data[m] = np.random.normal(0, 1, rows)
            for d in dates:
                response_data[d] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
            for c in categoricals:
                response_data[c] = np.random.choice(['A', 'B', 'C'], rows)

        # make an http request
        else:
            response = db.http.request(self.request, self.url, body=encoded_body, headers=self.headers)
            response_data = response.data.decode('utf-8')
            response_data = json.loads(response_data)

        df = pd.DataFrame(data=response_data)

        # align dataframe with data received

        # use supplied column map to rename columns
        df = df.rename(self.column_map, axis='columns')
        # fill in missing columns with nulls
        required_cols = db.get_column_names(table=table, schema=schema)
        missing_cols = list(set(required_cols) - set(df.columns))
        if len(missing_cols) > 0:
            kwargs = {'missing_cols': missing_cols}
            entity_type.trace_append(created_by=self, msg='http data was missing columns. Adding values.',
                                     log_method=logger.debug, **kwargs)
            for m in missing_cols:
                if m == entity_type._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m == 'devicetype':
                    df[m] = entity_type.logical_name
                else:
                    df[m] = None

        # remove columns that are not required
        df = df[required_cols]

        # write the dataframe to the database table
        self.write_frame(df=df, table_name=table)
        kwargs = {'table_name': table, 'schema': schema, 'row_count': len(df.index)}
        entity_type.trace_append(created_by=self, msg='Wrote data to table', log_method=logger.debug, **kwargs)

        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name='request', datatype=str, description='comma separated list of entity ids',
                                  values=['GET', 'POST', 'PUT', 'DELETE']))
        inputs.append(ui.UISingle(name='url', datatype=str, description='request url', tags=['TEXT'], required=True))
        inputs.append(ui.UISingle(name='headers', datatype=dict, description='request url', required=False))
        inputs.append(ui.UISingle(name='body', datatype=dict, description='request body', required=False))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIStatusFlag(name='output_item'))
        return (inputs, outputs)


class MultiplyTwoItems(BaseTransformer):
    '''
    Multiply two input items together to produce output column
    '''

    def __init__(self, input_item_1, input_item_2, output_item):
        self.input_item_1 = input_item_1
        self.input_item_2 = input_item_2
        self.output_item = output_item
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item_1] * df[self.input_item_2]
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingleItem(name='input_item_1', datatype=float, description='Input item 1'))
        inputs.append(ui.UISingleItem(name='input_item_2', datatype=float, description="Input item 2"))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name='output_item', datatype=float, description='output data'))
        return (inputs, outputs)


class MultiplyColumns(BaseTransformer):
    '''
    Columnwise multiplication of multiple data items
    '''

    def __init__(self, input_items, output_item='output_item'):
        self.input_items = input_items
        self.output_item = output_item

        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_items].product(axis=1)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(name='input_items', datatype=float, description='Choose columns to multiply',
                                     required=True, ))
        outputs = [ui.UIFunctionOutSingle(name='output_item', datatype=float)]
        return (inputs, outputs)


class MergeSampleTimeSeries(BaseDataSource):
    """
    Merge the contents of a table containing time series data with entity source data

    """
    merge_method = 'outer'  # or outer, concat, nearest
    # use concat when the source time series contains the same metrics as the entity type source data
    # use nearest to align the source time series to the entity source data
    # use outer to add new timestamps and metrics from the source
    merge_nearest_tolerance = pd.Timedelta('1D')
    merge_nearest_direction = 'nearest'
    source_table_name = 'sample_time_series'
    source_entity_id = 'deviceid'
    # metadata for generating sample
    sample_metrics = ['temp', 'pressure', 'velocity']
    sample_entities = ['entity1', 'entity2', 'entity3']
    sample_initial_days = 3
    sample_freq = '1min'
    sample_incremental_min = 5

    def __init__(self, input_items, output_items=None):
        super().__init__(input_items=input_items, output_items=output_items)

    def get_data(self, start_ts=None, end_ts=None, entities=None):

        self.load_sample_data()
        (query, table) = self._entity_type.db.query(self.source_table_name, schema=self._entity_type._db_schema)
        if not start_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] < end_ts)
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))

        parse_dates = [self._entity_type._timestamp]
        df = pd.read_sql_query(query.statement, con=self._entity_type.db.connection, parse_dates=parse_dates)
        df = df.astype(dtype={col: 'datetime64[ms]' for col in parse_dates}, errors='ignore')

        return df

    @classmethod
    def get_item_values(cls, arg, db=None):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':
            if db is None:
                db = cls._entity_type.db
            return db.get_column_names(cls.source_table_name)
        else:
            msg = 'No code implemented to gather available values for argument %s' % arg
            raise NotImplementedError(msg)

    def load_sample_data(self):

        if not self._entity_type.db.if_exists(self.source_table_name):
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, days=self.sample_initial_days,
                                            timestamp=self._entity_type._timestamp)
        else:
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, seconds=self.sample_incremental_min * 60,
                                            timestamp=self._entity_type._timestamp)

        df = generator.execute()
        self._entity_type.db.write_frame(df=df, table_name=self.source_table_name, version_db_writes=False,
                                         if_exists='append', schema=self._entity_type._db_schema,
                                         timestamp_col=self._entity_type._timestamp_col)

    def get_test_data(self):

        generator = TimeSeriesGenerator(metrics=['acceleration'], ids=self.sample_entities, freq=self.sample_freq,
                                        seconds=300, timestamp=self._entity_type._timestamp)

        df = generator.execute()
        df = self._entity_type.index_df(df)
        return df

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            ui.UIMulti(name='input_items', datatype=str, description='Choose columns to bring from source table',
                       required=True, output_item='output_items', is_output_datatype_derived=True))
        # outputs are included as an empty list as they are derived from inputs
        return (inputs, [])


class MultiplyByFactor(BaseTransformer):
    '''
    Multiply a list of input data items by a constant to produce a data output column
    for each input column in the list.
    '''

    def __init__(self, input_items, factor, output_items):
        self.input_items = input_items
        self.output_items = output_items
        self.factor = float(factor)
        super().__init__()

    def execute(self, df):
        df = df.copy()
        for i, input_item in enumerate(self.input_items):
            df[self.output_items[i]] = df[input_item] * self.factor
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(name='input_items', datatype=float, description="Data items adjust",
                                     output_item='output_items', is_output_datatype_derived=True))
        inputs.append(ui.UISingle(name='factor', datatype=float))
        outputs = []
        return (inputs, outputs)


class StatusFilter(BaseFilter):
    '''
    Demonstration function that filters on particular company codes.
    '''

    def __init__(self, status_input_item, include_only, output_item=None):
        super().__init__(dependent_items=[status_input_item], output_item=output_item)
        self.status_input_item = status_input_item
        self.include_only = include_only

    def get_item_values(self, arg, db=None):
        if arg == 'include_only':
            return (['active', 'inactive'])
        else:
            return None

    def filter(self, df):
        df = df[df['status'] == self.include_only]
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            ui.UISingleItem(name='status_input_item', datatype=None, description='Item name to use for status'))
        inputs.append(ui.UISingle(name='include_only', datatype=str,
                                  description='Filter to include only rows with a status of this'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name='output_item', datatype=bool,
                                              description='Item that contains the execution status of this function'))
        return (inputs, outputs)


'''
These functions have no build_ui() method. They are included to show function logic.
'''


class ComputationsOnStringArray(BaseTransformer):
    '''
    Perform computation on a string that contains a comma separated list of values
    '''
    # The metadata describes what columns of data are included in the array
    column_metadata = ['x1', 'x2', 'x3', 'x4', 'x5']

    def __init__(self, input_str, output_item='output_item'):
        self.input_str = input_str
        self.output_item = output_item

        super().__init__()
        self.itemDescriptions['input_str'] = 'Array of input items modeled as single comma delimited string'

    def execute(self, df):
        x_array = df['x_str'].str.split(',').to_dict()
        adf = pd.DataFrame(data=x_array).transpose()
        adf.columns = self.column_metadata
        adf[adf.columns] = adf[adf.columns].astype(float)
        df[self.output_item] = 0.5 + 0.25 * adf['x1'] + -.1 * adf['x2'] + 0.05 * adf['x3']

        return df

    def _get_str_array(self):
        out = ''
        for col in self.column_metadata:
            out = out + str(np.random.normal(1, 0.1)) + ','
        out = out[:-1]

        return out


class FillForwardByEntity(BaseTransformer):
    '''
    Fill null values forward from last item for the same entity instance
    '''

    execute_by = ['id']

    def __init__(self, input_item, output_item='output_item'):
        self.input_item = input_item
        self.output_item = output_item

        super().__init__()

    def _calc(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item].ffill()
        return df


class FlowRateMonitor(BaseTransformer):
    '''
    Check for leaks and other flow irregularies by comparing input flows with output flows
    '''

    def __init__(self, input_flows, output_flows, loss_threshold=0.005, output='output'):
        self.input_flows = input_flows
        self.output_flows = output_flows
        self.loss_threshold = float(loss_threshold)
        self.output = output

        super().__init__()

    def execute(self, df):
        df = df.copy()
        total_input = df[self.input_flows].sum(axis='columns')
        total_output = df[self.output_flows].sum(axis='columns')
        df[self.output] = np.where((total_input - total_output) / total_input > self.loss_threshold, True, False)

        return df


class InputsAndOutputsOfMultipleTypes(BaseTransformer):
    '''
    This sample function is just a pass through that demonstrates the use of multiple datatypes for inputs and outputs
    '''

    def __init__(self, input_number, input_date, input_str, output_number='output_number', output_date='output_date',
                 output_str='output_str'):
        self.input_number = input_number
        self.output_number = output_number
        self.input_date = input_date
        self.output_date = output_date
        self.input_str = input_str
        self.output_str = output_str

        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_number] = df[self.input_number]
        df[self.output_date] = df[self.input_date]
        df[self.output_str] = df[self.input_str]
        return df


class LookupCompany(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """

    # create the table and populate it using the data dict
    _auto_create_lookup_table = True

    def __init__(self, lookup_items=None, output_items=None):
        # sample data will be used to create table if it doesn't already exist
        # sample data will be converted into a dataframe before being written to a  table
        # make sure that the dictionary that you provide is understood by pandas
        # use lower case column names
        self.data = {'company': ['ABC', 'ACME', 'JDI'], 'currency_code': ['USD', 'CAD', 'USD'],
                     'employee_count': [100, 120, 352],
                     'inception_date': [dt.datetime.strptime('Feb 1 2005 7:00AM', '%b %d %Y %I:%M%p'),
                                        dt.datetime.strptime('Jan 1 1988 5:00AM', '%b %d %Y %I:%M%p'),
                                        dt.datetime.strptime('Feb 28 1967 9:00AM', '%b %d %Y %I:%M%p')]}

        # Must specify:
        # One or more business key for the lookup
        # The UI will ask you to map each business key to an AS Data Item
        company_key = 'company'
        # must specify a name of the lookup name even if you are supplying your own sql.
        # use lower case table names
        lookup_table_name = 'company'
        # Indicate which of the input parameters are lookup keys
        lookup_keys = [company_key]
        # Indicate which of the column returned should be converted into dates
        parse_dates = ['inception_date']

        super().__init__(lookup_table_name=lookup_table_name, lookup_keys=lookup_keys, lookup_items=lookup_items,
                         parse_dates=parse_dates, output_items=output_items)

        self.itemTags['output_items'] = ['DIMENSION']

        # The base class takes care of the rest  # No execute() method required


class LookupOperator(BaseSCDLookup):
    '''
    Lookup Operator information from a resource calendar
    A resource calendar is a table with a start_date, end_date, device_id and resource_id
    Resource assignments are defined for each device_id. Each assignment has a start date
    End dates are not currently used. Assignment is assumed to be valid until the next.
    '''

    def __init__(self, dummy_item, output_item=None):
        # at the moment there is a requirement to include at least one item is input to a function
        # a dummy will be added
        # TBD remove this restriction
        self.dummy_item = dummy_item
        table_name = 'widgets_scd_operator'
        super().__init__(table_name=table_name, output_item=output_item)


class LookupStatus(BaseSCDLookup):
    '''
    Lookup Operator information from a resource calendar
    A resource calendar is a table with a start_date, end_date, device_id and resource_id
    Resource assignments are defined for each device_id. Each assignment has a start date
    End dates are not currently used. Assignment is assumed to be valid until the next.
    '''

    def __init__(self, dummy_item, output_item=None):
        # at the moment there is a requirement to include at least one item is input to a function
        # a dummy will be added
        # TBD remove this restriction
        self.dummy_item = dummy_item
        table_name = 'widgets_scd_status'
        super().__init__(table_name=table_name, output_item=output_item)


class NegativeRemover(BaseTransformer):
    '''
    Replace negative values with NaN
    '''

    def __init__(self, names, sources=None):
        if names is None:
            raise RuntimeError("argument names must be provided")
        if sources is None:
            raise RuntimeError("argument sources must be provided")

        self.names = [names] if not isinstance(names, list) else names
        self.sources = [sources] if not isinstance(sources, list) else sources

        if len(self.names) != len(self.sources):
            raise RuntimeError("argument sources must be of the same length of names")

    def execute(self, df):
        for src, name in list(zip(self.sources, self.names)):
            df[name] = np.where(df[src] >= 0.0, df[src], np.nan)

        return df


class PivotRowsToColumns(BaseTransformer):
    '''
    Produce a column of data for each instance of a particular categoric value present
    '''

    def __init__(self, pivot_by_item, pivot_values, input_item=True, null_value=False, output_items=None):

        if not isinstance(pivot_values, list):
            raise TypeError('Expecting a list of pivot values. Got a %s.' % (type(pivot_values)))

        if output_items is None:
            output_items = ['%s_%s' % (x, input_item) for x in pivot_values]

        if len(pivot_values) != len(output_items):
            logger.exception('Pivot values: %s' % pivot_values)
            logger.exception('Output items: %s' % output_items)
            raise ValueError(
                'An output item name is required for each pivot value supplied. Length of the arrays must be equal')

        self.input_item = input_item
        self.null_value = null_value
        self.pivot_by_item = pivot_by_item
        self.pivot_values = pivot_values
        self.output_items = output_items

        super().__init__()

    def execute(self, df):

        df = df.copy()
        for i, value in enumerate(self.pivot_values):
            if not isinstance(self.input_item, bool):
                input_item = df[self.input_item]
            else:
                input_item = self.input_item
            if self.null_value is None:
                null_item = None,
            elif not isinstance(self.null_value, bool):
                null_item = df[self.null_value]
            else:
                null_item = self.null_value
            df[self.output_items[i]] = np.where(df[self.pivot_by_item] == value, input_item, null_item)

        self.log_df_info(df, 'After pivot rows to columns')

        return df


class OutlierRemover(BaseTransformer):
    '''
    Replace values outside of a threshold with NaN
    '''

    def __init__(self, name, source, min, max):
        if name is None:
            raise RuntimeError("argument name must be provided")
        if source is None:
            raise RuntimeError("argument source must be provided")
        if min is None and max is None:
            raise RuntimeError("one of argument min and max must be provided")

        self.name = name
        self.source = source
        self.min = min
        self.max = max

        super().__init__()

    def execute(self, df):
        if self.min is None:
            df[self.name] = np.where(df[self.source] <= self.max, df[self.source], np.nan)
        elif self.max is None:
            df[self.name] = np.where(self.min <= df[self.source], df[self.source], np.nan)
        else:
            df[self.name] = np.where(self.min <= df[self.source],
                                     np.where(df[self.source] <= self.max, df[self.source], np.nan), np.nan)
        return df


class SamplePreLoad(BasePreload):
    '''
    This is a demonstration function that logs the start of pipeline execution to a database table
    '''
    table_name = 'sample_pre_load'

    def __init__(self, dummy_items, output_item=None):
        super().__init__(dummy_items=dummy_items, output_item=output_item)

    def execute(self, start_ts=None, end_ts=None, entities=None):
        data = {'status': True, SystemLogTable._timestamp: dt.datetime.utcnow()}
        df = pd.DataFrame(data=data, index=[0])
        table = SystemLogTable(self.table_name, self._entity_type.db, Column('status', String(50)))
        table.insert(df)
        return True


class SampleActivityDuration(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities with start and end dates
    '''

    _is_instance_level_logged = False

    def __init__(self, input_activities, activity_duration=None):
        super().__init__(input_activities=input_activities, activity_duration=activity_duration)
        # define metadata for related activity tables
        self.activities_metadata['widget_maintenance_activity'] = ['PM', 'UM']
        self.activities_metadata['widget_transfer_activity'] = ['DT', 'IT']
        self.activities_custom_query_metadata = {}
        # self.activities_custom_query_metadata['CS'] = 'select effective_date as start_date, end_date, asset_id as deviceid from some_custom_activity_table'
        # registration
        self.constants = ['input_activities']
        self.outputs = ['activity_duration', 'shift_day', 'shift_id', 'shift_start_date', 'shift_end_date']
        self.optionalItems = ['dummy_items']


class TimeToFirstAndLastInDay(BaseTransformer):
    '''
    Calculate the time until the first occurance of a valid entry for an input_item in a period and
    the time until the end of period from the last occurance of a measurement
    '''
    execute_by = ['id', '_day']
    period_start = '_day'
    period_end = '_day_end'

    def __init__(self, input_item, time_to_first='time_to_first', time_from_last='time_from_last'):
        self.input_item = input_item
        self.time_to_first = time_to_first
        self.time_from_last = time_from_last
        super().__init__()

    def _calc(self, df):
        ts = df[self.input_item].dropna()
        ts = ts.index.get_level_values(self._entity_type._timestamp)
        first = ts.min()
        last = ts.max()
        df[self.time_to_first] = first
        df[self.time_from_last] = last

        return df

    def execute(self, df):
        df = df.copy()
        df = self._add_period_start_end(df)
        df = super().execute(df)
        df[self.time_to_first] = (df[self.time_to_first] - pd.to_datetime(
            df[self.period_start])).dt.total_seconds() / 60
        df[self.time_from_last] = (df[self.period_end] - df[self.time_from_last]).dt.total_seconds() / 60
        cols = [x for x in df.columns if x not in [self.period_start, self.period_end]]
        return df[cols]

    def _add_period_start_end(self, df):
        df[self.period_start] = df[self._entity_type._timestamp_col].dt.date
        df[self.period_end] = pd.to_datetime(df['_day']) + dt.timedelta(days=1)
        return df


class TimeToFirstAndLastInShift(TimeToFirstAndLastInDay):
    '''
    Calculate the time until the first occurance of a valid entry for an input_item in a period and
    the time until the end of period from the last occurance of a measurement
    '''
    execute_by = ['id', 'shift_day', 'shift_id']
    period_start = 'shift_start_date'
    period_end = 'shift_end_date'

    def __init__(self, input_item, time_to_first='time_to_first', time_from_last='time_from_last'):
        self.time_to_first = time_to_first
        self.time_from_last = time_from_last
        super().__init__(input_item=input_item, time_to_first=time_to_first, time_from_last=time_from_last)

    def _add_period_start_end(self, df):
        '''
        override parent method - instead of setting start and end dates from the gregorian calendar,
        set them from a custom calendar using a calendar object
        '''
        custom_calendar = self.get_custom_calendar()
        df = custom_calendar.execute(df)
        return df


class WriteDataFrame(BaseTransformer):
    '''
    Write the current contents of the pipeline to a database table
    '''
    out_table_prefix = ''
    version_db_writes = False
    out_table_if_exists = 'append'

    def __init__(self, input_items, out_table_name, output_status='output_status'):
        self.input_items = input_items
        self.output_status = output_status
        self.out_table_name = out_table_name
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_status] = self.write_frame(df=df[self.input_items])
        return df
