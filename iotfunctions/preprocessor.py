# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import math
import os
import urllib3
import numbers
import datetime as dt
import logging
import warnings
import json
import numpy as np
import pandas as pd
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, MetaData, ForeignKey, create_engine
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
import ibm_db
import ibm_db_dbi
from sqlalchemy.types import String,SmallInteger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm.session import sessionmaker
from inspect import getargspec
from collections import OrderedDict
from .db import Database, SystemLogTable
from .metadata import EntityType
from .automation import TimeSeriesGenerator
from .base import BaseFunction, BaseTransformer, BaseDataSource, BaseEvent,BaseFilter, BaseAggregator, BaseDatabaseLookup, BaseDBActivityMerge, BaseSCDLookup, BaseMetadataProvider, BasePreload
from .bif import IoTAlertOutOfRange as AlertThreshold #for compatibility
from .bif import IoTShiftCalendar
from .ui import UIFunctionOutSingle, UISingle, UISingleItem

'''
Sample functions
'''


logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
    
    
class CompanyFilter(BaseFilter):
    '''
    Demonstration function that filters on particular company codes. 
    '''
    
    def __init__(self, company_code, company,output_item = None):
        super().__init__(dependent_items = company_code, output_item = output_item)
        self.company_code = company_code
        self.company = company
        
    def get_item_values(self,arg):
        """
        Get list of columns from lookup table, Create lookup table from self.data if it doesn't exist.
        """
        if arg == 'company':           
            return(['AMCE','ABC','JDI'])
        
    def filter(self,df):
        df = df[df[self.company_code]==self.company]
        return df    
    
    
class ComputationsOnStringArray(BaseTransformer):
    '''
    Perform computation on a string that contains a comma separated list of values
    '''
    # The metadata describes what columns of data are included in the array
    column_metadata = ['x1','x2','x3','x4','x5']
    
    def __init__(self, input_str, output_item = 'output_item'):
    
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
    
    def get_test_data(self):
        
        data = {
                self._entity_type._entity_id : [1,1,1,1,1,2,2,2,2,2],
                self._entity_type._timestamp_col : [
                        dt.datetime.strptime('Oct 1 2018 1:33PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:37PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:39PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:38PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:29PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:39PM', '%b %d %Y %I:%M%p'),                
                ],                
                'x_str' : [self._get_str_array() for x in list(range(10))]
                }
        df = pd.DataFrame(data=data)
        df = self.conform_index(df)
        
        return df
    
    def _get_str_array(self):
        
        out = ''
        for col in self.column_metadata:
            out = out + str(np.random.normal(1,0.1)) + ','
        out = out[:-1]
        
        return out    
    
    
class EntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    """
    
    freq = '5min' 
    # ids of entities to generate. Change the value of the range() function to change the number of entities
    
    def __init__ (self, dummy_items, output_item = None):
        super().__init__(dummy_items = dummy_items, output_item = output_item)
        self.inputs = ['dummy_items']
        self.outputs = ['output_item']
        warnings.warn(('This sample function is deprecated.'
                       ' Use bif.IoTEntityDataGenerator.'),
                      DeprecationWarning)
        
    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):
        
        #This sample builds data with the TimeSeriesGenerator.
        
        if entities is None:
            entities = self.get_entity_ids()
            
        if not start_ts is None:
            seconds = (dt.datetime.utcnow() - start_ts).total_seconds()
        else:
            seconds = pd.to_timedelta(self.freq).total_seconds()
        
        df = self._entity_type.generate_data(entities=entities, days=0, seconds = seconds, freq = self.freq, write=True)        
        self.trace_append(msg='%s Generated data. ' %self.__class__.__name__,df=df)
        
        return True  
    
    
    def get_entity_ids(self):
        '''
        Generate a list of entity ids
        '''
        ids = [str(73000 + x) for x in list(range(5))]
        return (ids)
    
class FillForwardByEntity(BaseTransformer):    
    '''
    Fill null values forward from last item for the same entity instance
    '''

    execute_by = ['id']
    
    def __init__(self, input_item, output_item = 'output_item'):
    
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

    def __init__(self, input_flows, output_flows, loss_threshold = 0.005 , output = 'output' ):
        
        self.input_flows = input_flows
        self.output_flows = output_flows
        self.loss_threshold = float(loss_threshold)
        self.output = output
        
        super().__init__()
        
    def execute(self,df):
        df = df.copy()
        total_input = df[self.input_flows].sum(axis='columns')
        total_output = df[self.output_flows].sum(axis='columns')
        df[self.output] = np.where((total_input-total_output)/ total_input > self.loss_threshold, True, False)
        
        return df   
      
    
class GenerateCerealFillerData(BaseDataSource):
    """
    Replace automatically retrieved entity source data with new generated data. 
    Supply a comma separated list of input item names to be generated.
    """
    # The merge_method of any data source function governs how the new data retrieved will be combined with the pipeline
    merge_method = 'replace'
    # Parameters for data generator
    # Number of days worth of data to generate on initial execution
    days = 1
    # frequency of data load
    freq = '1min' 
    # ids of entities to generate. Change the value of the range() function to change the number of entities
    ids = [str(98000 + x) for x in list(range(5))]
    
    def __init__ (self,input_items=None, output_items=None):
        
        if input_items is None:
            input_items = ['temperature','humidity']
        
        super().__init__(input_items = input_items, output_items = output_items)
        self.optional_items = ['input_items']
        
    def get_data(self,
                 start_ts= None,
                 end_ts= None,
                 entities = None):
        '''
        This sample builds data with the TimeSeriesGenerator. You can get data from anywhere 
        using a custom source function. When the engine executes get_data(), after initial load
        it will pass a start date as it will be looking for data added since the last 
        checkpoint.
        
        The engine may also pass a set of entity ids to retrieve data for. Since this is a 
        custom source we will ignore the entity list provided by the engine.
        '''
        # distinguish between initial and incremental execution
        if start_ts is None:
            days = self.days
            seconds = 0
        else:
            days = 0
            seconds = (dt.datetime.utcnow() - start_ts).total_seconds()
        
        ts = TimeSeriesGenerator(metrics = self.input_items,ids=self.ids,days=days,seconds = seconds,
                                 timestamp = self._entity_type._timestamp)
        df = ts.execute()
        
        return df    

    
class InputsAndOutputsOfMultipleTypes(BaseTransformer):
    '''
    This sample function is just a pass through that demonstrates the use of multiple datatypes for inputs and outputs
    '''
    
    def __init__(self, input_number,input_date, input_str, 
                 output_number = 'output_number',
                 output_date =  'output_date',
                 output_str = 'output_str' ):   
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
    
class InputDataGenerator(BaseTransformer):
    '''
    This sample function was removed.
    '''
    def __init__(self):
        
        super().__init__()
        
    def execute(self,df):
        
        raise ImportError ('Error in function catalog. InputDataGenerator was removed from samples. Use EntityDataGenerator.')

class TempPressureVolumeGenerator(BaseTransformer):
    '''
    This sample function was removed.
    '''
    def __init__(self):
        
        super().__init__()
        
    def execute(self,df):
        
        raise ImportError ('Error in function catalog. TempPressureVolumeGenerator was removed from samples. Use EntityDataGenerator.')
    
    
    
class LookupCompany(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """
    
    #create the table and populate it using the data dict
    _auto_create_lookup_table = True

    def __init__ (self, lookup_items= None, output_items=None):
        
        # sample data will be used to create table if it doesn't already exist
        # sample data will be converted into a dataframe before being written to a  table
        # make sure that the dictionary that you provide is understood by pandas
        # use lower case column names
        self.data = {
                'company' : ['ABC','ACME','JDI'] ,
                'currency_code' : ['USD','CAD','USD'] ,
                'employee_count' : [100,120,352],
                'inception_date' : [
                        dt.datetime.strptime('Feb 1 2005 7:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Jan 1 1988 5:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Feb 28 1967 9:00AM', '%b %d %Y %I:%M%p')
                        ]
                }
        
        # Must specify:
        #One or more business key for the lookup
        #The UI will ask you to map each business key to an AS Data Item
        company_key = 'company'
        #must specify a name of the lookup name even if you are supplying your own sql.
        #use lower case table names
        lookup_table_name = 'company'
        # Indicate which of the input parameters are lookup keys
        lookup_keys = [company_key] 
        # Indicate which of the column returned should be converted into dates
        parse_dates = ['inception_date']
        
        super().__init__(
             lookup_table_name = lookup_table_name,
             lookup_keys= lookup_keys,
             lookup_items = lookup_items,
             parse_dates= parse_dates, 
             output_items = output_items
             )
        
        self.itemTags['output_items'] = ['DIMENSION']
    
        # The base class takes care of the rest
        # No execute() method required
                
        
class LookupOperator(BaseSCDLookup):
    '''
    Lookup Operator information from a resource calendar
    A resource calendar is a table with a start_date, end_date, device_id and resource_id
    Resource assignments are defined for each device_id. Each assignment has a start date
    End dates are not currently used. Assignment is assumed to be valid until the next.
    '''
    
    def __init__(self, dummy_item , output_item = None):
        
        #at the moment there is a requirement to include at least one item is input to a function
        #a dummy will be added
        # TBD remove this restriction
        self.dummy_item = dummy_item
        table_name = 'widgets_scd_operator'
        super().__init__(table_name = table_name, output_item = output_item)
        

class LookupStatus(BaseSCDLookup):
    '''
    Lookup Operator information from a resource calendar
    A resource calendar is a table with a start_date, end_date, device_id and resource_id
    Resource assignments are defined for each device_id. Each assignment has a start date
    End dates are not currently used. Assignment is assumed to be valid until the next.
    '''
    
    def __init__(self, dummy_item , output_item = None):
        
        #at the moment there is a requirement to include at least one item is input to a function
        #a dummy will be added
        # TBD remove this restriction
        self.dummy_item = dummy_item
        table_name = 'widgets_scd_status'
        super().__init__(table_name = table_name, output_item = output_item)
    

     
class MergeSampleTimeSeries(BaseDataSource):
    """
    Merge the contents of a table containing time series data with entity source data

    """
    merge_method = 'nearest' #or outer, concat
    #use concat when the source time series contains the same metrics as the entity type source data
    #use nearest to align the source time series to the entity source data
    #use outer to add new timestamps and metrics from the source
    merge_nearest_tolerance = pd.Timedelta('1D')
    merge_nearest_direction = 'nearest' 
    source_table_name = 'sample_time_series'
    source_entity_id = 'deviceid'
    #metadata for generating sample
    sample_metrics = ['temp','pressure','velocity']
    sample_entities = ['entity1','entity2','entity3']
    sample_initial_days = 3
    sample_freq = '1min'
    sample_incremental_min = 5
    
    def __init__(self, input_items, output_items=None):
        super().__init__(input_items = input_items, output_items = output_items)

    def get_data(self,start_ts=None,end_ts=None,entities=None):
        
        self.load_sample_data()
        (query,table) = self._entity_type.db.query(self.source_table_name,schema = self._entity_type._db_schema)
        if not start_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        df = pd.read_sql(query.statement,
                         con = self._entity_type.db.connection,
                         parse_dates=[self._entity_type._timestamp])        
        return df
    
    
    def get_item_values(self,arg):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':

            return self._entity_type.db.get_column_names(self.source_table_name)
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)      
        
    
    def load_sample_data(self):
                
        if not self._entity_type.db.if_exists(self.source_table_name):
            generator = TimeSeriesGenerator(metrics=self.sample_metrics,
                                            ids = self.sample_entities,
                                            freq = self.sample_freq,
                                            days = self.sample_initial_days,
                                            timestamp = self._entity_type._timestamp)
        else:
            generator = TimeSeriesGenerator(metrics=self.sample_metrics,
                                            ids = self.sample_entities,
                                            freq = self.sample_freq,
                                            seconds = self.sample_incremental_min*60,
                                            timestamp = self._entity_type._timestamp)
        
        df = generator.execute()
        self._entity_type.db.write_frame(df = df, table_name = self.source_table_name,
                       version_db_writes = False,
                       if_exists = 'append',
                       schema = self._entity_type._db_schema,
                       timestamp_col = self._entity_type.timestamp_col)
        
    def get_test_data(self):

        generator = TimeSeriesGenerator(metrics=['acceleration'],
                                        ids = self.sample_entities,
                                        freq = self.sample_freq,
                                        seconds = 300,
                                        timestamp = self._entity_type._timestamp)
        
        df = generator.execute()
        df = self.conform_index(df)
        return df

class MultiplyArrayByConstant(BaseTransformer):
    '''
    Multiply a list of input columns by a constant to produce a new output column for each input column in the list.
    The names of the new output columns are defined in a list(array) rather than as discrete parameters.
    '''
    
    def __init__(self, input_items, constant, output_items):
                
        self.input_items = input_items
        self.output_items = output_items
        self.constant = float(constant)
        super().__init__()

    def execute(self, df):
        df = df.copy()
        for i,input_item in enumerate(self.input_items):
            df[self.output_items[i]] = df[input_item] * self.constant
        return df


class MultiplyByTwo(BaseTransformer):
    '''
    Multiply input column by 2 to produce output column
    '''
    auto_register_args = {
        'input_item' : 'x_1'
        }
    
    def __init__(self, input_item, output_item = 'output_item'):
        
        self.input_item = input_item
        self.output_item = output_item
        
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item] * 2
        return df

class MultiplyByConstant(BaseTransformer):
    '''
    Multiply input column by a constant to produce output column
    '''
    
    def __init__(self, input_item, constant, output_item = 'output_item'):
                
        self.input_item = input_item
        self.output_item = output_item
        self.constant = float(constant)
        
        super().__init__()

    def execute(self, df):
        df = df.copy()        
        df[self.output_item] = df[self.input_item] * self.constant
        return df
    
class MultiplyByConstantPicklist(BaseTransformer):
    '''
    Multiply input value by a constant that will be entered via a picklist.
    '''
    
    def __init__(self, input_item, constant, output_item = 'output_item'):
                
        self.input_item = input_item
        self.output_item = output_item
        self.constant = float(constant)
        super().__init__()
        
        self.itemValues['constant'] = [-1,2,3,4,5]

    def execute(self, df):
        df = df.copy()        
        df[self.output_item] = df[self.input_item] * self.constant
        return df    

class MultiplyTwoItems(BaseTransformer):
    '''
    Multiply two input items together to produce output column
    '''
    
    def __init__(self, input_item_1, input_item_2, output_item = 'output_item'):
        self.input_item_1 = input_item_1
        self.input_item_2 = input_item_2
        self.output_item = output_item
        
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item_1] * df[self.input_item_2]
        return df        

class MultiplyNItems(BaseTransformer): 
    '''
    Multiply multiple items together to produce output column
    '''
    
    def __init__(self, input_items, output_item = 'output_item'):
    
        self.input_items = input_items
        self.output_item = output_item
        
        super().__init__()
        
    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_items].product(axis=1)
        return df

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
    
    def __init__(self, pivot_by_item, pivot_values, input_item=True, null_value=False, output_items = None):
        
        if not isinstance(pivot_values,list):
            raise TypeError('Expecting a list of pivot values. Got a %s.' %(type(pivot_values)))
        
        if output_items is None:
            output_items = [ '%s_%s' %(x,input_item) for x in pivot_values]
        
        if len(pivot_values) != len(output_items):
            logger.exception('Pivot values: %s' %pivot_values)
            logger.exception('Output items: %s' %output_items)
            raise ValueError('An output item name is required for each pivot value supplied. Length of the arrays must be equal')
            
        self.input_item = input_item
        self.null_value = null_value
        self.pivot_by_item = pivot_by_item
        self.pivot_values = pivot_values
        self.output_items = output_items
        
        super().__init__()
        
    def execute (self,df):
        
        df = df.copy()
        for i,value in enumerate(self.pivot_values):
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
            df[self.output_items[i]] = np.where(df[self.pivot_by_item]==value,input_item,null_item)
            
        self.log_df_info(df,'After pivot rows to columns')
            
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

    def __init__(self, dummy_items, output_item = None):
        super().__init__(dummy_items = dummy_items, output_item = output_item)
        
    def execute(self,start_ts = None,end_ts=None,entities=None):
        
        data = {'status': True, SystemLogTable._timestamp:dt.datetime.utcnow() }
        df = pd.DataFrame(data=data, index = [0])
        table = SystemLogTable(self.table_name,self._entity_type.db,
                               Column('status',String(50)))
        table.insert(df)    
        return True    
        

class SampleActivityMerge(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities with start and end dates
    '''
    
    warnings.warn(('This sample function is deprecated. Use IoTActivityDuration'
                   ' for simple cases involving a single table.'
                   ' Use SampleActivityDuration for an example of a complex'
                   ' case with multiple input tables'
                   ),
                  DeprecationWarning)
    
    execute_by = ['deviceid']    
    _is_instance_level_logged = False    
    def __init__(self,
                 input_activities,
                 activity_duration=None,
                 additional_items = None,
                 additional_output_names = None,
                 dummy_items = None):
        super().__init__(input_activities=input_activities,
                         activity_duration=activity_duration,
                         additional_items = additional_items,
                         additional_output_names = additional_output_names,
                         dummy_items = dummy_items )
        self.activities_metadata['widget_maintenance_activity'] = ['PM','UM']
        self.activities_metadata['widget_transfer_activity'] = ['DT','IT']
        self.activities_custom_query_metadata = {}
        #self.activities_custom_query_metadata['CS'] = 'select effective_date as start_date, end_date, asset_id as deviceid from some_custom_activity_table'
        et = self.get_entity_type()
        et.add_slowly_changing_dimension(scd_property = 'status', table_name = 'widgets_dec12b_scd_status')
        et.add_slowly_changing_dimension(scd_property = 'operator', table_name = 'widgets_dec12b_scd_operator')
        

class SampleActivityDuration(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities with start and end dates
    '''
    _is_instance_level_logged = False
    def __init__(self,input_activities, activity_duration=None):
        super().__init__(input_activities=input_activities,
                         activity_duration=activity_duration )
        # define metadata for related activity tables
        self.activities_metadata['widget_maintenance_activity'] = ['PM','UM']
        self.activities_metadata['widget_transfer_activity'] = ['DT','IT']
        self.activities_custom_query_metadata = {}
        #self.activities_custom_query_metadata['CS'] = 'select effective_date as start_date, end_date, asset_id as deviceid from some_custom_activity_table'         
        #registration
        self.constants = ['input_activities']
        self.outputs = ['activity_duration','shift_day','shift_id','shift_start_date','shift_end_date']
        self.optionalItems = ['dummy_items']

    
class StatusFilter(BaseFilter):
    '''
    Demonstration function that filters on particular company codes. 
    '''
    
    def __init__(self, status_input_item, include_only,output_item = None):
        super().__init__(dependent_items = [status_input_item], output_item = output_item)
        self.status_input_item = status_input_item
        self.include_only = include_only
        
    def get_item_values(self,arg):
        if arg == 'include_only':
            return(['active','inactive'])
        else:
            return None
        
    def filter(self,df):
        df = df[df['status']==self.include_only]
        return df

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs=[]
        inputs.append(UISingleItem(name = 'status_input_item',
                                              datatype=None,
                                              description = 'Item name to use for status'
                                              ))
        inputs.append(UISingle(name = 'include_only',
                                              datatype=str,
                                              description = 'Filter to include only rows with a status of this'
                                              ))        
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Item that contains the execution status of this function'
                                                     ))
        return (inputs,outputs)
            
    
class TimeToFirstAndLastInDay(BaseTransformer):
    '''
    Calculate the time until the first occurance of a valid entry for an input_item in a period and
    the time until the end of period from the last occurance of a measurement
    '''
    execute_by = ['id','_day']
    period_start = '_day'
    period_end = '_day_end'
    
    def __init__(self, input_item, time_to_first = 'time_to_first' , time_from_last = 'time_from_last'):
        
        self.input_item = input_item
        self.time_to_first = time_to_first
        self.time_from_last = time_from_last
        super().__init__()
        
    def _calc(self,df):
        
        ts = df[self.input_item].dropna()
        ts = ts.index.get_level_values(self._entity_type._timestamp)
        first = ts.min()
        last = ts.max() 
        df[self.time_to_first] = first
        df[self.time_from_last] = last
        
        return df
    
    def execute(self,df):
        
        df = df.copy()
        df = self._add_period_start_end(df)
        df = super().execute(df)
        df[self.time_to_first] = (df[self.time_to_first]-pd.to_datetime(df[self.period_start])).dt.total_seconds() / 60
        df[self.time_from_last] = (df[self.period_end] - df[self.time_from_last]).dt.total_seconds() / 60
        cols = [x for x in df.columns if x not in [self.period_start,self.period_end]]
        return df[cols]
    
    def _add_period_start_end(self,df):
        
        df[self.period_start] = df[self._entity_type._timestamp_col].dt.date
        df[self.period_end] = pd.to_datetime(df['_day']) + dt.timedelta(days=1)
        return df
    

class TimeToFirstAndLastInShift(TimeToFirstAndLastInDay):
    '''
    Calculate the time until the first occurance of a valid entry for an input_item in a period and
    the time until the end of period from the last occurance of a measurement
    '''
    execute_by = ['id','shift_day','shift_id']
    period_start = 'shift_start_date'
    period_end = 'shift_end_date'
    
    def __init__(self, input_item,
                 time_to_first = 'time_to_first' ,
                 time_from_last = 'time_from_last'
                 ):
        
        self.time_to_first = time_to_first
        self.time_from_last = time_from_last
        super().__init__(input_item = input_item,
                         time_to_first = time_to_first,
                         time_from_last = time_from_last)        
                
    def _add_period_start_end(self,df):
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

    def __init__(self, input_items, out_table_name, output_status= 'output_status'):
        self.input_items = input_items
        self.output_status = output_status
        self.out_table_name = out_table_name
        super().__init__()
        
    def execute (self, df):
        df = df.copy()
        df[self.output_status] = self.write_frame(df=df[self.input_items])
        return df
    
   
    
    

        

    



