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
The Built In Functions module contains extensions to the function catalog that
you may register and use.
'''

import datetime as dt
from collections import OrderedDict
import numpy as np
import re
import pandas as pd
import logging
import iotfunctions as iotf
from .base import BaseTransformer, BaseEvent, BaseSCDLookup, BaseMetadataProvider
from .ui import UISingle,UIMultiItem,UIFunctionOutSingle


logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'


class IoTAlertExpression(BaseEvent):
    '''
    Create alerts that are triggered when data values reach a particular range.
    '''
    def __init__(self, input_items, expression , alert_name):
        self.input_items = input_items
        self.expression = expression
        self.alert_name = alert_name
        super().__init__()
        # registration metadata
        self.inputs = ['input_items', 'expression']
        self.constants = ['expression']
        self.outputs = ['alert_name']
        self.tags = ['EVENT']
        
    def _calc(self,df):
        '''
        unused
        '''
        return df
        
    def execute(self, df):
        df = df.copy()
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
            msg = 'expression converted to %s' %expr
        else:
            expr = self.expression
            msg = 'expression (%s)' %expr
        self.trace_append(msg)
        df[self.alert_name] = np.where(eval(expr), True, np.nan)
        return df
    
    
class IoTAlertOutOfRange(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least one threshold.
    """
    
    optionalItems = ['lower_threshold','upper_threshold'] 
    
    def __init__ (self,input_item, lower_threshold=None, upper_threshold=None,
                  output_alert_upper = 'output_alert_upper', output_alert_lower = 'output_alert_lower'):
        
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
        
    def _calc(self,df):
        '''
        unused
        '''        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.output_alert_upper] = False
        df[self.output_alert_lower] = False
        
        if not self.lower_threshold is None:
            df[self.output_alert_lower] = np.where(df[self.input_item]<=self.lower_threshold,True,False)
        if not self.upper_threshold is None:
            df[self.output_alert_upper] = np.where(df[self.input_item]>=self.upper_threshold,True,False)
            
        return df  
    
    
class IoTAlertHighValue(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold'.
    """
    
    def __init__ (self,input_item,  upper_threshold=None,
                  alert_name = 'alert_name', ):
        self.input_item = input_item
        self.upper_threshold = float(upper_threshold)
        self.alert_name = alert_name
        super().__init__()
        
    def _calc(self,df):
        '''
        unused
        '''        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item]>=self.upper_threshold,True,False)
            
        return df     
    
class IoTAlertLowValue(BaseEvent):
    """
    Fire alert when metric goes below a threshold'.
    """
    
    def __init__ (self,input_item,  lower_threshold=None,
                  alert_name = 'alert_name', ):
        self.input_item = input_item
        self.lower_threshold = float(lower_threshold)
        self.alert_name = alert_name
        super().__init__()
        
    def _calc(self,df):
        '''
        unused
        '''
        return df        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item]<=self.lower_threshold,True,False)
            
        return df


class IoTCosFunction(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage. Function returns a single output.
    """        
    
    def __init__(self,function_name,input_items,output_item,parameters=None):
        
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
        #registration metadata
        self.optionalItems = ['parameters']
        self.inputs = ['input_items']
        self.outputs = ['ouput_item']
        self.itemDatatypes['output_item'] = None #choose datatype in UI
        
    def execute(self,df):
        db = self.get_db()
        bucket = self.get_bucket_name()    
        #first test execution could include a fnction object
        #serialize it
        if callable(self.function_name):
            db.cos_save(persisted_object=self.function_name,
                        filename=self.function_name.__name__,
                        bucket=bucket, binary=True)
            self.function_name = self.function_name.__name__
        # retrieve
        function = db.cos_load(filename=self.function_name,
                               bucket=bucket,
                               binary=True)
        #execute
        df = df.copy()
        rf = function(df,self.parameters)
        #rf will contain the orginal columns along with a single new output column.
        return rf
    
class IoTDropNull(BaseMetadataProvider):
    '''
    Drop any row that has all null metrics
    '''
    def __init__(self,exclude_items,drop_all_null_rows = True,output_item = 'drop_nulls'):
        
        kw = {'_custom_exclude_col_from_auto_drop_nulls': exclude_items,
              '_drop_all_null_rows' : drop_all_null_rows}
        super().__init__(dummy_items = exclude_items, output_item = output_item, **kw)
        self.exclude_items = exclude_items
        self.drop_all_null_rows = drop_all_null_rows
        
        
    @classmethod
    def get_metadata(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['exclude_items'] = UIMultiItem(name = 'exclude_items',
                                              datatype=None,
                                              description = 'Ignore non-null values in these columns when dropping rows'
                                              ).to_metadata()
        inputs['drop_all_null_rows'] = UISingle(name = 'drop_all_null_rows',
                                                datatype=bool,
                                                description = 'Enable or disable drop of all null rows'
                                                ).to_metadata()
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_item'] = UIFunctionOutSingle(name = 'output_item',datatype=bool,description='Returns a status flag of True when executed').to_metadata()
                
        return (inputs,outputs)            
    
    def _getMetadata(self, df = None, new_df = None, inputs = None, outputs = None, constants = None):        
                
        return self.get_metadata()    

    
class IoTExpression(BaseTransformer):
    '''
    Create a new item from an expression involving other items
    '''
    def __init__(self, expression , output_name):
        self.output_name = output_name
        super().__init__()
        #convert single quotes to double
        expression = expression.replace("'",'"')
        if '${' in expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", expression)
            msg = 'expression converted to %s' %expr
        else:
            expr = expression
            msg = 'expression (%s)' %expr
        self.trace_append(msg)
        self.expression = expression
        #registration
        self.constants = ['expression']
        self.outputs = ['output_name']
        
                
    def execute(self, df):
        df = df.copy()
        requested = list(self.get_input_items())
        msg = ' | function requested items %s using get_input_data. ' %','.join(requested)
        self.trace_append(msg)
        df[self.output_name] = eval(self.expression)
        return df
    
    def get_input_items(self):
        #get all quoted strings in expression
        possible_items = re.findall('"([^"]*)"', self.expression)
        #check if they have df[] wrapped around them
        items = [x for x in possible_items if 'df["%s"]'%x in self.expression]
        if len(items) == 0:
            msg = 'The expression %s does not contain any input items or the function has not been executed to obtain them.' %self.expression
            logger.debug(msg)
        return set(items)


class IoTRaiseError(BaseTransformer):
    """
    Halt execution of the pipeline raising an error that will be shown. This function is 
    useful for testing a pipeline that is running to completion but not delivering the expected results.
    By halting execution of the pipeline you can view useful diagnostic information in an error
    message displayed in the UI.
    """
    def __init__(self,halt_after, 
                 abort_execution = True,
                 output_item = 'pipeline_exception'):
                 
        super().__init__()
        self.halt_after = halt_after
        self.abort_execution = abort_execution
        self.output_item = output_item
        
    def execute(self,df):
        
        msg = self.log_df_info(df,'Prior to raising error')
        self.trace_append(msg)
        msg = 'Entity type metadata: %s' %self._entity_type.__dict__
        self.trace_append(msg)
        msg = 'Function metadata: %s' %self.__dict__
        self.trace_append(msg)
        msg = 'Calculation was halted deliberately by the inclusion of a function that raised an exception in the configuration of the pipeline'
        if self.abort_execution:
            raise RuntimeError(msg)
        
        df[self.output_item] = True
        return df
    
    @classmethod
    def get_metadata(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['halt_after'] = UIMultiItem(name = 'halt_after',
                                              datatype=None,
                                              description = 'Raise error after calculating items'
                                              ).to_metadata()
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_item'] = UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ).to_metadata()
    
        return (inputs,outputs)    

        
    def _getMetadata(self, df = None, new_df = None, inputs = None, outputs = None, constants = None):        
                
        return self.get_metadata()

            
    
class IoTPackageInfo(BaseTransformer):
    """
    Show the version number 
    """
    
    def __init__ (self, dummy_item, package_url = 'package_url', module = 'module', version = 'version'):
        
        self.dummy_item = dummy_item
        self.package_url = package_url
        self.module = module
        self.version = version
        super().__init__()
        
    def execute(self,df):
        
        df = df.copy()
        df[self.package_url] = self.url
        df[self.module] = self.__module__
        df[self.version] = iotf.__version__
        
        return df
    
class IoTSCDLookup(BaseSCDLookup):
    '''
    Lookup an scd property from a scd lookup table containing: 
    Start_date, end_date, device_id and property. End dates are not currently used.
    Previous lookup value is assumed to be valid until the next.
    '''
    
    def __init__(self, table_name , output_item = None):
        self.table_name = table_name
        super().__init__(table_name = table_name, output_item = output_item)    
    
class IoTShiftCalendar(BaseTransformer):
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
    def __init__ (self,shift_definition=None,
                  period_start_date = 'shift_start_date',
                  period_end_date = 'shift_end_date',
                  shift_day = 'shift_day',
                  shift_id = 'shift_id'):
        if shift_definition is None:
            shift_definition = {
               "1": [5.5, 14],
               "2": [14, 21],
               "3": [21, 29.5]
           }
        self.shift_definition = shift_definition
        self.period_start_date = period_start_date
        self.period_end_date = period_end_date
        self.shift_day = shift_day
        self.shift_id = shift_id
        super().__init__()
        #registration
        self.constants = ['shift_definition']
        self.outputs = ['period_start_date','period_end_date','shift_day','shift_id']
        self.itemTags['period_start_date'] = ['DIMENSION']
        self.itemTags['period_end_date'] = ['DIMENSION']
        self.itemTags['shift_day'] = ['DIMENSION']
        self.itemTags['shift_id'] = ['DIMENSION']
    
    def get_data(self,start_date,end_date):
        start_date = start_date.date()
        end_date = end_date.date()
        dates = pd.DatetimeIndex(start=start_date,end=end_date,freq='1D').tolist()
        dfs = []
        for shift_id,start_end in list(self.shift_definition.items()):
            data = {}
            data[self.shift_day] = dates
            data[self.shift_id] = shift_id
            data[self.period_start_date] = [x+dt.timedelta(hours=start_end[0]) for x in dates]
            data[self.period_end_date] = [x+dt.timedelta(hours=start_end[1]) for x in dates]
            dfs.append(pd.DataFrame(data))
        df = pd.concat(dfs)
        df[self.period_start_date] = pd.to_datetime(df[self.period_start_date])
        df[self.period_end_date] = pd.to_datetime(df[self.period_end_date])
        df.sort_values([self.period_start_date],inplace=True)
        return df
    
    def get_empty_data(self):
        cols = [self.shift_day, self.shift_id, self.period_start_date, self.period_end_date]
        df = pd.DataFrame(columns = cols)
        return df
    
    def execute(self,df):
        try:
            df.sort_values([self._entity_type._timestamp_col],inplace = True)
        except KeyError:
            msg = self.log_df_info(df,'key error when sorting on _timestamp during custom calendar lookup')
            raise RuntimeError(msg)
            
        calendar_df = self.get_data(start_date= df[self._entity_type._timestamp_col].min(), end_date = df[self._entity_type._timestamp_col].max())
        df = pd.merge_asof(left = df,
                           right = calendar_df,
                           left_on = self._entity_type._timestamp,
                           right_on = self.period_start_date,
                           direction = 'backward')
        if self.auto_conform_index:
            df = self.conform_index(df)
            
        return df
    

   
    