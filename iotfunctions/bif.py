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
import numpy as np
import re
import pandas as pd
import logging
import iotfunctions as iotf
from .base import BaseTransformer, BaseEvent


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
    def __init__ (self,shift_definition=None,
                  shift_start_date = 'shift_start_date',
                  shift_end_date = 'shift_end_date',
                  shift_day = 'shift_day',
                  shift_id = 'shift_id'):
        if shift_definition is None:
            shift_definition = {
               "1": [5.5, 14],
               "2": [14, 21],
               "3": [21, 29.5]
           }
        self.shift_definition = shift_definition
        self.shift_start_date = shift_start_date
        self.shift_end_date = shift_end_date
        self.shift_day = shift_day
        self.shift_id = shift_id
        super().__init__()
        self.constants = ['shift_definition']
        self.outputs = ['shift_start_date','shift_end_date','shift_day','shift_id']
        self.itemTags['shift_start_date'] = ['DIMENSION']
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
            data[self.shift_start_date] = [x+dt.timedelta(hours=start_end[0]) for x in dates]
            data[self.shift_end_date] = [x+dt.timedelta(hours=start_end[1]) for x in dates]
            dfs.append(pd.DataFrame(data))
        df = pd.concat(dfs)
        df[self.shift_start_date] = pd.to_datetime(df[self.shift_start_date])
        df[self.shift_end_date] = pd.to_datetime(df[self.shift_end_date])
        df.sort_values([self.shift_start_date],inplace=True)
        return df
    
    def get_empty_data(self):
        cols = [self.shift_day, self.shift_id, self.shift_start_date, self.shift_end_date]
        df = pd.DataFrame(columns = cols)
        return df
    
    def execute(self,df):
        df.sort_values([self._entity_type._timestamp_col],inplace = True)
        calendar_df = self.get_data(start_date= df[self._entity_type._timestamp_col].min(), end_date = df[self._entity_type._timestamp_col].max())
        df = pd.merge_asof(left = df,
                           right = calendar_df,
                           left_on = self._entity_type._timestamp,
                           right_on = self.shift_start_date,
                           direction = 'backward')
        df = self.conform_index(df)
        return df
    

   
    