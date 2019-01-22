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
from .base import BaseTransformer, BaseEvent, BaseSCDLookup, BaseMetadataProvider, BasePreload, BaseDatabaseLookup
from .ui import UISingle,UIMultiItem,UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti

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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['input_items'] = UIMultiItem(name = 'input_items',
                                              datatype=None,
                                              description = 'Input items'
                                              ).to_metadata()
        inputs['expression'] = UISingle(name = 'expression',
                                              datatype=str,
                                              description = "Define alert expression using pandas systax. Example: df['inlet_temperature']>50"
                                              ).to_metadata()        
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['alert_name'] = UIFunctionOutSingle(name = 'alert_name',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ).to_metadata()
    
        return (inputs,outputs)    
    
    
class IoTAlertOutOfRange(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least one threshold.
    """
    
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['input_item'] = UISingleItem(name = 'input_item',
                                              datatype=None,
                                              description = 'Item to alert on'
                                              ).to_metadata()
        inputs['lower_threshold'] = UISingle(name = 'lower_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is lower than this value',
                                              required = False,
                                              ).to_metadata()
        inputs['upper_threshold'] = UISingle(name = 'upper_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is higher than this value',
                                              required = False,
                                              ).to_metadata()  
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_alert_lower'] = UIFunctionOutSingle(name = 'output_alert_lower',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ).to_metadata()        
        outputs['output_alert_upper'] = UIFunctionOutSingle(name = 'output_alert_upper',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ).to_metadata()
    
        return (inputs,outputs)    
    
    
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name = 'input_item',
                                              datatype=None,
                                              description = 'Item to alert on'
                                              ))
        inputs.append(UISingle(name = 'upper_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is higher than this value'
                                              ))  
        #define arguments that behave as function outputs
        outputs = []     
        outputs.append(UIFunctionOutSingle(name = 'alert_name',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ))    
        return (inputs,outputs)    

        
    def _getMetadata(self, df = None, new_df = None, inputs = None, outputs = None, constants = None):        
                
        return self.build_ui()

    
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['input_item'] = UISingleItem(name = 'input_item',
                                              datatype=None,
                                              description = 'Item to alert on'
                                              ).to_metadata()
        inputs['lower_threshold'] = UISingle(name = 'upper_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is lower than this value'
                                              ).to_metadata()  
        #define arguments that behave as function outputs
        outputs = OrderedDict()       
        outputs['alert_name'] = UIFunctionOutSingle(name = 'alert_name',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ).to_metadata()
    
        return (inputs,outputs)    


class IoTCosFunction(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage. Function returns a single output.
    """        
    
    def __init__(self,function_name,input_items,output_item='output_item',parameters=None):
        
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        inputs.append(UISingle(name = 'function_name',
                                              datatype=float,
                                              description = 'Name of function object. Function object must be serialized to COS before you can use it'
                                              )
                     )
        inputs.append(UISingle(name = 'parameters',
                                              datatype=dict,
                                              required=False,
                                              description = 'Parameters required by the function are provides as json.'
                                              )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item'))

        return (inputs,outputs)
    
    
class IoTDatabaseLookup(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """
    
    #create the table and populate it using the data dict
    _auto_create_lookup_table = True

    def __init__ (self, lookup_table_name, lookup_keys, lookup_items, parse_dates=None, output_items=None):
                        
        super().__init__(
             lookup_table_name = lookup_table_name,
             lookup_keys= lookup_keys,
             lookup_items = lookup_items,
             parse_dates= parse_dates, 
             output_items = output_items
             )
        
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'lookup_table_name',
                                              datatype=str,
                                              description = 'Table name to perform lookup against'
                                              )),
        inputs.append(UIMulti(name = 'lookup_keys',
                                              datatype=str,
                                              description = 'Data items to use as a key to the lookup'
                                              ))
        inputs.append(UIMulti(name = 'lookup_items',
                                              datatype=str,
                                              description = 'columns to return from the lookup'
                                              )),
        inputs.append(UIMulti(name = 'parse_dates',
                                              datatype=str,
                                              description = 'columns that should be converted to dates',
                                              required = False
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutMulti(name = 'output_items',
                                                     cardinality_from = 'lookup_items',
                                                     is_datatype_derived = False,
                                                     description='Function output items',
                                                     tags = ['DIMENSION']
                                                     ))
        return (inputs,outputs) 
    
    def get_item_values(self,arg):
        raise NotImplementedError('No items values available for generic database lookup function. Implement a specific one for each table to define item values. ')
    
class IoTDeleteInputData(BasePreload):
    '''
    Delete data from time series input table for entity type
    '''
    
    def __init__(self,dummy_items,older_than_days, output_item = 'output_item'):
        
        super().__init__(dummy_items = dummy_items)
        self.older_than_days = older_than_days
        self.output_item = output_item
        
    def execute(self,df=None,start_ts=None,end_ts=None,entities=None):
        
        entity_type = self.get_entity_type()
        self.get_db().delete_data(table_name=entity_type.name,
                                  schema = entity_type._db_schema, 
                                  timestamp='evt_timestamp',
                                  older_than_days = self.older_than_days)
        msg = 'Deleted data for %s' %(self._entity_type.name)
        logger.debug(msg)
        return True
    
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'dummy_items',
                                              datatype=None,
                                              description = 'Dummy data items'
                                              ))
        inputs.append(UISingle(name = 'older_than_days',
                                              datatype=float,
                                              description = 'Delete data older than this many days'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(
                        name = 'output_item',datatype=bool,description='Returns a status flag of True when executed')
                        )
                
        return (inputs,outputs)            
        
    
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
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'exclude_items',
                                              datatype=None,
                                              description = 'Ignore non-null values in these columns when dropping rows'
                                              ))
        inputs.append(UISingle(name = 'drop_all_null_rows',
                                                datatype=bool,
                                                description = 'Enable or disable drop of all null rows'
                                                ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Returns a status flag of True when executed'))
                
        return (inputs,outputs)            

    
class IoTExpression(BaseTransformer):
    '''
    Create a new item from an expression involving other items
    '''
    def __init__(self, expression , output_name):
        self.output_name = output_name
        super().__init__()
        #convert single quotes to double
        self.expression = self.parse_expression(expression)
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
        items = self.get_expression_items(self.expression)
        return items
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['expression'] = UISingle(name = 'expression',
                                              datatype=str,
                                              description = "Define alert expression using pandas systax. Example: df['inlet_temperature']>50"
                                              ).to_metadata()        
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_name'] = UIFunctionOutSingle(name = 'output_name',
                                                     datatype=None,
                                                     description='Output of expression'
                                                     ).to_metadata()
    
        return (inputs,outputs)   
    
class IoTIfThenElse(BaseTransformer):
    """
    Set the value of a data item based on the value of a conditional expression
    """
    def __init__(self,conditional_expression, true_expression, false_expression, output_item = 'output_item'):
        
        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.true_expression = self.parse_expression(true_expression)
        self.false_expression = self.parse_expression(false_expression)
        self.output_item = output_item
        
    def execute(self,df):
        df = df.copy()
        df[self.output_item] = np.where(eval(self.conditional_expression),
                                        eval(self.true_expression),
                                        eval(self.false_expression))
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'conditional_expression',
                                              datatype=str,
                                              description = "expression that returns a True/False value, eg. if df['temp']>50 then df['temp'] else None"
                                              ))
        inputs.append(UISingle(name = 'true_expression',
                                              datatype=str,
                                              description = "expression when true, eg. df['temp']"
                                              ))
        inputs.append(UISingle(name = 'false_expression',
                                              datatype=str,
                                              description = 'expression when false, eg. None'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ))
    
        return (inputs,outputs)
    
    def get_input_items(self):
        items = self.get_expression_items(self.conditional_expression, self.true_expression, self.false_expression)
        return items    
        
    
class IoTConditionalItems(BaseTransformer):
    """
    Set the value of a data item based on the value of a conditional expression eg. if df["sensor_is_valid"]==True then df["temp"] and df["pressure"] are valid else null
    """
    def __init__(self,conditional_expression, conditional_items, output_items = None):
        
        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.conditional_items = conditional_items
        if output_items is None:
            output_items = ['conditional_%s' %x for x in conditional_items]
        self.output_items = output_items
        
    def execute(self,df):
        df = df.copy()
        result  = eval(self.conditional_expression)
        for i,o in enumerate(self.conditional_items):
            df[self.output_items[i]] = np.where(result,df[o],None)
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['conditional_expression'] = UISingle(name = 'conditional_expression',
                                              datatype=str,
                                              description = "expression that returns a True/False value, eg. if df['sensor_is_valid']==True"
                                              ).to_metadata()
        inputs['conditional_items'] = UIMultiItem(name = 'conditional_items',
                                              datatype=None,
                                              description = 'Data items that have conditional values, e.g. temp and pressure'
                                              ).to_metadata()        
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_items'] = UIFunctionOutMulti(name = 'output_items',
                                                     cardinality_from = 'conditional_items',
                                                     is_datatype_derived = False,
                                                     description='Function output items'
                                                     ).to_metadata()
        
        return (inputs,outputs)
    
    def get_input_items(self):
        items = self.get_expression_items(self.conditional_expression, self.true_expression, self.false_expression)
        return items    

        
        

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
    def build_ui(cls):
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'halt_after',
                                              datatype=None,
                                              description = 'Raise error after calculating items'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'package_url', datatype = str))
        outputs.append(UIFunctionOutSingle(name = 'module', datatype = str))
        outputs.append(UIFunctionOutSingle(name = 'version', datatype = str))
    
        return (inputs,outputs)     
    
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
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'shift_definition',
                                              datatype= dict,
                                              description = ''
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'period_start_date', datatype = dt.datetime, tags = ['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name = 'period_end_date', datatype = dt.datetime, tags = ['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name = 'shift_day', datatype = dt.datetime, tags = ['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name = 'shift_id', datatype = int, tags = ['DIMENSION']))
    
        return (inputs,outputs)        
    

   
    