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
import time
from collections import OrderedDict
import numpy as np
import re
import pandas as pd
import logging
import iotfunctions as iotf
from .metadata import EntityType
from .base import BaseTransformer, BaseEvent, BaseSCDLookup, BaseMetadataProvider, BasePreload, BaseDatabaseLookup, BaseDataSource, BaseDBActivityMerge
from .ui import UISingle,UIMultiItem,UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class IoTActivityDuration(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities. An activity table
    must have a deviceid, activity_code, start_date and end_date. The
    function returns an activity duration for each selected activity code.
    '''
    
    _is_instance_level_logged = False
    def __init__(self,table_name,activity_codes, activity_duration=None):
        super().__init__(input_activities=activity_codes,
                         activity_duration=activity_duration )
        
        self.activities_metadata[table_name] = activity_codes
        self.activities_custom_query_metadata = {}
        
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['table_name'] = UISingle(name = 'table_name',
                                        datatype = str,
                                        description = 'Source table name',
                                        )
        inputs['activity_codes'] = UIMulti(name = 'activity_codes',
                                              datatype=str,
                                              description = 'Comma separated list of activity codes',
                                              output_item = 'activity_duration',
                                              is_output_datatype_derived = False,
                                              output_datatype = float
                                              )
        outputs = OrderedDict()

        return (inputs,outputs)        


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
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
            msg = 'Expression converted to %s. ' %expr
        else:
            expr = self.expression
            msg = 'Expression (%s). ' %expr
        self.trace_append(msg)
        df[self.alert_name] = np.where(eval(expr), True, False)
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'input_items',
                                              datatype=None,
                                              description = 'Input items'
                                              ))
        inputs.append(UISingle(name = 'expression',
                                              datatype=str,
                                              description = "Define alert expression using pandas systax. Example: df['inlet_temperature']>50"
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'alert_name',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     ))
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
        c = self._entity_type.get_attributes_dict()
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
                                              )
        inputs['lower_threshold'] = UISingle(name = 'lower_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is lower than this value',
                                              required = False,
                                              )
        inputs['upper_threshold'] = UISingle(name = 'upper_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is higher than this value',
                                              required = False,
                                              )  
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_alert_lower'] = UIFunctionOutSingle(name = 'output_alert_lower',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     )
        outputs['output_alert_upper'] = UIFunctionOutSingle(name = 'output_alert_upper',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     )
    
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
        c = self._entity_type.get_attributes_dict()
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
        c = self._entity_type.get_attributes_dict()
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
                                              )
        inputs['lower_threshold'] = UISingle(name = 'upper_threshold',
                                              datatype=float,
                                              description = 'Alert when item value is lower than this value'
                                              )  
        #define arguments that behave as function outputs
        outputs = OrderedDict()       
        outputs['alert_name'] = UIFunctionOutSingle(name = 'alert_name',
                                                     datatype=bool,
                                                     description='Output of alert function'
                                                     )
    
        return (inputs,outputs)    



class IoTAutoTest(BaseTransformer):
    '''
    Test the results of pipeline execution against a known test dataset. 
    The test will compare columns calculated values with values in the test dataset.
    Discepancies will the written to a test output file.
    '''
    
    def __init__(self,test_datset_name,columns_to_test,result_col='test_result'):
        
        super().__init__()
        
        self.test_datset_name = test_datset_name
        self.columns_to_test = columns_to_test
        self.result_col = result_col
        
    def execute(self,df):
        
        db = self.get_db()
        bucket = self.get_bucket_name()
        
        file = db.cos_load(filename = self.test_datset_name,
                           bucket= bucket,
                           binary=False)
            
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['test_datset_name'] = UISingle(name = 'test_datset_name',
                                        datatype = str,
                                        description = ('Name of cos object containing'
                                                       ' test data. Object is a pickled '
                                                       ' dataframe. Object must be placed '
                                                       ' in the bos_runtime_bucket' )                            
                                        )        
        inputs['columns_to_test'] = UIMultiItem(name = 'input_items',
                                          datatype=None,
                                          description = ('Choose the data items that'
                                                         ' you would like to compare')
                                          )  
        outputs = OrderedDict()

        return (inputs,outputs)


class IoTCalcSettings(BaseMetadataProvider):
    """
    Overide default calculation settings for the entity type
    """
    
    def __init__ (self, 
                  checkpoint_by_entity = False,
                  pre_aggregate_time_grain = None,
                  auto_read_from_ts_table = True,
                  sum_items = None,
                  mean_items = None,
                  min_items = None,
                  max_items = None,
                  count_items = None,
                  sum_outputs = None,
                  mean_outputs = None,
                  min_outputs = None,
                  max_outputs = None,
                  count_outputs = None,                  
                  output_item = 'output_item'):
        
        #metadata for pre-aggregation:
        #pandas aggregate dict containing a list of aggregates for each item
        self._pre_agg_rules = {}
        #dict containing names of aggregate items produced for each item
        self._pre_agg_outputs = {}
        #assemble these metadata structures
        self._apply_pre_agg_metadata('sum',items = sum_items, outputs = sum_outputs)
        self._apply_pre_agg_metadata('mean',items = mean_items, outputs = mean_outputs)
        self._apply_pre_agg_metadata('min',items = min_items, outputs = min_outputs)
        self._apply_pre_agg_metadata('max',items = max_items, outputs = max_outputs)
        self._apply_pre_agg_metadata('count',items = count_items, outputs = count_outputs)
        #pass metadata to the entity type
        kwargs = {
                '_checkpoint_by_entity' : checkpoint_by_entity,
                '_pre_aggregate_time_grain' : pre_aggregate_time_grain,
                '_auto_read_from_ts_table' : auto_read_from_ts_table,
                '_pre_agg_rules' : self._pre_agg_rules,
                '_pre_agg_outputs' : self._pre_agg_outputs
                }
        super().__init__(dummy_items=[],output_item=output_item, **kwargs)
    
    def _apply_pre_agg_metadata(self,aggregate,items,outputs):
        '''
        convert UI inputs into a pandas aggregate dictioonary and a separate dictionary containing names of aggregate items
        '''
        if items is not None:
            if outputs is None:
                outputs = ['%s_%s'%(x,aggregate) for x in items]
            for i,item in enumerate(items):
                try:
                    self._pre_agg_rules[item].append(aggregate)
                    self._pre_agg_outputs[item].append(outputs[i])
                except KeyError:
                    self._pre_agg_rules[item] = [aggregate]
                    self._pre_agg_outputs[item] = [outputs[i]]
                except IndexError:
                    msg = 'Metadata for aggregate %s is not defined correctly. Outputs array should match length of items array.' %aggregate
                    raise ValueError(msg)
        
        return None
        
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle( 
                        name = 'auto_read_from_ts_table',
                        datatype=bool,
                        required = False,
                        description = 'By default, data retrieved is from the designated input table. Use this setting to disable.',
                                              ))
        inputs.append(UISingle(
                        name = 'checkpoint_by_entity',
                        datatype = bool,
                        required = False,
                        description = 'By default a single '
                    ))
        inputs.append(UISingle( 
                        name = 'pre_aggregate_time_grain',
                        datatype=str,
                        required = False,
                        description = 'By default, data is retrieved at the input grain. Use this setting to preaggregate data and reduce the volumne of data retrieved',
                        values = ['1min','5min','15min','30min','1H','2H','4H','8H','12H','day','week','month','year']
                                              ))
        inputs.append(UIMultiItem(
                        name = 'sum_items',
                        datatype = float,
                        required = False,
                        description = 'Choose items that should be added when aggregating',
                        output_item = 'sum_outputs',
                        is_output_datatype_derived = True
                ))
        inputs.append(UIMultiItem(
                        name = 'mean_items',
                        datatype = float,
                        required = False,
                        description = 'Choose items that should be averaged when aggregating',
                        output_item = 'mean_outputs',
                        is_output_datatype_derived = True
                ))        
        inputs.append(UIMultiItem(
                        name = 'min_items',
                        datatype = float,
                        required = False,
                        description = 'Choose items that the system should choose the smallest value when aggregating',
                        output_item = 'mean_outputs',
                        is_output_datatype_derived = True
                ))  
        inputs.append(UIMultiItem(
                        name = 'max_items',
                        datatype = float,
                        required = False,
                        description = 'Choose items that the system should choose the smallest value when aggregating',
                        output_item = 'mean_outputs',
                        is_output_datatype_derived = True
                ))   
        inputs.append(UIMultiItem(
                        name = 'count_items',
                        datatype = float,
                        required = False,
                        description = 'Choose items that the system should choose the smallest value when aggregating',
                        output_item = 'mean_outputs',
                        is_output_datatype_derived = True
                ))         
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ))
    
        return (inputs,outputs)  
    
    
class IoTCoalesceDimension(BaseTransformer):
    """
    Return first non-null value from a list of data items.
    """
    def __init__(self,data_items, output_item = 'output_item'):
        
        super().__init__()
        self.data_items = data_items
        self.output_item = output_item
        
    def execute(self,df):
        
        df[self.output_item] = df[self.data_items].bfill(axis=1).iloc[:, 0]
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', tags = ['DIMENSION']))

        return (inputs,outputs)

class IoTConditionalItems(BaseTransformer):
    """
    Set the value of a data item based on the value of a conditional expression 
    eg. if df["sensor_is_valid"]==True then df["temp"] and df["pressure"] are valid else null
    """
    def __init__(self,conditional_expression, conditional_items, output_items = None):
        
        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.conditional_items = conditional_items
        if output_items is None:
            output_items = ['conditional_%s' %x for x in conditional_items]
        self.output_items = output_items
        
    def execute(self,df):
        c = self._entity_type.get_attributes_dict()
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
                                              )
        inputs['conditional_items'] = UIMultiItem(name = 'conditional_items',
                                              datatype=None,
                                              description = 'Data items that have conditional values, e.g. temp and pressure'
                                              )        
        #define arguments that behave as function outputs
        outputs = OrderedDict()
        outputs['output_items'] = UIFunctionOutMulti(name = 'output_items',
                                                     cardinality_from = 'conditional_items',
                                                     is_datatype_derived = False,
                                                     description='Function output items'
                                                     )
        
        return (inputs,outputs)
    
    def get_input_items(self):
        items = self.get_expression_items(self.conditional_expression)
        return items  

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
                               datatype=str,
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
    
class DateDifference(BaseTransformer):
    """
    Calculate the difference between two date data items in days,ie: ie date_2 - date_1
    """
    
    def __init__ (self,date_1,date_2,num_days='num_days'):
        
        super().__init__()
        self.date_1 = date_1
        self.date_2 = date_2
        self.num_days = num_days
        
    def execute(self,df):
        
        if self.date_1 is None:
            ds_1 = self.get_timestamp_series(df)
            if isinstance(ds_1,pd.DatetimeIndex):
                ds_1 = pd.Series(data=ds_1,index=df.index)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]
            
        if self.date_2 is None:
            ds_2 = self.get_timestamp_series(df)
            if isinstance(ds_2,pd.DatetimeIndex):
                ds_2 = pd.Series(data=ds_2,index=df.index)
            ds_2 = pd.to_datetime(ds_2)
        else:
            ds_2 = df[self.date_2]         
        
        df[self.num_days] = (ds_2 - ds_1).\
                            dt.total_seconds() / (60*60*24)
        
        return df
        
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name = 'date_1',
                                  datatype=dt.datetime,
                                  required = False,
                                  description = ('Date data item. Use timestamp'
                                                 ' if no date specified' )
                                              ))
        inputs.append(UISingleItem(name = 'date_2',
                               datatype=dt.datetime,
                               required = False,
                                  description = ('Date data item. Use timestamp'
                                                 ' if no date specified' )
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                    name = 'num_days',
                    datatype=dt.datetime,
                    description='Number of days')
            )
                
        return (inputs,outputs) 


class DateDifferenceReference(BaseTransformer):
    """
    Calculate the difference between a data item and a reference value,
    ie: ie ref_date - date_1
    """
    
    def __init__ (self,date_1,ref_date,num_days='num_days'):
        
        super().__init__()
        self.date_1 = date_1
        self.ref_date = ref_date
        self.num_days = num_days
        
    def execute(self,df):
        
        if self.date_1 is None:
            ds_1 = self.get_timestamp_series(df)
            if isinstance(ds_1,pd.DatetimeIndex):
                ds_1 = pd.Series(data=ds_1,index=df.index)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]
        
        df[self.num_days] = (self.ref_date - ds_1).\
                            dt.total_seconds() / (60*60*24)
        
        return df
        
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name = 'date_1',
                                  datatype=dt.datetime,
                                  required = False,
                                  description = ('Date data item. Use timestamp'
                                                 ' if no date specified' )
                                  )
                    )
        inputs.append(UISingle(name = 'ref_date',
                               datatype=dt.datetime,
                               description = 'Date value'
                               )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                    name = 'num_days',
                    datatype=dt.datetime,
                    description='Number of days')
            )
                
        return (inputs,outputs) 
    
class DateDifferenceConstant(BaseTransformer):
    """
    Calculate the difference between a data item and a constant_date,
    ie: ie constant_date - date_1
    """
    
    def __init__ (self,date_1,date_constant,num_days='num_days'):
        
        super().__init__()
        self.date_1 = date_1
        self.date_constant = date_constant
        self.num_days = num_days
        
    def execute(self,df):
        
        if self.date_1 is None:
            ds_1 = self.get_timestamp_series(df)
            if isinstance(ds_1,pd.DatetimeIndex):
                ds_1 = pd.Series(data=ds_1,index=df.index)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]    
        
        c = self._entity_type.get_attributes_dict()
        constant_value = c[self.date_constant]
        ds_2 = pd.Series(data=constant_value,index=df.index)
        ds_2 = pd.to_datetime(ds_2)
        df[self.num_days] = (ds_2 - ds_1).\
                            dt.total_seconds() / (60*60*24)
        
        return df
        
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name = 'date_1',
                                  datatype=dt.datetime,
                                  required = False,
                                  description = ('Date data item. Use timestamp'
                                                 ' if no date specified' )
                                              ))
        inputs.append(UISingle(name = 'date_constant',
                               datatype=str,
                               description = 'Name of datetime constant'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(
                    name = 'num_days',
                    datatype=dt.datetime,
                    description='Number of days')
            )
                
        return (inputs,outputs)    

    
class IoTDatabaseLookup(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """
    
    #create the table and populate it using the data dict
    _auto_create_lookup_table = False

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


class IoTEntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    Time series columns defined on the entity data table will be populated
    with random data.
    """
    
    freq = '5min' 
    # ids of entities to generate. Change the value of the range() function to change the number of entities
    
    def __init__ (self, ids = None, output_item = 'entity_data_generator'):
        if ids is None:
            ids = self.get_entity_ids()
        super().__init__(dummy_items = [], output_item = output_item)
        self.ids = ids
        
    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):
        
        #This sample builds data with the TimeSeriesGenerator.
        
        if entities is None:
            entities = self.ids
            
        if not start_ts is None:
            seconds = (dt.datetime.utcnow() - start_ts).total_seconds()
        else:
            seconds = pd.to_timedelta(self.freq).total_seconds()
        
        df = self._entity_type.generate_data(entities=entities, days=0, seconds = seconds, freq = self.freq, write=True)
        
        kw = {'rows_generated' : len(df.index),
              'start_ts' : start_ts,
              'seconds' : seconds}
        self.trace_append(msg='%s Generated data. ' %self.__class__.__name__,df=df,**kw)
        
        return True  
    
    
    def get_entity_ids(self):
        '''
        Generate a list of entity ids
        '''
        ids = [str(73000 + x) for x in list(range(5))]
        return (ids)

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name = 'ids',
                                  datatype=str,
                                  description = 'Comma separate list of entity ids, e.g: X902-A01,X902-A03'
                                  )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Returns a status flag of True when executed'))
        
        return (inputs,outputs)            

class IoTEntityFilter(BaseMetadataProvider):
    '''
    Filter data source results on a list of entity ids
    '''
    
    def __init__(self, entity_list, output_item= 'is_filter_set'):
        
        dummy_items = ['deviceid']
        kwargs = { '_entity_filter_list' : entity_list
                 }
        super().__init__(dummy_items, output_item = 'is_parameters_set', **kwargs)
        
    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name = 'entity_list',
                                              datatype=str,
                                              description = 'comma separated list of entity ids'
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
        c = self._entity_type.get_attributes_dict()
        df = df.copy()
        requested = list(self.get_input_items())
        msg = self.expression + ' .'
        self.trace_append(msg)
        msg = 'Function requested items: %s . ' %','.join(requested)
        self.trace_append(msg)
        df[self.output_name] = eval(self.expression)
        return df
    
    def get_input_items(self):
        items = self.get_expression_items(self.expression)
        return items
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'expression',
                                              datatype=str,
                                              description = "Define alert expression using pandas systax. Example: df['inlet_temperature']>50"
                                              )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_name',
                                                     datatype=None,
                                                     description='Output of expression'
                                                     ))
    
        return (inputs,outputs)   
    
    
class IoTGetEntityData(BaseDataSource):
    """
    Get time series data from an entity type. Provide the table name for the entity type and
    specify the key column to use for mapping the source entity type to the destination. 
    e.g. Add temperature sensor data to a location entity type by selecting a location_id
    as the mapping key on the source entity type'
    """
    
    merge_method = 'outer'
    
    def __init__(self,source_entity_type_name, key_map_column, input_items,
                 output_items = None):
        self.source_entity_type_name = source_entity_type_name
        self.key_map_column = key_map_column
        super().__init__(input_items = input_items, output_items = output_items)

    def get_data(self,start_ts=None,end_ts=None,entities=None):
        
        db = self.get_db()
        target = self.get_entity_type()
        #get entity type metadata from the AS API
        source = db.get_entity_type(self.source_entity_type_name)
        source._checkpoint_by_entity = False
        source._pre_aggregate_time_grain = target._pre_aggregate_time_grain
        source._pre_agg_rules = target._pre_agg_rules
        source._pre_agg_outputs = target._pre_agg_outputs
        cols = [self.key_map_column, source._timestamp]
        cols.extend(self.input_items)
        renamed_cols = [target._entity_id, target._timestamp]
        renamed_cols.extend(self.output_items)
        df = source.get_data(start_ts=start_ts,end_ts = end_ts, entities=entities, columns = cols)
        df = self.rename_cols(df,cols,renamed_cols)
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'source_entity_type_name',
                               datatype=str,
                               description = "Enter the name of the entity type that you would like to retrieve data from")
                      )
        inputs.append(UISingle(name = 'key_map_column',
                               datatype=str,
                               description = "Enter the name of the column on the source entity type that represents the map to the device id of this entity type")
                      )        
        inputs.append(UIMulti(name = 'input_items',
                               datatype=str,
                               description = "Comma separated list of data item names to retrieve from the source entity type",
                               output_item = 'output_items',
                               is_output_datatype_derived = True)
                      )
        outputs = []
    
        return (inputs,outputs)          

class IoTEntityId(BaseTransformer):
    """
    Deliver a data item containing the id of each entity. Optionally only return the entity
    id when one or more data items are populated, else deliver a null value.
    """
    
    def __init__(self,data_items=None,output_item = 'entity_id'):
        
        super().__init__()
        self.data_items = data_items
        self.output_item = output_item
        
    def execute(self,df):
        
        df = df.copy()
        if self.data_items is None:
            df[self.output_item] = df[self.get_entity_type()._entity_id]
        else:
            df[self.output_item] = np.where(df[self.data_items].notna().max(axis=1),
                                        df[self.get_entity_type()._entity_id],
                                        None)
        return df    
        
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'data_items',
                                  datatype=None,
                                  required=False,
                                  description = 'Choose one or more data items. If data items are defined, entity id will only be shown if these data items are not null'
                                  ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ))
    
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
        c = self._entity_type.get_attributes_dict()
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
                
class IoTPackageInfo(BaseTransformer):
    """
    Show the version of a list of installed packages. Optionally install packages that are not installed.
    """
    
    def __init__ (self, package_names,add_to_trace=True, install_missing = True, version_output = None):
        
        self.package_names = package_names
        self.add_to_trace = add_to_trace
        self.install_missing = install_missing
        if version_output is None:
            version_output = ['%s_version' %x for x in package_names]
        self.version_output = version_output
        super().__init__()
        
    def execute(self,df):
        import importlib
        entity_type = self.get_entity_type()
        df = df.copy()
        for i,p in enumerate(self.package_names):
            ver = ''
            try:
                installed_package = importlib.import_module(p)
            except (ImportError,ModuleNotFoundError):
                if self.install_missing:
                    entity_type.db.install_package(p)
                    try:
                        installed_package = importlib.import_module(p)
                    except (ImportError,ModuleNotFoundError):
                        ver = 'Package could not be installed'
                    else:
                        try:
                            ver = 'installed %s' %installed_package.__version__
                        except AttributeError:
                            ver = 'Package has no __version__ attribute'
            else:
                try:
                    ver = installed_package.__version__
                except AttributeError:
                    ver = 'Package has no __version__ attribute'
            df[self.version_output[i]] = ver
            if self.add_to_trace:
                msg = '( %s : %s)' %(p, ver)
                self.trace_append(msg)
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name = 'package_names',
                              datatype=str,
                              description = 'Comma separate list of python package names',
                              output_item = 'version_output',
                              is_output_datatype_derived = False,
                              output_datatype = str
                                              ))
        inputs.append(UISingle(name='install_missing',datatype=bool))
        inputs.append(UISingle(name='add_to_trace',datatype=bool))
        #define arguments that behave as function outputs
        outputs = []
    
        return (inputs,outputs)    

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
        msg = 'The calculation was halted deliberately by the IoTRaiseError function. Remove the IoTRaiseError function or disable "abort_execution" in the function configuration. '
        if self.abort_execution:
            raise RuntimeError(msg)
        
        df[self.output_item] = True
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
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ))
    
        return (inputs,outputs)
    
    
class IoTRandomNormal(BaseTransformer):
    """
    Generate a normally distributed random number.
    """
    
    def __init__ (self, mean, standard_deviation, output_item = 'output_item'):
        
        super().__init__()
        self.mean = mean
        self.standard_deviation = standard_deviation
        self.output_item = output_item
        
    def execute(self,df):
        
        df[self.output_item] = np.random.normal(self.mean,self.standard_deviation,len(df.index))
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='mean',datatype=float))
        inputs.append(UISingle(name='standard_deviation',datatype=float))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                             datatype=float,
                                             description='Random output'
                                             ))
    
        return (inputs,outputs)  
                 

class IoTRandomChoice(BaseTransformer):
    """
    Generate a random categorical value.
    """
    
    def __init__ (self, domain_of_values, output_item = 'output_item'):
        
        super().__init__()
        self.domain_of_values = domain_of_values
        self.output_item = output_item
        
    def execute(self,df):
        
        df[self.output_item] = np.random.choice(self.domain_of_values,len(df.index))
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name='domain_of_values',datatype=str))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                             datatype=str,
                                             description='Random output',
                                             tags= ['DIMENSION']
                                             ))
    
        return (inputs,outputs)   


class IoTSaveCosDataFrame(BaseTransformer):
    """
    Serialize dataframe to COS
    """
    
    def __init__(self,
                 filename='job_output_df',
                 columns=None,
                 output_item='save_df_result'):
        
        super().__init__()
        self.filename = filename
        self.columns = columns
        self.output_item = output_item
        
    def execute(self,df):
        
        if self.columns is not None:
            df = df[self.columns]
        db = self.get_db()
        bucket = self.get_bucket_name()
        db.cos_save(persisted_object=df,
                    filename=self.filename,
                    bucket=bucket,
                    binary=True)
        
        df[self.output_item] = True
        
        return df
        
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='filename',datatype=str))
        inputs.append(UIMultiItem(name='columns'))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                             datatype=str,
                                             description='Result of save operation'
                                             ))
    
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
        df = df.reset_index()
        entity_type = self.get_entity_type()
        (df,ts_col) = entity_type.df_sort_timestamp(df)
        calendar_df = self.get_data(start_date= df[ts_col].min(), end_date = df[ts_col].max())
        df = pd.merge_asof(left = df,
                           right = calendar_df,
                           left_on = ts_col,
                           right_on = self.period_start_date,
                           direction = 'backward')
            
        df = self._entity_type.index_df(df)
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
    
class IoTSleep(BaseTransformer):
    
    """
    Wait for the designated number of seconds
    """
    def __init__(self,sleep_after, 
                 sleep_duration_seconds = 30,
                 output_item = 'sleep_status'):
                 
        super().__init__()
        self.sleep_after = sleep_after
        self.sleep_duration_seconds = sleep_duration_seconds
        self.output_item = output_item
        
    def execute(self,df):
        
        msg = 'Sleep duration: %s. ' %self.sleep_duration_seconds
        self.trace_append(msg)
        time.sleep(self.sleep_duration_seconds)
        df[self.output_item] = True
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'sleep_after',
                                              datatype=None,
                                              required = False,
                                              description = 'Sleep after calculating items'
                                              ))
        inputs.append(UISingle(name = 'sleep_duration_seconds', datatype = float))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                                     datatype=bool,
                                                     description='Dummy function output'
                                                     ))
    
        return (inputs,outputs)
    

class IoTTraceConstants(BaseTransformer):

    """
    Write the values of available constants to the trace
    """         
    
    def __init__(self,dummy_items,output_item = 'trace_written'):
        
        super().__init__()
        
        self.dummy_items = dummy_items
        self.output_item = output_item
        
    def execute(self,df):
        
        c = self._entity_type.get_attributes_dict()
        msg = 'entity constants retrieved'
        self.trace_append(msg,**c)
            
        df[self.output_item] = True
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'dummy_items',
                                              datatype=None,
                                              required = False,
                                              description = 'Not required'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Dummy function output'
                                          ))
        
        return (inputs,outputs)
    
class TimestampCol(BaseTransformer):
    """
    Deliver a data item containing the timestamp
    """
    
    def __init__(self,dummy_items=None,output_item = 'timestamp_col'):
        
        super().__init__()
        self.dummy_items = None
        self.output_item = output_item
        
    def execute(self,df):

        ds_1 = self.get_timestamp_series(df)
        if isinstance(ds_1,pd.DatetimeIndex):
            ds_1 = pd.Series(data=ds_1,index=df.index)
        ds_1 = pd.to_datetime(ds_1)
        df[self.output_item] = ds_1
        
        return df
        
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'dummy_items',
                                              datatype=None,
                                              required = False,
                                              description = 'Not required'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item',
                                           datatype=dt.datetime,
                                           description='Timestamp column name'
                                           ))     
        
        return (inputs,outputs)
                    
        
        
        
   
    