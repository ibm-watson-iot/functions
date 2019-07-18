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
Base classes for functions. Inhertit from these base classes when building custom functions.
'''

import math
import os
import urllib3
import numbers
import datetime as dt
import logging
import warnings
import json
import re
import numpy as np
import pandas as pd
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, MetaData, ForeignKey, create_engine
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
from sklearn import ensemble, linear_model, metrics, neural_network
from sklearn.model_selection import train_test_split, RandomizedSearchCV
import ibm_db
import ibm_db_dbi
from sqlalchemy.orm.session import sessionmaker
from inspect import getargspec
from collections import OrderedDict
from .db import Database, SystemLogTable
from .metadata import EntityType, Model
from .automation import TimeSeriesGenerator
from .pipeline import CalcPipeline, PipelineExpression
from .util import log_df_info
from .ui import UIFunctionOutSingle, UIMultiItem, UISingle

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class BaseFunction(object):
    """
    Base class for AS functions. Do not inherit directly from this class. Inherit from BaseTransformer or BaseAggregator
    """
    # _entity_type, An EntityType object will be added to the pipeline 
    # this will give the function access to all of the properties and methods of the entity type
    _entity_type = None 
    # metadata data parameters are instance variables to be added to the entity type
    _metadata_params = {}
    #function registration metadata 
    name = None # name of function
    description =  None # description of function shows as help text
    tags = None #list of stings to tag function with
    optionalItems = None #list: list of optional parameters
    inputs = None #list: list of explicit input parameters
    outputs = None #list: list of explicit output parameters
    constants = None #list: list of explicit constant parameters
    array_source = None #str: the input parmeter name that contains a list of items that will correspond with array outputs
    url = PACKAGE_URL #install url for function
    category = None 
    incremental_update = True
    auto_register_args = None  
    is_transient = False
    array_output_datatype_from_input = False
    # item level metadata for function registration
    itemDescriptions = None #dict: items descriptions show as help text
    itemLearnMore = None #dict: item learn more test 
    itemValues = None #dict: item values are used in pick lists
    itemJsonSchema = None #dict: schema is used to validate arrays and json type constants
    itemArraySource = None #dict: output arrays are derived from input arrays. 
    itemMaxCardinality = None # dict: Maximum number of members in an array
    itemDatatypes = None #dict: BOOLEAN, NUMBER, LITERAL, DATETIME
    itemTags = None #dict: Tags to be added to data items
    # processing settings
    execute_by = None #if function should be executed separately for each entity or some other key, capture this key here
    test_rows = 100 #rows of data to use when testing function
    base_initialized = True # use to test that object was initialized from BaseFunction
    merge_strategy = 'transform_only' #use to describe how this function's outputs are merged with outputs of the previous stage
    _abort_on_fail = True #allow pipeline to continue when a stage fails in execution create
    _is_instance_level_logged = False # Some operations are carried out at an entity instance level. If logged, they produce a lot of log.
    is_system_function = False #system functions are internal to AS, cannot be used as custom functions
    # cos connection
    cos_credentials = None #dict external cos instance
    bucket = None #str
    # custom output tables
    version_db_writes = False #write a new version timestamp to custom output table with each execution
    out_table_prefix = None
    out_table_if_exists = 'append'
    out_table_name = None 
    write_chunk_size = None #use db default
    # lookups
    # a slowly changing dimensions is use to record property changes to master data over time
    _entity_scd_dict = None
    _start_date = 'start_date'
    _end_date = 'end_date'    
    
    def __init__(self):
        
        if self.name is None:
            self.name = self.__class__.__name__
        if self.out_table_prefix is None:
            self.out_table_prefix = self.name            
        if self.description is None:
            self.description = self.__class__.__doc__            
        if self.inputs is None:
            self.inputs = []            
        if self.outputs is None:
            self.outputs = []
        if self.constants is None:
            self.constants = []                        
        if self.itemDescriptions is None:
            self.itemDescriptions = self._standard_item_descriptions()            
        if self.itemDatatypes is None:
            self.itemDatatypes = {}
        if self.itemLearnMore is None:
            self.itemLearnMore = {}            
        if self.itemJsonSchema is None:
            self.itemJsonSchema = {}            
        if self.itemValues is None:
            self.itemValues = {}            
        if self.itemMaxCardinality is None:
            self.itemMaxCardinality = {}
        if self.itemArraySource is None:
            self.itemArraySource = {}            
        if self.itemTags is None:
            self.itemTags = {}            
        if self.optionalItems is None:
            self.optionalItems = []
        if self.out_table_prefix is None:
            self.out_table_prefix = ''             
        if self.execute_by is None:
            self.execute_by = []   
        if self.tags is None:
            self.tags = []             
        if self._entity_scd_dict is None:
            self._entity_scd_dict= {}

        #if cos credentials are not explicitly  provided use environment variable
        if self.bucket is None:
            if not self.cos_credentials is None:
                try:
                   self.bucket = self.cos_credentials['bucket']
                except KeyError:
                    try:
                       self.bucket = os.environ.get('COS_BUCKET_KPI')
                    except KeyError:
                        pass
                    
    def _add_explicit_outputs(self,df):
        
        for o in self.outputs:
            df[o] = True
        return df
    
    def build_arg_metadata(self):
        
        try:
            (inputs,outputs) = self.build_ui()
        except (AttributeError,NotImplementedError):
            msg = ('Cant get function metadata for %s. Implement the'
                   ' build_metadata() method.' %self.name)
            raise NotImplementedError (msg)
            
        input_args ={}
        output_args ={}
        output_meta = {}
        
        for i in inputs:
            try:
                meta = i.to_metadata()
            except AttributeError:
                meta = i
            input_args[meta['name']] = getattr(self,meta['name'])
        for o in outputs:
            try:
                meta = o.to_metadata()
            except AttributeError:
                meta = o                        
            output_args[meta['name']] = getattr(self,meta['name'])
            
        return(input_args,output_args,output_meta)

    @classmethod
    def build_ui(cls):
        """
        Define metadata for function registration explicly.
        """
        
        raise NotImplementedError('Implement this method to enable registration of a class without creating an instance')                    
        
    def _calc(self,df):
        """
        If the function should be executed separately for each entity, describe the function logic in the _calc method
        """
        raise NotImplementedError('Class %s is not defined correctly. It should override the _calc() method of the base class.' %self.__class__.__name__)         
        
    def _coallesce_columns(self,df,cols,rsuffix='_new_'):
        '''
        Coallesce 2 columns into a single by replacing cols with a suffixed version of themselves when null
        '''
        done = []
        for i,o in enumerate(cols):
            try:
                drop = "%s%s" %(o,rsuffix)
                df[o] = df[o].fillna(df[drop])
                done.append(drop)
            except KeyError:
                pass
        if len(done) > 0:
            df = self._remove_cols_from_df(df,done)
            msg = 'Coallesced columns during merge %s' %done
            logger.debug(msg)
        return df
            
    def convertStrArgToList(self,string, argument, check_non_empty=False):
        '''
        Convert a comma delimited string to a list
        '''
        out = string
        if not string is None and isinstance(string, str):
            out = [n.strip() for n in string.split(',') if len(string.strip()) > 0]
        if not argument in self.optionalItems and check_non_empty:
            if out is None or len(out) == 0:
                raise ValueError("Required list output %s is null or empty" %argument)    
        return out
    
    def conform_index(self,df,entity_id_col = None, timestamp_col = None):
        '''
        Dataframes that contain timeseries data are expected to be indexed on an id and timestamp.
        The name on the id column will be id. The name of the timestamp col is the timestamp of the entity_type.
        Another deviceid and another column called timestamp will be added to the dataframe as a convenience.
        '''
        
        warnings.warn('conform_index() is deprecated. Use EntityType.index_df',
                      DeprecationWarning)
        
        #self.log_df_info(df,'incoming dataframe for conform index')
        if not df.index.names == [self._entity_type._df_index_entity_id,self._entity_type._timestamp]: 
            # index does not conform
            #look for explicitly provided timestamp and entity id cols
            # or designated column names for the entity type
            if entity_id_col is None:
                entity_id_col = self._entity_type._entity_id
            ids = [entity_id_col, self._entity_type._df_index_entity_id]
            try:
                id_series = self._get_series(df,col_names=ids)
            except KeyError as e:
                msg = 'Attempting to conform index. Cannot find an entity identifier column. Looking for %s' %ids
                raise KeyError(msg)    
            if timestamp_col is None:
                timestamp_col = self._entity_type._timestamp
            tss = [timestamp_col, self._entity_type._timestamp_col]
            try:
                timestamp_series = self._get_series(df,col_names=tss)
            except KeyError as e:
                msg = 'Attempting to conform index. Cannot find a timestamp column. Looking for %s ' %tss
                raise KeyError(msg)
            df[self._entity_type._df_index_entity_id] = id_series.astype(str)
            df[self._entity_type._timestamp] = pd.to_datetime(timestamp_series)
            df = df.set_index([self._entity_type._df_index_entity_id,self._entity_type._timestamp])
            msg = 'Dataframe had non-conforming index. Built new index on id and timestamp'
            logger.debug(msg)
        df[self._entity_type._timestamp_col] = df.index.get_level_values(self._entity_type._timestamp)
        df[self._entity_type._entity_id] = df.index.get_level_values(self._entity_type._df_index_entity_id)
        self.log_df_info(df,'after  conform index')
        
        return df
    
    def empty_dataframe(self,columns):
        
        cols = set(columns)
        cols.add(self._entity_type._timestamp_col)
        cols.add(self._entity_type._entity_id)
        cols= list(cols)
        df = pd.DataFrame(columns=cols)
        df = self.conform_index(df)                
        return df
    
    def execute(self,df):
        """
        AS calls the execute() method of your function to transform or aggregate data. The execute method accepts a dataframe as input and returns a dataframe as output.
        
        If the function should be executed on all entities combined you can replace the execute method wih a custom one
        If the function should be executed by entity instance, use the base execute method. Provide a custom _calc method instead.
        """
        group_base = []
        for s in self.execute_by:
            if s in df.columns:
                group_base.append(s)
            else:
                try:
                    x = df.index.get_level_values(s)
                except KeyError:
                    raise ValueError('This function executes by column %s. This column was not found in columns or index' %s)
                else:
                    group_base.append(pd.Grouper(axis=0, level=df.index.names.index(s)))
                    
        if len(group_base)>0:
            df = df.groupby(group_base).apply(self._calc)                
        else:
            df = self._calc(df)
            
        return df

    def generate_model_name(self,target_name, prefix = 'model', suffix = None):
        '''
        Generate a model name
        '''
        name = []
        if prefix is not None:
            name.append(prefix)
        name.extend([self._entity_type.name, self.name , target_name])
        if suffix is not None:
            name.append(suffix)
        name = '.'.join(name)
        return name     
        
    def _get_arg_metadata(self,isoformat_dates=True):
        
        metadata = {}    
        args = (getargspec(self.__init__))[0][1:]        
        for a in args:
            try:
                metadata[a] = self.__dict__[a]
            except KeyError:
                msg = 'Programming error. All arguments must have a corresponding instance variable of the same name. This function has no instance variable: %s' %a
                logger.exception(msg)
                raise
            
            if ((isoformat_dates) and  
                (isinstance(metadata[a],dt.datetime) or isinstance(metadata[a],dt.date))):
                
                metadata[a] = metadata[a].isoformat()
            
        return metadata
    
    def get_custom_calendar(self):
        '''
        Get the customer calendar from the entity type
        '''
        return self._entity_type._custom_calendar
    
    def _get_data_scope(self,df):
        '''
        Return the start, end and set of entity ids contained in a dataframe as a tuple
        '''
        ts_series = self.get_timestamp_series(df=df)
        start_ts = ts_series.min()
        end_ts = ts_series.max()
        entity_id_series = self.get_entity_id_series(df=df)
        entities = list(pd.unique(entity_id_series))
        return (start_ts,end_ts,entities)
    
    
    def get_db(self,credentials = None, tenant_id = None):
        '''
        Get the Database object associciated with the function's assigned entity type.
        If there is no entity type, get a new database object using optionally supplied
        credentials and tenant id. If no credentials are supplied, credentials will be
        derived from environment variables
        '''
        try:
            db = self._entity_type.db
        except AttributeError:
            db = None
            
        if db is None:
            db = Database(credentials = credentials, tenant_id = tenant_id)
            
        return db
    
    def get_entity_id_series(self,df):
        '''
        Return a series containing entity ids
        '''
        series = self._get_series(df,[self._entity_type._entity_id,self._entity_type._df_index_entity_id])
        return series
    
    
    def get_entity_type(self):
        '''
        Get the EntityType object assigned to the function instance
        '''
        return self._entity_type
    
    def get_entity_type_param(self,param):
        '''
        Get a metadata parameter from the entity type
        '''
        entity_type = self.get_entity_type()
        if entity_type is None:
            raise RuntimeError ('This function has no entity type associated with it. This is a programatic error. After creating a function instance, use set_entity_type to assign an entity type')
        out = entity_type.get_param(param)
        return out
    
    
    def get_expression_items(self,expressions):

        if isinstance(expressions,str):
            expressions = [expressions]
            
        all_items = set()
        for e in expressions:
            #get all quoted strings in expression
            possible_items = re.findall('"([^"]*)"', e)
            #check if they have df[] wrapped around them
            all_items |= set([x for x in possible_items if 'df["%s"]'%x in e])
            
        if len(all_items) == 0:
            msg = 'Expression in function %s does not contain input items' %self.__class__.__name__
            logger.debug(msg)
        return all_items
    
    def _getJsonDataType(self,datatype):
         
         if datatype == 'LITERAL':
             result = 'string'
         else:
             result = datatype.lower()
         return result
     
     
    def _getJsonSchema(self,column_metadata,datatype,min_items,arg,is_array,is_output,is_constant):
        
        #json schema may have been explicitly defined                
        try:
           column_metadata['jsonSchema'] = self.itemJsonSchema[arg] 
        except KeyError:               
            if is_array:
                column_metadata['jsonSchema'] = {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "array",
                    "minItems": min_items
                    }
                item_type = "string"
                if is_constant:
                    item_type = self._getJsonDataType(datatype)
                try:
                    column_metadata['jsonSchema']["maxItems"] = self.itemMaxCardinality[arg]
                except KeyError:
                    pass
                try:
                    column_metadata['jsonSchema']["items"] = {"type": item_type}
                except KeyError:
                    pass                
                msg = 'Argument %s is has no explicit json schema defined for it, built one for %s items' %(arg, item_type)
            else:
                msg = 'Non array arg %s - no json schema required' %(arg)
            logger.debug(msg)                 
        else:
            msg = 'Argument %s is has explicit json schema defined for it %s' %(arg, self.itemJsonSchema[arg])
            logger.debug(msg)
        return column_metadata
    
    def get_input_set(self):
        
        ins = set(self._input_set)
        ins |= set(self.get_input_items())
        
        return ins
    
    def get_timestamp_series(self,df):
        '''
        Return a series containing timestamps
        '''
        series = self._get_series(df,[self._entity_type._timestamp,self._entity_type._timestamp_col])
        return series
    
    def _get_series(self,df,col_names):
        
        if isinstance(col_names,str):
            col_names = [col_names]
        for col in col_names:
            try:
                series = df[col]
            except KeyError:
                try:
                    series = df.index.get_level_values(col)
                except KeyError:
                    pass
                else:
                    return series
            else:
                return series
        msg = 'Unable to locate series with names %s in either columns or index' %col_names
        raise KeyError(msg)
                
    
    def get_input_items(self):
        '''
        Implement this method to return a set of data items that should be
        retrieved when executing a KPI pipeline. By default only items that
        are explicly referenced in function inputs are included.
        '''
        return(set())
    
    def get_item_values(self,arg):
        """
        Implement this method when you want to supply values to a picklist in the UI
        """
        
        msg = 'No code implemented to gather available values for argument %s' %arg
        
        raise NotImplementedError (msg)
        
        
    def _getMetadata(self, df = None, new_df = None, inputs = None, outputs = None, constants = None):
        """
        Assemble a dictionary of ICS Analytics Function metadata. Used to submit
        classes the ICS Analytics Function Catalog.
        
        Parameters:
        -----------
        df: DataFrame
            pandas dataframe. Used as sample data to infer types
        inputs: list
            list of strings. name of input columns to the function.
        outputs: list
            list of strings. name of output columns produced by the function.
        constants: list
            list of strings. name of constant input parameters to the function
        """
        
        #if the class has a build_ui class method, use it and return (input,output) metadat
        try:
            return self.build_ui()
        except (AttributeError,NotImplementedError):
            #else infer metadata from the dataframes supplied
            pass
          
        if inputs is None:
            inputs = self.inputs
        if outputs is None:
            outputs = self.outputs        
        if constants is None:
            constants = self.constants
            
        returns_dataframe = True
        #run the function to produce a new dataframe that contains the function outputs
        if not df is None:
            if new_df is None:
                tf = df.head(self.test_rows)
                tf = tf.copy()
                tf = self.execute(tf)
            else:
                tf = new_df
            if isinstance(tf,bool):
                returns_dataframe = False
                tf = df.head(self.test_rows)
                tf = tf.copy()
                tf = self._add_explicit_outputs(tf)
            elif not isinstance(tf,pd.DataFrame):
                raise TypeError('The execute method of a custom function must return a pandas DataFrame object not %s' %tf)
            test_outputs = self._inferOutputs(before_df=df,after_df=tf)
            if len(test_outputs) ==0:
                raise ValueError('Could not locate output columns in the test dataframe. Check the execute method of the function to ensure that it returns a dataframe with output columns that are named differently from the input columns')
        else:
            tf = None
            raise NotImplementedError('Must supply a test dataframe for function registration. Explict metadata definition not suported')
        
        metadata_inputs = OrderedDict()
        metadata_outputs = OrderedDict()
        min_items = None
        array_outputs = []
        array_inputs = []

        #introspect function to get a list of argumnents
        args = (getargspec(self.__init__))[0][1:]        
        for a in args:
            if a is None:
                msg = 'Cannot infer metadata for argument %s as it was initialized with a value of None. Supply an appropriate value when initializing.' %(a)
                raise ValueError(msg)
            #identify which arguments are inputs, which are outputs and which are constants
            try:
                arg_value = eval('self.%s' %a)
            except AttributeError:
                raise AttributeError('Class %s has an argument %s but no corresponding property. Make sure your arguments and properties have the same name if you want to infer types.' %(self.__class__.__name__, a))
            is_array = False
            if isinstance(arg_value,list):
                is_array = True                
            column_metadata = {}
            column_metadata['name'] = a
            column_metadata['description'] = None
            column_metadata['learnMore'] = None                        
            if a in self.optionalItems:
                required = False
            else:
                required = True
                min_items = 1            
            #set metadata from class/instance variables
            if not self.itemDescriptions is None:
                try:
                    column_metadata['description'] = self.itemDescriptions[a]                    
                except KeyError:
                    pass
            if not self.itemLearnMore is None:
                try:
                    column_metadata['learnMore'] = self.itemLearnMore[a]
                except KeyError:
                    pass                            

            is_added = False
            is_constant = True
            is_output = False
            try:
                argtype = self.itemDatatypes[a]
            except KeyError:
                argtype = self._infer_type(arg_value)
            msg = 'Evaluating %s argument %s. Array: %s' %(argtype,a,is_array)
            logger.debug(msg)
            #check if parameter has been modeled explictly
            if a in constants:
                datatype = self._infer_type(arg_value,df=None)
                column_metadata['dataType'] = datatype 
                auto_desc = 'Supply a constant input parameter of type %s' %datatype 
                column_metadata['required'] = required
                is_added = True
                msg = 'Argument %s was explicitlty defined as a constant with datatype %s' %(a,datatype)
                logger.debug(msg)
            elif a in outputs:
                is_output = True
                is_constant = False
                datatype = self._infer_type(arg_value,df=tf)
                auto_desc =  'Provide a new data item name for the function output'
                is_added = True
                msg = 'Argument %s was explicitlty defined as output with datatype %s' %(a,datatype)
                logger.debug(msg)
                if is_array:
                    array_outputs.append((a,len(arg_value)))  
            elif a in inputs:
                is_constant = False
                column_metadata['type'] = 'DATA_ITEM' 
                datatype = self._infer_type(arg_value,df=df)
                column_metadata['dataType'] = datatype
                auto_desc =  'Choose data item/s to be used as function inputs'
                column_metadata['required'] = required
                is_added = True
                msg = 'Argument %s was explicitlty defined as a data item input' %(a)
                logger.debug(msg)
            #if argument is a number, date or dict it must be a constant
            elif argtype in ['NUMBER','JSON','BOOLEAN','TIMESTAMP']:
                datatype = argtype
                column_metadata['dataType'] = datatype
                auto_desc =  'Supply a constant input parameter of type %s' %datatype 
                column_metadata['required'] = required
                is_added = True 
                msg = 'Argument %s is not a string so it must be constant of type %s' %(a,datatype)
                logger.debug(msg)                 
            #look for items in the input and output dataframes
            elif not is_array:
                #look for value in test df and test outputs
                if arg_value in df.columns:
                    is_constant = False
                    column_metadata['type'] = 'DATA_ITEM' 
                    datatype = self._infer_type(arg_value,df=df)
                    auto_desc = 'Choose a single data item'
                    is_added = True
                    column_metadata['required'] = required
                    msg = 'Non array argument %s exists in the test input dataframe so it is a data item of type %s' %(a,datatype)
                    logger.debug(msg)                                         
                elif arg_value in test_outputs:
                    is_constant = False
                    is_output = True
                    datatype = self._infer_type(arg_value,df=tf)
                    column_metadata['dataType'] = datatype
                    auto_desc =  'Provide a new data item name for the function output'
                    metadata_outputs[a] = column_metadata    
                    is_added = True
                    msg = 'Non array argument %s exists in the test output dataframe so it is a data item of type %s' %(a,datatype)
                    logger.debug(msg)                                                             
            elif is_array:
                #look for contents of list in test df and test outputs
                if all(elem in df.columns for elem in arg_value):
                    is_constant = False
                    column_metadata['type'] = 'DATA_ITEM' 
                    datatype = self._infer_type(arg_value,df=df)
                    auto_desc =  'Choose data item/s to be used as function inputs'
                    array_inputs.append((a,len(arg_value)))
                    is_added = True
                    column_metadata['required'] = required
                    msg = 'Array argument %s exists in the test input dataframe so it is a data item of type %s' %(a,datatype)
                    logger.debug(msg)                                                                                                             
                elif all(elem in test_outputs for elem in arg_value):
                    is_output = True
                    is_constant = False
                    datatype = self._infer_type(arg_value,df=tf)
                    auto_desc =  'Provide a new data item name for the function output'
                    array_outputs.append((a,len(arg_value)))
                    is_added = True
                    msg = 'Array argument %s exists in the test output dataframe so it is a data item' %(a)
                    logger.debug(msg)                                                                                                                                     
            #if parameter was not explicitly modelled and does not exist in the input and output dataframes
            # it must be a constant
            if not is_added:
                is_constant = True
                datatype = self._infer_type(arg_value,df=None)
                column_metadata['description'] = 'Supply a constant input parameter of type %s' %datatype 
                metadata_inputs[a] = column_metadata
                msg = 'Argument %s is assumed to be a constant of type %s by ellimination' %(a,datatype)
                logger.debug(msg)  
            if is_output:
                column_metadata['dataType'] = datatype                
                try:
                    column_metadata['tags'] = self.itemTags[a]
                except KeyError:
                    pass
                metadata_outputs[a] = column_metadata
            else:
                metadata_inputs[a] = column_metadata
            # set datatype
            if not is_array:
                column_metadata['dataType'] = datatype
            else:
                column_metadata['dataType'] = 'ARRAY'
                if not datatype is None:
                    column_metadata['dataTypeForArray'] = [datatype]
                else:
                    column_metadata['dataTypeForArray'] = None
                    
            # add auto description
            if column_metadata['description'] is None:
                column_metadata['description'] = auto_desc
            column_metadata = self._getJsonSchema(column_metadata=column_metadata,
                                                  datatype = datatype,
                                                  min_items = min_items,
                                                  arg=a,
                                                  is_array = is_array,
                                                  is_output = is_output,
                                                  is_constant = is_constant)
            #constants may have explict values
            values = None
            if is_constant:
                msg = 'Constant argument %s is has no explicit values defined for it and no values available from the get_item_values() method' %a
                column_metadata['type'] = 'CONSTANT'
                try:
                    values = self.itemValues[a]
                except KeyError:            
                    try:
                        values = self.get_item_values(a)
                    except (NotImplementedError,AttributeError):
                        pass
                        if is_array:
                            msg = 'Array input %s has no predefined values. It will appear in the UI as a type-in field that accepts a comma separated list of values. To set values implement the get_item_values() method' %a
                            warnings.warn(msg)
                    else:
                        msg = 'Explicit values were found in the the get_item_values() method for constant argument %s ' %a
                else:
                    msg = 'Explicit values were found in the the itemValues dict for constant argument %s ' %a
                if not values is None:
                    column_metadata['values'] = values
                logger.debug(msg) 
                
        #array outputs are special. They inherit their datatype from an input array
        #that could be explicity defined, or use last array_input 
        for (array,length) in array_outputs:
            
            try:
                array_source =  self.itemArraySource[array]
                msg = 'Cardinality and datatype of array output %s were explicly set to be driven from %s' %(array,array_source)
                logger.debug(msg)
            except KeyError:
                array_source = self._infer_array_source(candidate_inputs= array_inputs,
                                                     output_length = length)     
            if array_source is None:
                raise ValueError('No candidate input array found to drive output array %s with length %s . Make sure input array and output array have the same length or explicity define the item_source_array. ' %(array,length))
            else:
                #if the output array is driven by an array of items infer data types from items
                if metadata_inputs[array_source]['type'] == 'DATA_ITEM' and self.array_output_datatype_from_input:    
                    metadata_outputs[array]['dataTypeFrom']=array_source
                else:
                    metadata_outputs[array]['dataTypeFrom']=None
                metadata_outputs[array]['cardinalityFrom']=array_source
                
                del metadata_outputs[array]['dataType']
                msg = 'Array argument %s is driven by %s so the cardinality and datatype are set from the source' %(a,array_source)
                logger.debug(msg)
        return (metadata_inputs,metadata_outputs)
    
    def get_output_list(self):
        
        return self._output_list
    
    def get_bucket_name(self):
        '''
        Get the name of the cos bucket used to store models
        '''
        try:
            bucket = self._entity_type.db.credentials['config']['bos_runtime_bucket']
        except KeyError:
            msg = 'Unable to read value of credentials.bos_runtime_bucket from credentials. COS read/write is disabled'
            logger.error(msg)
            bucket = '_unknown_'
        except AttributeError:
            msg = 'Could not find credentials for entity type. COS read/write is disabled '
            logger.error(msg)
            bucket = '_unknown_'
        return bucket    
    
    
    def get_scd_data(self,table_name,start_ts, end_ts, entities):
        '''
        Retrieve a slowly changing dimension property as a dataframe
        '''       
        (query,table) = self._entity_type.db.query(table_name,schema = self._entity_type._db_schema)
        if not start_ts is None:
            query = query.filter(table.c.end_date >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c.start_date < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        msg = 'reading scd %s from %s to %s using %s' %(table_name, start_ts, end_ts, query.statement)
        logger.debug(msg)
        df = pd.read_sql(query.statement,
                         con = self._entity_type.db.connection,
                         parse_dates=[self._start_date,self._end_date])
        return df
   
    
    def get_test_data(self):
        """
        Output a dataframe for testing function
        """
        
        if self._entity_type is None:
            self._entity_type = EntityType(name='<Null Entity Type>',db=None)
        
        data = {
                self._entity_type._entity_id : ['D1','D1','D1','D1','D1','D2','D2','D2','D2','D2'],
                self._entity_type._timestamp_col : [
                        dt.datetime.strptime('Oct 1 2018 1:33AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 11:37PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 6:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 3 2018 3:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:38PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:29PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 2 2018 1:39PM', '%b %d %Y %I:%M%p'),                
                ],                
                'x_1' : [8.7,3.2,4.5,6.8,8.1,2.4,2.9,2.5,2.6,3.6],
                'x_2' : [2.1,3.1,2.5,4.2,5.2,4.6,4.1,4.5,0.5,8.7],
                'x_3' : [7.4,4.3,5.2,3.4,3.3,8.1,5.6,4.9,4.2,9.9],
                'e_1' : [0,0,0,1,0,0,0,1,0,0],
                'e_2' : [0,0,0,0,1,0,0,0,1,0],
                'e_3' : [0,1,0,1,0,0,0,1,0,1],
                's_1' : ['A','B','A','A','A','A','B','B','A','A'],
                's_2' : ['C','C','C','D','D','D','E','E','C','D'],
                's_3' : ['F','G','H','I','J','K','L','M','N','O'],
                'x_null' : [4.1,4.2,None,4.1,3.9,None,3.2,3.1,None,3.4],
                'd_1' : [
                        dt.datetime.strptime('Sep 29 2018 1:33PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Sep 29 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Sep 30 2018 1:37PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:39PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Sep 1 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Sep 15 2018 1:38PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Aug 17 2018 1:29PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Sep 20 2018 1:39PM', '%b %d %Y %I:%M%p'),                
                ],
                'd_2' : [
                        dt.datetime.strptime('Oct 14 2018 1:33PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 13 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 12 2018 1:37PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 18 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 10 2018 1:39PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 11 2018 1:31PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 12 2018 1:35PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 13 2018 1:38PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 16 2018 1:29PM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 10 2018 1:39PM', '%b %d %Y %I:%M%p'),                
                ],                        
                'd_3' : [
                        dt.datetime.strptime('Oct 1 2018 10:05AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:02AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:03AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:01AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:08AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:29AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:02AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 9:55AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 10:25AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Oct 1 2018 11:02AM', '%b %d %Y %I:%M%p'),                
                ],
                'company_code' : ['ABC','ACME','JDI','ABC','ABC','ACME','JDI','ACME','JDI','ABC']
                }
        df = pd.DataFrame(data=data)
        df = self.conform_index(df)
        
        return df       
    
    
    def _get_scd_history(self,start_ts,end_ts,entities):
        '''
        Build a dict keyed on scd property and entity id
        '''
        x = {}
        scd_metadata = self._entity_type._get_scd_list()
        if len(scd_metadata) ==0:
            return None
        else:
            for (scd_property, table) in scd_metadata:
                df = self.get_scd_data(table_name=table, start_ts=start_ts, end_ts = end_ts, entities = entities)
                x[scd_property] = self._partition_df_by_id(df)
            return x
        
    
    def log_df_info(self,df,msg,include_data=False):
        '''
        Log a debugging entry showing first row and index structure
        This is a default logger. You can implement a custom one if
        there is something specific that you want to include.
        '''
        msg = log_df_info(df=df,msg=msg,include_data = include_data)
        return msg
            
    
    def _infer_array_source(self,candidate_inputs,output_length):
        '''
        Look for the last input array with the same length as the target
        '''
        source = None
    
        for input_parm,input_length in candidate_inputs:
            if input_length == output_length:
                msg = 'Found an input array %s with the same length (%s) as the output' %(input_parm,output_length)
                logger.debug(msg)
                source = input_parm
        
        return source
            
        

    def _inferOutputs(self,before_df,after_df):
        '''
        Work out which columns were added to the test dataframe by executing the function. These are the outputs.
        '''
        outputs = list(set(after_df.columns) - set(before_df.columns))
        outputs.sort()
        msg = 'Columns added to the pipeline by the function are %s' %outputs
        logger.debug(msg)
        return outputs
    
    
    def _infer_type(self,parm,df=None):
        """
        Infer datatype for constant or item in dataframe.
        """
        if not isinstance(parm,list):
            parm = [parm]
            
        prev_datatype = None
        multi_datatype = False
        found_types = []
        append_msg = ''
        
        datatype = None
        for value in parm:
            if is_dict_like(value):
                datatype = 'JSON'
            elif df is None:
                #value is a constant
                if isinstance(value,str):
                    datatype = 'LITERAL'              
                elif isinstance(value,numbers.Number):
                    datatype = 'NUMBER'              
                elif isinstance(value,bool):
                    datatype = 'BOOLEAN'              
                elif isinstance(value, dt.datetime):
                    datatype = 'TIMESTAMP'
                else: 
                    raise TypeError('Cannot infer type of argument value %s for parm %s. Supply a string, number, boolean, datetime, dict or list containing any of these types.' %(value,parm))
            else:      
                append_msg = 'by looking at items in test dataframe'
                try:
                    if is_string_dtype(df[value]):
                        datatype = 'LITERAL'
                    elif is_bool_dtype(df[value]):
                        datatype = 'BOOLEAN' 
                    elif is_numeric_dtype(df[value]):
                        datatype = 'NUMBER'
                    elif is_datetime64_any_dtype(df[value]):
                        datatype = 'TIMESTAMP'
                except KeyError:
                    pass
        
            found_types.append(datatype)
            if not prev_datatype is None:
                if datatype != prev_datatype:
                    multi_datatype = True
            prev_datatype = datatype
            
        if multi_datatype:
            datatype = None
            msg = 'Found multiple datatypes for array of items %s' %(found_types,)
        else:
            msg = 'Infered datatype of %s from values %s %s' %(datatype,parm, append_msg )
        logger.debug(msg)
        
        if datatype is None:
            msg = 'Cannot infer datatype for argument %s. Explicitly set the datatype as LITERAL, BOOLEAN, NUMBER or TIMESTAMP in the itemDataTypes dict' %parm
            logger.warning(msg)
            
        return datatype
    
    def parse_expression(self, expression):
        '''
        Convert a string expression into a form where it is ready to be executed
        '''
        expression = expression.replace("'",'"')
        if '${' in expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", expression)
            msg = 'expression converted to %s' %expr
        else:
            expr = expression
            msg = 'expression (%s)' %expr
            
        logger.debug(msg)

        return expr

        
    def _partition_df_by_id(self,df):
        '''
        Partition dataframe into a dictionary keyed by _entity_id
        '''
        d = {x: table for x, table in df.groupby(self._entity_type._entity_id)}
        return d    
    
    @classmethod
    def _standard_item_descriptions(cls):
        
        itemDescriptions = {}
        
        itemDescriptions['bucket']= 'Name of the COS bucket used for storage of data or serialized objects'
        itemDescriptions['cos_credentials']= 'External COS credentials'
        itemDescriptions['db_credentials']= 'Db2 credentials'
        itemDescriptions['expression'] = 'Expression involving data items. Refer to data items using ${item_name} or using pandas syntax df["item_name"]'
        itemDescriptions['function_name']= 'Name of python function to be called.'
        itemDescriptions['input_items']= 'List of input items required by the function.'
        itemDescriptions['input_item']= 'Single input required by the function.'
        itemDescriptions['lookup_key']= 'Data item/s to use as key/s in a lookup operation'
        itemDescriptions['lookup_keys']= 'Data item/s to use as key/s in a lookup operation'
        itemDescriptions['lookup_items']= 'Columns from a lookup to include as new items'
        itemDescriptions['lower_threshold']= 'Lower threshold value for alert'
        itemDescriptions['output_alert']= 'Item name for alert produced by function'
        itemDescriptions['output_item']= 'Item name for output produced by function'
        itemDescriptions['output_items']= 'Item names for outputs produced by function'
        itemDescriptions['upper_threshold']= 'Upper threshold value for alert'
        
        return itemDescriptions
    
    def _remove_cols_from_df(self,df,cols):
        '''
        Remove list of columns from a dataframe. Return dataframe.
        '''
        before = set(df.columns)
        cols = [x for x in list(df.columns) if x not in cols]
        df = df[cols]
        removed = set(df.columns) - before
        if len(removed) > 0:
            msg = 'Removed columns %s' %removed
            logger.debug(msg)
        
        return df
    
    def register(self,df,credentials=None,new_df = None,
                 name=None,url=None,constants = None, module=None,
                 description=None,incremental_update=None, 
                 outputs = None, show_metadata = False,
                 metadata_only = False):
        '''
        Register the function type with AS
        '''
        
        if self._entity_type is None:
            self._entity_type = EntityType(name='<Null Entity Type>',db=None)
        
        if not self.base_initialized:
            raise RuntimeError('Cannot register function. Did not call super().__init__() in constructor so defaults have not be set correctly.')
            
        if self.category is None:
            raise AttributeError('Class has no category. Class should inherit from BaseTransformer or BaseAggregator to obtain an appropriate category')
            
        if name is None:
            name = self.name
            
        if module is None:
            module = self.__class__.__module__
            
        if module == '__main__':
            raise RuntimeError('The function that you are attempting to register is not located in a package. It is located in __main__. Relocate it to an appropriate package module.')
            
        if description is None:
            description = self.description
            
        if url is None:
            url = self.url
            
        if incremental_update is None:
            incremental_update = self.incremental_update
        
        try:
            (metadata_input,metadata_output) = self.build_ui()
        except (AttributeError,NotImplementedError):
            (metadata_input,metadata_output) = self._getMetadata(df=df,new_df = new_df, outputs=outputs,constants = constants, inputs = self.inputs)
                    
        (input_list, output_list) = self._transform_metadata(metadata_input,metadata_output)

        module_and_target = '%s.%s' %(module,self.__class__.__name__)

        exec_str = 'from %s import %s as import_test' %(module,self.__class__.__name__)
        try:
            exec (exec_str)
        except ImportError:
            raise ValueError('Unable to register function as local import failed. Make sure it is installed locally and importable. %s ' %exec_str)
        
        exec_str_ver = 'import %s as import_test' %(module.split('.', 1)[0])
        exec(exec_str_ver)
        try:
            module_url = eval('import_test.%s.PACKAGE_URL' %(module.split('.', 1)[1]))        
        except Exception as e:            
            logger.exception('Error importing package. It has no PACKAGE_URL module variable')
            raise e
        if module_url == BaseFunction.url:
            logger.warning('The PACKAGE_URL for your module is the same as BaseFunction url. Make sure that your PACKAGE_URL points to your own package and not iotfunctions')            
        msg = 'Test import succeeded for function using %s with module url %s' %(exec_str, module_url)
        logger.debug(msg)            
        payload = {
            'name': name,
            'description': description,
            'category': self.category,
            'tags': self.tags,
            'moduleAndTargetName': module_and_target,
            'url': url,
            'input': input_list ,
            'output':output_list,
            'incremental_update': incremental_update if self.category == 'AGGREGATOR' else None
        }
        
        if not credentials is None:
            msg = 'Passing credentials for registration is preserved for compatibility. Use old style credentials when doing so, or omit credentials to use credentials associated with the Database object for the function'
            logger.info(msg)
            http = urllib3.PoolManager()
            encoded_payload = json.dumps(payload).encode('utf-8')
            if show_metadata or metadata_only:
                print(encoded_payload)
            
            try:
                headers = {
                    'Content-Type': "application/json",
                    'X-api-key' : credentials['as_api_key'],
                    'X-api-token' : credentials['as_api_token'],
                    'Cache-Control': "no-cache",
                }
            except KeyError:
                msg('Old style credentials are a dictionary with tennant_id.as_api_key, as_api_token and as_api_host')
            
            if not metadata_only:
                url = 'http://%s/api/catalog/v1/%s/function/%s' %(credentials['as_api_host'],credentials['tennant_id'],name)
                r = http.request("DELETE", url, body = encoded_payload, headers=headers)
                msg = 'Function registration deletion status: %s' %(r.data.decode('utf-8'))
                logger.info(msg)
                r = http.request("PUT", url, body = encoded_payload, headers=headers)     
                msg = 'Function registration status: %s' %(r.data.decode('utf-8'))
                logger.info(msg)
                return r.data.decode('utf-8')
            else:
                return encoded_payload
            
        else:
            if self._entity_type is None:
                msg ('Unable to register function as there is no _entity_type. Use set_entity_type to assign an EntityType')
                logger.warning(msg)
            
            try:
                response = self._entity_type.db.http_request(object_type = 'function',
                                     object_name = name,
                                     request = 'DELETE',
                                     payload = payload)
            except TypeError:
                msg = 'Unable to serialize payload %s' %payload
                raise TypeError(msg)
            msg = 'Unregistered function with response %s' %response
            logger.debug(msg)
            response = self._entity_type.db.http_request(object_type = 'function',
                                 object_name = name,
                                 request = 'PUT',
                                 payload = payload)
            msg = 'Registered function with response %s' %response
            logger.debug(msg)    
        
    
    def rename_cols(self, df, input_names, output_names ):
        '''
        Rename columns using a list or original input names and a list of required output names.
        '''
        if len(input_names) != len(output_names):
            raise ValueError('Error in function configuration. The number of values in an array output must match the inputs')
        column_names = {}
        for i, name in enumerate(input_names):
            column_names[name] = output_names[i]
        df = df.rename(columns=column_names)
        return df
    
    def set_entity_type(self,entity_type):
        """
        Set the _entity_type property of the function
        """
        self._entity_type = entity_type
        if self._metadata_params is not None and self._metadata_params != {}:
            self._entity_type.set_params(**self._metadata_params)
            msg = 'Metadata provider added parameters to entity type: %s' %self._metadata_params
            logger.debug(msg)
    
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self
    
    def __str__(self):
        
        return self.name
 

    def trace_append(self,msg,log_method=None,df=None,**kwargs):
        '''
        Add to the trace info collected during function execution
        '''        
                
        et = self.get_entity_type()
        et.trace_append(created_by = self,
                        msg=msg,
                        log_method=log_method,
                        **kwargs)
        

    @classmethod        
    def _transform_metadata(cls,metadata_input,metadata_output):
        '''
        legacy metadata structure is a dict containing a metadata dict
        new metadata structure is a list containing ui objects
        convert to a list containing a metadata dict to use legacy code
        '''
        
        if not isinstance(metadata_input,list):
            metadata_input = list(metadata_input.values())
        if not isinstance(metadata_output,list):
            metadata_output = list(metadata_output.values())
        output_list= []
        input_list= []
        for m in metadata_output:
            try:
                output_list.append(m.to_metadata())
            except AttributeError:
                output_list.append(m) 
        for m in metadata_input:
            try:
                input_list.append(m.to_metadata())
            except AttributeError:
                input_list.append(m)
            #some inputs can create outputs automatically
            try:
                output_metadata = m.to_output_metadata()
            except AttributeError:
                pass
            else:
                if output_metadata is not None:
                    output_list.append(output_metadata)

        for i in input_list:
            msg = 'Looking for item values for %s' %i
            logger.debug(msg)
            try:    
                item_values = cls.get_item_values(arg=i)
            except (AttributeError,NotImplementedError,TypeError):
                item_values = None
            if item_values is not None:
                metadata_values = None
                try:
                    metadata_values = i['value']
                except KeyError:
                    pass
                if metadata_values is None:
                    i['values'] = item_values 

        return(input_list,output_list)
        
    
    def write_frame(self,df,
                    table_name=None, 
                    version_db_writes = None,
                    if_exists = None):
        '''
        Write a dataframe to a database table
        
        Parameters
        ---------------------
        table_name: str (optional)
            table name to write to. If not provided, will use default for instance / class
        version_db_writes : boolean (optional)
            Add seprate version_date column to table. If not provided, will use default for instance / class
        if_exists : str (optional)
            What to do if table already exists. If not provided, will use default for instance / class
        
        Returns
        -----------
        numerical status. 1 for successful write.
            
        '''
        df = df.copy()
        
        if if_exists is None:
            if_exists = self.out_table_if_exists
            
        if table_name is None:
            if self.out_table_prefix != '':
                table_name='%s_%s' %(self.out_table_prefix, self.out_table_name)
            else:
                table_name = self.out_table_name
    
        status = self._entity_type.db.write_frame(df, table_name = table_name, 
                                     version_db_writes = version_db_writes,
                                     if_exists  = if_exists, 
                                     schema = self._entity_type._db_schema,
                                     timestamp_col = self._entity_type._timestamp_col)
        
        return status
    

class BaseTransformer(BaseFunction):
    """
    Base class for AS Transform Functions. Inherit from this class when building a custom function that adds new columns to a dataframe.

    """
    category =  'TRANSFORMER' 
    is_transformer = True
    
    def __init__(self):
        super().__init__()


class BaseDataSource(BaseTransformer):
    """
    Base class for functions that involve merging time series data from another data source.

    """
    is_data_source = True
    merge_method = 'outer' #or nearest, concat
    #use concat when the source time series contains the same metrics as the entity type source data
    #use nearest to align the source time series to the entity source data
    #use outer to add new timestamps and metrics from the source
    merge_nearest_tolerance = pd.Timedelta('1D')
    merge_nearest_direction = 'nearest' #or backward,forward
    source_entity_id = 'deviceid'
    source_timestamp = 'evt_timestamp'
    auto_conform_index = True
    
    def __init__(self, input_items, output_items=None, dummy_items = None):
        self.input_items = input_items
        if output_items is None:
            output_items = [x for x in self.input_items]
        self.output_items = output_items
        super().__init__()
        if dummy_items is None:
            dummy_items = []
        self.dummy_items = dummy_items
        # explicitly define input_items as a constants parameter so that it does not
        # look like an output parameter
        self.constants.append('input_items')
        # explicitly tie the array of outputs from the function to the inputs
        # the function will deliver an output item for each choosen input item
        self.itemArraySource['output_items'] = 'input_items'
        # in case input and output items are defined as a comma separated list, convert to array
        self.input_items = self.convertStrArgToList(input_items,argument = 'lookup_items')
        self.output_items = self.convertStrArgToList(output_items,argument = 'output_items')
        # define the list of values for the picklist of input items in the UI
        # registration
        self.optionalItems.extend([self.dummy_items])

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms

    def get_data(self,start_ts=None,end_ts=None,entities=None):
        '''
        The get_data() method is used to retrieve additional time series data that will be combined with existing pipeline data during pipeline execution.
        '''
        raise NotImplementedError('You must implement a get_data() method for any class that acts as a data source')
            
        
    def execute(self,df,start_ts=None,end_ts=None,entities=None):        
        '''
        Retrieve data and combine with pipeline data
        '''
        new_df = self.get_data(start_ts=start_ts,end_ts=end_ts,entities=entities)
        self.log_df_info(df,'source dataframe before merge')
        self.log_df_info(new_df,'additional data source to be merged')        
        overlapping_columns = list(set(new_df.columns.intersection(set(df.columns))))
        if self.merge_method == 'outer':
            #new_df is expected to be indexed on id and timestamp
            df = df.join(new_df,how='outer',sort=True,on=[self._entity_type._df_index_entity_id,self._entity_type._timestamp],rsuffix ='_new_')
            df = self._coallesce_columns(df=df,cols=overlapping_columns)
        elif self.merge_method == 'nearest': 
            overlapping_columns = [x for x in overlapping_columns if x not in [self._entity_type._entity_id,self._entity_type._timestamp]]
            try:
                df = pd.merge_asof(left=df,right=new_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp,tolerance=self.merge_nearest_tolerance,suffixes=[None,'_new_'])
            except ValueError:
                new_df = new_df.sort_values([self._entity_type._timestamp,self._entity_type._entity_id])
                try:
                    df = pd.merge_asof(left=df,right=new_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp,tolerance=self.merge_nearest_tolerance,suffixes=[None,'_new_'])
                except ValueError:
                    df = df.sort_values([self._entity_type._timestamp_col,self._entity_type._entity_id])
                    df = pd.merge_asof(left=df,right=new_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp_col,tolerance=self.merge_nearest_tolerance,suffixes=[None,'_new_'])
            df = self._coallesce_columns(df=df,cols=overlapping_columns)
        elif self.merge_method == 'concat':
            df = pd.concat([df,new_df],sort=True)
        elif self.merge_method == 'replace':
            orginal_df = df
            df = new_df
            #add back item names from the original df so that they don't vanish from the pipeline
            for i in orginal_df.columns:
                if i not in df.columns:
                    df[i] = orginal_df[i].max() #preserve type. value is not important
        else:
            raise ValueError('Error in function definition. Invalid merge_method (%s) specified for time series merge. Use outer, concat or nearest')
        df = self.rename_cols(df,input_names=self.input_items,output_names = self.output_items)
        if self.auto_conform_index:
            df = self.conform_index(df)
        return df

class BaseEvent(BaseTransformer):
    """
    Base class for AS Functions that product events or alerts. 

    """
    
    def __init__(self):
        super().__init__()
        self.tags.append('EVENT')
        

class BaseFilter(BaseTransformer):
    """
    Base class for filters. Filters act on existing pipeline columns (reducing the number of rows).
    Filters work differently from other transformers as they have no real output items
    The ouput item from a filter is a bolean that indicates that the filter was processed.
    """
    is_filter = True
    
    def __init__(self, dependent_items, output_item = None):
        super().__init__()
        self.dependent_items = dependent_items
        self.output_item = self.name.lower()
        self.inputs.extend([self.dependent_items])
        self.outputs.extend([self.output_item])
        self.optionalItems.extend([self.dependent_items])
        
    def execute(self,df):
        '''
        The execute method for a filter calls a filter method. Define filter logic in the filter method.
        '''
        df = self.filter(df)
        df[self.output_item] = True
        return df

    def filter(self,df):
        '''
        Define your custom filter logic in a filter() method
        '''
        raise NotImplementedError('This function has no filter method defined. You must implement a custom filter method for a filter function')
        return df



class BaseAggregator(BaseFunction):
    """
    Base class for AS Aggregator Functions. Inherit from this class when building a custom function that aggregates a dataframe.
    """
    
    category =  'AGGREGATOR'
    
    def __init__(self):
        super().__init__()
        
class BaseDatabaseLookup(BaseTransformer):
    """
    Base class for lookup functions.
    """
    '''
    Optionally provide sample data for lookup
    this data will be used to create a new lookup function
    data should be provided as a dictionary and used to create a DataFrame
    '''
    data = None
    #Even this function returns new data to the pipeline, it is not considered a data source
    #as it behaves like any other transformer, ie: adds columns not rows to the pipeline
    is_data_source = False
    # database
    db = None
    _auto_create_lookup_table = False
    
    def __init__(self,
                 lookup_table_name,
                 lookup_items,
                 lookup_keys,
                 parse_dates=None,
                 output_items=None,
                 sql = None):
            
        self.lookup_table_name = lookup_table_name
        if lookup_items is None:
            msg = 'You must provide a list of columns for the lookup_items argument'
            raise ValueError(msg)
        self.lookup_items = lookup_items
        self.sql = sql
        super().__init__()
        #drive the output cardinality and data type from the items choosen for the lookup
        self.itemArraySource['output_items'] = 'lookup_items'
        self.lookup_keys = lookup_keys
        self.itemMaxCardinality['lookup_keys'] = len(self.lookup_keys)
        if parse_dates is None:
            parse_dates = []
        self.parse_dates = parse_dates 
        if output_items is None:
            #concatentate lookup name to output to make it unique
            output_items = ['%s_%s' %(self.lookup_table_name,x) for x in lookup_items]        
        self.output_items = output_items
        
    def get_item_values(self,arg):
        """
        Get list of columns from lookup table, Create lookup table from self.data if it doesn't exist.
        """
        if arg == 'lookup_items':
            '''
            Get a list of columns returned by the lookup
            '''
            lup_keys = [x.upper() for x in self.lookup_keys]
            date_cols = [x.upper() for x in self.parse_dates]
            df = pd.read_sql(self.sql, con = self.db, index_col=lup_keys, parse_dates=date_cols)
            df.columns = [x.lower() for x in list(df.columns)]            
            return(list(df.columns))
                        
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)
    
    def execute(self, df):
        '''
        Execute transformation function of DataFrame to return a DataFrame
        '''                
        self.db = self.get_db()
        if self._auto_create_lookup_table:
            self.create_lookup_table(df=None,table_name=self.lookup_table_name)
            
        if self.sql is None:
            query, table = self._entity_type.db.query(table_name = self.lookup_table_name,
                                                      schema = self._entity_type._db_schema)
            self.sql = query.statement

        msg = ' function attempted to excecute sql %s. ' %self.sql
        self.trace_append(msg)
        df_sql = pd.read_sql(self.sql, 
                             self.db.connection,
                             index_col=self.lookup_keys,
                             parse_dates=self.parse_dates)
        msg = 'Lookup returned columns %s. ' %','.join(list(df_sql.columns))
        self.trace_append(msg)
        
        df_sql = df_sql[self.lookup_items]
                
        if len(self.output_items) > len(df_sql.columns):
            raise RuntimeError('length of names (%d) is larger than the length of query result (%d)' % (len(self.output_items), len(df_sql)))

        df = df.join(df_sql,on= self.lookup_keys, how='left')
        
        df = self.rename_cols(df,input_names = self.lookup_items,output_names=self.output_items)

        return df
    
    def create_lookup_table(self,df=None, table_name=None):
        '''
        Create and populate lookup table
        '''
        if self.db is None:
            self.get_db()
        if df is None:        
            if self.data is not None:
                df = pd.DataFrame(data=self.data)
                df = df.set_index(keys=self.lookup_keys)            
            else:
                msg = 'Cannot create lookup table as data instance or class variable is not set and no dataframe provided'
                raise ValueError(msg)
        if table_name is None:
            table_name = self.lookup_table_name
        self.write_frame(df=df,table_name = table_name, if_exists = 'replace')
        msg = 'Created or replaced lookup table %s' %table_name
        logger.warning(msg)
        
    def get_input_items(self):
        '''
        Lookup must always include the lookup keys
        '''
        return set(self.lookup_keys)
        
        
class BaseDBActivityMerge(BaseDataSource):
    '''
    Merge actitivity data with time series data.
    Activies are events that have a start and end date and generally occur sporadically.
    Activity tables contain an activity column that indicates the type of activity performed.
    Activities can also be sourced by means of custom tables.
    This function flattens multiple activity types from multiple activity tables into columns indicating the duration of each activity.
    When aggregating activity data the dimenions over which you aggregate may change during the time taken to perform the activity.
    To make allowance for thise slowly changing dimenions, you may include a customer calendar lookup and one or more resource lookups
    '''
    
    # automatically build queries to merge in data from one or more db.ActivityTable
    activities_metadata = None
    # merge in data from one or more custom sql statement
    activities_custom_query_metadata = None
    # decide on a strategy for removing gaps
    remove_gaps =  'within_single' # 'across_all'
    # column name metadata
    # the start and end dates for activities are assumed to be designated by specific columns
    # the type of activity performed on or using an entity is designated by the 'activity' column
    _activity = 'activity'
    
    def __init__(self,
                 input_activities,
                 activity_duration= None, 
                 additional_items= None,
                 additional_output_names = None,
                 dummy_items = None):
    
        if self.activities_metadata is None:
            self.activities_metadata = {}
        if self.activities_custom_query_metadata is None:
            self.activities_custom_query_metadata = {}  
        self.input_activities = input_activities
        if additional_items is None:
            additional_items = []
        if activity_duration is None:
            activity_duration = ['duration_%s' %x for x in self.input_activities]
        self.activity_duration = activity_duration
        self.additional_items = additional_items
        if additional_output_names is None:
            additional_output_names = ['output_%s' %x for x in self.additional_items]
        self.additional_output_names = additional_output_names
        self.available_non_activity_cols = []
            
        super().__init__(input_items = input_activities , output_items = None,
                         dummy_items = dummy_items)
        #for any function that requires database access, create a database object
        self.itemArraySource['activity_duration'] = 'input_activities'
        self.itemArraySource['additional_output_names'] = 'additional_items'
        self.constants.extend(['input_activities','input_activities'])
        self.outputs.extend(['activity_duration','additional_output_names'])
        self.optionalItems.extend(['additional_items'])
        
    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        
        self.execute_by = [self._entity_type._entity_id]
        df = super().execute(df, start_ts=start_ts, end_ts=end_ts, entities=entities)
        return df
        
    def get_data(self,
                    start_ts= None,
                    end_ts= None,
                    entities = None):
        
        dfs = []
        #build sql and execute it 
        for table_name,activities in list(self.activities_metadata.items()):
            for a in activities:
                af = self.read_activity_data(table_name=table_name,
                                   activity_code=a,
                                   start_ts = start_ts,
                                   end_ts = end_ts,
                                   entities = entities)
                
                af[self._activity] = a
                msg = 'Read activity table %s' %table_name
                self.log_df_info(af,msg)
                dfs.append(af)
                self.available_non_activity_cols.append(self._get_non_activity_cols(af))
        #execute sql provided explictly
        for activity, sql in list(self.activities_custom_query_metadata.items()):
            try:
                af = pd.read_sql_query(sql,
                                       con=self._entity_type.db.connection,
                                       parse_dates=[self._start_date,self._end_date])
            except:
                logger.warning('Function attempted to retrieve data for a merge operation using custom sql. There was a problem with this retrieval operation. Confirm that the sql is valid and contains column aliases for start_date,end_date and device_id')
                logger.warning(sql)
                raise 
            af[self._activity] = activity
            dfs.append(af)
            self.available_non_activity_cols.append(self._get_non_activity_cols(af))
            
        if len(dfs) == 0:
            cols = []
            cols.append(self.activity_duration)
            cols.append(self.additional_items)
            adf = self.empty_dataframe(columns=cols)            
        else:
            adf = pd.concat(dfs,sort=False)
            self.log_df_info(adf,'After merging activity data from all sources')
            #get shift changes
            self.add_dates = []
            self.custom_calendar_df = None
            custom_calendar = self.get_custom_calendar()
            if not custom_calendar is None:
                if len(adf.index) > 0:
                    start_date =  adf[self._start_date].min()
                    end_date = adf[self._end_date].max()
                    self.custom_calendar_df = custom_calendar.get_data(start_date= start_date, end_date = end_date)
                    add_dates = set(self.custom_calendar_df[custom_calendar.period_start_date].tolist())
                    add_dates |= set(self.custom_calendar_df[custom_calendar.period_end_date].tolist())
                    self.add_dates = list(add_dates)
                else:
                    self.add_dates = []
                    self.custom_calendar_df = custom_calendar.get_empty_data()
            #get scd changes
            self._entity_scd_dict = self._get_scd_history(start_ts= adf[self._start_date].min(), end_ts = adf[self._end_date].max(),entities=entities)
            #merge takes place separately by entity instance
            if self.remove_gaps == 'across_all':    
                group_base = []
            elif self.remove_gaps == 'within_single':
                group_base = ['activity']
            else:
                msg = 'Value of %s for remove_gaps is invalid. Use across_all or within_single' % self.remove_gaps
                raise ValueError(msg)
            levels = []
            for s in self.execute_by:
                if s in adf.columns:
                    group_base.append(s)
                else:
                    try:
                        adf.index.get_level_values(s)
                    except KeyError:
                        raise ValueError('This function executes by column %s. This column was not found in columns or index' %s)
                    else:
                        group_base.append(pd.Grouper(axis=0, level=adf.index.names.index(s)))
                levels.append(s)
            try:
                group = adf.groupby(group_base)             
            except KeyError:
                msg = 'Attempt to execute combine activities by %s. One or more group by column was not found' %levels
                logger.debug(msg)
                raise
            else:
                try:
                    cdf = group.apply(self._combine_activities)
                except KeyError:
                    msg = 'combine activities requires deviceid, start_date, end_date and activity. supplied columns are %s' %list(adf.columns)
                    logger.debug(msg)
                    raise
                #if the original dataframe was empty, the apply() will not run
                # any columns added by the applied method will be missing. Need to add them
                if cdf.empty:
                    cdf = self._get_empty_combine_data()
                    self.log_df_info(cdf,'No data in merge source, processing empty dataframe')
                else:
                    self.log_df_info(cdf,'combined activity data after removing overlap')
                    cdf['duration'] = round((cdf[self._end_date] - cdf[self._start_date]).dt.total_seconds()) / 60
                    
            for i,value in enumerate(self.input_activities):
                cdf[self.activity_duration[i]] = np.where(cdf[self._activity]==value, cdf['duration'], None)
                cdf[self.activity_duration[i]] = cdf[self.activity_duration[i]].astype(float)
            
            self.log_df_info(cdf,'After pivot rows to columns')
                        
            for i,nadf in enumerate(dfs):
               add_cols = [x for x in self.available_non_activity_cols[i] if x in self.additional_items]
               if len(add_cols) > 0:
                   include = []
                   include.extend(add_cols)
                   include.extend(['start_date', self._entity_type._entity_id])
                   nadf = nadf[include]
                   cdf = cdf.merge(nadf,
                                   on = ['start_date', self._entity_type._entity_id],
                                   how = 'left', suffixes = ('','_new_'))
                   self.log_df_info(cdf,'post merge')
                   cdf = self._coallesce_columns(cdf,add_cols)
                   self.log_df_info(cdf,'post coallesce')
            #rename initial outputs
            cdf = self.rename_cols(cdf,self.additional_items,self.additional_output_names)
            cdf = self.conform_index(cdf,timestamp_col = self._start_date) 
            #add end dates
            cdf[self._end_date] = cdf[self._start_date].shift(-1)
            
        return cdf
    
    def get_item_values(self,arg):
        '''
        Define picklist values
        '''
        msg = 'Getting item values for arg %s' %arg
        logger.debug(msg)
        if arg == 'input_activities':
            all_activities = []
            for table_name,activities in list(self.activities_metadata.items()):
                all_activities.extend(activities)
                msg = 'Added activity list %s' %activities
                logger.debug(msg)
            for activity,sql in list(self.activities_custom_query_metadata.items()):
                all_activities.append(activity)                
                msg = 'Added to activity list %s' %activity
                logger.debug(msg)
            return(all_activities)
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)
        
    
    def _get_non_activity_cols(self,df):
        
        activity_cols = [self._entity_type._timestamp_col, self._entity_type._entity_id, 'start_date', 'end_date', 'activity']
        cols = [x for x in df.columns if x not in activity_cols]
        return cols
                    
    def _combine_activities(self,df):
        '''
        incoming dataframe has start date , end date and activity code.
        activities may overlap.
        output dataframe corrects overlapping activities.
        activities with later start dates take precidence over activies with earlier start dates when resolving.
        '''
        #dataframe expected to contain start_date,end_date,activity for a single deviceid
        is_logged = self._is_instance_level_logged
        entity = df[self._entity_type._entity_id].max()                
        if is_logged:
            self.log_df_info(df,'Incoming data for combine activities. Entity Id: %s' %entity)

        #create a continuous range
        early_date = pd.Timestamp.min
        late_date = pd.Timestamp.max        
        #create a new start date for each potential interruption of the continuous range
        dates = set([early_date,late_date])
        dates |= set((df[self._start_date].tolist()))
        dates |= set((df[self._end_date].tolist()))
        dates |= set(self.add_dates)
        #scd changes are another potential interruption
        if self._entity_scd_dict is not None:
            has_scd={}
            for scd_property,entity_data in list(self._entity_scd_dict.items()):
                has_scd[scd_property] = True
                try:
                    dates |= set(entity_data[entity][self._start_date])
                except KeyError:
                    has_scd[scd_property]=False
        dates = list(dates)
        dates.sort()
        
        #initialize series to track history of activities
        c = pd.Series(data='_gap_',index = dates)
        c.index = pd.to_datetime(c.index)
        c.name = self._activity
        c.index.name = self._start_date
        #use original data to update the new set of intervals in slices
        for index, row in df.iterrows():
            end_date = row[self._end_date] - dt.timedelta(microseconds=1)
            c[row[self._start_date]:end_date] = row[self._activity]    
        df = c.to_frame().reset_index()
        if is_logged:
            self.log_df_info(df,'Merging activity details. Initial dataframe with dates')
        
        #add end dates
        df[self._end_date] = df[self._start_date].shift(-1)
        df[self._end_date] = df[self._end_date] - dt.timedelta(microseconds=1)
        
        #remove gaps
        if self.remove_gaps:
            df = df[df[self._activity]!='_gap_']
            if is_logged:
                self.log_df_info(df,'after removing gaps') 
    
        #combined activities dataframe has start_date,end_date,device_id, activity 
        return df

    def _get_empty_combine_data(self):
        '''
        In the case where the merged resultset is empty, need a empty dateframe with all of the columns that would have
        been inlcuded.
        '''
        
        cols = [self._start_date, self._end_date, self._activity, 'duration']
        cols.extend(self.execute_by)
        if self.custom_calendar_df is not None:
            cols.extend(['shift_id','shift_day'])
        if self._entity_scd_dict is not None:
            scd_properties = list(self._entity_scd_dict.keys())
            cols.extend(scd_properties)
        return pd.DataFrame(columns = cols)
                
    def read_activity_data(self,table_name,activity_code,start_ts=None,end_ts=None,entities=None):
        """
        Issue a query to return a dataframe. Subject is an activity table with columns: deviceid, start_date, end_date, activity
        
        Parameters
        ----------
        table_name: str
            Name of source table
        activity_code: str
            The specific activity code for which to receive data
        start_ts : datetime (optional)
            Date filter
        end_ts : datetime (optional)
            Date filter            
        entities: list (optional)
            Filter on list of device ids
        Returns
        -------
        Dataframe
        """
        
        (query,table) = self._entity_type.db.query(table_name,schema = self._entity_type._db_schema)
        query = query.filter(table.c.activity == activity_code)
        if not start_ts is None:
            query = query.filter(table.c.end_date >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c.start_date < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        msg = 'reading activity %s from %s to %s using %s' %(activity_code,start_ts,end_ts,query.statement )
        logger.debug(msg)
        df = pd.read_sql(query.statement,
                         con = self._entity_type.db.connection,
                         parse_dates=[self._start_date,self._end_date])
        
        return df


class BaseSCDLookup(BaseTransformer):
    '''
    Lookup a slowly changing property
    '''
    _start_date = 'start_date'
    _end_date = 'end_date'
    merge_nearest_tolerance = None # or something like pd.Timedelta('1D')
    is_scd_lookup = True
    
    def __init__ (self, table_name, output_item = None):
        
        self.table_name = table_name
        if output_item is None:
            output_item = self.table_name
        self.output_item = output_item
        super().__init__()
        self.itemTags['output_item'] = ['DIMENSION']
        
    def execute(self,df):
        
        msg = 'Starting scd lookup of %s from table %s. ' %(self.output_item,self.table_name)
        msg = self.log_df_info(df,msg) 
        self.trace_append(msg)
        
        (start_ts, end_ts, entities) = self._get_data_scope(df)
        resource_df = self.get_scd_data(table_name = self.table_name, start_ts = start_ts, end_ts=end_ts, entities=entities)
        msg = 'df for resource lookup' 
        msg = self.log_df_info(resource_df,msg) + '. '
        self.trace_append(msg)       
        system_cols = [self._start_date,self._end_date,self._entity_type._entity_id]
        try:
            scd_property = [x for x in resource_df.columns if x not in system_cols][0]
        except:
            msg = 'Error looking up scd_property from table %s. Make sure that table name is an scd with start_data, end_date, deviceid and a property name' %self.table_name
            logger.exception(msg)
            raise
        
        resource_df = resource_df.rename(columns = {scd_property:self.output_item,
                                          'start_date': self._entity_type._timestamp})
        cols = [x for x in resource_df.columns if x not in ['end_date']]
        resource_df = resource_df[cols]
        try:
            df = pd.merge_asof(left=df,right=resource_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp,tolerance=self.merge_nearest_tolerance)
        except ValueError:
            resource_df = resource_df.sort_values([self._entity_type._timestamp,self._entity_type._entity_id])
            try:
                df = pd.merge_asof(left=df,right=resource_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp,tolerance=self.merge_nearest_tolerance)
            except ValueError:
                df = df.sort_values([self._entity_type._timestamp,self._entity_type._entity_id])
                df = pd.merge_asof(left=df,right=resource_df,by=self._entity_type._entity_id,on=self._entity_type._timestamp,tolerance=self.merge_nearest_tolerance)
        
        msg = 'After scd lookup of %s from table %s. ' %(scd_property,self.table_name)
        self.trace_append(msg, df = df)
        df = self.conform_index(df)  
        
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name = 'table_name',
                               datatype=str,
                               description = 'Table name to use as source for lookup'
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name = 'output_item'))    
        return (inputs,outputs)   
    
    
class BasePreload(BaseTransformer):
    """
    Preload functions execute before loading entity data into the pipeline
    Preload functions have no input items or output items
    Preload functions do not take a dataframe as input
    Preload functions return a single boolean output on execution. Pipeline will proceed when True.
    You guessed it, preload methods have no boundaries. You can use them to do anything!
    They are monitored. Excessive resource consumption will be billed by estimating an equivalent number of function executions. 
    """
    is_preload = True
    
    def __init__(self, dummy_items, output_item = None):
        super().__init__()
        self.dummy_items = dummy_items
        self.output_item = self.name.lower()
        self.inputs.extend([self.dummy_items])
        self.outputs.extend([self.output_item])
        self.optionalItems.extend([self.dummy_items])
        self.itemDatatypes['dummy_items'] = None
        self.itemDatatypes['output_items'] = 'BOOLEAN'
        
    def execute(self,df,start_ts = None,end_ts=None,entities=None):
        '''
        Execute function may optionally use a start_ts,end_ts and entities passed to the pipeline for processing
        '''
        raise NotImplementedError('This function has no execute method defined. You must implement a custom execute for any preload function')
        return True
    
    def _getMetadata(self, df = None, new_df = None, inputs = None, outputs = None, constants = None):
        '''
        Preload function has no dataframe in or out so standard _getMetadata() does not work
        '''
        #define arguments that behave as function inputs
        inputs = {}
        inputs['dummy_items'] = UIMultiItem(name = 'dummy_items',datatype=None).to_metadata()
        #define arguments that behave as function outputs
        outputs = {}
        outputs['output_item'] = UIFunctionOutSingle(name = 'output_item',datatype=bool).to_metadata()
                
        return (inputs,outputs)
        
    
class BaseMetadataProvider(BasePreload):
    """
    Metadata providers do not transform data. They merely add metadata to the entity type
    to make it available to other functions in the pipeline.
    """
    
    def __init__(self, dummy_items, output_item = 'is_parameters_set', **kwargs):
        super().__init__(dummy_items = dummy_items, output_item= output_item)
        self._metadata_params = kwargs
        
    def execute(self,df,start_ts=None,end_ts=None,entities=None):
        '''
        A metadata provider does not do anything except set _metadata_params
        _metadata_params are automatically copied to the _entity_type when the
        _entity_type is set using set_entity_type
        '''
        return True
    

class BaseEstimatorFunction(BaseTransformer):
    '''
    Base class for functions that train, evaluate and predict using sklearn 
    compatible estimators.
    '''
    shelf_life_days = None
    # Train automatically
    auto_train = True
    experiments_per_execution = 1
    parameter_tuning_iterations = 3
    #cross_validation
    cv = None #(default)
    eval_metric = None
    # Test Train split
    test_size = 0.2
    # Model evaluation
    stop_auto_improve_at = 0.85
    acceptable_score_for_model_acceptance = 0
    greater_is_better = True
    version_model_writes = False
    def __init__(self, features, targets, predictions):
        self.features = features
        self.targets = targets
        #Name predictions based on targets if predictions is None
        if predictions is None:
            predictions = ['predicted_%s'%x for x in self.targets]
        self.predictions = predictions
        super().__init__()
        self._preprocessors = OrderedDict()
        self.estimators = OrderedDict()
        
    def add_preprocessor(self,stage):
        '''
        Add a pre-processor stage
        '''
        self._preprocessors[stage.name] = stage
        
    def add_training_expression(self,name,expression):
        '''
        Add a new pre-processor stage using an expression
        '''
        stage = PipelineExpression(name=name,expression=expression,
                                   entity_type = self.get_entity_type())
        self.add_preprocessor(stage)
        
    def get_models_for_training(self, db, df, bucket=None):
        '''
        Get a list of models that require training
        '''
        if bucket is None:
            bucket = self.get_bucket_name()
        models = []
        unprocessed_targets = []
        unprocessed_targets.extend(self.targets)
        for i,target in enumerate(self.targets):
            logger.debug('processing target %s' %target)
            features = self.make_feature_list(features=self.features,
                                              df = df,
                                              unprocessed_targets = unprocessed_targets)
            model_name = self.get_model_name(target)
            #retrieve existing model
            model = db.cos_load(filename= model_name,
                                bucket=bucket,
                                binary=True)
            if self.decide_training_required(model):
                if model is None:
                    model = Model(name = model_name,
                                  estimator = None,
                                  estimator_name = None ,
                                  params = None,
                                  features = features,
                                  target = target,
                                  eval_metric_name = self.eval_metric.__name__,
                                  eval_metric_train = None,
                                  shelf_life_days = None
                                  )
                models.append(model)
            unprocessed_targets.append(target)
        return(models)
        
    def get_model_info(self, db):
        '''
        Display model metdata
        '''
        
    def get_models_for_predict(self, db, bucket=None):
        '''
        Get a list of models
        '''
        if bucket is None:
            bucket = self.get_bucket_name()
        models = []
        for i,target in enumerate(self.targets):
            model_name = self.get_model_name(target)
            #retrieve existing model
            model = db.cos_load(filename= model_name,
                                bucket=bucket,
                                binary=True)
            models.append(model)
        return(models)        
        
    def decide_training_required(self,model):
        if self.auto_train:
            if model is None:
                msg = 'Training required because there is no existing model'
                logger.debug(msg)
                return True
            elif model.expiry_date is not None and model.expiry_date <= dt.datetime.utcnow():
                msg = 'Training required model expired on %s' %model.expiry_date
                logger.debug(msg)                
                return True
            elif self.greater_is_better and model.eval_metric_test < self.stop_auto_improve_at:
                msg = 'Training required because eval metric of %s is lower than threshold %s ' %(model.eval_metric_test,self.stop_auto_improve_at)
                logger.debug(msg)                                
                return True
            elif not self.greater_is_better and model.eval_metric_test > self.stop_auto_improve_at:
                msg = 'Training required because eval metric of %s is higher than threshold %s ' %(model.eval_metric_test,self.stop_auto_improve_at)
                logger.debug(msg)                                
                return True            
        else:
            return False
        
    def delete_models(self,model_names=None):
        '''
        Delete models stored in COS for this estimator
        '''
        if model_names is None:
            model_names = []
            for target in self.targets:
               model_names.append(self.get_model_name(target))
        for m in model_names:
            self._entity_type.db.cos_delete(m, bucket= self.get_bucket_name())
        
    def execute(self, df):
        df  = df.copy()
        db = self._entity_type.db
        bucket = self.get_bucket_name()
        # transform incoming data using any preprocessors
        # include whatever preprocessing stages are required by implementing a set_preprocessors method
        required_models = self.get_models_for_training(db=db,df=df,bucket=bucket)
        if len(required_models) > 0:   
            df = self.execute_preprocessing(df)
            df_train,df_test = self.execute_train_test_split(df)
        #training
        for model in required_models:
            msg = 'Prepare to train model %s' %model
            logger.info(msg) 
            best_model = self.find_best_model(df_train=df_train,
                                             df_test=df_test,
                                             target = model.target,
                                             features = model.features,
                                             existing_model=model)
            msg = 'Trained model: %s' %best_model
            logger.debug(msg)
            best_model.test(df_test)                
            self.evaluate_and_write_model(new_model = best_model,
                                          current_model = model,
                                          db = db,
                                          bucket=bucket)
            msg = 'Finished training model %s' %model.name
            logger.info(msg)             
        #predictions   
        required_models = self.get_models_for_predict(db=db,bucket=bucket)
        for i,model in enumerate(required_models):        
            if model is not None:        
                df[self.predictions[i]] = model.predict(df)
                self.log_df_info(df,'After adding predictions for target %s' %model.target)
            else:
                df[self.predictions[i]] = None
                logger.debug('No suitable model found. Created null predictions')            
        return df
    
    def execute_preprocessing(self, df):
        '''
        Execute training specific pre-processing transformation stages
        '''
        self.set_preprocessors()
        preprocessors = list(self._preprocessors.values())
        pl = CalcPipeline(stages = preprocessors , entity_type = self._entity_type)
        df = pl.execute(df)
        msg = 'Completed preprocessing'
        logger.debug(msg)
        return df
        
    def execute_train_test_split(self,df):
        '''
        Split dataframe into test and training sets
        '''
        df_train, df_test = train_test_split(df,test_size=self.test_size)
        self.log_df_info(df_train,msg='training set',include_data=False)
        self.log_df_info(df_test,msg='test set',include_data=False)        
        return (df_train,df_test)        
    
    def find_best_model(self,df_train, df_test, target, features, existing_model):
        metric_name = self.eval_metric.__name__
        estimators = self.make_estimators(names=None, count = self.experiments_per_execution)
        
        print()
        
        if existing_model is None:
            trained_models = []
            best_test_metric = None
            best_model = None
        else:
            trained_models = [existing_model]
            best_test_metric = existing_model.eval_metric_test
            best_model = existing_model
        for (name,estimator,params) in estimators:
            estimator = self.fit_with_search_cv(estimator = estimator,
                                                params = params,
                                                df_train = df_train,
                                                target = target,
                                                features = features)
            eval_metric_train = estimator.score(df_train[features],df_train[target])
            msg = 'Trained estimator %s with an %s score of %s' %(self.__class__.__name__, metric_name, eval_metric_train)
            logger.debug(msg)
            model = Model(name = self.get_model_name(target_name = target),
                          target = target,
                          features = features,
                          params = estimator.best_params_,
                          eval_metric_name = metric_name,
                          eval_metric_train = eval_metric_train,
                          estimator = estimator,
                          estimator_name = name,
                          shelf_life_days = self.shelf_life_days)
            eval_metric_test = model.score(df_test)
            trained_models.append(model)
            if best_test_metric is None:
                best_model = model
                best_test_metric = eval_metric_test
                msg = 'No prior model, first created is best'
                logger.debug(msg)
            elif self.greater_is_better and eval_metric_test > best_test_metric:
                msg = 'Higher than previous best of %s. New metric is %s' %(best_test_metric,eval_metric_test)
                best_model = model
                best_test_metric = eval_metric_test        
                logger.debug(msg)
            elif not self.greater_is_better and eval_metric_test < best_test_metric:
                msg = 'Lower than previous best of %s. New metric is %s' %(best_test_metric,eval_metric_test)
                best_model = model
                best_test_metric = eval_metric_test
                logger.debug(msg)
        
        return best_model
    
    def evaluate_and_write_model(self,new_model,current_model,db,bucket):
        '''
        Decide whether new model is an improvement over current model and write new model
        '''
        write_model = False
        if current_model.trained_date != new_model.trained_date:
            if self.greater_is_better and new_model.eval_metric_test > self.acceptable_score_for_model_acceptance:
                write_model = True
            elif not self.greater_is_better and new_model.eval_metric_test < self.acceptable_score_for_model_acceptance:
                write_model = True
            else:
                msg = 'Training process did not create a model that passed the acceptance critera. Model evaluaton result was %s' %new_model.eval_metric_test
                logger.debug(msg)
        if write_model:
            if self.version_model_writes:
                version_name = '%s.version.%s' %(current_model.name, current_model.trained_date)
                db.cos_save(persisted_object=new_model, filename=version_name, bucket=bucket, binary=True)
                msg = 'wrote current model as version %s' %version_name
                logger.debug(msg)
            db.cos_save(persisted_object=new_model, filename=new_model.name, bucket=bucket, binary=True)
            msg =' wrote new model %s '%new_model.name
            logger.debug(msg)
            
        return write_model    
                
                
    def fit_with_search_cv(self, estimator, params, df_train, target, features):
        
        scorer = self.make_scorer()        
        search = RandomizedSearchCV(estimator = estimator,
                                    param_distributions = params,
                                    n_iter= self.parameter_tuning_iterations,
                                    scoring=scorer, refit=True, 
                                    cv= self.cv, return_train_score = False)
        estimator = search.fit(X=df_train[features], y = df_train[target])
        msg = 'Used randomize search cross validation to find best hyper parameters for estimator %s' %estimator.__class__.__name__
        logger.debug(msg)
        
        return estimator

    def set_estimators(self):
        '''
        Set the list of candidate estimators and associated parameters
        '''
        # populate the estimators dict with a list of tuples containing instance of an estimator and parameters for estimator
        raise NotImplementedError('You must implement a set estimator method')
        
    def set_preprocessors(self):
        '''
        Add the preprocessing stages that will transform data prior to training, evaluation or making prediction
        '''
        #self.add_preprocessor(ClassName(args))

    def get_model_name(self, target_name, suffix=None):
        return self.generate_model_name(target_name=target_name, suffix=suffix)
    
    def make_estimators(self, names = None, count = None):
        '''
        Make a list of candidate estimators based on available estimator classes
        '''
        self.set_estimators()
        if names is None:
            estimators = list(self.estimators.keys())
            if len(estimators) == 0:
                msg = 'No estimators defined. Implement the set_estimators method to define estimators'
                raise ValueError(msg)
            if count is not None:
                names = list(np.random.choice(estimators,count))
            else:
                names = estimators

        msg = 'Selected estimators %s' %names
        logger.debug(msg)
        
        out = []
        for e in names:
            (e_cls, parameters) = self.estimators[e]
            out.append((e,e_cls(),parameters))
            
        return out
            
    def make_scorer(self):
        '''
        Make a scorer
        '''                
        return metrics.make_scorer(self.eval_metric,greater_is_better = self.greater_is_better)
    
    def make_feature_list(self,df,features,unprocessed_targets):
        '''
        Simple feature selector. Includes all candidate features that to not
        involve targets that have not yet been processed. Use a custom implementation
        of this method to do more advanced feature selection.
        '''
        features = [x for x in features if x not in unprocessed_targets]
        return features
    
    
class BaseRegressor(BaseEstimatorFunction):
    '''
    Base class for building regression models
    '''
    eval_metric = staticmethod(metrics.r2_score)
    
    def set_estimators(self):
        #gradient_boosted
        params = {'n_estimators': [100,250,500,1000],
                   'max_depth': [2,4,10], 
                   'min_samples_split': [2,5,9],
                   'learning_rate': [0.01,0.02,0.05],
                   'loss': ['ls']}
        self.estimators['gradient_boosted_regressor'] = (ensemble.GradientBoostingRegressor,params)
        #sgd
        params = {'max_iter': [250,1000,5000,10000],
                  'tol' : [0.001, 0.002, 0.005] }
        self.estimators['sgd_regressor'] = (linear_model.SGDRegressor,params)   
        

class BaseClassifier(BaseEstimatorFunction):
    '''
    Base class for building classification models
    '''
    eval_metric = staticmethod(metrics.f1_score)
    
    def set_estimators(self):

        params = {'n_estimators': [10,50,100,500,1000],
                   'max_depth': [2,4,10], 
                   'min_samples_split': [2,5,9],
                   'max_features': [0.2,0.5,1],
                   'oob_score' : [True,False],
                   'min_samples_leaf' : [15,50,75,10]}
        self.estimators['random_forest'] = (ensemble.RandomForestClassifier,params)
        #mlp
        params = { 'hidden_layer_sizes' : [50,100,200,500]
                }
        self.estimators['mlp'] = (neural_network.MLPClassifier, params)        
    
    

        

    



