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
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
import ibm_db
import ibm_db_dbi
from sqlalchemy.types import String,SmallInteger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm.session import sessionmaker
from inspect import getargspec
from collections import OrderedDict
from .util import cosLoad, cosSave
from .db import Database

logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class BaseFunction(object):
    """
    Base class for AS functions. Do not inherit directly from this class. Inherit from BaseTransformer or BaseAggregator
    """
    name = None # name of function
    description =  None # description of function shows as help text
    tags = None #list of stings to tag function with
    optionalItems = None #list: list of optional parameters
    inputs = None #list: list of explicit input parameters
    outputs = None #list: list of explicit output parameters
    constants = None #list: list of explicit constant parameters
    # item level metadata
    itemDescriptions = None #dict: items descriptions show as help text
    itemLearnMore = None #dict: item learn more test 
    itemValues = None #dict: item values are used in pick lists
    itemJsonSchema = None #dict: schema is used to validate arrays and json type constants
    itemArraySource = None #dict: output arrays are derived from input arrays. 
    itemMaxCardinality = None # dict: Maximum number of members in an array
    
    execute_by = None #if function should be executed separately for each entity or some other key, capture this key here
    test_rows = 100 #rows of data to use when testing function
    array_source = None #str: the input parmeter name that contains a list of items that will correspond with array outputs
    # cos connection
    cos_credentials = None #dict external cos instance
    bucket = None #str
    # database connection
    db_credentials = None #dict
    db = None
    # custom output tables
    version_db_writes = False #write a new version timestamp to custom output table with each execution
    out_table_prefix = None
    out_table_if_exists = 'append'
    out_table_name = None 
    write_chunk_size = 1000
    # registration metadata
    url = PACKAGE_URL
    category = None
    incremental_update = True
    # test that object was initialized from BaseFunction
    base_initialized = True
    # lookups
    # a resource calendar is use to identify other resources (e.g. people, organizations) associated with the device
    # these associations may change over time.
    resource_calendar = None
    _entity_resource_dict = None
    # predefined column names
    _entity_id = 'deviceid'
    _timestamp = 'evt_timestamp'
    # when entity id or timestamp are present in a dataframe index they have different names
    _df_index_entity_id = 'id'
    _df_index_timestamp = 'timestamp'
    # when generating dimension data provide a domain of values keyed on the dimenion name
    domain = None
    _default_dimension_domain = None
    
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
            
        if self.optionalItems is None:
            self.optionalItems = []

        if self.out_table_prefix is None:
            self.out_table_prefix = '' 
            
        if self.execute_by is None:
            self.execute_by = []   

        if self.tags is None:
            self.tags = []  

        if self.resource_calendar is None:
            self.resource_calendar= {}
            
        if self._entity_resource_dict is None:
            self._entity_resource_dict= {}            

        if self.domain is None:
            self.domain = {}   

        if self._default_dimension_domain is None:
            self._default_dimension_domain= ['AA','BA','ZA','CT','BV'] 
            

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
                    
    def add_resource_calendar(self, resource_name, table_name):
        
        self.resource_calendar[resource_name] = table_name
                    
    
    def register(self,credentials,df,
                 name=None,url=None,constants = None, module=None,
                 description=None,incremental_update=None, 
                 outputs = None, show_metadata = False,
                 metadata_only = False):
        
        if not self.base_initialized:
            raise RuntimeError('Cannot register function. Did not call super().__init__() in constructor so defaults have not be set correctly.')
        
        try:
            as_api_host = credentials['as_api_host']
        except KeyError:
            raise ValueError('No as_api_host provided in credentials. Unable to register function')
            
        if self.category is None:
            raise AttributeError('Class has no categoty. Class should inherit from BaseTransformer or BaseAggregator to obtain an appropriate category')
            
        if name is None:
            name = self.name
            
        if module is None:
            module = self.__class__.__module__
            
        if description is None:
            description = self.description
            
        if url is None:
            url = self.url
            
        if incremental_update is None:
            incremental_update = self.incremental_update
            
        (metadata_input,metadata_output) = self._getMetadata(df=df,outputs=outputs,constants = constants, inputs = self.inputs)

        module_and_target = '%s.%s' %(module,self.__class__.__name__)

        exec_str = 'from %s import %s as import_test' %(module,self.__class__.__name__)
        try:
            exec (exec_str)
        except ImportError:
            raise ValueError('Unable to register function as local import failed. Make sure it is installed locally and importable. %s ' %exec_str)
        
        exec_str_ver = 'import %s as import_test' %(module.split('.', 1)[0])
        exec(exec_str_ver)
        version = eval('import_test.__version__')
        msg = 'Test import succeeded for function version %s using %s' %(version,exec_str)
        logger.debug(msg)
        
        payload = {
            'name': name,
            'description': description,
            'category': self.category,
            'tags': self.tags,
            'moduleAndTargetName': module_and_target,
            'url': url,
            'input': list(metadata_input.values()),
            'output':list(metadata_output.values()),
            'incremental_update': incremental_update if self.category == 'AGGREGATOR' else None
        }
        
    
        http = urllib3.PoolManager()
        encoded_payload = json.dumps(payload).encode('utf-8')
        if show_metadata or metadata_only:
            print(encoded_payload)
        
        headers = {
            'Content-Type': "application/json",
            'X-api-key' : credentials['as_api_key'],
            'X-api-token' : credentials['as_api_token'],
            'Cache-Control': "no-cache",
        }
        
        if not metadata_only:
            url = 'http://%s/api/catalog/v1/%s/function/%s' %(as_api_host,credentials['tennant_id'],name)
            r = http.request("DELETE", url, body = encoded_payload, headers=headers)
            msg = 'Function registration deletion status: %s' %(r.data.decode('utf-8'))
            logger.info(msg)
            r = http.request("PUT", url, body = encoded_payload, headers=headers)     
            msg = 'Function registration status: %s' %(r.data.decode('utf-8'))
            logger.info(msg)
            return r.data.decode('utf-8')
        else:
            return encoded_payload
        
    def unregister(self,credentials,name = None):
        '''
        Unregister function
        '''
        if name is None:
            name = self.name
            
        try:
            as_api_host = credentials['as_api_host']
        except KeyError:
            raise ValueError('No as_api_host provided in credentials')
         
        http = urllib3.PoolManager()
        headers = {
            'Content-Type': "application/json",
            'X-api-key' : credentials['as_api_key'],
            'X-api-token' : credentials['as_api_token'],
            'Cache-Control': "no-cache",
        }

        url = 'http://%s/api/catalog/v1/%s/function/%s' %(as_api_host,credentials['tennant_id'],name)
        r = http.request("DELETE", url, body = {}, headers=headers)
        msg = 'Function registration deletion status: %s' %(r.data.decode('utf-8'))
        logger.info(msg)        
        
    
    def acquire_db_connection(self, start_session = False, credentials = None):
        '''
        Use environment variable or explicit connection metadata to connection to create a connection and session maker
        
        Returns
        -------
        tuple containing connection and session objects if start_session is True
        connection object is start_session is False
        
        '''
        
        if credentials is None:
            credentials = self.db_credentials
        
        #If explicit credentials provided these allow connection to a db other than the ICS one.
        if not credentials is None:
            connection_string = 'db2+ibm_db://%s:%s@%s:%s/%s;' %(credentials['username'],credentials['password'],credentials['host'],credentials['port'],credentials['database'])
            connection =  create_engine(connection_string)
        else:
            # look for environment vaiable for the ICS DB2
            try:
               connection_string = os.environ.get('DB_CONNECTION_STRING')
            except KeyError:
                raise ValueError('Function requires a database connection but one could not be established. Pass appropriate db_credentials or ensure that the DB_CONNECTION_STRING is set')
            else:
               ibm_connection = ibm_db.connect(connection_string, '', '')
               connection = ibm_db_dbi.Connection(ibm_connection)
            
        Session = sessionmaker(bind=connection)
        
        if start_session:
            session = Session()
            return (connection,session)
        else:
            return connection
            
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
        Dataframes that contain timeseries data are expected to be indexed on an id string and timestamp.
        When you can't avoid changing the index during a transform, use conform_index() to get a dataframe
        back into the expected shape for further processing in a pipeline.
        '''
        
        self.log_df_info(df,'incoming dataframe for conform index')
        if not df.index.names == [self._df_index_entity_id,self._df_index_timestamp]:            
            if entity_id_col is None:
                entity_id_col = self._entity_id
            if timestamp_col is None:
                timestamp_col = self._timestamp
            try:
                df[self._df_index_entity_id] = df[entity_id_col].astype(str)
                df[self._df_index_timestamp] = pd.to_datetime(df[timestamp_col])
            except KeyError:
                try:
                    df = df.reset_index()
                    df[self._df_index_entity_id] = df[entity_id_col].astype(str)
                    df[self._df_index_timestamp] = pd.to_datetime(df[timestamp_col])
                    df = df.set_index([self._df_index_entity_id,self._df_index_timestamp])
                except:
                    msg = '''
                    There is an error in the function code.
                    A dataframe used in the function is being converted into standard form,
                    but does not have a deviceid and timestamp column 
                    '''
                    self.log_df_info(df,'before raising error ')
                    raise KeyError(msg)
                else:
                    msg = 'Dataframe was not indexed correctly. Recovered id and timestamp from existing index and built a new one.'
                    logger.debug(msg)
            else:
                msg = 'Dataframe was not indexed correctly. Built new index on id and timestamp'
                df = df.set_index([self._df_index_entity_id,self._df_index_timestamp])
                logger.debug(msg)
                
        df[self._timestamp] = df.index.get_level_values(self._df_index_timestamp)
        df[self._entity_id] = df.index.get_level_values(self._df_index_entity_id)
        self.log_df_info(df,'after  conform index')
        
        return df
            
    
    def _getJsonDataType(self,datatype):
         
         if datatype == 'LITERAL':
             result = 'string'
         else:
             result = datatype.lower()
         return result
     
        
    def _get_data_scope(self,df):
        '''
        Return the start, end and set of entity ids contained in a dataframe as a tuple
        '''
        
        start_ts = df[self._timestamp].min()
        end_ts = df[self._timestamp].max()
        entities = list(pd.unique(df[self._entity_id]))
        
        return (start_ts,end_ts,entities)
     
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
            msg = 'Argument %s is has explicit json schema defined for it %s' %a, self.itemJsonSchema[arg]
            logger.debug(msg)
        return column_metadata
    
    def get_domain(self,dimension):
        
        try:
            domain = self.domain[dimension]
        except KeyError:
            domain = self._default_dimension_domain
            
        return domain
    
    def get_item_values(self,arg):
        """
        
        """
        
        msg = 'No code implemented to gather available values for argument %s' %arg
        
        raise NotImplementedError (msg)
        
    def _getMetadata(self, df = None, inputs = None, outputs = None, constants = None):
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
          
        if inputs is None:
            inputs = self.inputs
        if outputs is None:
            outputs = self.outputs        
        if constants is None:
            constants = self.constants
        
        #run the function to produce a new dataframe that contains the function outputs
        if not df is None:
            tf = df.head(self.test_rows).copy()
            tf = self.execute(tf)
            if not isinstance(tf,pd.DataFrame):
                raise TypeError('The execute method of a custom function must return a pandas DataFrame object not %s' %tf)
            self.validate_df(input_df = df,output_df = tf)
            test_outputs = self._inferOutputs(before_df=df,after_df=tf)
            if len(test_outputs) ==0:
                raise ValueError('Could not locate output columns in the test dataframe. Check the execute method of the function to ensure that it returns a dataframe with output columns that are named differently from the input columns')
        else:
            tf = None
            raise NotImplementedError('Must supply a test dataframe for function registration. Explict metadata definition not suported')
        
        metadata_inputs = {}
        metadata_outputs = {}
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
                column_metadata['dataType'] = datatype
                auto_desc =  'Provide a new data item name for the function output'
                is_added = True
                msg = 'Argument %s was explicitlty defined as output with datatype %s' %(a,datatype)
                logger.debug(msg)
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
            # adjust metadata for json schema if required
            if is_output:
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
            if is_constant:
                column_metadata['type'] = 'CONSTANT'
                try:
                    values = self.itemValues[a]
                except KeyError:            
                    try:
                        values = self.get_item_values(a)
                    except KeyError:
                        msg = 'Constant argument %s is has no explicit values defined for it and no values available from the get_item_values() method' %a
                        logger.debug(msg)
                        if is_array:
                            msg = 'Array input %s has no predefined values. It will appear in the UI as a type-in field that accepts a comma separated list of values. To set values implement the set_values() method' %a
                            warnings.warn(msg)
                else:
                    column_metadata['values'] = values
                    msg = 'Constant argument %s is has explicit values defined for it %s' %(a, values)
                    logger.debug(msg)                                           
                
        #array outputs are special. They inherit their datatype from an input array
        #that could be explicity defined, or use last array_input 
        for (array,length) in array_outputs:
            try:
                array_source =  self.itemArraySource[array]
                msg = 'Cardinality and datatype of array output %s wer explicly set to be driven from %s' %(array,array_source)
                logger.debug(msg)
            except KeyError:
                array_source = self._infer_array_source(candidate_inputs= array_inputs,
                                                     output_length = length)
                
            if array_source is None:
                raise ValueError('No candidate input array found to drive output array %s with length %s . Make sure input array and output array have the same length or explicity define the item_source_array. ' %(array,length))
            else:
                #if the output array is driven by an array of items infer data types from items
                if metadata_inputs[array_source]['type'] == 'DATA_ITEM':    
                    metadata_outputs[array]['dataTypeFrom']=array_source
                else:
                    metadata_outputs[array]['dataTypeFrom']=None
                metadata_outputs[array]['cardinalityFrom']=array_source
                del metadata_outputs[array]['dataType']
                msg = 'Array argument %s is driven by %s so the cardinality and datatype are set from the source' %(a,array_source)
                logger.debug(msg)
    
        return (metadata_inputs,metadata_outputs)
    
    def get_params(self):
        '''
        Get metadata parameters
        '''
        params = {
                '_timestamp' : self._timestamp,
                'db' : self.db
                }
        return params
    
    def get_resource_calendar_data(self,table_name,start_ts, end_ts, entities):
        '''
        Retrieve a resource calendar as a dataframe
        '''
        
        (query,table) = self.db.query(table_name)
        if not start_ts is None:
            query = query.filter(table.c.end_date >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c.start_date < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        df = pd.read_sql(query.statement, con = self.db.connection,  parse_dates=[self._start_date,self._end_date])
        
        return df
        
    
    def _get_resource_assignment(self,start_ts,end_ts,entities):
        '''
        Build a dict keyed on resource type and entity id for each type of resource
        '''
        x = {}
        if self.resource_calendar is None:
            return None
        else:
            for resource, table in list(self.resource_calendar.items()):
                df = self.get_resource_calendar_data(table_name=table, start_ts=start_ts, end_ts = end_ts, entities = entities)
                #build the resource assignment dict
                x[resource] = self._partition_df_by_id(df)
            return x
            
    def _partition_df_by_id(self,df):
        '''
        Partition dataframe into a dictionary keyed by _entity_id
        '''
        d = {x: table for x, table in df.groupby(self._entity_id)}
        return d
            
    
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
             
            if df is None:
                #value is a constant
                if isinstance(value,str):
                    datatype = 'LITERAL'              
                elif isinstance(value,numbers.Number):
                    datatype = 'NUMBER'              
                elif isinstance(value,bool):
                    datatype = 'BOOLEAN'              
                elif isinstance(value, dt.datetime):
                    datatype = 'TIMESTAMP'              
                elif is_dict_like(value):
                    datatype = 'JSON'
                else: 
                    raise TypeError('Cannot infer type of argument value %s for parm %s. Supply a string, number, boolean, datetime, dict or list containing any of these types.' %(value,parm))
            else:      
                append_msg = 'by looking at items in test dataframe'
                if is_string_dtype(df[value]):
                    datatype = 'LITERAL'
                elif is_bool_dtype(df[value]):
                    datatype = 'BOOLEAN' 
                elif is_numeric_dtype(df[value]):
                    datatype = 'NUMBER'
                elif is_datetime64_any_dtype(df[value]):
                    datatype = 'TIMESTAMP'
                else:
                    raise TypeError('Cannot infer type of argument value %s. Data items used as inputs must be strings, numbers, booleans, datetimes or list containing any of these types.' %parm)
            
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
            
            
        return datatype
    
    
    def _calc(self,df):
        """
        If the function should be executed separately for each entity, describe the function logic in the _calc method
        """
        raise NotImplementedError('Class %s is not defined correctly. It should override the _calc() method of the base class.' %self.__class__.__name__) 

    def _standard_item_descriptions(self):
        
        itemDescriptions = {}
        
        itemDescriptions['bucket']= 'Name of the COS bucket used for storage of data or serialized objects'
        itemDescriptions['cos_credentials']= 'External COS credentials'
        itemDescriptions['db_credentials']= 'Db2 credentials'
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
        self.log_df_info(df,'Before remove of cols %s' %cols)
        cols = [x for x in list(df.columns) if x not in cols]
        df = df[cols]
        self.log_df_info(df,'After removal of cols %s' %cols)
        
        return df
        
    
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
    
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self
                
    
    def write_frame(self,df,
                    db_credentials = None,
                    table_name=None, 
                    version_db_writes = None,
                    if_exists = None):
        '''
        Write a dataframe to a database table
        
        Parameters
        ---------------------
        db_credentials: dict (optional)
            db2 database credentials. If not provided, will look for environment variable
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
    
        if self.db is None:
            self.db = Database(credentials = db_credentials)
        status = self.db.write_frame(df, table_name = table_name, 
                                     version_db_writes = version_db_writes,
                                     if_exists  = if_exists,
                                     chunksize = self.write_chunk_size)
        
        return status

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
    
    def log_df_info(self,df,msg):
        '''
        Log a debugging entry showing dataframe structure
        '''
        msg = msg + ' columns %s, index %s' % (df.columns, df.index.names)
        logger.debug(msg)
        logger.debug(df.head())
    
    def get_test_data(self):
        """
        Output a dataframe for testing function
        """
        
        data = {
                self._entity_id : ['D1','D1','D1','D1','D1','D2','D2','D2','D2','D2'],
                self._timestamp : [
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
    
    def generate_entity_data(self,entity_name, entities, days, seconds = 0, freq = '1min', credentials = None, write=True):
        '''
        Generate random time series and dimension data for entities
        
        Parameters
        ----------
        entity_name : str
            Name of entity to generate data for
        entities: list
            List of entity ids to genenerate data for
        days: number
            Number of days worth of data to generate (back from system date)
        seconds: number
            Number of seconds of wotht of data to generate (back from system date)
        freq: str
            Pandas frequency string - interval of time between subsequent rows of data
        credentials: dict
            credentials dictionary
        write: bool
            write generated data back to table with same name as entity
        
        '''
        if self.db is None:
            self.db = Database(credentials=credentials)
        table = self.db.get_table(entity_name)
        metrics = []
        dims = []
        dates = []
        others = []
        for c in self.db.get_column_names(table):
            if not c in ['deviceid','devicetype','format','updated_utc']:
                data_type = str(table.c[c].type)                
                if data_type == 'FLOAT':
                    metrics.append(c)
                elif data_type[:7] == 'VARCHAR':
                    dims.append(c)
                elif data_type == 'DATETIME':
                    dates.append(c)
                else:
                    others.append(c)
        ts = TimeSeriesGenerator(metrics=metrics,ids=entities,days=days,seconds=seconds,freq=freq, dims = dims, dates = dates)
        df = ts.execute()
        if write:
            for o in others:
                if o not in df.columns:
                    df[o] = None
            df['logicalinterface_id'] = ''
            df['devicetype'] = entity_name
            df['format'] = ''
            df['updated_utc'] = None
            self.db.write_frame(table_name = entity_name, df = df)
        
        return df
    
    
    def validate_df(self,input_df, output_df):
        
        validation_result = {}
        validation_types = {}
        for (df,df_name) in [(input_df,'input'),(output_df,'output')]:
            validation_types[df_name] = {}
            for c in list(df.columns):
                try:
                  validation_types[df_name][df[c].dtype].add(c)
                except KeyError:
                  validation_types[df_name][df[c].dtype] = {c}
            
            validation_result[df_name] = {}
            validation_result[df_name]['row_count'] = len(df.index)
            validation_result[df_name]['columns'] = set(df.columns)
            is_str_0 = False
            try:
                if is_string_dtype(df.index.get_level_values(self._df_index_entity_id)):
                    is_str_0 = True
            except KeyError:
                pass
            is_dt_1 = False                
            try:
                if is_datetime64_any_dtype(df.index.get_level_values(self._df_index_timestamp)):
                    is_dt_1 = True
            except KeyError:
                pass
            validation_result[df_name]['is_index_0_str'] = is_str_0
            validation_result[df_name]['is_index_1_datetime'] = is_dt_1
            
        if validation_result['input']['row_count'] == 0:
            logger.warning('Input dataframe has no rows of data')
        elif validation_result['output']['row_count'] == 0:
            logger.warning('Output dataframe has no rows of data')
        
        if not validation_result['input']['is_index_0_str']:
            logger.warning('Input dataframe index does not conform. First part not a string called %s' %self._df_index_entity_id)
        if not validation_result['output']['is_index_0_str']:
            logger.warning('Output dataframe index does not conform. First part not a string called %s' %self._df_index_entity_id)

        if not validation_result['input']['is_index_1_datetime']:
            logger.warning('Input dataframe index does not conform. Second part not a string called %s' %self._df_index_timestamp)
        if not validation_result['output']['is_index_1_datetime']:
            logger.warning('Output dataframe index does not conform. Second part not a string called %s' %self._df_index_timestamp)
            
        for dtype,cols in list(validation_types['input'].items()):
            missing = cols - validation_types['output'][dtype]
            if len(missing) != 0 :
                msg = 'Output dataframe is missing columns %s of type %s. Either the type has changed or column was dropped' %(missing,dtype)
                logger.warning(msg)
            
        return(validation_result,validation_types)
        
    
class BaseTransformer(BaseFunction):
    """
    Base class for AS Transform Functions. Inherit from this class when building a custom function that adds new columns to a dataframe.

    """
    category =  'TRANSFORMER'
    
    def __init__(self):
        super().__init__()


class BaseLoader(BaseTransformer):
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
    
    def __init__(self, input_items, output_items=None):
        self.input_items = input_items
        if output_items is None:
            output_items = [x for x in self.input_items]
        self.output_items = output_items
        super().__init__()
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
        new_df = self.get_data(start_ts=None,end_ts=None,entities=None)
        self.log_df_info(df,'source datafram before merge')
        self.log_df_info(new_df,'additional data source to be merged')        
        if self.merge_method == 'outer':        
            new_df = self._remove_cols_from_df(new_df,list(df.columns))
            df = df.join(new_df,how='outer',sort=True,on=[self._df_index_entity_id,self._df_index_timestamp])
        elif self.merge_method == 'nearest': 
            new_df = self._remove_cols_from_df(new_df,list(df.columns))
            try:
                df = pd.merge_asof(left=df,right=new_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
            except ValueError:
                new_df = new_df.sort_values([self._timestamp,self._entity_id])
                try:
                    df = pd.merge_asof(left=df,right=new_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
                except ValueError:
                    df = df.sort_values([self._timestamp,self._entity_id])
                    df = pd.merge_asof(left=df,right=new_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
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
        
        return df


class BaseEvent(BaseTransformer):
    """
    Base class for AS Functions that product events or alerts. 

    """
    
    def __init__(self):
        super().__init__()
        self.tags.append('EVENT')

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
    
    def __init__(self,
                 lookup_table_name,
                 lookup_items = None,
                 sql=None,
                 lookup_keys = None,
                 parse_dates=None,
                 output_items=None):

        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')
            
        self.lookup_table_name = lookup_table_name
        if lookup_items is None:
            msg = 'You must provide a list of columns for the lookup_items argument'
            raise ValueError(msg)
        self.lookup_items = self.convertStrArgToList(lookup_items,argument = 'lookup_items')
        self.sql = sql
        super().__init__()
        #drive the output cardinality and data type from the items choosen for the lookup
        self.itemArraySource['output_items'] = 'lookup_items'
        self.lookup_keys = lookup_keys
        self.itemMaxCardinality['lookup_keys'] = len(self.lookup_keys)
        self.parse_dates = parse_dates 
        if output_items is None:
            #concatentate lookup name to output to make it unique
            output_items = ['%s_%s' %(self.lookup_table_name,x) for x in lookup_items]        
        self.output_items = self.convertStrArgToList(output_items,argument = 'output_items')
        
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
            try:
                df = pd.read_sql(self.sql, con = self.db.connection, index_col=lup_keys, parse_dates=date_cols)
            except :
                if not self.data is None:
                    df = pd.DataFrame(data=self.data)
                    df = df.set_index(keys=self.lookup_keys)
                    self.create_lookup_table(df=df, table_name = self.lookup_table_name)
                    df = pd.read_sql(self.sql, self.db.connection, index_col=self.lookup_keys, parse_dates=self.parse_dates)
                else:
                    msg = 'Unable to retrieve data from lookup table using %s. Check that database table exists and credentials are correct. Include data in your function definition to automatically create a lookup table.' %sql
                    raise(msg)
            
            df.columns = [x.lower() for x in list(df.columns)]            
            return(list(df.columns))
                        
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)
    
    def execute(self, df):
        '''
        Execute transformation function of DataFrame to return a DataFrame
        '''
        if self.db is None:
            self.db = Database(credentials = credentials)
        df_sql = pd.read_sql(self.sql, self.db.connection, index_col=self.lookup_keys, parse_dates=self.parse_dates)
        df_sql = df_sql[self.lookup_items]
                
        if len(self.output_items) > len(df_sql.columns):
            raise RuntimeError('length of names (%d) is larger than the length of query result (%d)' % (len(self.names), len(df_sql)))

        df = df.join(df_sql,on= self.lookup_keys, how='left')
        df = self.rename_cols(df,input_names = self.lookup_items,output_names=self.output_items)

        return df
    
    def create_lookup_table(self,df, table_name):
        
        self.write_frame(df=df,table_name = table_name, if_exists = 'replace')
        
class BaseDBActivityMerge(BaseLoader):
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
    activities_metadata = {}
    # merge in data from one or more custom sql statement
    activities_custom_query_metadata = {}
    # optionally align with a custom calendar
    custom_calendar = None
    # optionally align with one or more resource calendar
    remove_gaps = True
    # column name metadata
    # the start and end dates for activities are assumed to be designated by specific columns
    # the type of activity performed on or using an entity is designated by the 'activity' column
    _start_date = 'start_date'
    _end_date = 'end_date'
    _activity = 'activity'
    
    def __init__(self,input_activities,activity_duration=None):
    
        self.input_activities = input_activities
        if activity_duration is None:
            activity_duration = ['duration_%s' %x for x in self.input_activities]
        self.activity_duration = activity_duration
        super().__init__(input_items = input_activities , output_items = None)
        #for any function that requires database access, create a database object
        self.itemArraySource['activity_duration'] = 'input_activities'
        
    def get_data(self,
                    start_ts= None,
                    end_ts= None,
                    entities = None):
        
        if self.db is None:
            self.db = Database(credentials = credentials)
        
        dfs = []
        #build sql and executive it 
        for table_name,activities in list(self.activities_metadata.items()):
            for a in activities:
                af = self.read_activity_data(table_name=table_name,
                                   activity_code=a,
                                   start_ts = start_ts,
                                   end_ts = end_ts,
                                   entities = entities)
                af[self._activity] = a
                dfs.append(af)
        #execute sql provided explictly
        for activity, sql in list(self.activities_custom_query_metadata.items()):
                try:
                    af = pd.read_sql(sql, con = self.db.connection,  parse_dates=[self._start_date,self._end_date])
                except:
                    logger.warning('Function attempted to retrieve data for a merge operation using custom sql. There was a problem with this retrieval operation. Confirm that the sql is valid and contains column aliases for start_date,end_date and device_id')
                    logger.warning(sql)
                    raise 
                af[self._activity] = activity
                dfs.append(af)      
        
        adf = pd.concat(dfs,sort=False)
        #get shift changes
        self.add_dates = []
        self.custom_calendar_df = None
        if not self.custom_calendar is None:
            self.custom_calendar_df = self.custom_calendar.get_data(start_date= adf[self._start_date].min(), end_date = adf[self._end_date].max())
            add_dates = set(self.custom_calendar_df[self._start_date].tolist())
            add_dates |= set(self.custom_calendar_df[self._end_date].tolist())
            self.add_dates = list(add_dates)
        adf = self.conform_index(adf, timestamp_col = self._start_date)
        #get resource assignment changes
        self._entity_resource_dict = self._get_resource_assignment(start_ts= adf[self._start_date].min(), end_ts = adf[self._end_date].max(),entities=entities)
        #merge takes place separately by entity instance
        
        group_base = []
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
        if len(group_base)>0:
            adf = adf.groupby(group_base).apply(self._combine_activities)             
        else:
            raise ValueError('This function executes by entity. execute_by should be "id"')
            
        adf['duration'] = (adf[self._end_date] - adf[self._start_date]).dt.total_seconds() / 60
            
        pivot_start = PivotRowsToColumns(
                pivot_by_item = self._activity,
                pivot_values = self.input_activities,
                input_item='duration',
                null_value=None,
                output_items = self.activity_duration
                    )        
        adf = pivot_start.execute(adf)
        
        adf = self.conform_index(adf,timestamp_col = self._start_date)
    
        return adf
        
                    
    def _combine_activities(self,df):
        '''
        incoming dataframe has start date , end date and activity code.
        activities may overlap.
        output dataframe corrects overlapping activities.
        activities with later start dates take precidence over activies with earlier start dates when resolving.
        '''
        
        msg = 'Combining activities contained in df with cols %s and index %s' %(df.columns,df.index.names)
        logger.debug(msg)
        entity = df.index.get_level_values(self._df_index_entity_id).max()
        
        #create a continuous range
        early_date = pd.Timestamp.min
        late_date = pd.Timestamp.max
        
        df = df.sort_values([self._start_date])
        #create a new start date for each potential interruption of the continuous range
        dates = set([early_date,late_date])
        dates |= set((df[self._start_date].tolist()))
        dates |= set((df[self._end_date].tolist()))
        dates |= set(self.add_dates)
        #resource calendar changes are another potential interruption
        for resource,entity_data in list(self._entity_resource_dict.items()):
            dates |= set(entity_data[entity][self._start_date])
        dates = list(dates)
        dates.sort()
        #initialize series to track history of activities
        c = pd.Series(data='_gap_',index = dates)
        c.index = pd.to_datetime(c.index)
        c.name = self._activity
        c.index.name = self._start_date
        #use original data to update the new set of intervals in slices
        for index, row in df.iterrows():
            end_date = row[self._end_date] - dt.timedelta(seconds=1)
            c[row[self._start_date]:end_date] = row[self._activity]    
        df = c.to_frame().reset_index()
        #add custom calendar data
        if not self.custom_calendar_df is None:
            cols = [x for x in self.custom_calendar_df.columns if x != 'end_date']            
            df = pd.merge(left=df, right = self.custom_calendar_df[cols], how ='left', left_on = self._start_date, right_on = 'start_date')
            df['shift_id'] = df['shift_id'].fillna(method='ffill')
            df['shift_day'] = df['shift_day'].fillna(method='ffill')
            df[self._activity].fillna(method='ffill')
        #perform resource lookup
        for resource,entity_data in list(self._entity_resource_dict.items()):
            dfr = entity_data[entity]
            resource_data = dfr['resource_id']
            resource_data.index = dfr[self._start_date]
            resource_data.name = resource
            df = df.join(resource_data, how ='left', on = self._start_date)
            df[resource] = df[resource].fillna(method='ffill')
        #add end dates
        df[self._end_date] = df[self._start_date].shift(-1)
        df[self._end_date] = df[self._end_date] - dt.timedelta(seconds=1)
        
        #remove gaps
        if self.remove_gaps:
            df = df[df[self._activity]!='_gap_']
        
        return df          
            
                
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
        
        if self.db is None:
            self.db = Database(credentials = credentials)
        
        (query,table) = self.db.query(table_name)
        query = query.filter(table.c.activity == activity_code)
        if not start_ts is None:
            query = query.filter(table.c.end_date >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c.start_date < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        df = pd.read_sql(query.statement, con = self.db.connection,  parse_dates=[self._start_date,self._end_date])
        return df


class BaseResourceLookup(BaseTransformer):
    '''
    Lookup a resource assigment from a resource calendar
    '''
    _start_date = 'start_date'
    _end_date = 'end_date'
    merge_nearest_tolerance = None #pd.Timedelta('1D')
    
    def __init__ (self, table_name, output_item = None):
        
        self.table_name = table_name
        if output_item is None:
            output_item = self.table_name
        self.output_item = output_item
        super().__init__()
        
    def execute(self,df):
        
        (start_ts, end_ts, entities) = self._get_data_scope(df)
        resource_df = self.get_resource_calendar_data(table_name = self.table_name, start_ts = start_ts, end_ts=end_ts, entities=entities)
        resource_df = resource_df.rename(columns = {'resource_id':self.output_item,
                                          'start_date': self._timestamp})
        cols = [x for x in resource_df.columns if x not in ['end_date']]
        resource_df = resource_df[cols]
        self.log_df_info(df,'source dataframe before merge')
        self.log_df_info(resource_df,'resource data source to be merged')                
        try:
            df = pd.merge_asof(left=df,right=resource_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
        except ValueError:
            resource_df = resource_df.sort_values([self._timestamp,self._entity_id])
            try:
                df = pd.merge_asof(left=df,right=resource_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
            except ValueError:
                df = df.sort_values([self._timestamp,self._entity_id])
                df = pd.merge_asof(left=df,right=resource_df,by=self._entity_id,on=self._timestamp,tolerance=self.merge_nearest_tolerance)
        
        df = self.conform_index(df)        
        self.log_df_info(df,'post resource calendar merge') 
        return df
        

class AlertThreshold(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least on threshold.
    """
    
    optionalItems = ['lower_threshold','upper_threshold']     
    
    def __init__ (self,input_item, lower_threshold=None, upper_threshold=None,
                  output_alert_upper = 'output_alert_upper', output_alert_lower = 'output_alert_lower'):
        
        self.input_item = input_item
        self.lower_threshold = float(lower_threshold)
        self.upper_threshold = float(upper_threshold)
        self.output_alert_lower = output_alert_lower
        self.output_alert_upper = output_alert_upper
        super().__init__()
        
    def execute(self,df):
        
        df = df.copy()
        df[self.output_alert_upper] = False
        df[self.output_alert_lower] = False
        
        if not self.lower_threshold is None:
            df[self.output_alert_lower] = np.where(df[self.input_item]<=self.lower_threshold,True,False)
        if not self.upper_threshold is None:
            df[self.output_alert_upper] = np.where(df[self.input_item]>=self.upper_threshold,True,False)
            
        return df
    
class GenerateCerealFillerData(BaseLoader):
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
        
        ts = TimeSeriesGenerator(metrics = self.input_items,ids=self.ids,days=days,seconds = seconds)
        df = ts.execute()
        
        return df
    
    
class InputDataGenerator(BaseLoader):
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
    ids = [str(7300 + x) for x in list(range(5))]
    
    def __init__ (self, input_items, output_items=None):
        super().__init__(input_items = input_items, output_items = output_items)
        
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
            seconds = (dt.datetime.dt.datetime.utcnow() - start_ts).total_seconds()
        
        ts = TimeSeriesGenerator(metrics = self.input_items,ids=self.ids,days=days,seconds = seconds)
        df = ts.execute()
        return df
    
class TempPressureVolumeGenerator(InputDataGenerator):
    """
    Generate new data for Temperature, Pressure and Volume. Replace input data with this new data.
    """
    # The merge_method of any data source function governs how the new data retrieved will be combined with the pipeline
    merge_method = 'replace'
    # Parameters for data generator
    # Number of days worth of data to generate on initial execution
    days = 1
    # frequency of data load
    freq = '1min' 
    # ids of entities to generate. Change the value of the range() function to change the number of entities
    ids = [str(7300 + x) for x in list(range(5))]
    
    def __init__ (self, input_items=None, output_items=None):
        if input_items is None:
            input_items = self.generate_items
        super().__init__(input_items = input_items, output_items = output_items)
        
        
    def get_item_values(self,arg):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':
            return ['pressure','temperature','volume']
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)        
    

class ExecuteFunctionSingleOut(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage. Function returns a single output.
    """ 

    optionalItems = ['parameters','bucket']       
    
    def __init__(self,function_name,cos_credentials,input_items,output_item,parameters=None,bucket=None):
        
        # the function name may be passed as a function object of function name (string)
        # if astring is provided, it is assumed that the function object has already been serialized to COS
        # if a function onbject is supplied, it will be serialized to cos 
        
        self.bucket = bucket
        self.cos_credentials = cos_credentials
        self.input_items = input_items
        self.output_item = output_item
        super().__init__()

        if callable(function_name):
            cosSave(function_name,bucket=bucket,credentials=cos_credentials,filename=function_name.__name__)
            function_name = function_name.__name__
        self.function_name = function_name
        
        # The function called during execution accepts a single dictionary as input
        # add all instance variables to the parameters dict in case the function needs them
        if parameters is None:
            parameters = {}
        parameters = {**parameters, **self.__dict__}
        self.parameters = parameters
        
    def execute(self,df):
        
        # retrieve
        function = cosLoad(bucket=self.parameters['bucket'],
                           credentials=self.parameters['cos_credentials'],
                           filename=self.parameters['function_name'])
        #execute
        rf = function(df,self.parameters)
        #rf will contain a single new output column. The name of this output column will be set to the 
        #value of parameters['output_item'] by the function
        
        return rf
    
    
    
    
class LookupCompany(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """

    def __init__ (self, company_key , lookup_items= None, output_items=None):
        
        # sample data will be used to create table if it doesn't already exist
        # sample data will be converted into a dataframe before being written to a  table
        # make sure that the dictionary that you provide is understood by pandas
        # use lower case column names
        self.data = {
                'company_code' : ['ABC','ACME','JDI'] ,
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
        self.company_key = company_key
        #must specify a name of the lookup name even if you are supplying your own sql.
        #use lower case table names
        lookup_table_name = 'company'                
        # A sql statement that will be used to retrieve data for the loookup
        sql = 'select * from %s' %lookup_table_name
        # Indicate which of the input parameters are lookup keys
        lookup_keys = [company_key] 
        # Indicate which of the column returned should be converted into dates
        parse_dates = ['inception_date']
        
        #db credentials are optional. Only required to connect to a non AS DB2.
        #Make use that you have your DB_CONNECTION_STRING environment variable set to test locally
        #Or supply credentials in an instance variable
        #self.db_credentials = <blah>
        
        super().__init__(
             lookup_table_name = lookup_table_name,
             sql= sql,
             lookup_keys= lookup_keys,
             lookup_items = lookup_items,
             parse_dates= parse_dates, 
             output_items = output_items
             )
    
        # The base class takes care of the rest
        # No execute() method required
                
        
class LookupOperator(BaseResourceLookup):
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
        table_name = 'operator_lookup'
        super().__init__(table_name = table_name, output_item = output_item)
        

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
    

    
class MergeActivityData(BaseDBActivityMerge):
    '''
    Merge data from multiple tables containing activities with start and end dates
    '''
    execute_by = ['deviceid']
    
    def __init__(self,input_activities,
                 activity_duration=None):
        
        super().__init__(input_activities=input_activities,
                         activity_duration=activity_duration)

        self.activities_metadata['mike_maintenance'] = ['A','B','C','D','E']
        self.activities_metadata['mike_breakdown'] = ['BD']
        self.activities_custom_query_metadata = {}
        self.activities_custom_query_metadata['CS'] = 'select effective_date as start_date, end_date, asset_id as deviceid from mike_custom_activity'
        self.custom_calendar = ShiftCalendar(
                shift_definition = 
                    {
                       "1": (5.5, 14), #shift 1 starts at 5.5 hours after midnight (5:30) and ends at 14:00
                       "2": (14, 21),
                       "3": (21, 29.5)
                       
                       },
                 shift_start_date = 'start_date',
                 shift_end_date = 'end_date' 
                )
        
        self.add_resource_calendar(resource_name = 'operator', table_name = 'operator_lookup')
        
        
class MergeSampleTimeSeries(BaseLoader):
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
    source_timestamp = 'evt_timestamp'
    #metadata for generating sample
    sample_metrics = ['temp','pressure','velocity']
    sample_entities = ['entity1','entity2','entity3']
    sample_initial_days = 3
    sample_freq = '1min'
    sample_incremental_min = 5
    
    def __init__(self, input_items, output_items=None):
        super().__init__(input_items = input_items, output_items = output_items)
        if self.db is None:
            self.db = Database(credentials = self.db_credentials )

    def get_data(self,start_ts=None,end_ts=None,entities=None):
        
        self.load_sample_data()
        (query,table) = self.db.query(self.source_table_name)
        if not start_ts is None:
            query = query.filter(table.c.timestamp >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c.timestamp < end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        df = pd.read_sql(query.statement, con = self.db.connection,  parse_dates=[self.source_timestamp])        
        return df
    
    
    def get_item_values(self,arg):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':
            if self.db is None:
                self.db = Database(credentials = self.db_credentials )
            return self.db.get_column_names(self.source_table_name)
        else:
            msg = 'No code implemented to gather available values for argument %s' %arg
            raise NotImplementedError(msg)      
        
    
    def load_sample_data(self):
        
        if not self.db.if_exists(self.source_table_name):
            generator = TimeSeriesGenerator(metrics=self.sample_metrics,
                                            ids = self.sample_entities,
                                            freq = self.sample_freq,
                                            days = self.sample_initial_days)
        else:
            generator = TimeSeriesGenerator(metrics=self.sample_metrics,
                                            ids = self.sample_entities,
                                            freq = self.sample_freq,
                                            seconds = self.sample_incremental_min*60)
            
        df = generator.execute()
        self.db.write_frame(df = df, table_name = self.source_table_name,
                       version_db_writes = False,
                       if_exists = 'append',
                       chunksize = 1000 )
        
    def get_test_data(self):
        
        generator = TimeSeriesGenerator(metrics=['acceleration'],
                                        ids = self.sample_entities,
                                        freq = self.sample_freq,
                                        seconds = 300)
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

class ShiftCalendar(BaseTransformer):
    '''
    Generate data for a shift calendar using a shift_definition in the form of a dict keyed on shift_id
    Dict contains a tuple with the start and end hours of the shift expressed as numbers. Example:
          {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 29.5)
           },    
    '''
    def __init__ (self,shift_definition=None,
                  shift_start_date = 'shift_start_date',
                  shift_end_date = 'shift_end_date',
                  shift_day = 'shift_day',
                  shift_id = 'shift_id'):
        if shift_definition is None:
            shift_definition = {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 29.5)
           }
        self.shift_definition = shift_definition
        self.shift_start_date = shift_start_date
        self.shift_end_date = shift_end_date
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
            data[self.shift_start_date] = [x+dt.timedelta(hours=start_end[0]) for x in dates]
            data[self.shift_end_date] = [x+dt.timedelta(hours=start_end[1]) for x in dates]
            dfs.append(pd.DataFrame(data))
        df = pd.concat(dfs)
        df[self.shift_start_date] = pd.to_datetime(df[self.shift_start_date])
        df[self.shift_end_date] = pd.to_datetime(df[self.shift_end_date])
        df.sort_values([self.shift_start_date],inplace=True)
        return df
    
    def execute(self,df):
        df.sort_values([self._timestamp],inplace = True)
        calendar_df = self.get_data(start_date= df[self._timestamp].min(), end_date = df[self._timestamp].max())
        df = pd.merge_asof(left = df,
                           right = calendar_df,
                           left_on = self._timestamp,
                           right_on = self.shift_start_date,
                           direction = 'backward')
        df = self.conform_index(df)
        return df  
    
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
        ts = ts.index.get_level_values(self._df_index_timestamp)
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
        
        df[self.period_start] = df[self._timestamp].dt.date
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
    
    def get_custom_calendar(self):
        '''
        create a custom calendar object
        '''
        custom_calendar = ShiftCalendar(
            {
               "1": (5.5, 14), #shift 1 starts at 5.5 hours after midnight (5:30) and ends at 14:00
               "2": (14, 21),
               "3": (21, 29.5)
               },
            shift_start_date = self.period_start,
            shift_end_date = self.period_end)        
        #inject metdata into this custom calendar
        custom_calendar.set_params(**self.get_params())
        
        return custom_calendar
    

class TimeSeriesGenerator(BaseLoader):

    ''' 
    Used to generate sample data. Not a registerable function. Call from within a function. 
    '''
    
    increase_per_day = 0.0001
    noise = 0.1 
    ref_date = dt.datetime(2018, 1, 1, 0, 0, 0, 0)
    day_harmonic = 0.1
    day_of_week_harmonic = 0.2
    
    def __init__(self,metrics=None,ids=None,days=30,seconds=0,freq='1min', dims = None, dates=None):
    
        if metrics is None:
            metrics = ['x1','x2','x3']
        
        if dims is None:
            dims = []
            
        if dates is None:
            dates = []
            
        self.metrics = metrics
        self.dims = dims
        self.dates = dates
        
        if ids is None:
            ids = ['sample_%s' %x for x in list(range(10))]
        self.ids = ids
        
        self.days = days
        self.seconds = seconds
        self.freq = freq
        #optionally scale using dict keyed on metric name
        self.mean = {}
        self.sd = {}
        
    def get_data(self,start_ts=None,end_ts=None,entities=None):
        
        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=self.days)
        start = start - dt.timedelta(seconds=self.seconds)
        
        ts = pd.date_range(end=end,start=start,freq=self.freq)
        y_cols = []
        y_cols.extend(self.metrics)
        y_cols.extend(self.dates)
        y_count = len(y_cols)
        rows = len(ts)
        noise = np.random.normal(0,1,(rows,y_count))
        
        df = pd.DataFrame(data=noise,columns=y_cols)
        df[self._entity_id] = np.random.choice(self.ids, rows)
        df[self._timestamp] = ts
        
        days_from_ref = (df[self._timestamp] - self.ref_date).dt.total_seconds() / (60*60*24)
        day = df[self._timestamp].dt.day
        day_of_week = df[self._timestamp].dt.dayofweek
        
        for m in y_cols:
            try:
                df[m] = df[m] * self.sd[m]
            except KeyError:
                pass            
            df[m] = df[m] + days_from_ref * self.increase_per_day
            df[m] = df[m] + np.sin(day*4*math.pi/364.25) * self.day_harmonic
            df[m] = df[m] + np.sin(day_of_week*2*math.pi/6) * self.day_of_week_harmonic
            try:
                df[m] = df[m] + self.mean[m]
            except KeyError:
                pass
            
        for d in self.dims:
            df[d] = np.random.choice(self.get_domain(d), len(df.index))
            
        for t in self.dates:
            df[t] = dt.datetime.utcnow() + pd._to_timesdelta(df[t],unit = 'D')
            df[t] = pd.to_datetime(df[t])
            
        df.set_index([self._entity_id,self._timestamp])
        msg = 'Generated %s rows of time series data from %s to %s' %(rows,start,end)
        logger.debug(msg)
        
        return df
    
    def execute(self,df=None):
        df = self.get_data()
        return df
    
    def set_mean(self,metric,mean):
        self.mean[metric] = mean
        
    def set_sd(self,metric,sd):
        self.mean[sd] = sd
        
    
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
                'id' : [1,1,1,1,1,2,2,2,2,2],
                'evt_timestamp' : [
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
        
        return df
    
    def _get_str_array(self):
        
        out = ''
        for col in self.column_metadata:
            out = out + str(np.random.normal(1,0.1)) + ','
        out = out[:-1]
        
        return out    

class WriteDataFrame(BaseTransformer):
    '''
    Write the current contents of the pipeline to a database table
    '''    
    out_table_prefix = ''
    version_db_writes = False
    out_table_if_exists = 'append'

    def __init__(self, input_items, out_table_name, output_status= 'output_status', db_credentials=None):
        self.input_items = input_items
        self.output_status = output_status
        self.db_credentials = db_credentials
        self.out_table_name = out_table_name
        super().__init__()
        
    def execute (self, df):
        df = df.copy()
        df[self.output_status] = self.write_frame(df=df[self.input_items]        
        )
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
            logger.warning('Pivot values: %s' %pivot_values)
            logger.warning('Output items: %s' %output_items)
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
    


