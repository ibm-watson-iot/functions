# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import os
import urllib3
import json
import numpy as np
import pandas as pd
import numbers
import datetime as dt
import logging
import ibm_db
import ibm_db_dbi
from sqlalchemy.types import String,SmallInteger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm.session import sessionmaker
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
from inspect import getargspec
from collections import OrderedDict
from .util import cosLoad, cosSave

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
    # custom output tables
    version_db_writes = False #write a new version timestamp to custom output table with each execution
    out_table_prefix = None
    out_table_if_exists = 'append'
    out_table_if_changed = 'replace'
    out_table_name = None 
    # registration metadata
    url = '<>'
    category = None
    incremental_update = True
    # status
    base_initialized = True
    # processing options
    jsonSchemaAsStr = False
    
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
            
        if self.url is None:
            self.url = '<>'

        if self.tags is None:
            self.tags = []  


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
            
        if constants is None:
            constants = []
            
        if module is None:
            module = self.__class__.__module__
            
        if description is None:
            description = self.description
            
        if url is None:
            url = self.url
            
        if incremental_update is None:
            incremental_update = self.incremental_update
            
        if constants is None:
            constants = self.constants

        if constants is None:
            constants = self.constants

        if outputs is None:
            outputs = self.outputs
            
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
        print ('Test import succeeded for function version %s using %s' %(version,exec_str))
        
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
            print ('Function registration deletion status: ',r.data.decode('utf-8'))
            r = http.request("PUT", url, body = encoded_payload, headers=headers)     
            print ('Function registration status: ',r.data.decode('utf-8'))
            return r.data.decode('utf-8')
        else:
            return encoded_payload
    
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
            
    
    def _getJsonDataType(self,datatype):
         
         if datatype == 'LITERAL':
             result = 'string'
         else:
             result = datatype.lower()
         return result
        
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
            inputs = []
            
        if outputs is None:
            outputs = []            
        
        if constants is None:
            constants = []
        
        #run the function to produce a new dataframe that contains the function outputs
        if not df is None:
            tf = df.head(self.test_rows).copy()
            tf = self.execute(tf)
            if not isinstance(tf,pd.DataFrame):
                raise TypeError('The execute method of a custom function must return a pandas DataFrame object not %s' %tf)
            test_outputs = self._inferOutputs(before_df=df,after_df=tf)
            if len(test_outputs) ==0:
                raise ValueError('Could not locate output columns in the test dataframe. Check the execute method of the function to ensure that it returns a dataframe with output columns that are named differently from the input columns')
        else:
            tf = None
            #TBD expect all inputs, outputs and constants to be explicitly defined
            raise NotImplementedError('Must supply a test dataframe for function registration. Explict metadata definited not suported')
        
        metadata_inputs = {}
        metadata_outputs = {}
        min_items = None
        array_outputs = []
        array_inputs = []

        #introspect function to get a list of argumnents
        args = (getargspec(self.__init__))[0][1:]        
        for a in args:
            #identify which arguments are inputs, which are ouputs and which are constants
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
            argtype = self._infer_type(arg_value)
            #check if parameter has been modeled explictly
            if a in constants:
                datatype = self._infer_type(arg_value,df=None)
                column_metadata['dataType'] = datatype 
                column_metadata['type'] = 'CONSTANT'
                if column_metadata['description'] is None:
                    column_metadata['description'] = 'Supply a constant input parameter of type %s' %datatype 
                metadata_inputs[a] = column_metadata
                if a in self.optionalItems:
                    column_metadata['required'] = False
                else:
                    column_metadata['required'] = True
                    min_items = 1
                is_added = True
            elif a in outputs:
                datatype = self._infer_type(arg_value,df=tf)
                column_metadata['dataType'] = datatype
                if column_metadata['description'] is None:
                    column_metadata['description'] = 'Provide a new data item name for the function output'
                metadata_outputs[a] = column_metadata
                is_added = True
            elif a in inputs:
                column_metadata['type'] = 'DATA_ITEM' 
                datatype = self._infer_type(arg_value,df=df)
                column_metadata['dataType'] = datatype
                if column_metadata['description'] is None:
                    column_metadata['description'] = 'Choose data item/s to be used as function inputs'
                metadata_inputs[a] = column_metadata
                if a in self.optionalItems:
                    column_metadata['required'] = False
                else:
                    column_metadata['required'] = True
                    min_items = 1                
                is_added = True
            #if argument is a number, date or dict it must be a constant
            elif argtype in ['NUMBER','JSON','BOOLEAN','TIMESTAMP']:
                datatype = argtype
                column_metadata['type'] = 'CONSTANT'
                column_metadata['dataType'] = datatype
                if column_metadata['description'] is None:
                    column_metadata['description'] = 'Supply a constant input parameter of type %s' %datatype 
                metadata_inputs[a] = column_metadata
                if a in self.optionalItems:
                    column_metadata['required'] = False
                else:
                    column_metadata['required'] = True
                    min_items = 1                
                is_added = True                
            #look for items in the input and output dataframes
            elif not is_array:
                #look for value in test df and test outputs
                if arg_value in df.columns:
                    column_metadata['type'] = 'DATA_ITEM' 
                    datatype = self._infer_type(arg_value,df=df)
                    column_metadata['dataType'] = datatype
                    if column_metadata['description'] is None:
                        column_metadata['description'] = 'Choose data item/s to be used as function inputs'
                    metadata_inputs[a] = column_metadata
                    is_added = True
                    if a in self.optionalItems:
                        column_metadata['required'] = False
                    else:
                        column_metadata['required'] = True
                        min_items = 1                    
                elif arg_value in test_outputs:
                    datatype = self._infer_type(arg_value,df=tf)
                    column_metadata['dataType'] = datatype
                    if column_metadata['description'] is None:
                        column_metadata['description'] = 'Provide a new data item name for the function output'
                    metadata_outputs[a] = column_metadata    
                    is_added = True
            elif is_array:
                #look for contents of list in test df and test outputs
                if all(elem in df.columns for elem in arg_value):
                    column_metadata['type'] = 'DATA_ITEM' 
                    datatype = self._infer_type(arg_value,df=df)
                    if column_metadata['description'] is None:
                        column_metadata['description'] = 'Choose data item/s to be used as function inputs'
                    metadata_inputs[a] = column_metadata
                    array_inputs.append((a,len(arg_value)))
                    is_added = True
                    if a in self.optionalItems:
                        column_metadata['required'] = False
                    else:
                        column_metadata['required'] = True
                        min_items = 1
                elif all(elem in test_outputs for elem in arg_value):
                    datatype = self._infer_type(arg_value,df=tf)
                    if column_metadata['description'] is None:
                        column_metadata['description'] = 'Provide a new data item name for the function output'
                    metadata_outputs[a] = column_metadata
                    array_outputs.append((a,len(arg_value)))
                    is_added = True
                #arrays are specical. They need a json schema.
                column_metadata['dataType'] = 'ARRAY'
                if not datatype is None:
                    column_metadata['dataTypeForArray'] = [datatype]
                else:
                    column_metadata['dataTypeForArray'] = None
                column_metadata['jsonSchema'] = {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "array",
                    "minItems": min_items
                    }
                try:
                    column_metadata['jsonSchema']["maxItems"] = self.itemMaxCardinality[a]
                except KeyError:
                    pass
                if not datatype is None:
                    column_metadata['jsonSchema']["items"] = {"type":"string"}
                    
                if self.jsonSchemaAsStr:
                    column_metadata['jsonSchema'] = str(column_metadata['jsonSchema'] )
                    column_metadata['jsonSchema'] = column_metadata['jsonSchema'].replace("'",'"')                    
                    
            #if parameter was not explicitly modelled and does not exist in the input and output dataframes
            # it must be a constant
            if not is_added:
                datatype = self._infer_type(arg_value,df=None)
                column_metadata['description'] = 'Supply a constant input parameter of type %s' %datatype 
                column_metadata['type'] = 'CONSTANT'
                metadata_inputs[a] = column_metadata
            
            #constants may have explict values
            try:
                col_type = column_metadata['type']
            except KeyError:
                pass
            else:
                if col_type  == 'CONSTANT':
                    try:
                        values = self.itemValues[a]
                    except KeyError:
                        pass
                    else:
                        column_metadata['values'] = values
                                           
            #json schema may have been explicitly defined                
            try:
               column_metadata['jsonSchema'] = self.itemJsonSchema[a] 
            except KeyError:
                pass
                
        #array outputs are special. They inherit their datatype from an input array
        #that could be explicity defined, or use last array_input 
           
        for (array,length) in array_outputs:
            try:
                array_source =  self.itemArraySource[array]
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
    
        return (metadata_inputs,metadata_outputs)    
    
    def _infer_array_source(self,candidate_inputs,output_length):
        '''
        Look for the last input array with the same length as the target
        '''
        source = None
    
        for input_parm,input_length in candidate_inputs:
            if input_length == output_length:
                source = input_parm
        
        return source
            
        

    def _inferOutputs(self,before_df,after_df):
        '''
        Work out which columns were added to the test dataframe by executing the function. These are the outputs.
        '''
        outputs = list(set(after_df.columns) - set(before_df.columns))
        outputs.sort()
        return outputs
    
    
    def _infer_type(self,parm,df=None):
        """
        Infer datatype for constant or item in dataframe.
        """
        if not isinstance(parm,list):
            parm = [parm]
            
        prev_datatype = None
        multi_datatype = False
            
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
                    raise TypeError('Cannot infer type of argument value %s. Supply a string, number, boolean, datetime, dict or list containing any of these types.' %parm)
            else:                
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
                    
            if not prev_datatype is None:
                if datatype != prev_datatype:
                    multi_datatype = True
            prev_datatype = datatype
            
        if multi_datatype:
            datatype = None
            
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
    
    def write_frame(self,df,
                    db_credentials = None,
                    table_name=None, 
                    version_db_writes = None,
                    if_exists = None,
                    if_changed = None):
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
            
        status = 0
        df = df.reset_index()
        
        if version_db_writes is None:
            version_db_writes = self.version_db_writes
        if version_db_writes:
            df['version_date'] = dt.datetime.now()
            
        if if_exists is None:
            if_exists = self.out_table_if_exists
        if if_changed is None:
            if_changed = self.out_table_if_changed            
            
        if table_name is None:
            if self.out_table_prefix != '':
                table_name='%s_%s' %(self.out_table_prefix, self.out_table_name)
            else:
                table_name = self.out_table_name
        
        if table_name is None:
            raise ValueError('Function attempted to write data to a table. A name was not supplied. Specify an instance variable for out_table_name. Optionally include an out_table_prefix too')
        
        dtypes = {}
        
        #replace default mappings to clobs and booleans
        for c in list(df.columns):
            if is_string_dtype(df[c]):
                dtypes[c] = String(255)
            elif is_bool_dtype(df[c]):
                dtypes[c] = SmallInteger()    
        
        (connection,session) = self.acquire_db_connection(credentials=db_credentials,
                                                        start_session=True)        
        try:
            df.to_sql(name = table_name , index = False,
                      con=connection, if_exists=if_exists,
                      chunksize = 1000, dtype = dtypes)        
            session.commit()
        except:
            try:
                df.to_sql(name = table_name , index = False,
                          con=connection, if_exists=if_changed,
                          chunksize = 1000, dtype = dtypes)        
                session.commit()
            except:
                session.rollback()
                status = - 1
                raise
        finally:
            session.close()
            status = 1
        
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
    
    
    def get_test_data(self):
        """
        Output a dataframe for testing function
        """
        
        data = {
                'id' : [1,1,1,1,1,2,2,2,2,2],
                'timestamp' : [
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
        
        return df
        
    
class BaseTransformer(BaseFunction):
    """
    Base class for AS Transform Functions. Inherit from this class when building a custom function that adds new columns to a dataframe.

    """
    category =  'TRANSFORMER'
    
    def __init__(self):
        super().__init__()


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
    
    def __init__(self,
                 lookup_table_name,
                 sql=None,
                 lookup_keys = None,
                 parse_dates=None,
                 lookup_items = None,
                 output_items=None):

        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')
            
        self.lookup_table_name = lookup_table_name
        self.sql = sql
        super().__init__()
        #drive the output cardinality and data type from the items choosen for the lookup
        self.itemArraySource['output_items'] = 'lookup_items'
        self.lookup_keys = self.convertStrArgToList(lookup_keys,argument = 'lookup_keys', check_non_empty = True)
        self.itemMaxCardinality['lookup_keys'] = len(self.lookup_keys)
        self.parse_dates = self.convertStrArgToList(parse_dates, argument = 'parse_dates', check_non_empty = False) 
        
        self.connection = self.acquire_db_connection(start_session = False)
        cols =  self.get_lookup_columns(connection=self.connection)
        if lookup_items is None:
            lookup_items = cols
        if output_items is None:
            #concatentate lookup name to output to make it unique
            output_items = ['%s_%s' %(self.lookup_table_name,x) for x in lookup_items]
        
        self.lookup_items = self.convertStrArgToList(lookup_items,argument = 'lookup_items')
        self.output_items = self.convertStrArgToList(output_items,argument = 'output_items')
                       
        
        self.itemValues['lookup_items'] = cols
        
    def get_lookup_columns(self,connection):
        lup_keys = [x.upper() for x in self.lookup_keys]
        date_cols = [x.upper() for x in self.parse_dates]
        try:
            df = pd.read_sql(self.sql, con = connection, index_col=lup_keys, parse_dates=date_cols)
        except :
            df = pd.DataFrame(data=self.data)
            df = df.set_index(keys=self.lookup_keys)
            self.create_lookup_table(df=df, table_name = self.lookup_table_name)
            df = pd.read_sql(self.sql, connection, index_col=self.lookup_keys, parse_dates=self.parse_dates)
            
        df.columns = [x.lower() for x in list(df.columns)]
            
        return(list(df.columns))
    
    def execute(self, df):
        '''
        Execute transformation function of DataFrame to return a DataFrame
        '''
        lup_keys = [x.upper() for x in self.lookup_keys]
        date_cols = [x.upper() for x in self.parse_dates]
        df_sql = pd.read_sql(self.sql, self.connection, index_col=lup_keys, parse_dates=date_cols)
        df_sql.columns = [x.lower() for x in list(df_sql.columns)]
        df_sql = df_sql[self.lookup_items]
                
        if len(self.output_items) > len(df_sql.columns):
            raise RuntimeError('length of names (%d) is larger than the length of query result (%d)' % (len(self.names), len(df_sql)))

        df = df.join(df_sql,on= self.lookup_keys, how='left')

        renamed_cols = {df_sql.columns[idx]: name for idx, name in enumerate(self.output_items)}
        df = df.rename(columns=renamed_cols)
        

        return df
    
    def create_lookup_table(self,df, table_name):
        
        self.write_frame(df=df,table_name = table_name, if_exists = 'replace')

class AlertThreshold(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least on threshold.
    """
    
    url = PACKAGE_URL
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
    

class ExecuteFunctionSingleOut(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage. Function returns a single output.
    """ 
    
    url = PACKAGE_URL
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
    url = PACKAGE_URL

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
        
class MergeActivityData(BaseTransformer):
    '''
    Merge actitivity data with time series data.
    Activies are events that have a start and end date and generally occur sporadically.
    Activity tables may contain an activity type.
    This function flattens multiple activity types from multiple activity tables into separate start and end date columns for each activity.
    '''
    
    url = PACKAGE_URL
    activities_metadata = {}
    activities_metadata['maintenance'] = ['pm','sm']
    activities_metadata['breakdown'] = ['refuel','flat']
    execute_by = ['id']
    db_credentials = {
          "connection" : "dashdb",
          "hostname": "dashdb-entry-yp-dal10-01.services.dal.bluemix.net",
          "password": "iq__BljDTG34",
          "port": 50000,
          "host": "dashdb-entry-yp-dal10-01.services.dal.bluemix.net",
          "db": "BLUDB",
          "database": "BLUDB",
          "username": "dash100277",
          "tennant_id":"DEMO-AS"
      }
    
    def __init__(self,input_activities,activity_start=None,activity_end=None):
    
        self.input_activities = input_activities
        self.activity_start = activity_start
        self.activity_end = activity_end
        super().__init__()
        
        self.connection = self.acquire_db_connection(start_session = False)
        
    def execute(self,df):
        
        dfs = []
        for table,activities in list(self.activities_metadata.items()):
            for a in activities:
                af = self.get_data(table=table,activity=a)
                af["activity"] = a
                dfs.append(af)
        adf = pd.concat(dfs)

        group_base = []
        for s in self.execute_by:
            if s in adf.columns:
                group_base.append(s)
            else:
                try:
                    x = dfs.index.get_level_values(s)
                except KeyError:
                    raise ValueError('This function executes by column %s. This column was not found in columns or index' %s)
                else:
                    group_base.append(pd.Grouper(axis=0, level=df.index.names.index(s)))
                    
        if len(group_base)>0:
            df = df.groupby(group_base).apply(self._combine_activities())                
        else:
            raise ValueError('This function executes by entity. execute_by should be ["<id_column>"]')
                    
    def _combine_activities(df):
        '''
        incoming dataframe has start date , end date and activity code.
        activities may overlap.
        output dataframe corrects overlapping activities.
        activities with later start dates take precidence over activies with earlier start dates when resolving.
        '''
        
        #create a continuous range
        early_date = dt.datetime.min
        late_date = dt.datetime.max
        
        #create a new start date for each potential interruption of the continuous range
        df = df.sort_values(['start_date'])
        dates = [early_date]
        dates.extend (df['start_date'].tolist())
        end_dates = df['end_date'].tolist()
        dates.extend (end_dates)
        dates.sort()
        dates.append(late_date)
        c = pd.Series(data=None,index = dates)
        c.name = "activity"
        c.index.name = "start_date"
        
        #use original data to update the new set of intervals in slices
        for index, row in df.iterrows():
            end_date = row['end_date'] - dt.timedelta(seconds=1)
            c[row['start_date']:end_date] = row['activity']
    
        #remove duplicates and nulls and add end date
        dup = c.eq(c.shift())
        c = c[~dup]
        df =  c.to_frame().reset_index()
        df['end_date'] = df['start_date'].shift(-1)
        df['end_date'] = df['end_date'] - dt.timedelta(seconds=1)
        df = df.dropna()
        
        return df          
            
                
    def get_data(self,table,activity):
        
        sql = 'select id, start_date, end_date from %s where activity = "%s"'
        df = pd.read_sql(sql, con = self.connection,  parse_dates=['start_date','end_date'])
        
        return df
        
    

class NegativeRemover(BaseTransformer):
    '''
    Replace negative values with NaN
    '''
    
    url = PACKAGE_URL

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
    
    url = PACKAGE_URL

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


class MultiplyArrayByConstant(BaseTransformer):
    '''
    Multiply a list of input columns by a constant to produce a new output column for each input column in the list.
    The names of the new output columns are defined in a list(array) rather than as discrete parameters.
    '''
    
    url = PACKAGE_URL
    
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
    
    url = PACKAGE_URL
    
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
    
    url = PACKAGE_URL
    
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
    
    url = PACKAGE_URL
    
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
    
    url = PACKAGE_URL
    
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
    
    url = PACKAGE_URL
    
    def __init__(self, input_items, output_item = 'output_item'):
    
        self.input_items = input_items
        self.output_item = output_item
        
        super().__init__()
        
    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_items].product(axis=1)
        return df


    
class FillForwardByEntity(BaseTransformer):    
    '''
    Fill null values forward from last item for the same entity instance
    '''
    
    url = PACKAGE_URL
    execute_by = ['id']
    
    def __init__(self, input_item, output_item = 'output_item'):
    
        self.input_item = input_item
        self.output_item = output_item
        
        super().__init__()
        
    def _calc(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item].ffill()
        return df
    
class InputsAndOutputsOfMultipleTypes(BaseTransformer):
    '''
    This sample function is just a pass through that demonstrates the use of multiple datatypes for inputs and outputs
    '''
    
    url = PACKAGE_URL
    
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
    url = PACKAGE_URL
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
                'timestamp' : [
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
        
        return ','.join(str(np.random.normal(1,0.1)) for col in self.column_metadata)

class WriteDataFrame(BaseTransformer):
    '''
    Write the current contents of the pipeline to a database table
    '''
    
    url = PACKAGE_URL
    out_table_prefix = ''

    def __init__(self, input_items, out_table_name, output_status= 'output_status', db_credentials=None):
        self.input_items = input_items
        self.output_status = output_status
        self.db_credentials = db_credentials
        self.out_table_name = out_table_name
        
        super().__init__()
        
    def execute (self, df):
        df = df.copy()
        df[self.output_status] = self.write_frame(df=df[self.input_items])
        return df   
            
class FlowRateMonitor(BaseTransformer):  
    '''
    Check for leaks and other flow irregularies by comparing input flows with output flows
    '''
    
    url = PACKAGE_URL

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
