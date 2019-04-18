# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
import datetime as dt
import sys
import numpy as np
import json
import importlib
import time
import threading
import warnings
from collections import OrderedDict, defaultdict
import pandas as pd
from collections import OrderedDict
from pandas.api.types import is_bool, is_number, is_string_dtype, is_timedelta64_dtype
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, Float, func
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE
from . import db as db_module
from .automation import TimeSeriesGenerator, DateGenerator, MetricGenerator, CategoricalGenerator
from .pipeline import CalcPipeline, DataReader
from .util import MemoryOptimizer, StageException

logger = logging.getLogger(__name__)

def build_schedules(metadata):
    '''
    Build a dictionary of schedule metdata from the schedules contained
    within function definitions.
    
    The schedule dictionary is keyed on a pandas freq string. This
    frequency denotes the schedule interval. The dictionary contains a
    tuple (start_hour,start_minute,backtrack_days)
    
    Example
    -------
    
    { '5min': [16,3,7] } 
    5 minute schedule interval with a start time of 4:03pm and backtrack of 7 days.
    
    '''
    
    freqs = {}
    
    for f in metadata:
        
        if f['schedule'] is not None:
            
            freq = f['schedule']['every']
            start = time.strptime(f['schedule']['starting_at'],'%H:%M:%S')
            start_hour = start[3]
            start_min = start[4]
            backtrack = f['backtrack']
            backtrack_days = (backtrack['days'] +
                              backtrack['hours']/24 +
                              backtrack['minutes']/1440)
            
            
            existing_schedule = freqs.get(freq,None)
            if existing_schedule is None:
                f[freq] = (start_hour,start_min,backtrack_days)
            else:
                if existing_schedule[0] > start_hour:
                    f[freq] = (start_hour,existing_schedule[1],existing_schedule[2])
                    logger.warning(
                        ('There is a conflict in the schedule metadata.'
                         ' Picked the earliest start hour of %s'
                         ' for schedule %s.' %(freq,start_hour)
                                    ))
                if existing_schedule[1] > start_min:
                    f[freq] = (existing_schedule[0],start_min,existing_schedule[2])
                    logger.warning(
                        ('There is a conflict in the schedule metadata.'
                         ' Picked the earliest start minute of %s'
                         ' for schedule %s.' %(freq,start_min)
                                    ))
                if existing_schedule[1] < backtrack_days:
                    f[freq] = (existing_schedule[0],existing_schedule[0],backtrack_days)        
                    logger.warning(
                        ('There is a conflict in the schedule metadata.'
                         ' Picked the longest backtrack of %s'
                         ' for schedule %s.' %(freq,backtrack_days)
                                    ))
            freqs[freq] = f[freq] 
    
    return freqs



def make_sample_entity(db,schema=None,
                      name = 'as_sample_entity',
                      register = False,
                      data_days = 1,
                      float_cols = None,
                      string_cols = None,
                      drop_existing = True):
    """
    Get a sample entity to use for testing
    
    Parameters
    ----------
    db : Database object
        database where entity resides.
    schema: str (optional)
        name of database schema. Will be placed in the default schema if none specified.
    name: str (optional)
        by default the entity type will be called as_sample_entity
    register: bool
        register so that it is available in the UI
    data_days : number
        Number of days of sample data to generate
    float_cols: list
        Name of float columns to add
    string_cols : list
        Name of string columns to add
    """
    
    if float_cols is None:
        float_cols = ['temp', 'grade', 'throttle' ]
    if string_cols is None:
        string_cols = ['company']
        
    if drop_existing:
        db.drop_table(table_name=name, schema=schema)        
        
    float_cols = [Column(x,Float()) for x in float_cols]
    string_cols = [Column(x,String(255)) for x in string_cols]
    args = []
    args.extend(float_cols)
    args.extend(string_cols)
    
    entity = EntityType(name,db, *args,
                      **{
                        '_timestamp' : 'evt_timestamp',
                        '_db_schema' : schema
                         })
    entity.generate_data(days=data_days, drop_existing = True)
    if register:
        entity.register()
    return entity

def retrieve_entity_type_metadata(**kwargs):
    '''
    Get server metadata for entity type
    '''
    db = kwargs['_db']
    # get kpi functions metadata
    meta = db.http_request(object_type = 'engineInput',
                                object_name = kwargs['logical_name'],
                                request= 'GET')
    try:
        meta = json.loads(meta)
    except (TypeError, json.JSONDecodeError):
        meta = None        
    if meta is None or 'exception' in meta:
        raise RuntimeError((
                'API call to server did not retrieve valid entity '
                ' type properties for %s.' %kwargs['logical_name']))
        
    #cache function catalog metadata in the db object
    function_list = [x['functionName'] for x in meta['kpiDeclarations']] 
    db.load_catalog(install_missing=True, function_list=function_list)
    
    #map server properties
    params = {}
    params['_entity_type_id']  =meta['entityTypeId']
    params['_db_schema'] = meta['schemaName']
    params['name'] = meta['metricsTableName']
    params['_timestamp'] = meta['metricTimestampColumn']
    params['_dimension_table_name'] = meta['dimensionsTable']
    params['_data_items'] = meta['dataItems']
    
    #constants
    c_meta = db.http_request(object_type = 'constants',
                           object_name = kwargs['logical_name'],
                           request= 'GET')
    try:
        c_meta = json.loads(c_meta)
    except (TypeError, json.JSONDecodeError):
        logger.debug(('API call to server did not retrieve valid entity type'
                      ' properties. No properties set.'))
    else:
        for p in c_meta:
            key = p['name']
            if isinstance(p['value'],dict):
                params[key] = p['value'].get('value',p['value'])
            else:
                params[key] = p['value']
            logger.debug('Retrieved server constant %s with value %s',key,params[key])
            
    params = {**kwargs,**params}
    
    return (params, meta)


class EntityType(object):
    '''
    Data is organised around Entity Types. Entity Types have one or more 
    physical database object for their data. When creating a new Entity Type,
    it will attempt to connect itself to a table of the same name in the
    database. If no table exists the Entity Type will create one.
    
    Parameters
    ----------
    name: str
        Name of the entity type. Use lower case. Will be used as the physical
        database table name so don't use database reserved works of special
        characters. 
    db: Database object
        Contains the connection info for the database
    *args:
        Additional positional arguments are used to add the list of SQL Alchemy
        Column objects contained within this table. Similar to the style of a
        CREATE TABLE sql statement. There is no need to specify column names 
        if you are using an existing database table as an entity type.
    **kwargs
        Additional keywork args. 
        _timestamp: str
            Overide the timestamp column name from the default of 'evt_timestamp'
    '''    
    
    auto_create_table = True        
    log_table = 'KPI_LOGGING' #deprecated, to be removed
    checkpoint_table = 'KPI_CHECKPOINT' #deprecated,to be removed
    default_backtrack = None
    trace_df_changes = False
    # These two columns will be available in the dataframe of a pipeline
    _entity_id = 'deviceid' #identify the instance
    _timestamp_col = '_timestamp' #copy of the event timestamp from the index
    # This column will identify an instance in the index
    _df_index_entity_id = 'id'
    # when automatically creating a new dimension, use this suffix
    _auto_dim_suffix = '_auto_dim'
    # generator
    _scd_frequency = '2D'
    _activity_frequency = '3D'
    _start_entity_id = 73000 #used to build entity ids
    _auto_entity_count = 5 #default number of entities to generate data for

    # variabes that will be set when loading from the server
    _entity_type_id = None
    logical_name = None
    _timestamp = 'evt_timestamp'
    _dimension_table_name = None
    _db_connection_dbi = None
    _db_schema = None
    _data_items = None
    tenant_id = None
    _entity_filter_list = None
    _start_ts_override = None
    _end_ts_override = None
    _stages = None
    _schedules_dict = None
    _granularities_dict = None
    _input_set = None
    _output_list = None
    # processing defaults
    _checkpoint_by_entity = True # manage a separate checkpoint for each entity instance
    _pre_aggregate_time_grain = None # aggregate incoming data before processing
    _auto_read_from_ts_table = True # read new data from designated time series table for the entity
    _pre_agg_rules = None # pandas agg dictionary containing list of aggregates to apply for each item
    _pre_agg_outputs = None #dictionary containing list of output items names for each item
    _data_reader = DataReader
    _abort_on_fail = False
    _auto_save_trace = 30
    save_trace_to_file = False
    
    def __init__ (self,name,db, *args, **kwargs):
        self.name = name.lower()
        self.activity_tables = {}
        self.scd = {}
        self.db = db
        if self.db is not None:
            self.tenant_id = self.db.tenant_id
        self._system_columns = [self._entity_id,self._timestamp_col,
                                'logicalinterface_id', 'devicetype','format',
                                'updated_utc', self._timestamp]
        self._stage_type_map = self.default_stage_type_map()
        self._custom_exclude_col_from_auto_drop_nulls = []
        self._drop_all_null_rows = True
        #pipeline work variables stages
        self._dimension_table = None
        self._scd_stages = []
        self._custom_calendar = None
        self._is_initial_transform = True
        self._is_preload_complete = False
        if self._data_items is None:
            self._data_items = []

        #additional params set from kwargs
        self.set_params(**kwargs)
        
        #Start a trace to record activity on the entity type
        self._trace = Trace(name=None,parent=self,db=db)

        # attach to time series table
        if self._db_schema is None:
            logger.warning(('No _db_schema specified in **kwargs. Using'
                             'default database schema.'))
        if self.logical_name is None:
            self.logical_name = self.name
        self._mandatory_columns = [self._timestamp,self._entity_id]
        
        (cols,functions) = self.separate_args(args)
        
        #create a database table if needed
        if name is not None and db is not None:            
            try:
                self.table = self.db.get_table(self.name,self._db_schema)
            except KeyError:
                if self.auto_create_table:
                    ts = db_module.TimeSeriesTable(self.name ,self.db, *cols, **kwargs)
                    self.table = ts.table
                    self.db.create()
                    msg = 'Create table %s' %self.name
                    logger.info(msg)
                else:

                    msg = ('Database table %s not found. Unable to create'
                           ' entity type instance. Provide a valid table name'
                           ' or use the auto_create_table = True keyword arg'
                           ' to create a table. ' %(name) )
                    raise ValueError (msg)
            #populate the data items metadata from the supplied columns
            self._data_items = self.build_item_metadata(self.table)
        else:
            logger.warning((
                    'Created a logical entity type. It is not connected to a real database table, so it cannot perform any database operations.'
                    ))
            
        #add functions
        self.build_stage_metadata(*functions)
            
    def add_activity_table(self, name, activities, *args, **kwargs):
        '''
        add an activity table for this entity type. 
        
        parameters
        ----------
        name: str
            table name
        activities: list of strs
            activity type codes: these identify the nature of the activity, e.g. PM is Preventative Maintenance
        *args: Column objects
            other columns describing the activity, e.g. materials_cost
        '''
        kwargs['_activities'] = activities
        kwargs['schema'] = self._db_schema
        name = name.lower()
        table = db_module.ActivityTable(name, self.db,*args, **kwargs)
        try:
            sqltable = self.db.get_table(name, self._db_schema)
        except KeyError:
            table.create()
        self.activity_tables[name] = table
        
    def add_slowly_changing_dimension(self,property_name,datatype,**kwargs):
        '''
        add a slowly changing dimension table containing a single property for this entity type
        
        parameters
        ----------
        property_name : str
            name of property, e.g. firmware_version (lower case, no database reserved words)
        datatype: sqlalchemy datatype
        '''
        
        property_name = property_name.lower()
        
        name= '%s_scd_%s' %(self.name,property_name)
        kwargs['schema'] = self._db_schema        
        table = db_module.SlowlyChangingDimension(name = name,
                                   database=self.db,
                                   property_name = property_name,
                                   datatype = datatype,
                                   **kwargs) 
        
        try:
            sqltable = self.db.get_table(name,self._db_schema)
        except KeyError:
            table.create()
        self.scd[property_name] = table
        
    def _add_scd_pipeline_stage(self, scd_lookup):
        
        self._scd_stages.append(scd_lookup)
        
    def build_flat_stage_list(self):
        '''
        Build a flat list of all function objects defined for entity type
        '''
        
        stages = []
        
        for s_list in list(self._stages.values()):
            for stage in s_list:
                #some functions are automatically added by the system
                #do not include these
                try:
                    is_system = stage.is_system_function
                except AttributeError:
                    is_system = False
                    logger.warning(('Function %s has no is_system_function property.'
                              ' This means it was not inherited from '
                              ' an iotfunctions base class. AS authors are'
                              ' strongly encouraged to always inherit '
                              ' from iotfunctions base classes' ),
                             stage.__class__.__name__)
            
                if not is_system:
                    stages.append(stage)
        return stages
    
    def build_granularities(self,grain_meta,freq_lookup):
        '''
        Convert AS granularity metadata to a list of granularity objects.
        '''
        out = {}
        for g in grain_meta:
            grouper = []
            freq = None
            entity_id = None
            if g['entityFirst']:
                grouper.append(pd.Grouper(key=self._entity_id))
                entity_id = self._entity_id
            if g['frequency'] is not None:
                freq = (
                        self.get_grain_freq(g['frequency'],freq_lookup,None)
                        )
                if freq is None:
                    raise ValueError((
                            'Invalid frequency name %s. The frequency name'
                            ' must exist in the frequency lookup %s' %(
                            g['frequency'],freq_lookup)
                            ))
                grouper.append(pd.Grouper(key = self._timestamp,
                                      freq = freq))
            custom_calendar = None
            custom_calendar_keys = []
            dimensions = []
            #differentiate between dimensions and custom calendar items
            for d in g['dataItems']:
                grouper.append(pd.Grouper(key=d))
                if self._custom_calendar is not None:
                    if d in self._custom_calendar.get_output_list():
                        custom_calendar_keys.append(d)
                dimensions.append(d)
                           
            granularity = Granularity(
                    name= g['name'],
                    grouper = grouper,
                    dimensions = dimensions,
                    entity_id = entity_id,
                    custom_calendar_keys = custom_calendar_keys,
                    freq = freq,
                    custom_calendar = custom_calendar)            
            
            out[g['name']] = granularity
            
        return out
    
    def build_item_metadata(self,table):
        '''
        Build a client generated version of AS server metadata from a 
        sql alachemy table object.
        '''
        
        for col_name,col in list(table.c.items()):
            item = {}
            if col_name not in self.get_excluded_cols():
                item['name'] = col_name
                item['type'] = 'METRIC'
                item['parentDataItem'] = None 
                item['kpiFunctionDto'] = None
                item['columnName'] = col.name
                item['columnType'] = self.db.get_as_datatype(col)
                item['sourceTableName'] = self.name
                item['tags'] = []
                item['transient'] = False
                self._data_items.append(item)
                
        return self._data_items

        
    
    def build_stages(self, function_meta, granularities_dict):
        '''
        Create a dictionary of stage objects. Dictionary is keyed by 
        stage type and a granularity obj. It contains a list of stage
        objects.
        '''
    
        if function_meta is None:
            function_meta = []
        stage_metadata = dict()
        disabled = []
        invalid = []
        
        # Execute the payload's get data method using the data_reader
        if self._auto_read_from_ts_table:
            auto_reader = self._data_reader(name='read_entity_data',
                                           obj = self)
            stage_type = self.get_stage_type(auto_reader)
            stage_metadata[(stage_type,None)] = [auto_reader]
            auto_reader._input_set = set()
            auto_reader._output_list = self.get_output_items()
            auto_reader._schedule = None
            auto_reader._entity_type = self
        else:
            logger.debug(('Skipped auto read of payload data as'
                          ' payload does not have _auto_read_from_ts_table'
                          ' set to True'))
        
        for s in function_meta:
            if not s.get('enabled', False):
              disabled.append(s)              
              continue
            meta = {}
            try:
                (package,module) = self.db.get_catalog_module(s['functionName'])
            except KeyError:
                obj = s.get('object_instance',None)
                if obj is None:
                    msg = 'Function %s not found in the catalog metadata' %s['functionName']
                    logger.warning(msg)
                    invalid.append(s)
                    continue
                else:
                    logger.debug('Using local function instance %s',
                                 s.get('name','unknown'))
            else:
                meta['__module__'] = '%s.%s' %(package,module)
                meta['__class__'] =  s['functionName']
                mod = importlib.import_module('%s.%s' %(package,module))
                meta = {**meta,**s['input']}
                meta = {**meta,**s['output']}
                
                cls = getattr(mod,s['functionName'])
                try:
                    obj = cls(**meta)
                except TypeError as e:
                    logger.warning(
                        ('Unable build %s object. The arguments are mismatched'
                         ' with the function metadata. You may need to'
                         ' re-register the function. %s' %(s['functionName'],str(e))
                         )
                                 )
                    invalid.append(s)
                    continue

            #add metadata to stage
            try:
                obj.name
            except AttributeError:
                obj.name = obj.__class__.__name__
            obj._entity_type = self
            schedule = s.get('schedule',None)
            if schedule is not None:
                schedule = schedule.get('every',None)
            obj._schedule = schedule
            stage_type = self.get_stage_type(obj)
            granularity_name = s.get('granularity',None)
            if granularity_name is not None:
                granularity = granularities_dict.get(granularity_name,None)
            else:
                granularity = None
            try:
                stage_metadata[(stage_type,granularity)].append(obj)
            except KeyError:
                stage_metadata[(stage_type,granularity)]=[obj]
            #add input and output items
            if stage_type != 'preload':
                obj._input_set = self.get_stage_input_item_set(
                                stage=obj,
                                arg_meta = s.get('input',{}))
            else:
                #as a legacy quirk, a preload stage may have 
                # dummy input items that should always be ignored
                obj._input_set = set()
            obj._output_list = self.get_stage_output_item_list(
                                arg_meta = s.get('output',[]))
            
        logger.debug('skipping disabled stages: %s . Ignoring outputs: %s' , 
                     [s['functionName'] for s in disabled],
                     [s['output'] for s in disabled]
                     )
        logger.debug('skipping invalid stages: %s Ignoring outputs: %s' , 
                     [s['functionName'] for s in invalid],
                     [s['output'] for s in disabled])
        
        return stage_metadata
    
    def build_stage_metadata(self,*args):
        '''
        Make a new JobController payload from a list of local function objects
        '''
        metadata = []
        for f in args:
            fn = {}
            try:
                name = f.name
            except AttributeError:
                name = f.__class__.__name__
            fn['name'] = name
            fn['object_instance'] = f
            fn['description'] = f.__doc__
            fn['functionName'] = f.__class__.__name__
            fn['enabled'] = True
            fn['execStatus'] = False
            fn['schedule'] = None
            fn['backtrack'] = None
            fn['granularity'] = None
            (fn['input'],fn['output'],fn['outputMeta']) = f.build_arg_metadata()
            fn['inputMeta'] : None
            metadata.append(fn)
            
        self._stages = self.build_stages(function_meta = metadata,
                                         granularities_dict = {})
        return metadata
    
    def index_df(self,df):
        '''
        Create an index on the deviceid and the timestamp
        '''
    
        if df.index.names != [self._entity_id,
                              self._timestamp]: 
            try:
                df = df.set_index([self._entity_id,
                                   self._timestamp])
            except KeyError:
                df = df.reset_index()
                try:
                    df = df.set_index([self._entity_id,
                                       self._timestamp])
                except KeyError:
                    raise KeyError(('Error attempting to index time series'
                                    ' dataframe. Unable to locate index'
                                    ' columns: %s, %s') 
                                    %(self._entity_id,self._timestamp)
                                    )
            logger.debug(('Indexed dataframe on %s, %s'),self._entity_id,
                         self._timestamp)
            
        else:
            logger.debug(('Found existing index on %s, %s.'
                          'No need to recreate index'),self._entity_id,
                         self._timestamp)
        
        return df    

        
    def cos_save(self):
        
        name = ['entity_type', self.name]
        name = '.'.join(name)
        self.db.cos_save(self, name)
        
    @classmethod
    def default_stage_type_map(cls):
        '''
        Configure how properties of stages are used to set the stage type
        that is used by the job controller to decide how to process a stage
        '''
    
        return [ ('preload', 'is_preload'), 
                 ('get_data', 'is_data_source'),
                 ('transform', 'is_transformer'),
                 ('simple_aggregate', 'is_simple_aggregate'),
                 ('complex_aggregate', 'is_complex_aggregate'),
                ]
        
    def df_sort_timestamp(self,df):

        '''
        Sort a dataframe on the timestamp column. Returns a tuple containing
        the sorted dataframe and a column_name for the timestamp column.
        '''
        
        ts_col_name = self._timestamp
        
        #timestamp may be column or in index
        try:
            df.sort_values([ts_col_name],inplace = True)
        except KeyError:
            try:
                #legacy check for a redundant _timestamp alternative column
                df.sort_values([self._timestamp_col],inplace = True)
                ts_col_name = self._timestamp_col
            except KeyError:
                try:
                    df.sort_index(level=[ts_col_name],inplace = True)
                except:
                    raise
        
        return (df,ts_col_name)
        
    def drop_child_tables(self):
        '''
        Drop all child tables
        '''
        tables = []
        tables.extend(self.activity_tables.values())
        tables.extend(self.scd.values())
        [self.db.drop_table(x,self._db_schema) for x in tables]
        msg = 'dropped tables %s' %tables
        logger.info(msg)
        
    def exec_pipeline(self, *args, to_csv = False, register = False,
                      start_ts = None, publish = False):
        '''
        Test an AS function instance using entity data.
        Provide one or more functions as args.
        '''
        stages = list(args)
        pl = self.get_calc_pipeline(stages=stages)
        df = pl.execute(to_csv = to_csv, register = register, start_ts = start_ts)
        if publish:
            pl.publish()
        return df
    
    
    def get_attributes_dict(self):
        '''
        Produce a dictionary containing all attributes
        '''
        c = {}
        for att in dir(self):
            value = getattr(self,att)
            if not callable(value):
                c[att] = value
        return c
    
    def get_calc_pipeline(self,stages=None):
        '''
        Make a new CalcPipeline object. Reset processing variables.
        '''
        warnings.warn('get_calc_pipeline() is deprecated. Use build_job()',
                      DeprecationWarning)
        self._scd_stages = []
        self._custom_calendar = None
        self._is_initial_transform = True
        return CalcPipeline(stages=stages, entity_type = self)


    def get_custom_calendar(self):
        
        return self._custom_calendar
        
        
    def get_data(self,start_ts =None,end_ts=None,entities=None,columns=None):
        '''
        Retrieve entity data at input grain or preaggregated
        '''
        
        tw = {} #info to add to trace
        if entities is None:
            tw['entity_filter'] = 'all'
        else:
            tw['entity_filter'] = '% entities' %len(entities)
        
        if self._pre_aggregate_time_grain is None:    
            df = self.db.read_table(
                    table_name = self.name,
                    schema = self._db_schema,
                    timestamp_col = self._timestamp,
                    parse_dates = None,
                    columns = columns,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    dimension = self._dimension_table_name
                    ) 
            tw['pre-aggregeted'] = None
            
        else:
            (metrics,dates,categoricals,others) = self.db.get_column_lists_by_type(self.name,self._db_schema)
            if self._dimension_table_name is not None:
                categoricals.extend(self.db.get_column_names(self._dimension_table_name,self._db_schema))
            if columns is None:
                columns = []
                columns.extend(metrics)
                columns.extend(dates)
                columns.extend(categoricals)
                columns.extend(others)
            
            #make sure each column is in the aggregate dictionary
            #apply a default aggregate for each column not specified in the aggregation metadata
            if self._pre_agg_rules is None:
                self._pre_agg_rules = {}
                self._pre_agg_outputs = {}
            for c in columns:
                try:
                    self._pre_agg_rules[c]
                except KeyError:                    
                    if c not in [self._timestamp,self._entity_id]:
                        if c in metrics:
                            self._pre_agg_rules[c] = 'mean'
                            self._pre_agg_outputs[c] = 'mean_%s' %c
                        else: 
                            self._pre_agg_rules[c] = 'max'
                            self._pre_agg_outputs[c] = 'max_%s' %c
                else:
                    pass
                        
            df = self.db.read_agg(
                    table_name = self.name,
                    schema = self._db_schema,
                    groupby = [self._entity_id],
                    timestamp = self._timestamp,
                    time_grain = self._pre_aggregate_time_grain,
                    agg_dict = self._pre_agg_rules,
                    agg_outputs = self._pre_agg_outputs,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    dimension = self._dimension_table_name                    
                    )

            tw['pre-aggregeted'] = self._pre_aggregate_time_grain
        
        tw['rows_retrieved'] = len(df.index)
        tw['start_ts'] = start_ts
        tw['end_ts'] = end_ts
        self.trace_append(created_by = self,
                          msg='Retrieved entity timeseries data for %s' %self.name,
                          **tw)

        # Optimizing the data frame size using downcasting
        memo = MemoryOptimizer()

        #df = memo.downcastNumeric(df)
        df = self.index_df(df)
        
        return df
            

    def get_data_items(self):
        '''
        Get the list of data items defined

        :return: list of dicts containting data item metadata
        '''
        return self._data_items
    
    def get_excluded_cols(self):
        '''
        Return a list of physical columns that should be excluded when returning
        the list of data items
        '''
        
        return [ 'logicalinterface_id',
                 'format',
                 'updated_utc'    ]
    
    
    def get_grain_freq(self,grain_name,lookup,default):
        '''
        Lookup a pandas frequency string from an AS granularity name
        '''
        if lookup is None:
            lookup = self._grain_freq_lookup
        for l in lookup:
            if grain_name == l['name']:
                return l['alias']
        return default
    
    
    def get_output_items(self):
        '''
        Get a list of non calculated items: outputs from the time series table
        '''
        
        items = [x.get('columnName') for x in self._data_items
                 if x.get('type') == 'METRIC']
        logger.debug('Data items for entity type are %s' %items)
    
        return items
    
        
    def get_log(self,rows = 100):
        '''
        Get KPI execution log info. Returns a dataframe.
        '''
        query, log = self.db.query(self.log_table, self._db_schema)
        query = query.filter(log.c.entity_type==self.name).\
                      order_by(log.c.timestamp_utc.desc()).\
                      limit(rows)
        df = self.db.get_query_data(query)
        return df
    
    def get_latest_log_entry(self):
        '''
        Get the most recent log entry. Returns dict.
        '''
        last = self.get_log(rows = 1)
        last = last.to_dict('records')[0]
        return last
    
    def get_param(self,param):
        
        return getattr(self, param)

    def get_end_ts_override(self):
        if self._end_ts_override is not None:
            date_time_obj = dt.datetime.strptime(self._end_ts_override[0], '%Y-%m-%d %H:%M:%S')
            return date_time_obj
        return None
    
    def get_stage_input_item_set(self,stage,arg_meta):
        
        try:
            candidate_items = stage.get_input_items()
            if len(candidate_items) > 0:
                logger.debug(('Stage %s requested additional input items %s'
                              ' using the get_input_items() method,' ),
                              stage.name, candidate_items)
        except AttributeError:
            logger.debug(('Stage %s has no get_input_items() method so it'
                          ' can only request data items through its '
                          ' arguments. Implement a get_input_items() method'
                          ' to request items programatically'),
                            stage.name, candidate_items)            
            candidate_items = set()
            
        for arg,value in list(arg_meta.items()):
            
            if isinstance(value,str):
                candidate_items.add(value)
            elif isinstance(value,(list,set)):
                candidate_items |= set(value)
            else:
                logger.debug(
                    ('Argument %s was not considered as a data item as it has'
                     ' a type %s' ), arg, type(value)
                    )
                
        items = set([x['name'] for x in self._data_items])
            
        return items.intersection(candidate_items)
    
    def get_stage_output_item_list(self,arg_meta):
        
        items = []
        for arg,value in list(arg_meta.items()):
            if not isinstance(value,list):
                items.append(value)
            else:
                items.extend(value)
        
        return items
    
    def get_stage_type(self,stage):
        '''
        Examine the stage object to determine how it should be processed by
        the JobController
        
        Sets the stage type to the first valid entry in the stage map
        the stage map is a list of tuples containing a stage type and
        a boolean property name:
        
        example: 
            [('get_data','is_data_source'),
             ('simple_aggregate','is_simple_aggregate')]
        
        if a stage has both an is_data_source = True and
        a is_simple_aggregate = True, the stage type will be returned as
        'get_data'
        '''
        
        for (stage_type, prop) in self._stage_type_map:
            try:
                prop_value = getattr(stage,prop)
            except AttributeError:
                pass
            else:
                if prop_value:
                    return stage_type
        
        raise TypeError(('Could not identify stage type for stage'
                        ' %s for the stage map. Adjust the stage map'
                        ' for the entity type or define an appropriate'
                        ' is_<something> property on the class of the '
                        ' stage. Stage map is %s' % (stage.name, 
                        self._stage_type_map)
                        ))
    
    def get_start_ts_override(self):
        if self._start_ts_override is not None:
            date_time_obj = dt.datetime.strptime(self._start_ts_override[0], '%Y-%m-%d %H:%M:%S')
            return date_time_obj
        return None
        
    
    def generate_data(self, entities = None, days=0, seconds = 300, 
                      freq = '1min', write=True, drop_existing = False):
        '''
        Generate random time series data for entities
        
        Parameters
        ----------
        entities: list
            List of entity ids to genenerate data for
        days: number
            Number of days worth of data to generate (back from system date)
        seconds: number
            Number of seconds of worth of data to generate (back from system date)
        freq: str
            Pandas frequency string - interval of time between subsequent rows of data
        write: bool
            write generated data back to table with same name as entity
        
        '''
        if entities is None:
            entities = [str(self._start_entity_id + x) for x in list(range(self._auto_entity_count))]
        metrics = []
        categoricals = []
        dates = []
        others = []

        if drop_existing:
            self.db.drop_table(self.name, schema = self._db_schema)
            self.drop_child_tables()
                
        exclude_cols =  ['deviceid','devicetype','format','updated_utc','logicalinterface_id',self._timestamp]  
        if self.db is None:
            write = False
            msg = 'This is a null entity with no database connection, test data will not be written'
            logger.debug(msg)
            metrics = ['x_1','x_2','x_3']
            dates = ['d_1','d_2','d_3']
            categoricals = ['c_1','c_2','c_3']
            others = []
        else:
            (metrics,dates,categoricals,others ) = self.db.get_column_lists_by_type(self.table,self._db_schema,exclude_cols = exclude_cols)
        msg = 'Generating data for %s with metrics %s and dimensions %s and dates %s' %(self.name,metrics,categoricals,dates)
        logger.debug(msg)
        ts = TimeSeriesGenerator(metrics=metrics,ids=entities,
                                 days=days,seconds=seconds,
                                 freq=freq, categoricals = categoricals,
                                 dates = dates, timestamp = self._timestamp)    
        df = ts.execute()
        
        if self._dimension_table_name is not None:
            self.generate_dimension_data(entities, write = write)
        
        if write:
            for o in others:
                if o not in df.columns:
                    df[o] = None
            df['logicalinterface_id'] = ''
            df['devicetype'] = self.name
            df['format'] = ''
            df['updated_utc'] = None
            self.db.write_frame(table_name = self.name, df = df, 
                                schema = self._db_schema ,
                                timestamp_col = self._timestamp)
            
        for (at_name,at_table) in list(self.activity_tables.items()):
            adf = self.generate_activity_data(table_name = at_name, activities = at_table._activities,entities = entities, days = days, seconds = seconds, write = write)            
            msg = 'generated data for activity table %s' %at_name
            logger.debug(msg)
            
        for scd in list(self.scd.values()):
            sdf = self.generate_scd_data(scd_obj = scd, entities = entities, days = days, seconds = seconds, write = write)
            msg = 'generated data for scd table %s' %scd.name
            logger.debug(msg)
        
        return df
    
    def generate_activity_data(self,table_name, activities, entities,days,seconds,write=True):
        
        (metrics, dates, categoricals,others) = self.db.get_column_lists_by_type(table_name,self._db_schema,exclude_cols=[self._entity_id,'start_date','end_date'])
        metrics.append('duration')
        categoricals.append('activity')
        ts = TimeSeriesGenerator(metrics=metrics,dates = dates,categoricals=categoricals,
                                 ids = entities, days = days, seconds = seconds, freq = self._activity_frequency)
        ts.set_domain('activity',activities)               
        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        duration = df['duration'].abs()
        df['end_date'] = df['start_date'] + pd.to_timedelta(duration, unit='h')
        # probability that an activity took place in the interval
        p_activity = (days*60*60*24 +seconds) / pd.to_timedelta(self._activity_frequency).total_seconds() 
        is_activity = p_activity >= np.random.uniform(0,1,len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in ['duration',self._timestamp]]
        df = df[cols]
        if write:
            msg = 'Generated %s rows of data and inserted into %s' %(len(df.index),table_name)
            self.db.write_frame(table_name = table_name, df = df, schema = self._db_schema)       
        return df
    
    
    def generate_dimension_data(self,entities,write=True):
        
        (metrics, dates,categoricals, others ) = self.db.get_column_lists_by_type(
                                                self._dimension_table_name,
                                                self._db_schema,exclude_cols = [self._entity_id])
        
        rows = len(entities)
        data = {}
        
        for m in metrics:
            data[m] = MetricGenerator(m).get_data(rows=rows)

        for c in categoricals:
            data[c] = CategoricalGenerator(c).get_data(rows=rows)            
            
        data[self._entity_id] = entities
            
        df = pd.DataFrame(data = data)
        
        for d in dates:
            df[d] = DateGenerator(d).get_data(rows=rows)
            df[d] = pd.to_datetime(df[d])
            
        if write:
            self.db.write_frame(df,
                                table_name = self._dimension_table_name,
                                if_exists = 'replace',
                                schema = self._db_schema)
            
    def get_entity_filter(self):
        '''
        Get the list of entity ids that are valid for pipeline processing. 
        '''
        
        return self._entity_filter_list
        
    
    def get_last_checkpoint(self):
        '''
        Get the last checkpoint recorded for entity type
        '''
        
        (query,table) = self.db.query_column_aggregate(
                                table_name = self.checkpoint_table,
                                schema = self._db_schema,
                                column = 'TIMESTAMP',
                                aggregate = 'max')
        
        query.filter(table.c.entity_type_id==self._entity_type_id)                
        return query.scalar()
                
    
    def generate_scd_data(self,scd_obj,entities,days,seconds,write=True):
        
        table_name = scd_obj.name
        msg = 'generating data for %s for %s days and %s seconds' %(table_name,days,seconds)
        (metrics, dates, categoricals,others) = self.db.get_column_lists_by_type(table_name,self._db_schema,exclude_cols=[self._entity_id,'start_date','end_date'])
        msg = msg + ' with metrics %s, dates %s, categorials %s and others %s' %(metrics, dates, categoricals,others)
        logger.debug(msg)
        ts = TimeSeriesGenerator(metrics=metrics,dates = dates,categoricals=categoricals,
                                 ids = entities, days = days, seconds = seconds, freq = self._scd_frequency)
        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        # probability that a change took place in the interval
        p_activity = (days*60*60*24 +seconds) / pd.to_timedelta(self._scd_frequency).total_seconds() 
        is_activity = p_activity >= np.random.uniform(0,1,len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in [self._timestamp]]
        df = df[cols]
        df['end_date'] = None
        query,table = self.db.query(table_name, self._db_schema)
        try:
            edf = self.db.get_query_data(query)
        except:
            edf = pd.DataFrame()
        df = pd.concat([df,edf],ignore_index = True,sort=False)
        if len(df.index) > 0:
            df = df.groupby([self._entity_id]).apply(self._set_end_date)
            try:
                self.db.truncate(table_name, schema = self._db_schema)
            except KeyError:
                pass
            if write:
                msg = 'Generated %s rows of data and inserted into %s' %(len(df.index),table_name)
                logger.debug(msg)
                self.db.write_frame(table_name = table_name, df = df, schema = self._db_schema) 
        return df  
    
    def _get_scd_list(self):
        return [(s.output_item,s.table_name) for s in self._scd_stages ]
    
    def is_base_item(self,item_name):
        '''
        Base items are non calculated data items.
        '''
        
        item_type = self._data_items[item_name]['columnType']
        if item_type == 'METRIC':
            return True
        else:
            return False

    def is_data_item(self,name):
        '''
        Determine whether an item is a data item
        '''
        
        if name in [x.name for x in self._data_items]:
            return True
        else:
            return False
        
    
    def load_entity_type_functions(self,meta=None):
        
        '''
        Retrieve AS metadata for entity type. Returns a dict of
        properties to be set.
        '''
        
        if meta is None:
            meta = self.db.http_request(object_type = 'engineInput',
                                        object_name = self.logical_name,
                                        request= 'GET')
            try:
                meta = json.loads(meta)
            except (TypeError, json.JSONDecodeError):
                raise RuntimeError((
                        'API call to server did not retrieve valid entity '
                        ' type properties. No metadata received.'))
                    
        #build a dictionary of the schedule objects keyed by freq
        schedules_dict = build_schedules(meta.get('kpiDeclarations',[]))
        
        #build a dictionary of granularity objects keyed by granularity name
        grains_metadata = self.build_granularities(
                            grain_meta = meta['granularities'],
                            freq_lookup = meta.get('frequencies')
                            )        
        
        #build a dictionary of stages
        stages = self.build_stages(
                    function_meta = meta.get('kpiDeclarations',[]),
                    granularities_dict =grains_metadata
                    )
        
        params = {
                '_stages' : stages, 
                '_schedules_dict' : schedules_dict,
                '_granularities_dict' : grains_metadata
                }
        
        self.set_params(**params)
        
        return params
        
    def make_dimension(self,name = None, *args, **kw):
        '''
        Add dimension table by specifying additional columns
        
        Parameters
        ----------
        name: str
            dimension table name
        *args: sql alchemchy Column objects
        * kw: : schema
        '''
        kw['schema'] = self._db_schema
        if name is None:
            name = '%s_dimension' %self.name
            
        name = name.lower()
            
        self._dimension_table_name = name
    
        try:
            self._dimension_table = self.db.get_table(name,self._db_schema)
        except KeyError:
            dim = db_module.Dimension(
                self._dimension_table_name, self.db,
                *args,
                **kw
                )
            self._dimension_table = dim.table
            dim.create()
            msg = 'Creates dimension table %s' %self._dimension_table_name
            logger.debug(msg)
            
    def publish_kpis(self):
        '''
        Publish the stages assigned to this entity type to the AS Server
        '''   
        export = []
        stages = self.build_flat_stage_list()
            
        self.db.register_functions(stages)
                
        for s in stages:
            try:
                name = s.name
            except AttributeError:
                name = s.__class__.__name__
                logger.debug(('Function class %s has no name property.'
                              ' Using the class name'),
                             name)                                
             
            try:
                args = s._get_arg_metadata()
            except AttributeError:
                msg = ('Attempting to publish kpis for an entity type.'
                       ' Function %s has no _get_arg_spec() method.'
                       ' It cannot be published' ) %name
                raise NotImplementedError(msg)
                
            metadata  = { 
                    'name' : name ,
                    'args' : args
                    }
            export.append(metadata)
                
        response = self.db.http_request(object_type = 'kpiFunctions',
                                        object_name = self.logical_name,
                                        request = 'POST',
                                        payload = export)    
        return response

    def raise_error(self,exception,msg='',abort_on_fail=False,stageName=None):
        '''
        Raise an exception. Append a message and the current trace to the stacktrace.
        '''
        msg = msg + ' Trace follows: ' + str(self._trace)
         
        msg = 'Execution of stage %s failed because of %s: %s - %s ' %(stageName,type(exception).__name__,str(exception),msg)
        if abort_on_fail:
            try:
                tb = sys.exc_info()[2]
            except TypeError:
                raise StageException(msg, stageName)
            else:
                raise StageException(msg, stageName).with_traceback(tb)
        else:
            logger.warning(msg)
            msg = 'An exception occurred during execution of the pipeline stage %s. The stage is configured to continue after an execution failure' % (stageName)
            logger.warning(msg)

    def register(self,publish_kpis=False):
        '''
        Register entity type so that it appears in the UI. Create a table for input data.
        
        Parameters
        ----------
        credentials: dict
            credentials for the ICS metadata service

        '''
        cols = []
        columns = []
        table = {}
        table['name'] = self.logical_name
        table['metricTableName'] = self.name
        table['metricTimestampColumn'] = self._timestamp
        for c in self.db.get_column_names(self.table, schema = self._db_schema):
            cols.append((self.table,c,'METRIC'))
        if self._dimension_table is not None:            
            table['dimensionTableName'] = self._dimension_table_name
            for c in self.db.get_column_names(self._dimension_table, schema = self._db_schema):
                if c not in cols:
                    cols.append((self._dimension_table,c,'DIMENSION'))
        for (table_obj,column_name,col_type) in cols:
            msg = 'found %s column %s' %(col_type,column_name)
            logger.debug(msg)
            if column_name not in self.get_excluded_cols():
                data_type = table_obj.c[column_name].type
                if isinstance(data_type,DOUBLE) or isinstance(data_type,Float) or isinstance(data_type,Integer):
                    data_type = 'NUMBER'
                elif isinstance(data_type,VARCHAR) or isinstance(data_type,String):
                    data_type = 'LITERAL'
                elif isinstance(data_type,TIMESTAMP) or isinstance(data_type,DateTime):
                    data_type = 'TIMESTAMP'
                else:
                    data_type = str(data_type)
                    logger.warning('Unknown datatype %s for column %s' %(data_type,c))
                columns.append({ 
                        'name' : column_name,
                        'type' : col_type,
                        'columnName' : column_name,
                        'columnType'  : data_type,
                        'tags' : None,
                        'transient' : False
                        })                
        table['dataItemDto'] = columns
        if self._db_schema is not None:
            table['schemaName'] = self._db_schema
        else:
            try:
                table['schemaName'] = self.db.credentials['db2']['username']
            except KeyError:
                raise KeyError('No db2 credentials found. Unable to register table.')
        payload = [table]
        response = self.db.http_request(request='POST',
                                     object_type = 'entityType',
                                     object_name = self.name,
                                     payload = payload,
                                     raise_error = True)

        msg = 'Metadata registered for table %s '%self.name
        logger.debug(msg)
        if publish_kpis:
            self.publish_kpis()
        
        return response
    
        
    def trace_append(self,created_by,msg,log_method=None,**kwargs):
        '''
        Write to entity type trace
        '''
        self._trace.write(created_by = created_by,
                          log_method = log_method,
                          text = msg,
                          **kwargs)
        
    def separate_args(self,args):
        '''
        Separate arguments into columns and functions
        '''
        
        cols = []
        functions = []
        for a in args:
            if isinstance(a,Column):
                cols.append(a)
            else:
                functions.append(a)
            
        return (cols,functions)
            
    def set_custom_calendar(self,custom_calendar):
        '''
        Set a custom calendar for the entity type.
        '''
        if custom_calendar is not None:
            self._custom_calendar = custom_calendar
            
    def get_server_params(self):
        '''
        Retrieve the set of properties assigned through the UI
        Assign to instance variables        
        '''
        meta = self.db.http_request(object_type = 'constants',
                                    object_name = self.logical_name,
                                   request= 'GET')
        try:
            meta = json.loads(meta)
        except (TypeError, json.JSONDecodeError):
            params = {}
            logger.debug('API call to server did not retrieve valid entity type properties. No properties set.')
        else:
            params = {}
            for p in meta:
                key = p['name']
                if isinstance(p['value'],dict):
                    params[key] = p['value'].get('value',p['value'])
                else:
                    params[key] = p['value']
                logger.debug('Adding server property %s with value %s to entity type',key,params[key])
            self.set_params(**params)
        
        return params
            
     
    def _set_end_date(self,df):
        
        df['end_date'] = df['start_date'].shift(-1)
        df['end_date'] = df['end_date'] - pd.Timedelta(seconds = 1)
        df['end_date'] = df['end_date'].fillna(pd.Timestamp.max)
        return df
    
    def __str__(self):
        out = self.name
        return out
    

    
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self
    
    def write_unmatched_members(self,df):
        '''
        Write a row to the dimension table for every entity instance in the dataframe supplied
        '''
        
        if df.empty:
            return []
        else:            
            # add a dimension table if there is none
            if self._dimension_table_name is None:
                new_dim_name = '%s%s' %(self.name, self._auto_dim_suffix)
                self.make_dimension(name=new_dim_name)
                self.register()
            #get existing dimension keys
            ef = self.db.read_table(self._dimension_table_name, schema = self._db_schema, columns = [self._entity_id])
            ids = set(ef[self._entity_id].unique())
            #get new members from the dataframe supplied
            new_ids = set(df[self._entity_id].unique()) - ids
            #write
            self.db.start_session()
            table = self.db.get_table(self._dimension_table_name, self._db_schema)
            for i in new_ids:
                stmt = table.insert().values({self._entity_id:i})
                self.db.connection.execute(stmt)
            self.db.commit()
            return new_ids

    
    
class Granularity(object):
    
    '''
    Describe granularity level in terms of the pandas grouper that is used
    to aggregate to this grain and any custom calendar object that may have 
    been used to create it.
    
    Parameters:
    -----------
    name: str
        
    grouper: pandas Grouper object
    
    dimensions: list of data items used as dimension keys in group by
    
    entity_id: str. column name used to group by entity id. None if not
    an entity level summary
    
    freq = Pandas frequency string
    
    custom_calendar_keys: list of strs containing custom calendar data item
    names to be grouped on
    
    custom_calendar: function object
    
    '''
    
    def __init__(self,
                 name,
                 grouper,
                 dimensions,
                 entity_id,
                 freq,
                 custom_calendar_keys,
                 custom_calendar=None):
                     
        self.name = name
        self.grouper = grouper
        self.dimensions = dimensions
        self.entity_id = entity_id
        self.custom_calendar_key = custom_calendar_keys
        self.freq = freq
        self.custom_calendar = custom_calendar
        
    def __str__(self):
        
        return self.name


class Trace(object)    :
    '''
    Gather status and diagnostic information to report back in the UI
    '''
    
    save_trace_to_file = False
    
    primary_df = 'df'
    def __init__(self,name=None,parent=None,db=None):
        if parent is None:
            parent = self
        self.parent = parent            
        self.db = db
        self.auto_save = None
        self.auto_save_thread = None
        self.stop_event = None
        if name is None:
            name = self.build_trace_name()
        self.name = name
        self.data = []
        self.df_cols = set()
        self.df_index = set()
        self.df_count = 0
        self.prev_ts = dt.datetime.utcnow()
        logger.debug('Starting trace')
        logger.debug('Trace name: %s',self.name )
        logger.debug('auto_save %s',self.auto_save)
        self.write(created_by=parent,text='Trace started. ')
        
        
    def as_json(self):        
        return json.dumps(self.data,indent=4)        
        
    def build_trace_name(self):
        
        execute_str = f'{dt.datetime.utcnow():%Y%m%d%H%M%S%f}' 
        
        return 'auto_trace_%s_%s' %(self.parent.__class__.__name__,
                               execute_str)
        
    def reset(self,name=None,auto_save=None):
        '''
        Clear trace information and rename trace
        '''
        self.df_cols = set()
        self.df_index = set()
        self.df_count = 0        
        self.prev_ts = dt.datetime.utcnow()
        self.auto_save = auto_save
        if self.auto_save_thread is not None:
            logger.debug('Reseting trace %s', self.name)
            self.stop()
        self.data = []
        if name is None:
            name = self._trace.build_trace_name()
        self.name = name
        logger.debug('Started a new trace %s ', self.name)
        if self.auto_save is not None:
            logger.debug('Initiating auto save for trace')
            self.stop_event = threading.Event()
            self.auto_save_thread = threading.Thread(
                    target=self.run_auto_save,
                    args=[self.stop_event])
            self.auto_save_thread.start()
            
    def run_auto_save(self,stop_event):
        '''
        Run auto save. Auto save is intended to be run in a separate thread.
        '''
        last_trace = None
        next_autosave = dt.datetime.utcnow()
        while not stop_event.is_set():
            if next_autosave >= dt.datetime.utcnow():
                if self.data != last_trace:
                    logger.debug('Auto save trace %s' %self.name)
                    self.save()
                    last_trace = self.data
                next_autosave = dt.datetime.utcnow() + dt.timedelta(seconds = self.auto_save)                
            time.sleep(0.1)
        logger.debug('%s autosave thread has stopped',self.name)
        
    def save(self):
        '''
        Write trace to COS
        '''
        
        if len(self.data) == 0:
            trace = None
            logger.debug('Trace is empty. Nothing to save.')
        else:
            if self.db is None:
                logger.warning('Cannot save trace. No db object supplied')
                trace = None
            else:
                trace = str(self.as_json())
                self.db.cos_save(persisted_object=trace,
                         filename=self.name,
                         binary=False)
                logger.debug('Saved trace to cos %s', self.name)
        try:
            save_to_file = self.parent.save_trace_to_file
        except AttributeError:
            save_to_file = self.save_trace_to_file
        if trace is not None and save_to_file:
            with open('%s.json' %self.name, 'w') as fp:
                fp.write(trace)
            logger.debug('wrote trace to file %s.json' %self.name)
        
        return trace

    def stop(self):
        '''
        Stop autosave thead
        '''
        self.auto_save = None
        if not self.stop_event is None:
            self.stop_event.set()
        if self.auto_save_thread is not None:
            self.auto_save_thread.join()
            self.auto_save_thread = None
            logger.debug('Stopping autosave on trace %s',self.name)
            
    def update_last_entry(self,last,**kw):
        '''
        Update the last trace entry. Include the contents of **kw.
        '''
        kw['updated'] = dt.datetime.utcnow()
        
        try:
            last = self.data.pop()
        except IndexError:
            last = {}
            logger.debug(('Tried to update the last entry of an empty trace.'
                          ' Nothing to update. New entry will be inserted.'))
        last = {**last,**kw}
        self.data.append(last)
        
        return last
          
        
    def write(self,created_by,text,log_method=None,**kwargs):
        ts = dt.datetime.utcnow()
        text = str(text)
        elapsed = (ts - self.prev_ts).total_seconds()
        self.prev_ts = ts
        kwargs['elapsed_time'] = elapsed
        try:
            created_by_name = created_by.name
        except AttributeError:
            created_by_name = str(created_by)
        entry = { 'timestamp' : str(ts),
          'created_by' : created_by_name,
          'text': text,
          'elapsed_time' : elapsed
        }
        for key,value in list(kwargs.items()):            
            if not isinstance(value,str):
                kwargs[key] = str(value)
        entry = {**entry,**kwargs}
        
        # The trace can track changes in a dataframe between writes
        # The dataframe my ne passed using the df argument
        df = kwargs.get(self.primary_df,None)
        
        if df is not None:
            (df_info,msg) = self._df_as_dict(df=df,prefix=self.primary_df)
            entry = {**entry,**df_info}
        
        self.data.append(entry)
         
        try:
            if log_method is not None:
                log_method(text)
        except TypeError:
            msg = 'A write to the trace called an invalid logging method. Logging as warning: %s' %text
            logger.warning(msg)
            
    def _df_as_dict(self,df,prefix):
        
        if df is None:
            logger.warning('Trace info ignoring null dateframe')
            df = pd.DataFrame()
        elif not isinstance(df,pd.DataFrame):
            logger.warning('Trace info ignoring non dataframe %s of type %s', df, type(df))
            df = pd.DataFrame()
        
        if len(df.index)>0:
            msg = ''
            data = {}
            prev_count = self.df_count
            prev_cols = self.df_cols  
            self.df_count = len(df.index)
            if df.index.names is None:
                self.df_index = {}
            else:
                self.df_index = set(df.index.names)
            self.df_cols = set(df.columns)
            # stats
            data['%s_count' %prefix] = self.df_count
            data['%s_index' %prefix] = self.df_index
            data['%s_columns' %prefix] = self.df_cols        
            #look at changes
            if self.df_count != prev_count:
                data['%s_rowcount_change' %prefix] = self.df_count - prev_count
            if len(self.df_cols-prev_cols)>0:
                data['%s_added_columns' %prefix] = self.df_cols-prev_cols
            if len(prev_cols-self.df_cols)>0:
                data['%s_added_columns' %prefix] = prev_cols-self.df_cols
        else:
            data = {'df': None}
            msg = ''
            
        return(data,msg)
    
    def __str__(self):
        
        out = ''
        for entry in self.data:
            out = out + entry['text'] 
            
        return out
    

                
    


class Model(object):
    '''
    Predictive model
    '''
    def __init__(self, name , estimator, estimator_name , params,
                 features, target, eval_metric_name, eval_metric_train,
                 shelf_life_days):
        
        self.name = name
        self.target = target
        self.features = features
        self.estimator = estimator
        self.estimator_name = estimator_name
        self.params = params
        self.eval_metric_name = eval_metric_name
        self.eval_metric_train = None
        self.eval_metric_test = None
        if self.estimator is None:
            self.trained_date = None
        else:
            self.trained_date = dt.datetime.utcnow()
        if self.trained_date is not None and shelf_life_days is not None:
            self.expiry_date = self.trained_date + dt.timedelta(days = shelf_life_days)
        else:
            self.expiry_date = None
        self.viz = {}

    def add_viz(self, name, cos_credentials, viz_obj, bucket):
        self.db.cos_save(persisted_object=viz_obj, filename= name, bucket=bucket)
        
    def fit(self,df):
        self.estimator = self.estimator.fit(df[self.features],df[self.target])
        self.trained_date = dt.datetime.utcnow()
        self.eval_metric_train = self.score(df)
        if self.shelf_life_days is not None:
            self.expiry_date = self.trained_date + dt.timedelta(days = self.shelf_life_days)        
        msg= 'trained model %s with evaluation metric value %s' %(self.name,self.eval_metric_train)
        logger.info(msg)
        return self.estimator

    def predict(self,df):
        result = self.estimator.predict(df[self.features])
        msg= 'predicted using model %s' %(self.name)
        logger.info(msg)        
        return result

    def score (self,df):
        result = self.estimator.score(df[self.features],df[self.target])     
        return result    
    
    def test(self,df):
        self.eval_metric_test = self.score(df)
        msg= 'evaluated model %s with evaluation metric value %s' %(self.name,self.eval_metric_test)
        logger.info(msg)        
        return self.eval_metric_test
        
    def save(self, cos_credentials, bucket):
        self.db.cos_save(persisted_object=self, filename=self.name, bucket=bucket)
        
    def __str__(self):
        out = {}
        output = ['name','target','features', 'estimator_name','eval_metric_name','eval_metric_train','eval_metric_test','trained_date','expiry_date']
        for o in output:
            try:
                out[o] = getattr(self, o)
            except AttributeError:
                out[o] = '_missing_'
        if out['trained_date'] is not None:
            out['trained_date'] = out['trained_date'].isoformat()
        if out['expiry_date'] is not None:
            out['expiry_date'] = out['expiry_date'].isoformat()
        return json.dumps(out,indent=1)