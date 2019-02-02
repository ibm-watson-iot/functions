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
import pandas as pd
from collections import OrderedDict
from pandas.api.types import is_bool, is_number, is_string_dtype, is_timedelta64_dtype
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, Float, func
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE
from .db import Database, TimeSeriesTable, ActivityTable, SlowlyChangingDimension, Dimension
from .automation import TimeSeriesGenerator, DateGenerator, MetricGenerator, CategoricalGenerator
from .pipeline import CalcPipeline
from .util import log_df_info

logger = logging.getLogger(__name__)

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
        Additional positional arguments are used to add the list of SQL Alchemy Column
        objects contained within this table. Similar to the style of a CREATE TABLE sql statement.
        There is no need to specify column names if you are using an existing database table as
        an entity type.
    **kwargs
        Additional keywork args. 
        _timestamp: str
            Overide the timestamp column name from the default of 'evt_timestamp'
    '''    
            
    log_table = 'KPI_LOGGING'
    checkpoint_table = 'KPI_CHECKPOINT'
    # These two columns will be available in the dataframe of a pipeline
    _entity_id = 'deviceid' #identify the instance
    _timestamp_col = '_timestamp' #copy of the event timestamp from the index
    # This column will identify an instance in the index
    _df_index_entity_id = 'id'
    # generator
    _scd_frequency = '2D'
    _activity_frequency = '3D'
    _start_entity_id = 73000 #used to build entity ids
    _auto_entity_count = 5 #default number of entities to generate data for
    # variabes that will be set when run inside an engine pipeline, or may be set as kwargs
    _entity_type_id = None
    logical_name = None
    _timestamp = 'evt_timestamp'
    _dimension_table_name = None
    _db_connection_dbi = None
    _db_schema = None
    _data_items = None
    tenant_id = None
    # processing defaults
    _checkpoint_by_entity = True # manage a separate checkpoint for each entity instance
    _pre_aggregate_time_grain = None # aggregate incoming data before processing
    _auto_read_from_ts_table = True # read new data from designated time series table for the entity
    _pre_agg_rules = None # pandas agg dictionary containing list of aggregates to apply for each item
    _pre_agg_outputs = None #dictionary containing list of output items names for each item
    def __init__ (self,name,db, *args, **kwargs):
        self.name = name.lower()
        self.activity_tables = {}
        self.scd = {}
        self.db = db
        if self.db is not None:
            self.tenant_id = self.db.tenant_id
        self._system_columns = [self._entity_id,self._timestamp_col,'logicalinterface_id',
                                'devicetype','format','updated_utc', self._timestamp]
        #pipeline processing options
        self._custom_exclude_col_from_auto_drop_nulls = []
        self._drop_all_null_rows = True
        #pipeline work variables stages
        self._dimension_table = None
        self._scd_stages = []
        self._custom_calendar = None
        self._is_initial_transform = True
        self._trace = Trace(self)
        self._is_preload_complete = False
        #initialize
        self.set_params(**kwargs)
        if self._db_schema is None:
            msg = 'No _db_schema specified in **kwargs. Using default database schema.'
            logger.warn(msg)
        if self.logical_name is None:
            self.logical_name = self.name         
        if name is not None and db is not None:            
            try:
                self.table = self.db.get_table(self.name,self._db_schema)
            except KeyError:
                ts = TimeSeriesTable(self.name ,self.db, *args, **kwargs)
                self.table = ts.table
                self.db.create()
                msg = 'Create table %s' %self.name
                logger.debug(msg)
        else:
            msg = 'Created a logical entity type. It is not connected to a real database table, so it cannot perform any database operations.'
            logger.debug(msg)
            
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
        table = ActivityTable(name, self.db,*args, **kwargs)
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
        table = SlowlyChangingDimension(name = name,
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
        
        
    def cos_save(self):
        
        name = ['entity_type', self.name]
        name = '.'.join(name)
        self.db.cos_save(self, name)
                
        
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
                
    
    def get_calc_pipeline(self,stages=None):
        '''
        Make a new CalcPipeline object. Reset processing variables.
        '''
        self._scd_stages = []
        self._custom_calendar = None
        self._is_initial_transform = True
        return CalcPipeline(stages=stages, entity_type = self)
        
        
    def get_data(self,start_ts =None,end_ts=None,entities=None,columns=None):
        '''
        Retrieve entity data at input grain or preaggregated
        '''
        
        if self._pre_aggregate_time_grain is None:    
            df = self.db.read_table(
                    table_name = self.name,
                    schema = self._db_schema,
                    parse_dates = None,
                    columns = columns,
                    start_ts = start_ts,
                    end_ts = end_ts,
                    entities = entities,
                    dimension = self._dimension_table_name
                    ) 
            self.trace_append(' Read source data from date: %s' %start_ts)
            
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

            msg = ' Read input data start %s aggregated to %s'  %(start_ts,self._pre_aggregate_time_grain)
            self.trace_append(self,'Get time series data',msg=msg)
            
        msg = log_df_info(df,'after read source')
        self.trace_append(self,'Get time series data',msg=msg)

        return df   

    def get_data_items(self):
        '''
        Get the list of data items defined
        :return: list of data items
        '''
        return self._data_items
        
        
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
            dim = Dimension(
                self._dimension_table_name, self.db,
                *args,
                **kw
                )
            self._dimension_table = dim.table
            dim.create()
            msg = 'Creates dimension table %s' %self._dimension_table_name
            logger.debug(msg)
            
            
    def raise_error(self,exception,msg='',abort_on_fail=False):
        '''
        Raise an exception. Append a message and the current trace to the stacktrace.
        '''
        msg = msg + '. ' + str(self._trace)
        
        msg = '%s - %s : ' %(str(exception),msg)
        if abort_on_fail:
            try:
                tb = sys.exc_info()[2]
            except TypeError:
                raise type(exception)(msg)
            else:
                raise type(exception)(msg).with_traceback(tb)
        else:
            logger.warn(msg)
            msg = 'An exception occured during execution of a pipeline stage. The stage is configured to continue after an execution failure'
            logger.warn(msg)
    
    def register(self):
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
        if self._dimension_table is not None:
            table['dimensionTableName'] = self._dimension_table_name
            for c in self.db.get_column_names(self._dimension_table, schema = self._db_schema):
                cols.append((self._dimension_table,c,'DIMENSION'))
        for c in self.db.get_column_names(self.table, schema = self._db_schema):
            cols.append((self.table,c,'METRIC'))
        for (table_obj,column_name,col_type) in cols:
            msg = 'found %s column %s' %(col_type,column_name)
            logger.debug(msg)
            if column_name not in ['logicalinterface_id','format','updated_utc']:
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
                                     payload = payload)

        msg = 'Metadata registered for table %s '%self.name
        logger.debug(msg)
        #response = self.cos_save()
        #msg = 'Entity type saved to cos %s '%response
        #logger.debug(msg)
        return response
    
        
    def trace_append(self,created_by,title,msg,log_method=None,**kwargs):
        '''
        Write to entity type trace
        '''
        self._trace.write(created_by = created_by,
                          title = title,
                          log_method = log_method,
                          text = msg,
                          **kwargs)
        
            
    def set_custom_calendar(self,custom_calendar):
        '''
        Set a custom calendar for the entity type.
        '''
        if custom_calendar is not None:
            self._custom_calendar = custom_calendar
     
    def _set_end_date(self,df):
        
        df['end_date'] = df['start_date'].shift(-1)
        df['end_date'] = df['end_date'] - pd.Timedelta(seconds = 1)
        df['end_date'] = df['end_date'].fillna(pd.Timestamp.max)
        return df
    
    def __str__(self):
        out = self.name
        return out
    
    def exec_pipeline(self, *args, to_csv = False, register = False, start_ts = None, publish = False):
        '''
        Test an AS function instance using entity data. Provide one or more functions as args.
        '''
        stages = list(args)
        pl = self.get_calc_pipeline(stages=stages)
        df = pl.execute(to_csv = to_csv, register = register, start_ts = start_ts)
        if publish:
            pl.publish()
        return df
        
    
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self


class Trace(object)    :
    '''
    Gather status and diagnostic information to report back in the UI
    '''    
    def __init__(self,parent=None):
        self.parent = parent
        self.data = OrderedDict()
        
    def write(self,created_by,title,text,log_method=None,**kwargs):

        entry = { 'timestamp' : str(dt.datetime.utcnow()),
                  'created_by' : str(created_by),
                  'text': str(text)
                }
        for key,value in list(kwargs.values()):
            if isinstance(value,pd.DataFrame):
                kwargs[key] = self._df_as_dict(value)
            elif not isinstance(value,str):
                kwargs[key] = str(value)
        entry = {**entry,**kwargs}
        
        try:
            self.data[title].append(entry)
        except KeyError:
            self.data[title] = [entry]
            
        if log_method is not None:
            log_method(text)
            
    def _df_as_dict(self,df):
        
        data = {}
        data['df_count'] = len(df.index)
        data['df_index'] = list(df.index.names)
        data['df_columns'] = list(df.columns)
        return(data)
        
    def as_json(self):
        
        return json.dumps(self.data)
    
    def __str__(self):
        
        out = ''
        for title,values in list(self.data.items()):
            out = out + title + '>'
            for v in values:
                out = out + v['text'] + '; '
            out = out + ' |'
            
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
            
        
        
    

        

        
        
        
        

        
            
            
    
    
    