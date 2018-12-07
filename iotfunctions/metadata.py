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
import json
import urllib3
import pandas as pd
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, Float
from .db import Database, TimeSeriesTable, ActivityTable, SlowlyChangingDimension, Dimension
from .automation import TimeSeriesGenerator, DateGenerator, MetricGenerator, CategoricalGenerator
from .pipeline import CalcPipeline
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE

logger = logging.getLogger(__name__)

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
    _start_entity_id = 73000 #used to build entity ids
    _auto_entity_count = 5 #default number of entities to generate data for
    # These two columns will be available in the dataframe of a pipeline
    _entity_id = 'deviceid' #identify the instance
    _timestamp_col = '_timestamp' #copy of the event timestamp from the index
    # These two column names will be used to name the index of a pipeline dataframe
    _timestamp = 'evt_timestamp'
    _df_index_entity_id = 'id'
       
    
    def __init__ (self,name,db, *args, **kwargs):
        self.name = name
        self.activity_tables = {}
        self.scd = {}
        self.db = db
        if self.db is not None:
            self.tenant_id = self.db.tenant_id
        self.logical_name = None
        self._db_connection_dbi = None
        self._dimension_table = None
        self._dimension_table_name = None
        self._db_schema = None
        self._data_items = None
        self.set_params(**kwargs)
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
        
        table = ActivityTable(name, self.db,*args, **kwargs)
        try:
            sqltable = self.db.get_table(name, self._db_schema)
        except KeyError:
            self.create()
        self.activity_tables[name] = table
        
        
        
    def add_slowly_changing_dimension(self,property_name,datatype):
        '''
        add a slowly changing dimension table containing a single property for this entity type
        
        parameters
        ----------
        property_name : str
            name of property, e.g. firmware_version (lower case, no database reserved words)
        datatype: sqlalchemy datatype
        '''
        
        name= '%s_scd_%s' %(self.name,property_name)

        table = SlowlyChangingDimension(name = name,
                                   database=self.db,
                                   property_name = property_name,
                                   datatype = datatype)        
        try:
            sqltable = self.db.get_table(name,self._db_schema)
        except KeyError:
            table.create()
        self.scd[property_name] = table
        
    def drop_child_tables(self):
        '''
        Drop all child tables
        '''
        tables = []
        tables.extend(self.activity_tables.values())
        tables.extend(self.scd.values())
        [self.db.drop_table(x) for x in tables]
        msg = 'dropped tables %s' %tables
        logger.info(msg)
                
    
    def get_calc_pipeline(self,stages=None):
        '''
        Get a CalcPipeline object
        '''
        return CalcPipeline(stages=stages, entity_type = self)
        
        
    def get_data(self,start_ts =None,end_ts=None,entities=None):
        '''
        Retrieve entity data
        '''
        (query,table) = self.db.query(self.name)
        if not start_ts is None:
            query = query.filter(table.c[self._timestamp] >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c[self._timestamp] <= end_ts)  
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))
        df = pd.read_sql(query.statement, con = self.db.connection)
        
        return df       
        
        
    def get_log(self,rows = 100):
        '''
        Get KPI execution log info. Returns a dataframe.
        '''
        query, log = self.db.query(self.log_table)
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
            self.db.drop_table(self.name)
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
            
        for at in list(self.activity_tables.values()):
            adf = at.generate_data(entities = entities, days = days, seconds = seconds, write = write)
            msg = 'generated data for activity table %s' %at.name
            logger.debug(msg)
            
        for scd in list(self.scd.values()):
            sdf = scd.generate_data(entities = entities, days = days, seconds = seconds, write = write)
            msg = 'generated data for scd table %s' %scd.name
            logger.debug(msg)
        
        return df
    
    def register(self):
        '''
        Register entity type so that it appears in the UI. Create a table for input data.
        
        Parameters
        ----------
        credentials: dict
            credentials for the ICS metadata service

        '''
        columns = []
        dates = []
        for c in self.db.get_column_names(self.table):
            if c not in ['logicalinterface_id','format','updated_utc']:
                data_type = self.table.c[c].type
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
                        'name' : c,
                        'type' : 'METRIC',
                        'columnName' : c,
                        'columnType'  : data_type,
                        'tags' : None,
                        'transient' : False
                        })
        table = {}
        table['name'] = self.name
        table['dataItemDto'] = columns
        table['metricTableName'] = self.name
        table['metricTimestampColumn'] = self._timestamp
        try:
            table['schemaName'] = self.db.credentials['db2']['username']
        except KeyError:
            raise KeyError('No db2 credentials found. Unable to register table.')
        payload = [table]
        response = self.db.http_request(request='POST',
                                     object_type = 'entityType',
                                     object_name = self.name,
                                     payload = payload)

        msg = 'Metadata registerd for table %s '%self.name
        logger.debug(msg)
        return response
    
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
        kw = {
                'schema' : self._db_schema
                }
        if name is None:
            name = '%s_dimension' %self.name
            
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
        
        self.db.write_frame(df,table_name = self._dimension_table_name, if_exists = 'replace')
        
    
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self        
        
        
        
        

        
            
            
    
    