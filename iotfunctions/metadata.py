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
from .db import Database, TimeSeriesTable
from .automation import TimeSeriesGenerator
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE

logger = logging.getLogger(__name__)
    
class EntityType(object):
    '''
    Analytic service entity type    
    '''    
    
    log_table = 'KPI_LOGGING'
    checkpoint_table = 'KPI_CHECKPOINT'
    _timestamp = 'evt_timestamp'
    
    def __init__ (self,name,db, timestamp_col, *args, **kw):
        self.name = name
        if db is None:
            db = Database()
        self.db = db
        if not timestamp_col is None:
            self._timestamp = timestamp_col
        try:
            self.table = self.db.get_table(self.name)
        except KeyError:
            ts = TimeSeriesTable(self.name ,self.db, *args, **kw)
            self.table = ts.table
            self.table.create(self.db.connection)
        
        
    def get_params(self):
        
        params = {
                'entity_type_name' : self.name,
                '_timestamp' : self._timestamp,
                'db' : self.db,
                'source_table' : self.table
                }
        return params
        
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
                if isinstance(data_type,DOUBLE):
                    data_type = 'NUMBER'
                elif isinstance(data_type,TIMESTAMP):
                    data_type = 'TIMESTAMP'
                    dates.append(c)
                elif isinstance(data_type,VARCHAR):
                    data_type = 'LITERAL'
                else:
                    data_type = str(data_type)
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
        table['schemaName'] = self.db.credentials['db2']['username']
        payload = [table]
        response = self.db.http_request(request='POST',
                                     object_type = 'entityType',
                                     object_name = self.name,
                                     payload = payload)

        msg = 'Metadata registerd for table %s '%self.name
        logger.debug(msg)
        return response
        
        
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
        
        query, log = self.db.query(self.log_table)
        query = query.filter(log.c.entity_type==self.name).\
                      order_by(log.c.timestamp_utc.desc()).\
                      limit(rows)
        df = self.db.get_query_data(query)
        return df
    
    def generate_data(self, entities, days, seconds = 0, freq = '1min', write=True):
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
        metrics = []
        categoricals = []
        dates = []
        others = []
        for c in self.db.get_column_names(self.table):
            if not c in ['deviceid','devicetype','format','updated_utc','logicalinterface_id',self._timestamp]:
                data_type = self.table.c[c].type
                if isinstance(data_type,DOUBLE) or isinstance(data_type,Float):
                    metrics.append(c)
                elif isinstance(data_type,VARCHAR) or isinstance(data_type,String):
                    categoricals.append(c)
                elif isinstance(data_type,TIMESTAMP) or isinstance(data_type,DateTime):
                    dates.append(c)
                else:
                    others.append(c)
                    msg = 'Encountered column %s of unknown data type %s' %(c,data_type.__class__.__name__)
                    raise TypeError(msg)
        msg = 'Generating data for %s with metrics %s and dimensions %s and dates %s' %(self.name,metrics,categoricals,dates)
        logger.debug(msg)
        ts = TimeSeriesGenerator(metrics=metrics,ids=entities,days=days,seconds=seconds,freq=freq, categoricals = categoricals, dates = dates)
        df = ts.execute()
        if write:
            for o in others:
                if o not in df.columns:
                    df[o] = None
            df['logicalinterface_id'] = ''
            df['devicetype'] = self.name
            df['format'] = ''
            df['updated_utc'] = None
            self.db.write_frame(table_name = self.name, df = df)
        
        return df
    
    
        
        
        
        
        

        
            
            
    
    