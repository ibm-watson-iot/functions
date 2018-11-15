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
from .db import Database, TimeSeriesTable

logger = logging.getLogger(__name__)
    

class EntityType(object):
    
    log_table = 'KPI_LOGGING'
    checkpoint_table = 'KPI_CHECKPOINT'
    
    '''
    Analytic service entity type
    '''
    def __init__ (self,name,credentials, *args, **kw):
        self.name = name
        self.database = Database(credentials = credentials, start_session = False)
        self.credentials = credentials
        try:
            self.table = self.database.get_table(self.name)
        except KeyError:
            ts = TimeSeriesTable(self.name ,self.database, *args, **kw)
            self.table = ts.table
            self.database.create_all()
        self.register()
        
    def register(self):
        '''
        Register entity type so that it appears in the UI. Create a table for input data.
        
        Parameters
        ----------
        credentials: dict
            credentials for the ICS metadata service

        '''
        columns = []
        for c in self.database.get_column_names(self.table):
            if c not in ['logicalinterface_id','format','updated_utc']:
                data_type = str(self.table.c[c].type)
                if data_type == 'FLOAT':
                    data_type = 'NUMBER'
                elif data_type == 'DATETIME':
                    data_type = 'TIMESTAMP'
                elif data_type[:7] == 'VARCHAR':
                    data_type = 'LITERAL'                 
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
        table['metricTimestampColumn'] = 'evt_timestamp'
        table['schemaName'] = self.credentials['username']
        payload = [table]
        try:
            as_api_host = self.credentials['as_api_host']
        except KeyError:
            return 'No as_api_host supplied in credentials - did not register metadata'
        else:
            http = urllib3.PoolManager()
            encoded_payload = json.dumps(payload).encode('utf-8')    
            headers = {
                'Content-Type': "application/json",
                'X-api-key' : self.credentials['as_api_key'],
                'X-api-token' : self.credentials['as_api_token'],
                'Cache-Control': "no-cache",
            }    
            url = 'http://%s/api/meta/v1/%s/entityType' %(as_api_host,self.credentials['tennant_id'])
            r = http.request("POST", url, body = encoded_payload, headers=headers)
            print ('Metadata Registered: ',r.data.decode('utf-8'))
            return r.data.decode('utf-8')
        
    def get_log(self,rows = 100):
        
        query, log = self.database.query(self.log_table)
        query = query.filter(log.c.entity_type==self.name).\
                      order_by(log.c.timestamp_utc.desc()).\
                      limit(rows)
        df = self.database.get_query_data(query)
        return df
        
        
        
        
        

        
            
            
    
    