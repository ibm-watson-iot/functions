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
    '''
    Analytic service entity type
    '''
    def __init__ (self,name,credentials, *args, **kw):
        self.name = name
        self.database = Database(credentials = credentials, start_session = False)
        self.credentials = credentials
        self.ts = TimeSeriesTable(self.name ,self.database, *args, **kw)
        self.table = self.ts.table
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
        for c in self.ts.get_columns():
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

class ShiftCalendar(object):
    '''
    Generate data for a shift calendar using a shift_definition in the form of a dict keyed on shift_id
    Dict contains a tuple with the start and end hours of the shift expressed as numbers. Example:
          {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 5.5)
           },    
    '''
    def __init__ (self,shift_definition=None):
        if shift_definition is None:
            shift_definition = {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 5.5)
           }
        self.shift_definition = shift_definition
    
    def get_data(self,start_date,end_date):
        start_date = start_date.date()
        end_date = end_date.date()
        dates = pd.DatetimeIndex(start=start_date,end=end_date,freq='1D').tolist()
        dfs = []
        for shift_id,start_end in list(self.shift_definition.items()):
            data = {}
            data['shift_day'] = dates
            data['shift_id'] = shift_id
            data['end_date'] = [x+dt.timedelta(hours=start_end[1]) for x in dates]
            index = [x+dt.timedelta(hours=start_end[0]) for x in dates]
            dfs.append(pd.DataFrame(data, index = index))
        df = pd.concat(dfs)
        df.index = pd.to_datetime(df.index)
        df['end_date'] = pd.to_datetime(df['end_date'])
        return df
            
            
            
    
    