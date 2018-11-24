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
import datetime as dt
import logging
import urllib3
import json
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, MetaData, ForeignKey, create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.exc import NoSuchTableError
import ibm_db
import ibm_db_dbi

logger = logging.getLogger(__name__)

class Database(object):
    '''
    Use Database objects to establish database connectivity, manage database metadata and sessions, build queries and write DataFrames to tables.
    
    Parameters:
    -----------
    credentials: dict (optional)
        Database credentials. If none specifiedm use DB_CONNECTION_STRING environment variable
    start_session: bool
        Start a session when establishing connection
    echo: bool
        Output sql to log
    '''
    
    def __init__(self,credentials = None, start_session = False, echo = False, tenant_id = None):
        
        logger.debug('Requesting db connection')
        self.credentials = {}
        if tenant_id is None:
            try:
                tenant_id = credentials['tenant_id']
            except (KeyError,TypeError):
                try:
                    tenant_id = credentials['tennant_id']
                except (KeyError,TypeError):
                    try:
                        tenant_id = credentials['tennantId']
                    except (KeyError,TypeError):        
                        msg = 'No tenant_id supplied. You will not be able to use the db object to communicate with the API'
                        logger.info(msg)
                        tenant_id = None
        self.credentials['tenant_id'] = tenant_id
        try:
            self.credentials['iotp']= credentials['iotp']
        except (KeyError,TypeError):
            self.credentials['iotp'] = None
        try:
            self.credentials['db2']= credentials['db2']
        except (KeyError,TypeError):
            try:
                credentials['host']
            except (KeyError,TypeError):
                try:
                    connection_string = os.environ.get('DB_CONNECTION_STRING')
                except KeyError:
                    raise ValueError('Unable to connect to the database. Supply valid credentials or provide a DB_CONNECTION_STRING environment variable')
                else:
                    if connection_string.endswith(';'):
                        connection_string = connection_string[:-1]
                        ev = dict(item.split("=") for item in connection_string.split(";"))
                        self.credentials['db2'] =  {
                                    "username": ev['UID'],
                                    "password": ev['PWD'],
                                    "database": ev['DATABASE'] ,
                                    "port": ev['PORT'],
                                    "host": ev['HOSTNAME'] 
                            }
            else:
                self.credentials['db2']= credentials
                logger.warning('Old style credentials still work just fine, but will be depreciated in the future. Check the usage section of the UI for the updated credentials dictionary')
                self.credentials['as']= credentials
                
        try:
            self.credentials['message_hub']= credentials['messageHub']
        except (KeyError,TypeError):
            self.credentials['message_hub'] = None
            msg = 'Unable to locate message_hub credentials. Database object created, but it will not be able interact with message hub.'
            logger.debug(msg)
        
        try:
            self.credentials['cos']= credentials['cos']
        except (KeyError,TypeError):
            self.credentials['cos'] = None        
            msg = 'Unable to locate cos credentials. Database object created, but it will not be able interact with object storage'
            logger.debug(msg)

        try:
            self.credentials['config']= credentials['config']
        except (KeyError,TypeError):
            self.credentials['config'] = None 
            msg = 'Unable to locate config credentials. Database object created, but it will not be able interact with object storage'
            logger.debug(msg)
        
        try:
            as_api_host = credentials['as_api_host']
            as_api_key = credentials['as_api_key'] 
            as_api_token = credentials['as_api_token']
        except (KeyError,TypeError):
            try:
               as_api_host = os.environ.get('API_BASEURL')
               as_api_key = os.environ.get('API_KEY')
               as_api_token = os.environ.get('API_TOKEN')
            except KeyError:
               as_api_host = None
               as_api_key = None
               as_api_token = None
               msg = 'Unable to locate as credentials or environment variable. db will not be able to connect to the AS API'
               logger.debug(msg)

        self.credentials['as'] = {
                'host' : as_api_host,
                'api_key' : as_api_key,
                'api_token' : as_api_token                
                }

        self.tenant_id = tenant_id
        
        connection_string = 'db2+ibm_db://%s:%s@%s:%s/%s;' %(self.credentials['db2']['username'],
                                                         self.credentials['db2']['password'],
                                                         self.credentials['db2']['host'],
                                                         self.credentials['db2']['port'],
                                                         self.credentials['db2']['database'])            
        
        self.connection =  create_engine(connection_string, echo = echo)
        self.Session = sessionmaker(bind=self.connection)
        if start_session:
            self.session = self.Session()
        else:
            self.session = None
        
        self.metadata = MetaData(self.connection)
        #TDB support alternative schema
        self.schema = None
        logger.debug('Db connection established')
        self.http = urllib3.PoolManager()
        
    def http_request(self, object_type,object_name, request, payload):
        '''
        Make an api call to AS
        
        Parameters
        ----------
        object_type : str 
            function, entityType
        object_name : str
            name of object
        request : str
            GET, POST, DELETE, PUT
        payload : dict
            Dictionary will be encoded as JSON
        
        '''
        base_url = 'http://%s/api' %(self.credentials['as']['host'])
        self.url = {}
        self.url[('entityType','POST')] = '/'.join([base_url,'meta','v1',self.tenant_id,object_type])
        self.url[('entityType','GET')] = '/'.join([base_url,'meta','v1',self.tenant_id,object_type,object_name])
        self.url[('function','GET')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
        self.url[('function','DELETE')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
        self.url[('function','PUT')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
            
        encoded_payload = json.dumps(payload).encode('utf-8')
        
        headers = {
            'Content-Type': "application/json",
            'X-api-key' : self.credentials['as']['api_key'],
            'X-api-token' : self.credentials['as']['api_token'],
            'Cache-Control': "no-cache",
        }
        
        try:
            r = self.http.request(request,self.url[(object_type,request)], body = encoded_payload, headers=headers)
        except KeyError:
            raise ValueError ('This combination  of request_type and object_type is not supported by the python api')
                
        response= r.data.decode('utf-8')
        return response

    def commit(self):
        '''
        Commit the active session
        '''
        self.session.commit()
        self.session.close()
        self.session = None        
        
    def create_all(self,tables = None, checkfirst = True ):
        '''
        Create database tables for logical tables defined in the database metadata
        '''
        self.metadata.create_all(tables = tables, checkfirst = checkfirst)

    def drop_all(self,tables = None, checkfirst = True ):
        '''
        Drop database tables.
        '''
        self.metadata.drop_all(tables = tables, checkfirst = checkfirst)        
        
    def get_table(self,table_name):
        '''
        Get sql alchemchy table object for table name
        '''
        try:
            table = Table(table_name, self.metadata, autoload=True,autoload_with=self.connection)        
        except NoSuchTableError:
            raise KeyError ('Table %s does not exist in the database' %table_name)
        else:
            return table
        
    def get_column_names(self,table):
        """
        Get a list of columns names for a table object or table name
        """
        if isinstance(table,str):
            table = self.get_table(table)
        
        return [column.key for column in table.columns]        
        
    def if_exists(self,table_name):
        '''
        Return True if table exists in the database
        '''
        try:
            self.get_table(table_name)
        except KeyError:
            return False
        
        return True
    
    def get_query_data(self, query):
        '''
        Execute a query and a return a dataframe containing results
        
        Parameters
        ----------
        query : sqlalchemy Query object
            query to execute
        
        '''
        
        df = pd.read_sql(sql=query.statement, con = self.connection )
        return df
        
        
    def start_session(self):
        '''
        Start a database session. 
        '''
        if self.session is None:
            self.session = self.Session()
        
    def query(self,table_name):
        '''
        Build a sqlalchemy query object for a table. You can further manipulate the query object using standard sqlalchemcy operations to do things like filter and join.
        
        Parameters
        ----------
        table_name : str
        
        Returns
        -------
        tuple containing a sqlalchemy query object and a sqlalchemy table object
        '''
        
        msg = 'Starting build of db query for table %s' %table_name
        logger.debug(msg)
        
        self.start_session()
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)
        q = self.session.query(table)
        
        msg = 'Query object built %s' %q.statement
        logger.debug(msg)
        
        return (q,table)
    
    def write_frame(self,df,
                    table_name, 
                    version_db_writes = False,
                    if_exists = 'append',
                    chunksize = 1000):
        '''
        Write a dataframe to a database table
        
        Parameters
        ---------------------
        db_credentials: dict (optional)
            db2 database credentials. If not provided, will look for environment variable
        table_name: str 
            table name to write to.
        version_db_writes : boolean (optional)
            Add seprate version_date column to table. If not provided, will use default for instance / class
        if_exists : str (optional)
            What to do if table already exists. If not provided, will use default for instance / class
        chunksize : int
            batch size for writes
        Returns
        -----------
        numerical status. 1 for successful write.
            
        '''
        df = df.reset_index()
        # the column names id, timestamp and index are reserverd as level names. They are also reserved words
        # in db2 so we don't use them in db2 tables.
        # deviceid and evt_timestamp are used instead
        if 'deviceid' not in df.columns and 'id' in df.columns:
            df['deviceid'] = df['id']
            df = df[[x for x in df.columns if x !='id']]
        if 'evt_timestamp' not in df.columns and 'timestamp' in df.columns:
            df['evt_timestamp'] = df['timestamp']
            df = df[[x for x in df.columns if x !='timestamp']]
        df = df[[x for x in df.columns if x !='index']]
        if version_db_writes:
            df['version_date'] = dt.datetime.utcnow()
        if table_name is None:
            raise ValueError('Function attempted to write data to a table. A name was not supplied. Specify an instance variable for out_table_name. Optionally include an out_table_prefix too')
        dtypes = {}        
        #replace default mappings to clobs and booleans
        for c in list(df.columns):
            if is_string_dtype(df[c]):
                dtypes[c] = String(255)
            elif is_bool_dtype(df[c]):
                dtypes[c] = SmallInteger()
        table_exists = False
        cols = None
        if if_exists == 'append':
            #check table exists
            try:
                table = self.get_table(table_name)
            except KeyError:
                pass
            else:
                table_exists = True
                cols = [column.key for column in table.columns]
                extra_cols = set([x for x in df.columns if x !='index'])-set(cols)
                if len(extra_cols) > 0:
                    logger.warning('Dataframe includes column/s %s that are not present in the table. They will be ignored.' %extra_cols)            
                try: 
                    df = df[cols]
                except KeyError:
                    raise KeyError('Dataframe does not have required columns %s' %cols)                
        self.start_session()
        try:        
            df.to_sql(name = table_name, con = self.connection, schema = self.schema,
                  if_exists = if_exists, index = False, chunksize = chunksize, dtype = dtypes)
        except:
            self.session.rollback()
            logger.info('Attempted write of %s data to table %s ' %(cols,table_name))
            raise
        finally:
            self.commit()
            logger.info('Wrote data to table %s ' %table_name)
        return 1    
        
class BaseTable(object):

    is_table = True
    _entity_id = 'deviceid'
    _timestamp = 'evt_timestamp'
    
    def __init__ (self,name,database,*args, **kw):
        self.name = name
        self.database= database
        self.table = Table(self.name,self.database.metadata, *args,**kw )
        self.id_col = Column(self._entity_id,String(50))
        
    def create(self):
        self.table.create(self.database.connection)
        
    def get_table(self,table_name):
        """
        Get a sql alchmemy logical table from database metadata
        """
        self.database.start_session()
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)
        return table
        
    def get_columns(self):
        """
        Get a list of columns names
        """
        return [column.key for column in self.table.columns]

    def insert(self,df, chunksize = 1000):
        """
        Insert a dataframe into table. Dataframe column names are expected to match table column names.
        """
        
        df = df.reset_index()
        cols = self.get_columns()
        
        extra_cols = set([x for x in df.columns if x !='index'])-set(cols)            
        if len(extra_cols) > 0:
            logger.warning('Dataframe includes column/s %s that are not present in the table. They will be ignored.' %extra_cols)
            
        dtypes = {}        
        #replace default mappings to clobs and booleans
        for c in list(df.columns):
            if is_string_dtype(df[c]):
                dtypes[c] = String(255)
            elif is_bool_dtype(df[c]):
                dtypes[c] = SmallInteger()
                
        try: 
            df = df[cols]
        except KeyError:
            msg = 'Dataframe does not have required columns %s. It has columns: %s and index: %s' %(cols,df.columns,df.index.names)
            raise KeyError(msg)
        self.database.start_session()
        try:        
            df.to_sql(name = self.name, con = self.database.connection, schema = self.database.schema,
                  if_exists = 'append', index = False, chunksize = chunksize,dtype=dtypes)
        except:
            self.database.session.rollback()
            raise
        finally:
            self.database.session.close()
            
    def query(self):
        """
        Return a sql alchemy query object for the table. 
        """        
        self.db.start_session()
        q = self.db.session.query(self.table)
        return q            


class SystemLogTable(BaseTable):
    """
    A log table only has a timestamp as a predefined column
    """
    
    def __init__(self,name,database,*args,**kw):
 
        self.timestamp = Column(self._timestamp,DateTime)
        super().__init__(name,database,self.timestamp,*args, **kw)
        
                    

class ActivityTable(BaseTable):
    """
    An activity table is a special class of table that iotfunctions understands to contain data containing activities performed using or on an entity.
    The table contains a device id, start date and end date of the activity and an activity code to indicate what type of activity was performed.
    The table can have any number of additional Column objects supplied as arguments.
    """
        
    def __init__ (self,name,database,*args, **kw):

        self.id_col = Column(self._entity_id,String(50))
        self.start_date = Column('start_date',DateTime)
        self.end_date = Column('end_date',DateTime)
        self.activity = Column('activity',String(255))
        super().__init__(name,database,self.id_col,self.start_date,self.end_date,self.activity, *args, **kw)
        
class ResourceCalendarTable(BaseTable):
    """
    A resource calendar table is a special class of table that iotfunctions understands to contain data that can be used to understand what resource/s were assigned to an entity
    The table contains a device id, start date and end date and the resource_id. 
    Create a separte table for each different type of resource, e.g. operator, owner , company
    The table can have any number of additional Column objects supplied as arguments.
    """    
        
    def __init__ (self,name,database,*args, **kw):

        self.start_date = Column('start_date',DateTime)
        self.end_date = Column('end_date',DateTime)
        self.resource_id = Column('resource_id',String(255))
        self.id_col = Column(self._entity_id,String(50))
        super().__init__(name,database,self.id_col,self.start_date,self.end_date,self.resource_id, *args, **kw)
        
class TimeSeriesTable(BaseTable):
    """
    A time series table contains a timestamp and one or more metrics.
    """

    def __init__ (self,name,database,*args, **kw):

        self.id_col = Column(self._entity_id,String(50))
        self.evt_timestamp = Column(self._timestamp,DateTime)
        self.device_type = Column('devicetype',String(50))
        self.logical_inteface = Column('logicalinterface_id',String(64))
        self.format = Column('format',String(64))
        self.updated_timestamp = Column('updated_utc',DateTime)
        super().__init__(name,database,self.id_col,self.evt_timestamp,
                 self.device_type, self.logical_inteface, self.format , 
                 self.updated_timestamp,
                 *args, **kw)


