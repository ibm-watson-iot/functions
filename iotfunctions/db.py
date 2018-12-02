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
import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype, is_dict_like
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, MetaData, ForeignKey, create_engine, Float
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.exc import NoSuchTableError    
from .automation import TimeSeriesGenerator
logger = logging.getLogger(__name__)
DB2_INSTALLED = True
try:
    import ibm_db
    import ibm_db_dbi
except ImportError:
    DB2_INSTALLED = False
    msg = 'IBM_DB is not installed. Reverting to sqlite for local development with limited functionality'
    logger.warning(msg)

class Database(object):
    '''
    Use Database objects to establish database connectivity, manage database metadata and sessions, build queries and write DataFrames to tables.
    
    Parameters:
    -----------
    credentials: dict (optional)
        Database credentials. If none specified use DB_CONNECTION_STRING environment variable
    start_session: bool
        Start a session when establishing connection
    echo: bool
        Output sql to log
    '''
    
    def __init__(self,credentials = None, start_session = False, echo = False, tenant_id = None):
        self.write_chunk_size = 1000
        
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
                pass
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
               
        if as_api_host.startswith('https://'):
            as_api_host = as_api_host[8:]

        self.credentials['as'] = {
                'host' : as_api_host,
                'api_key' : as_api_key,
                'api_token' : as_api_token                
                }

        self.tenant_id = self.credentials['tenant_id']
        
        if DB2_INSTALLED:
            connection_kwargs = {
                            'pool_size' : 1
                             }
            
            # sqlite is not included included in the AS credentials. It is only intended to be used if db2 is not istalled.
            # There is a back door to for using it instead of db2 for local development only. 
            # It will be used only when explicitly added to the credentials as credentials['sqlite'] = filename
            try:
                connection_string = 'sqlite:///%s' %(self.credentials['sqlite'])
            except KeyError:
                try:        
                    connection_string = 'db2+ibm_db://%s:%s@%s:%s/%s;' %(self.credentials['db2']['username'],
                                                                     self.credentials['db2']['password'],
                                                                     self.credentials['db2']['host'],
                                                                     self.credentials['db2']['port'],
                                                                     self.credentials['db2']['database'])
                except KeyError:
                    # look for environment variable for the ICS DB2
                    try:
                       msg = 'Function requires a database connection but one could not be established. Pass appropriate db_credentials or ensure that the DB_CONNECTION_STRING is set'
                       connection_string = os.environ.get('DB_CONNECTION_STRING')
                    except KeyError:
                        raise ValueError(msg)
                    else:
                       if not connection_string is None:
                           if connection_string.endswith(';'):
                               connection_string = connection_string[:-1]
                           ev = dict(item.split("=") for item in connection_string.split(";"))
                           connection_string  = 'db2+ibm_db://%s:%s@%s:%s/%s;' %(ev['UID'],ev['PWD'],ev['HOSTNAME'],ev['PORT'],ev['DATABASE'])
                           self.credentials['db2'] =  {
                                            "username": ev['UID'],
                                            "password": ev['PWD'],
                                            "database": ev['DATABASE'] ,
                                            "port": ev['PORT'],
                                            "host": ev['HOSTNAME'] 
                                    }
                       else:
                           raise ValueError(msg)
            else:
                connection_kwargs = {} 
                msg = 'Using sqlite connection for local testing. Note sqlite can only be used for local testing. It is not a supported AS database.'
                logger.warning(msg)                
                self.write_chunk_size = 100
        else:
            self.write_chunk_size = 100
            connection_string = 'sqlite:///sqldb.db'
            connection_kwargs = {}
            msg = 'Created a default sqlite database. Database file is in your working directory. Filename is sqldb.db'
            logger.info(msg)
                
        self.connection =  create_engine(connection_string, echo = echo, **connection_kwargs)
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
        if self.tenant_id is None:
            msg = 'tenant_id instance variable is not set. database object was not initialized with valid credentials'
            raise ValueError(msg)
        
        base_url = 'http://%s/api' %(self.credentials['as']['host'])
        self.url = {}
        self.url[('entityType','POST')] = '/'.join([base_url,'meta','v1',self.tenant_id,object_type])
        self.url[('entityType','GET')] = '/'.join([base_url,'meta','v1',self.tenant_id,object_type,object_name])
        self.url[('function','GET')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
        self.url[('function','DELETE')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
        self.url[('function','PUT')] = '/'.join([base_url,'catalog','v1',self.tenant_id,object_type,object_name])
        self.url[('kpiFunctions','POST')] = '/'.join([base_url,self.tenant_id,'entityType',object_name,object_type,'import'])
            
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
        if not self.session is None:
            self.session.commit()
            self.session.close()
            self.session = None        
        
    def create_all(self,tables = None, checkfirst = True ):
        '''
        Create database tables for logical tables defined in the database metadata
        '''
        self.metadata.create_all(tables = tables, checkfirst = checkfirst)

        
    def drop_table(self,table_name):
        
        try:
            table = self.get_table(table_name)
        except KeyError:
            msg = 'Didnt drop table %s becuase it doesnt exist in the the database' %table_name
        else:
            self.metadata.drop_all(tables = [table], checkfirst = True) 
            msg = 'Dropped table name %s' %table_name
        logger.debug(msg)
               
        
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
            
    def truncate(self,table_name):
        
        try:
            table = self.get_table(table_name)
        except KeyError:
            msg = 'Table %s doesnt exist in the the database' %table_name
            raise KeyError(msg)
        else:
            self.start_session()
            table.delete()
            self.commit()
            msg = 'Truncated table name %s' %table_name
        logger.debug(msg)             
        
    def query(self,table_name):
        '''
        Build a sqlalchemy query object for a table. You can further manipulate the query object using standard sqlalchemcy operations to do things like filter and join.
        
        Parameters
        ----------
        table_name : str or Table object
        
        Returns
        -------
        tuple containing a sqlalchemy query object and a sqlalchemy table object
        '''        
        self.start_session()
        if isinstance(table_name, str):
            try:
                table = Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)
            except:
                msg = 'Error retrieving table %s' %table_name
                logger.exception(msg)
                raise
        else:
            table = table_name
        q = self.session.query(table)        
        msg = 'Query object built %s' %q.statement
        logger.debug(msg)
        
        return (q,table)
    
    
    def unregister_functions(self,function_names):
        '''
        Unregister functions by name. Accepts a list of function names.
        '''
        if not isinstance(function_names,list):
            function_names = [function_names]
    
        for f in function_names:
            payload = {
                'name' : f
                }
            self.http_request(object_type='function',object_name=f, request = 'DELETE', payload=payload)
            msg = 'Function registration deletion status: %s' %(r.data.decode('utf-8'))
            logger.info(msg) 
    
    def write_frame(self,df,
                    table_name, 
                    version_db_writes = False,
                    if_exists = 'append',
                    chunksize = None):
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
        
        if chunksize is None:
            chunksize = self.write_chunk_size
            
        df = df.reset_index()
        # the column names id, timestamp and index are reserverd as level names. They are also reserved words
        # in db2 so we don't use them in db2 tables.
        # deviceid and evt_timestamp are used instead
        if 'deviceid' not in df.columns and 'id' in df.columns:
            df['deviceid'] = df['id']
            df = df[[x for x in df.columns if x !='id']]
        if 'evt_timestamp' not in df.columns and '_timestamp' in df.columns:
            df['evt_timestamp'] = df['_timestamp']
            df = df[[x for x in df.columns if x !='_timestamp']]
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
        as_keywords = ['_timestamp','_timestamp_col','_activities','_freq','_entity_id','_df_index_entity_id','_tenant_id']
        self.name = name
        self.database= database
        # the keyword arguments may contain properties and sql alchemy dialect specific options
        # set all of them
        self.set_params(**kw)
        # delete the designated AS metadata properties as sql alchemy will not understand them
        for k in as_keywords:
            try:
                del kw[k]
            except KeyError:
                pass
        kw['extend_existing'] = True
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
        
    def get_column_names(self):
        """
        Get a list of columns names
        """
        return [column.key for column in self.table.columns]
    
    def get_column_lists_by_type(self, exclude_cols = None):
        """
        Get metrics, dates and categoricals and others
        """
        if exclude_cols is None:
            exclude_cols = []
        metrics = []
        dates = []
        categoricals = []
        others = []
        
        for c in self.database.get_column_names(self.table):
            if not c in exclude_cols:
                data_type = self.table.c[c].type
                if isinstance(data_type,DOUBLE) or isinstance(data_type,Float):
                    metrics.append(c)
                elif isinstance(data_type,VARCHAR) or isinstance(data_type,String):
                    categoricals.append(c)
                elif isinstance(data_type,TIMESTAMP) or isinstance(data_type,DateTime):
                    dates.append(c)
                else:
                    others.append(c)
                    msg = 'Ignored column %s of unknown data type %s' %(c,data_type.__class__.__name__)
                    logger.warning(msg)
                    
        return (metrics,dates,categoricals,others)
                

    def insert(self,df, chunksize = None):
        """
        Insert a dataframe into table. Dataframe column names are expected to match table column names.
        """
        
        if chunksize is None:
            chunksize = self.database.write_chunk_size
        
        df = df.reset_index()
        cols = self.get_column_names()
        
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
            
    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key,value in list(params.items()):
            setattr(self, key, value)
        return self               
            
    def query(self):
        """
        Return a sql alchemy query object for the table. 
        """        
        (q,table) =self.database.query(self.table)
        return (q,table)           


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
    Also supply a keyword argument containing "activities" a list of activity codes contained in this table
    """
        
    def __init__ (self,name,database,*args, **kw):
        self._freq = '1D' #default activity interval
        self.id_col = Column(self._entity_id,String(50))
        self.start_date = Column('start_date',DateTime)
        self.end_date = Column('end_date',DateTime)
        self.activity = Column('activity',String(255))
        super().__init__(name,database,self.id_col,self.start_date,self.end_date,self.activity, *args, **kw)
        
    def generate_data(self,entities,days,seconds,write=True):
        
        (metrics, dates, categoricals,others) = self.get_column_lists_by_type(exclude_cols=[self._entity_id,'start_date','end_date'])
        metrics.append('duration')
        categoricals.append('activity')
        ts = TimeSeriesGenerator(metrics=metrics,dates = dates,categoricals=categoricals,
                                 ids = entities, days = days, seconds = seconds, freq = self._freq)
        try:
            ts.set_domain('activity',self._activities)
        except AttributeError:
            msg = 'Unable to find domain of activity data for %s. Set "_activities" instance variable using a keyword arg to override defaults' %self.name
            logger.warning(msg)                 
        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        duration = df['duration'].abs()
        df['end_date'] = df['start_date'] + pd.to_timedelta(duration, unit='h')
        # probability that an activity took place in the interval
        p_activity = (days*60*60*24 +seconds) / pd.to_timedelta(self._freq).total_seconds() 
        is_activity = p_activity >= np.random.uniform(0,1,len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in ['duration',self._timestamp]]
        df = df[cols]
        if write:
            msg = 'Generated %s rows of data and inserted into %s' %(len(df.index),self.table.name)
            self.insert(df)        
        return df
        
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
        
class SlowlyChangingDimension(BaseTable):
    """
    A slowly changing dimension table tracks changes to a property of an entitity over time
    The table contains a device id, start date and end date and the property 
    Create a separate table for each property, e.g. firmware_version, owner
    """    
        
    def __init__ (self,name,database,property_name,datatype):
        self._freq = '3D'
        self.start_date = Column('start_date',DateTime)
        self.end_date = Column('end_date',DateTime)
        self.property_name = Column(property_name,datatype)
        self.id_col = Column(self._entity_id,String(50))
        super().__init__(name,database,self.id_col,self.start_date,self.end_date,self.property_name )

    def generate_data(self,entities,days,seconds,write=True):
        
        msg = 'generating data for %s for %s days and %s seconds' %(self.table.name,days,seconds)
        (metrics, dates, categoricals,others) = self.get_column_lists_by_type(exclude_cols=[self._entity_id,'start_date','end_date'])
        msg = msg + ' with metrics %s, dates %s, categorials %s and others %s' %(metrics, dates, categoricals,others)
        ts = TimeSeriesGenerator(metrics=metrics,dates = dates,categoricals=categoricals,
                                 ids = entities, days = days, seconds = seconds, freq = self._freq)
        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        # probability that a change took place in the interval
        p_activity = (days*60*60*24 +seconds) / pd.to_timedelta(self._freq).total_seconds() 
        is_activity = p_activity >= np.random.uniform(0,1,len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in [self._timestamp]]
        df = df[cols]
        df['end_date'] = None
        query,table = self.database.query(self.table)
        try:
            edf = self.database.get_query_data(query)
        except:
            edf = pd.DataFrame()
        df = pd.concat([df,edf],ignore_index = True,sort=False)
        if len(df.index) > 0:
            df = df.groupby([self._entity_id]).apply(self._set_end_date)
            try:
                self.database.truncate(self.table)
            except KeyError:
                pass
            if write:
                msg = 'Generated %s rows of data and inserted into %s' %(len(df.index),self.table.name)
            self.insert(df)
        return df
    
    def _set_end_date(self,df):
        
        df['end_date'] = df['start_date'].shift(-1)
        df['end_date'] = df['end_date'] - pd.Timedelta(seconds = 1)
        df['end_date'] = df['end_date'].fillna(pd.Timestamp.max)
        return df
