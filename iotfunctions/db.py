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
    
    def __init__(self,credentials = None, start_session = False, echo = False):
        
        #If explicit credentials provided these allow connection to a db other than the ICS one.
        if not credentials is None:
            connection_string = 'db2+ibm_db://%s:%s@%s:%s/%s;' %(credentials['username'],credentials['password'],credentials['host'],credentials['port'],credentials['database'])
            self.connection =  create_engine(connection_string, echo = echo)
        else:
            # look for environment vaiable for the ICS DB2
            try:
               connection_string = os.environ.get('DB_CONNECTION_STRING')
            except KeyError:
                raise ValueError('Function requires a database connection but one could not be established. Pass appropriate db_credentials or ensure that the DB_CONNECTION_STRING is set')
            else:
               ibm_connection = ibm_db.connect(connection_string, '', '')
               self.connection = ibm_db_dbi.Connection(ibm_connection)
            
        self.Session = sessionmaker(bind=self.connection)
        
        if start_session:
            self.session = self.Session()
        else:
            self.session = None
        
        self.metadata = MetaData(self.connection)
        #TBS support alternative schema
        self.schema = None

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
            raise ValueError ('Table %s does not exist in the database' %table_name)
        else:
            return table
        
    def if_exists(self,table_name):
        '''
        Return True if table exists in the database
        '''
        try:
            self.get_table(table_name)
        except ValueError:
            return False
        
        return True
        
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
        
        self.start_session()
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)
        q = self.session.query(table)
        
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
        status = 0
        df = df.reset_index()
        if version_db_writes:
            df['version_date'] = dt.datetime.now()
        
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
            except ValueError:
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
            raise
        finally:
            self.commit()
        
        return status    
        
class BaseTable(object):

    is_table = True
    _entity_id = 'deviceid'
    
    def __init__ (self,name,database,*args, **kw):
        self.name = name
        self.database= database
        self.table = Table(self.name,self.database.metadata, *args,**kw )
        self.id_col = Column(self._entity_id,String(50))
        
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
        
        try: 
            df = df[cols]
        except KeyError:
            raise KeyError('Dataframe does not have required columns %s' %cols)
            
            
        self.database.start_session()
        try:        
            df.to_sql(name = self.name, con = self.database.connection, schema = self.database.schema,
                  if_exists = 'append', index = False, chunksize = chunksize)
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
                    

class ActivityTable(BaseTable):
    """
    An activity table is a special class of table that iotfunctions understands to contain data containing activities performed using or on an entity.
    The table contains a device id, start date and end date of the activity and an activity code to indicate what type of activity was performed.
    The table can have any number of additional Column objects supplied as arguments.
    """
        
    def __init__ (self,name,database,*args, **kw):

        self.start_date = Column('start_date',DateTime)
        self.end_date = Column('end_date',DateTime)
        self.activity = Column('activity',String(255))
        self.id_col = Column(self._entity_id,String(50))
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
        self.timestamp = Column('timestamp',DateTime)
        self.device_type = Column('devicetype',String(50))
        self.logical_inteface = Column('logicalinterface_id',String(64))
        self.format = Column('format',String(64))
        self.updated_timestamp = Column('updated_utc',DateTime)
        super().__init__(name,database,self.id_col,self.timestamp,
                 self.device_type, self.logical_inteface, self.format , 
                 self.updated_timestamp,
                 *args, **kw)


