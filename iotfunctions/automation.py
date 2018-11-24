# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import inspect
import logging
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE
from .preprocessor import TimeSeriesGenerator

logger = logging.getLogger(__name__)


def generate_entity_data(db,entity_name, entities, days, seconds = 0, freq = '1min', credentials = None, write=True):
    '''
    Generate random time series and dimension data for entities
    
    Parameters
    ----------
    entity_name : str
        Name of entity to generate data for
    entities: list
        List of entity ids to genenerate data for
    days: number
        Number of days worth of data to generate (back from system date)
    seconds: number
        Number of seconds of wotht of data to generate (back from system date)
    freq: str
        Pandas frequency string - interval of time between subsequent rows of data
    credentials: dict
        credentials dictionary
    write: bool
        write generated data back to table with same name as entity
    
    '''
    table = db.get_table(entity_name)
    metrics = []
    dims = []
    dates = []
    others = []
    for c in db.get_column_names(table):
        if not c in ['deviceid','devicetype','format','updated_utc']:
            data_type = table.c[c].type                
            if isinstance(data_type,DOUBLE):
                metrics.append(c)
            elif isinstance(data_type,VARCHAR):
                dims.append(c)
            elif isinstance(data_type,TIMESTAMP):
                dates.append(c)
            else:
                others.append(c)
    msg = 'Generating data for %s with metrics %s and dimensions %s and dates %s' %(entity_name,metrics,dims,dates)
    logger.debug(msg)
    ts = TimeSeriesGenerator(metrics=metrics,ids=entities,days=days,seconds=seconds,freq=freq, dims = dims, dates = dates)
    df = ts.execute()
    if write:
        for o in others:
            if o not in df.columns:
                df[o] = None
        df['logicalinterface_id'] = ''
        df['devicetype'] = entity_name
        df['format'] = ''
        df['updated_utc'] = None
        db.write_frame(table_name = entity_name, df = df)
    
    return df

def register(module,credentials):
    '''
    Automatically register all functions in a module. To be regiserable, a function
    must have an class variable containing a dictionary of arguments (auto_register_args)
    '''
    
    mod = module

    for (name,cls) in inspect.getmembers(module):
        try:
            kwargs = cls.auto_register_args
        except AttributeError:
            kwargs = None
        
        if not kwargs is None:
            args,varargs,keywords,defaults = (inspect.getargspec(cls.__init__))
            args = args[1:]
            stmt = 'mod.%s(**%s)' %(name,kwargs)
            instance = eval(stmt)
            df = instance.get_test_data()
            instance.register(credentials = credentials,df=df)
        
            
        
        
    



