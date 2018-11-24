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
        
            
        
        
    



