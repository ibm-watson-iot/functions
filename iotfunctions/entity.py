# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

'''
The entity module contains sample entity types
'''

import logging
import datetime as dt
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from .metadata import EntityType
from .bif import IoTEntityDataGenerator,TimestampCol,DateDifferenceReference

logger = logging.getLogger(__name__)


class EmptyEntityType(EntityType):
    is_entity_type = True
    def __init__(self,name,db,db_schema=None,timestamp='evt_timestamp'):
        args = []
        kw = {'_timestamp' : 'evt_timestamp',
              '_db_schema' : db_schema
              }        
        super().__init__(name,db, *args,**kw)

class Boiler(EntityType):

    def __init__(self,name,db,db_schema=None,timestamp='evt_timestamp'):
        args = []
        args.append(Column('company_code',String(50)))
        args.append(Column('temp_set_point',Float()))
        args.append(Column('temperature',Float()))
        args.append(Column('pressure',Float()))
        args.append(Column('input_flow_rate',Float()))
        args.append(Column('output_flow_rate',Float()))
        args.append(Column('discharge_rate',Float()))
        args.append(Column('fuel_flow_rate',Float()))
        args.append(Column('air_flow_rate',Float()))
        args.append(IoTEntityDataGenerator(ids=None))
        args.append(TimestampCol(dummy_items = None, output_item = 'timestamp_col'))
        args.append(DateDifferenceReference(date_1='timestamp_col',ref_date=dt.datetime.utcnow()))
        
        kw = {'_timestamp' : timestamp,
              '_db_schema' : db_schema
              }
        
        super().__init__(name,db, *args,**kw)
        
    
    
    
    
    