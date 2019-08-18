import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType, ServerEntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.base import BasePreload
from iotfunctions import ui

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Executing server pipelines
-------------------------

When troubleshooting problems with functions or developing new functions, it is 
useful to run these locally to debug. This script shows how to do this.

'''

db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

entity_name = 'JOB_Test_1'  # choose a valid entity type name
test_entity = ServerEntityType(entity_name,db=db,db_schema=db_schema)

'''
Examine the entity type metadata
'''
print (test_entity)


'''
To test the execution of kpi calculations defined for the entity type locally
use 'test_local_pipeline'.

A local test will not update the server job log or write kpi data to the AS data
lake. Instead kpi data is written to the local filesystem in csv form.

'''

test_entity.exec_local_pipeline(_abort_on_fail=True)
