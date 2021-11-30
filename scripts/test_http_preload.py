import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif, sample
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------

When creating a set of functions you can test how they these functions will
work together by creating a test pipeline. You can also connect the test
pipeline to real entity data so that you can what the actual results that the
function will deliver.

'''

'''
A database object is our connection to the mother ship
'''
db = Database(credentials=credentials)
db_schema = None  # set if you are not using the default

'''
To do anything with IoT Platform Analytics, you will need one or more entity type. 
You can create entity types through the IoT Platform or using the python API.

When defining an entity type, you can describe the raw input data items for the
entity type, as well as the functions , constants and granularities that apply to it.

The "widgets" entity type below has 3 input data items. Dataitems are denoted by the
SqlAlchemy column objects company_code, temp and pressure.

It also has a function EntityDataGenerator

The keyword args dict specifies extra properties. The database schema is only
needed if you are not using the default schema. You can also rename the timestamp.

'''
entity_name = 'test_http_preload'
db_schema = None  # replace if you are not using the default schema
db.drop_table(entity_name, schema=db_schema)
entity = EntityType(entity_name, db, Column('company_code', String(50)), Column('temp', Float()),
                    Column('pressure', Float()),
                    sample.HTTPPreload(request='GET', url='internal_test', output_item='http_preload_done'),
                    bif.PythonExpression(expression='df["temp"]*df["pressure"]', output_name='volume'),
                    **{'_timestamp': 'evt_timestamp', '_db_schema': db_schema})
'''
When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data

After creating an EntityType you will need to register it so that it visible in the UI.
To also register the functions and constants associated with the entity type, specify
'publish_kpis' = True.
'''
entity.register(raise_error=False)
db.register_functions([sample.HTTPPreload])

'''
To test the execution of kpi calculations defined for the entity type locally
use 'test_local_pipeline'.

A local test will not update the server job log or write kpi data to the AS data
lake. Instead kpi data is written to the local filesystem in csv form.

'''

entity.exec_local_pipeline()

'''
view entity data
'''

df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())

'''
The initial test used an internal test to produce data. Now we will use an actual rest service.

Use the script "test_local_rest_service.py" to run a local service. 
'''

entity_name = 'test_http_preload'
db.drop_table(entity_name, schema=db_schema)
entity = EntityType(entity_name, db, Column('company_code', String(50)), Column('temp', Float()),
                    Column('pressure', Float()),
                    sample.HTTPPreload(request='GET', url='http://localhost:8080/', output_item='http_preload_done'),
                    bif.PythonExpression(expression='df["temp"]*df["pressure"]', output_name='volume'),
                    **{'_timestamp': 'evt_timestamp', '_db_schema': db_schema})

entity.exec_local_pipeline()
