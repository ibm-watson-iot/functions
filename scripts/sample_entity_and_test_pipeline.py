import datetime as dt
import json
import os
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.preprocessor import MultiplyByTwo, CompanyFilter, EntityDataGenerator
from iotfunctions.bif import IoTAlertOutOfRange
from iotfunctions.preprocessor import BaseTransformer
from iotfunctions.bif import IoTExpression
from iotfunctions.metadata import EntityType, make_sample_entity
from iotfunctions.db import Database
from iotfunctions.estimator import SimpleAnomaly

#replace with a credentials dictionary or provide a credentials file
with open('credentials.json', encoding='utf-8') as F:
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
db = Database(credentials = credentials)
db_schema = None #set if you are not using the default

'''
To do anything with IoT Platform Analytics, you will need one or more entity type. 
You can create entity types through the IoT Platform or using the python API.
Here is a basic entity type that has three data items: company_code, temperature and pressure
'''
entity_name = 'widgets' 
db_schema = None # replace if you are not using the default schema
db.drop_table(entity_name, schema = db_schema)
entity = EntityType(entity_name,db,
                          Column('company_code',String(50)),
                          Column('temp',Float()),
                          Column('pressure', Float()),
                          **{
                            '_timestamp' : 'evt_timestamp',
                            '_db_schema' : db_schema
                             })
'''
When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data

After creating an EntityType you will need to register it so that it visible in the UI.
'''
entity.register()
'''
Entities can get pretty lonely without data. You can feed your entity data by
writing directly to the entity table or you can cheat and generate data.

'''
entity.generate_data(days=0.5, drop_existing = True)
df = db.read_table(table_name=entity_name, schema = db_schema)
df.head()
'''
We now have 12 hours of historical data. We can use it to do some calculations.
The calculations will be placed into a container called a pipeline. The 
pipeline is constructed from multiple stages. Each stage performs a transforms
the data. Let's multiply create a new "double_temp" by multiplying "temp" by 2.
'''

m_fn = MultiplyByTwo(input_item = 'temp', output_item='double_temp')
df = entity.exec_pipeline(m_fn)
df.head(1).transpose()

'''
The execute() method retrieves entity data and carries out the transformations.
By specifying 'to_csv = True', we also csv output dumped at the end of 
each stage. This is useful for testing. 
'register=true' took care of function registration so the MultiplyByTwo 
function will be available in the ui.
You can use the outputs of one calculation in another. To demonstrate this
we will add an alert on "double_temp" and while we are at, filter the
data down to a single company.
'''
a_fn = IoTAlertOutOfRange(input_item = 'double_temp', lower_threshold = -5, upper_threshold = 5)
f_fn = CompanyFilter(company_code = 'company_code', company = 'ACME')
df = entity.exec_pipeline(m_fn,a_fn,f_fn)
df.head(1).transpose()
'''
The 12 hours of historical data we loaded  won't keep these widgets
happy for very long. IoT Platform Analytics performs calculations on new data
received, so if we add a stage to the pipeline that generates new data each time
the pipeline runs, we can keep our widgets well fed with new data.
'''
g_fn = EntityDataGenerator(dummy_items=['temp'])
df = entity.exec_pipeline(g_fn,m_fn,a_fn,f_fn)
df.head(1).transpose()
'''
When this pipeline executed, it added more data to the widgets input table
and then completed the tranform and filter stages.

To register all of the functions tested, use register = True
'''
df = entity.exec_pipeline(g_fn,m_fn,a_fn,f_fn, register = True)
df.head(1).transpose()

