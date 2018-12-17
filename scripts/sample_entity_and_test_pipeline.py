import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import pandas as pd
import datetime as dt
import json

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

import os
os.environ['DB_CONNECTION_STRING'] = 'DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %(credentials["database"],credentials["hostname"],credentials["port"],credentials["username"],credentials["password"])
os.environ['API_BASEURL'] = 'https://%s' %credentials['as_api_host']
os.environ['API_KEY'] = credentials['as_api_key']
os.environ['API_TOKEN'] = credentials['as_api_token']

from iotfunctions.db import Database
from iotfunctions.metadata import EntityType
from iotfunctions.preprocessor import TimeToFirstAndLastInShift, LookupOperator, MergeActivityData,SamplePreLoad,CompanyFilter,GenerateException, MultiplyByTwo, MergeSampleTimeSeries, EntityDataGenerator
from iotfunctions.bif import IoTAlertOutOfRange
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean

'''
Developing Test Pipelines
-------------------------

When creating a set of functions you can test how they these functions will
work together by creating a test pipeline. You can also connect the test
pipeline to real entity data so that you can what the actula results that the
function will deliver.

'''
    
'''
A database object is our connection to the mother ship
'''
db = Database(credentials = credentials, tenant_id=credentials['tennant_id'])
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
'''
We now have 12 hours of historical data. We can use it to do some calculations.
The calculations will be placed into a container called a pipeline. The 
pipeline is constructed from multiple stages. Each stage performs a transforms
the data. Let's multiply create a new "double_temp" by multiplying "temp" by 2.
'''
pl = entity.get_calc_pipeline()
pl.add_stage(MultiplyByTwo(input_item = 'temp', output_item='double_temp'))
df = pl.execute(to_csv= True,start_ts=None, register=True)
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
pl.add_stage(IoTAlertOutOfRange(input_item = 'double_temp', lower_threshold = -5, upper_threshold = 5))
pl.add_stage(CompanyFilter(company_code = 'company_code', company = 'ACME'))
df = pl.execute(to_csv= True,start_ts=None, register=True)
'''
The 12 hours of historical data we loaded  won't keep these widgets
happy for very long. IoT Platform Analytics performs calculations on new data
received, so if we add a stage to the pipeline that generates new data each time
the pipeline runs, we can keep our widgets well fed with new data.
'''
pl = entity.get_calc_pipeline()
pl.add_stage(EntityDataGenerator(dummy_items=['temp']))
pl.add_stage(MultiplyByTwo(input_item = 'temp', output_item='double_temp'))
pl.add_stage(IoTAlertOutOfRange(input_item = 'double_temp', lower_threshold = -5, upper_threshold = 5))
pl.add_stage(CompanyFilter(company_code = 'company_code', company = 'ACME'))    
df = pl.execute(to_csv= True,start_ts=None, register=True)
'''
When this pipeline executed, it added more data to the widgets input table
and then completed the tranform and filter stages.
'''