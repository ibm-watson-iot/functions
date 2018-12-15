import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import json

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

import os
os.environ['DB_CONNECTION_STRING'] = 'DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %(credentials["database"],credentials["hostname"],credentials["port"],credentials["username"],credentials["password"])
os.environ['API_BASEURL'] = 'https://%s' %credentials['as_api_host']
os.environ['API_KEY'] = credentials['as_api_key']
os.environ['API_TOKEN'] = credentials['as_api_token']

os.environ['COS_REGION'] = credentials['objectStorage']['region']
os.environ['COS_HMAC_ACCESS_KEY_ID'] = credentials['objectStorage']['username']
os.environ['COS_HMAC_SECRET_ACCESS_KEY'] = credentials['objectStorage']['password']
os.environ['COS_ENDPOINT'] = credentials['config']['objectStorageEndpoint']
os.environ['COS_BUCKET_KPI'] = credentials['config']['bos_runtime_bucket']

from iotfunctions.db import Database
from iotfunctions.metadata import EntityType
from iotfunctions.estimator import SampleAnomalySGDRegressor


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
entity_name = 'cerealfiller_lm'
db_schema = None # replace if you are not using the default schema
db.drop_table(entity_name)
entity = EntityType(entity_name,db,
                          Column('fill_mass',String(50)),
                          Column('fill_time',Float()),
                          Column('temp', Float()),
                          Column('humidity', Float()),
                          Column('wait_time', Float()),
                          Column('size_sd', Float()),
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
entity.generate_data(days=20, drop_existing = True)
'''
We now have 12 hours of historical data. We can use it to do some calculations.
The calculations will be placed into a container called a pipeline. The 
pipeline is constructed from multiple stages. Each stage performs a transforms
the data. 


Add more info here
'''
pl = entity.get_calc_pipeline()

features = ['temp', 'humidity']
targets = ['fill_time']

pl.add_stage(SampleAnomalySGDRegressor(features=features, targets=targets))
df = pl.execute(to_csv= True,start_ts=None, register=True)
'''
Add more info here
'''