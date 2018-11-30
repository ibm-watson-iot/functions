import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import pandas as pd
import datetime as dt
import json
import time

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

import os
os.environ['DB_CONNECTION_STRING'] = 'DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %(credentials["database"],credentials["hostname"],credentials["port"],credentials["username"],credentials["password"])
os.environ['API_BASEURL'] = 'https://%s' %credentials['as_api_host']
os.environ['API_KEY'] = credentials['as_api_key']
os.environ['API_TOKEN'] = credentials['as_api_token']

#you can do local development using the sqlite database
'''
credentials = {'sqlite':'sqldb.db',
               'tenant_id' : 'local'}
'''

from iotfunctions.db import Database
from iotfunctions.metadata import EntityType
from iotfunctions.preprocessor import TimeToFirstAndLastInShift, LookupOperator, MergeActivityData,SamplePreLoad,CompanyFilter,GenerateException, MultiplyByTwo, MergeSampleTimeSeries, EntityDataGenerator, AlertThreshold, MergeActivityData, LookupStatus, StatusFilter
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
db = Database(credentials=credentials)
'''
To do anything with IoT Platform Analytics, you will need one or more entity type. 
You can create entity types through the IoT Platform or using the python API.
Here is a basic entity type that has three data items: company_code, temperature and pressure
'''
entity_name = 'widgets'
db.drop_table(entity_name)
db.drop_table('widget_maintenance_activity')
db.drop_table('widget_transfer_activity')
db.drop_table('widgets_scd_status')
db.drop_table('widgets_scd_operator')
entity = EntityType(entity_name,db,
                          Column('company_code',String(50)),
                          Column('temp',Float()),
                          Column('pressure', Float()))
'''

When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data

After creating an EntityType you will need to register it so that it visible in the UI.
'''
entity.register()
'''
To make life more interesting we will also add some actity data so we can keep track of what 
is going on with our widgets.
Widgets receive planned and unplanned maintenance and get transfered from time to time.
Maintenance activities carry materials cost. 
Transfers incur a transfer cost.
This activity data is separate from the time series data - it will be recorded in different tables.
'''
entity.add_activity_table('widget_maintenance_activity',['PM','UM'],Column('materials_cost',Float()))
entity.add_activity_table('widget_transfer_activity',['DT','IT'],Column('transit_cost',Float()))
'''
There are a couple of charactertistics of widgets that change over time. Their status and the
person assigned as an operator. Status and Operator are refered to as slowly changing dimensions.
They get stored in separate tables too.
'''
entity.add_slowly_changing_dimension('status',String(15))
entity.add_slowly_changing_dimension('operator',String(50))

'''
Entities can get pretty lonely without data. You can feed your entity data by
writing directly to the entity table or you can cheat and generate data.
'''
entity.generate_data(days = 20)
'''
The one liner above generated 20 days of data for a few widgets in 5 different tables.

Next we will test a few functions to transform this data.
'''
pl = entity.get_calc_pipeline()
# generate new entity data
pl.add_stage(EntityDataGenerator('temp'))
# lookup the status and operator
pl.add_stage(LookupOperator('temp','operator'))
pl.add_stage(LookupStatus('temp','status'))
# Calculate the time to first and last temperature measurement in a shift
pl.add_stage(TimeToFirstAndLastInShift('temp'))
# Merge in the maintenance and transfer activity data. Calculate durations for each activity. Also include materials cost and transit cost.
pl.add_stage(MergeActivityData(input_activities = ['PM','UM','DT','IT'], additional_items = ['materials_cost','transit_cost'] ))
# Filter data. Only include the active widgets
pl.add_stage(StatusFilter('status','active'))
# Execute this whole list of calculations in a pipeline.
# Ouput the results of each to a file for validation
# Register the functions used so that they are available in the UI
df = pl.execute(to_csv= True,start_ts=None, register=True)


'''
Keep sending data every 2 minutes
'''
while 1==1:
    entity.generate_data(days = 0 , seconds = 120)
    l = entity.get_log()
    print(l.head(1).transpose())
    time.sleep(120)

  
    