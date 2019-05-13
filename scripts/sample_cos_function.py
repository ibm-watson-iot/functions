import datetime as dt
import json
import pandas as pd
import numpy as np
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
import iotfunctions.bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = 'BLUADMIN'
db = Database(credentials=credentials)

'''
You can serialize simple functions to Cloud Object Storage to avoid having to 
paste replicas of them in the UI or avoid the need to manage them in a
git repository

See offline simple functions sample to see how to create simple functions.

Here is a simple function:

'''

def f(df,parameters = None):
    #  generate an 2-D array of random numbers
    output = np.random.normal(1,0.1,len(df.index))
    return output

'''
First save the function in cloud object storage
'''

db.cos_save(persisted_object=f,filename='random_1',binary=True)

'''
Test the function by adding it to an entity type
'''

test_function = bif.PythonFunction(
    function_code = 'random_1',
    input_items = ['speed'],
    output_item = 'random_1_out',
    parameters = {}
        )

et = EntityType('test_entity',
                db,
                Column('speed',Float),
                test_function,
                **{'_db_schema': db_schema})
et.generate_data(days = 1)
et.exec_local_pipeline()



