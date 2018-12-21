import datetime as dt
import json
import os
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.preprocessor import BaseTransformer
from iotfunctions.bif import IoTExpression
from iotfunctions.metadata import EntityType, make_sample_entity
from iotfunctions.db import Database
from iotfunctions.estimator import SimpleAnomaly

#replace with a credentials dictionary or provide a credentials file
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

#create a sample entity to work with
db_schema = None #set if you are not using the default
db = Database(credentials=credentials)
numeric_columns = ['fill_time','temp','humidity','wait_time','size_sd']
table_name = 'as_sample_cereal'
entity = make_sample_entity(db=db, schema = db_schema,
                            float_cols = numeric_columns,
                            name = table_name,
                            register = True)
entity.name

#examine the sample entity
df = db.read_table(entity.name,schema=db_schema)
df.head(1).transpose()

#configure an expression function
expression = '510 + 15*df["temp"] + 5*df["humidity"]'
mass_fn = IoTExpression(expression=expression, output_name='fill_mass')
df = entity.exec_pipeline(mass_fn)
df.head(1).transpose()

#build an anomaly model
features = ['temp', 'humidity', 'wait_time']
targets = ['fill_mass']
anomaly_fn = SimpleAnomaly(features=['temp','humidity','fill_time'],targets=['fill_mass'],threshold=0.01)
df = entity.exec_pipeline(mass_fn,anomaly_fn)
df.head(1).transpose()

