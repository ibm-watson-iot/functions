import datetime as dt
import json
import os
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.preprocessor import BaseTransformer
from iotfunctions.bif import IoTExpression
from iotfunctions.metadata import EntityType, make_sample_entity
from iotfunctions.db import Database

#replace with a credentials dictionary or provide a credentials file
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

#create a sample entity to work with
db_schema = None #set if you are not using the default
db = Database(credentials=credentials)
entity = make_sample_entity(db=db, schema = db_schema)

#examine the sample entity
df = db.read_table(entity.name,schema=db_schema)
df.head(1).transpose()

#configure an expression function
expression = 'df["throttle"]/df["grade"]'
fn = IoTExpression(expression=expression, output_name='expression_out')
df = entity.exec_pipeline(fn)
df.head(1).transpose()


