import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

'''
You can use the db object to make queries against the AS data lake
'''

# replace with valid table and column names

db_schema = None   # only required if you are not using the default
table_name = 'test_packaging_hopper'
dim_table_name = 'test_packaging_hopper_dimension'
timestamp = 'evt_timestamp'

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db = Database(credentials=credentials)


# Get the min and max ambient temperature and mean ambient humidity

agg = {
        'ambient_temp': ['min','max','first','last'],
        'ambient_humidity' : ['mean']
}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg
)
print(df)

# Get these values by day

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp= timestamp,
                 time_grain = 'day'
)
print(df)

# Get these values by manufacturer

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 dimension = dim_table_name,
                 groupby = ['manufacturer']
)
print(df)

# Restrict to a date range

end_date = dt.datetime.utcnow()
start_date = end_date - dt.timedelta(days=14)

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp= timestamp,
                 dimension = dim_table_name,
                 groupby = ['manufacturer'],
                 end_ts = end_date,
                 start_ts = start_date
)
print(df)


# Restrict to an entity

end_date = dt.datetime.utcnow()
start_date = end_date - dt.timedelta(days=14)

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp= timestamp,
                 dimension = dim_table_name,
                 groupby = ['manufacturer'],
                 end_ts = end_date,
                 start_ts = start_date,
                 entities= ['73001']
)
print(df)
