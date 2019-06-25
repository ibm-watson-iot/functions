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
table_name = 'test_packaging_hopper_june_19'  # change to a valid entity time series table name
dim_table_name = 'test_packaging_hopper_june_19_dimension' # change to a entity dimenstion table name
timestamp = 'evt_timestamp'

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db = Database(credentials=credentials)

now = dt.datetime.utcnow()


# Retrieve a single data item using a standard aggregation function

agg = {'ambient_temp':['mean']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True
)
print(df)

# Calculate average for 30 days worth of data

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True,
                 end_ts = now,
                 period_type = 'days',
                 period_count = 30
)
print(df)

# Calculate the average by day

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True,
                 end_ts = now,
                 period_type = 'days',
                 period_count = 30,
                 time_grain='day'
)
print(df)

# Use a special aggregate function (last or first) on a single data item

agg = {'ambient_temp':['last']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True
)
print(df)

# Last value per day over 30 days

agg = {'ambient_temp':['last']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True,
                 end_ts = now,
                 period_type = 'days',
                 period_count = 30,
                 time_grain='day'
)
print(df)

# Month to date min and max. When using 'mtd' data will be filtered from the start of the month

agg = {'ambient_temp':['min','max']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True,
                 end_ts = now,
                 period_type = 'mtd'
)
print(df)


# Year to date min and max. When using 'ytd' data will be filtered from the start of the year

agg = {'ambient_temp':['min','max']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True,
                 end_ts = now,
                 period_type = 'ytd'
)
print(df)

# Use a groupby to aggregate by one or more dimension

agg = {'ambient_temp':['last']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp = 'evt_timestamp',
                 dimension = dim_table_name,
                 groupby = ['manufacturer'],
                 to_csv = True
)
print(df)

# Aggregate by a combination of day and manufacturer

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp = 'evt_timestamp',
                 dimension = dim_table_name,
                 groupby = ['manufacturer'],
                 time_grain= 'day',
                 to_csv = True
)
print(df)


# Filter by a particular manufacturer, show the devices for the manufacturer

agg = {'ambient_temp':['min','last']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp = 'evt_timestamp',
                 dimension = dim_table_name,
                 groupby = ['deviceid'],
                 to_csv = True,
                 filters = {'manufacturer':'GHI Industries'}
)
print(df)


# for comparison, here is the data for a different manufacturer


df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp = 'evt_timestamp',
                 dimension = dim_table_name,
                 groupby = ['deviceid'],
                 to_csv = True,
                 filters = {'manufacturer':'Rentech'}
)
print(df)


# Retrieve multiple data items


agg = {
        'ambient_temp': ['min','max','first','last'],
        'ambient_humidity' : ['mean']
}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True
)
print(df)



# Get a collection of aggregations by day

agg = {
        'ambient_temp': ['min','max','first','last'],
        'ambient_humidity' : ['mean']
}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp= timestamp,
                 time_grain = 'day',
                 to_csv = True
)
print(df)

# Restrict to an entity


agg = {
        'ambient_temp': ['min','max','first','last'],
        'ambient_humidity' : ['mean']
}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 agg_dict = agg,
                 timestamp= timestamp,
                 dimension = dim_table_name,
                 groupby = ['manufacturer'],
                 entities= ['73001'],
                 to_csv = True
)
print(df)

# Summarize categorical data to return metrics derived from dimensions

agg = {'company_code':['last']}

df = db.read_agg(table_name = table_name,
                 schema = db_schema,
                 timestamp = 'evt_timestamp',
                 agg_dict = agg,
                 to_csv = True
)
print(df)

# invalid table name

try:

    df = db.read_agg(table_name = 'some_bad_table_name',
                     schema = db_schema,
                     timestamp = 'evt_timestamp',
                     agg_dict = agg,
                     to_csv = True)

except KeyError:

    logging.info('Key error for bad table name failed as expected')

else:

    raise RuntimeError('Query on invalid table name should have failed')




# bad dimension table is not used in query


agg = {'ambient_temp':['min','max']}

try:

    df = db.read_agg(table_name = table_name,
                     schema = db_schema,
                     timestamp = 'evt_timestamp',
                     dimension= 'some_bad_dimension_name',
                     agg_dict = agg,
                     to_csv = True)

except KeyError:

    raise

else:

    print(df)

# bad dimension table is not used in query

agg = {'ambient_temp': ['last']}

try:

    df = db.read_agg(table_name=table_name,
                     schema=db_schema,
                     timestamp='evt_timestamp',
                     dimension='some_bad_dimension_name',
                     agg_dict=agg,
                     to_csv=True)

except KeyError:

    raise

else:

    print(df)

# bad dimension table is used in query

try:

    df = db.read_agg(table_name=table_name,
                     schema=db_schema,
                     groupby=['manufacturer'],
                     timestamp='evt_timestamp',
                     dimension='some_bad_dimension_name',
                     agg_dict=agg,
                     to_csv=True)

except KeyError as e:

    logging.info('Key error for bad table name failed as expected: %s' %e)

else:

    raise RuntimeError('Query on invalid table name should have failed')
