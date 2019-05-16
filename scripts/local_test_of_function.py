import datetime as dt
import json
import pandas as pd
import numpy as np
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.base import BaseTransformer
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions import ui

'''
You can test functions locally before registering them on the server to
understand how they work.

In this script I am using a function from iotfunctions, but you can
do this with any function derived from the iotfunctions base classes.

'''

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

'''
Import and instantiate the functions to be tested
'''

from iotfunctions.bif import DateDifference

fn = DateDifference(date_1 = 'd1', date_2 = 'd2', num_days = 'difference')

'''
Execute the local test.

The local test will generate data instead of using server data.
By default it will assume that the input data items are numeric.
This function requires date imputs. To indicate that the function should
be tested using date inputs declare two date columns as below.

'''

fn.execute_local_test(columns = [
        Column('d1',DateTime),
        Column('d2',DateTime)
        ])

