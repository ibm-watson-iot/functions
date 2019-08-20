import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.base import BaseTransformer
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions import ui
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

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

The local test will generate data instead of using server data.
By default it will assume that the input data items are numeric.

Required data items will be inferred from the function inputs.

The function below executes an expression involving a column called x1
The local test function will generate data dataframe containing the column x1

By default test results are written to a file named df_test_entity_for_<function_name>
This file will be written to the working directory.

'''

from iotfunctions.bif import AlertExpression
fn = AlertExpression(
        expression = 'df["x1"] > 1',
        alert_name = 'is_high_x1')
fn.execute_local_test(db=db,db_schema=db_schema)

from iotfunctions.bif import DateDifference
fn = DateDifference(date_1 = 'd1', date_2 = 'd2', num_days = 'difference')

'''
This function requires date imputs. To indicate that the function should
be tested using date inputs declare two date columns as below.

'''

fn.execute_local_test(columns = [
        Column('d1',DateTime),
        Column('d2',DateTime)
        ])


'''
If the function that you are testing requires assess to server resources,
pass a Database object
'''

from iotfunctions.bif import SaveCosDataFrame

fn = SaveCosDataFrame(
        filename = 'test_df_write',
        columns = ['x1','x2'],
        output_item = 'wrote_df'
        )

fn.execute_local_test(db=db,db_schema=db_schema)


