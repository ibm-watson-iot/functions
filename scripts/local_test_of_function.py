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
You can test functions locally before registering them on the server to
understand how they work.

Supply credentials by pasting them from the usage section into the UI.
Place your credentials in a separate file that you don't check into the repo. 

'''

with open('credentials_as.json', encoding='utf-8') as F:
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
from customagg.functions import HelloWorldAggregator

fn = HelloWorldAggregator(
        source = ['speed'],
        expression = '${GROUP}.max()'
        )

df = fn.execute_local_test(db=db, db_schema=db_schema,to_csv=False)
print(df)
