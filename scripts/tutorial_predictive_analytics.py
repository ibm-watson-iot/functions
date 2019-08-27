import json
import logging
import numpy as np
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
import datetime as dt

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Predictive Analytics Tutorial
-----------------------------

Note: The estimator functions are still in experimental state. 
They are not pre-registered. To use them in the AS UI you will need to register them.

In this tutorial you will learn how to use the built in estimator functions to build
and score using regression models and classification models. You will also build
an anomaly detection model. 

First will build a simulation using the EntityDataGenerator for random variables
and functions for dependent target variables.

'''

entity_name = 'predict_test'                    # you can give your entity type a better nane
db = Database(credentials = credentials)
db_schema = None                                # set if you are not using the default
db.drop_table(entity_name)
entity = EntityType(entity_name,db,
                    Column('x1',Float()),
                    Column('x2',Float()),
                    Column('x3',Float()),
                    Column('y_unpredicatable',Float()),
                    bif.EntityDataGenerator(output_item='generator_ok'),
                    bif.PythonExpression(
                        '5*df["x1"]+df["x2"]-0.5*df["x3"]',
                        'y1'),
                    bif.PythonExpression(
                        '5*df["x1"]*df["x1"]+np.sqrt(df["x2"])-0.5*df["x3"]',
                        'y2'
                    ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date)


