import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.entity import make_sample_entity
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.base import BasePreload
from iotfunctions import ui

EngineLogging.configure_console_logging(logging.WARNING)

# replace with a credentials dictionary or provide a credentials file
with open('../scripts/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

db = Database(credentials=credentials)
db_schema = None  # set if you are not using the default


entity_1 = make_sample_entity(
    db = db,
    schema = db_schema,
    name = 'test_10_years',
    register = True,
    data_days = 3640,
    freq = '1d',
    entity_count = 2,
    float_cols = 3,
    date_cols = 2,
    string_cols = 1,
    bool_cols = 0
)

entity_2 = make_sample_entity(
    db = db,
    schema = db_schema,
    name = 'test_300_entities',
    register = True,
    data_days = 365,
    freq = '1min',
    entity_count = 300,
    float_cols = 40,
    date_cols = 5,
    string_cols = 5,
    bool_cols = 0
)


entity_3= make_sample_entity(
    db = db,
    schema = db_schema,
    name = 'test_200ms',
    register = True,
    data_days = 3,
    freq = '200ms',
    entity_count = 5,
    float_cols = 10,
    date_cols = 1,
    string_cols = 1,
    bool_cols = 0
)


