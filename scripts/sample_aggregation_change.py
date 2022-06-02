# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import json
import logging
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType, BaseCustomEntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('/Users/ryan/watson-iot/functions/scripts/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

db = Database(credentials=credentials)
db_schema = None  # set if you are not using the default

table = db.get_table('MIKE_ROBOT_JUNE_25')
dim = db.get_table('MIKE_ROBOT_JUNE_25_DIMENSION')

group_by = {'plant_abv': func.left(table.c['plant_code'], 3), 'manufacturer': dim.c['manufacturer']}
aggs = {'avg_speed': (table.c['speed'], func.avg)}


def prepare_aggregate_query(group_by, aggs):
    # build a sub query.
    sargs = []
    for alias, expression in list(group_by.items()):
        sargs.append(expression.label(alias))
    for alias, (metric, agg) in list(aggs.items()):
        sargs.append(metric.label(alias))
    db.start_session()
    query = db.session.query(*sargs)

    return query


def build_aggregate_query(subquery, group_by, aggs):
    # turn the subquery into a selectable
    subquery = subquery.subquery('a').selectable

    # build an aggregation query
    args = []
    grp = []
    for alias, expression in list(group_by.items()):
        args.append(subquery.c[alias])
        grp.append(subquery.c[alias])
    for alias, (metric, agg) in list(aggs.items()):
        args.append(agg(subquery.c[alias]).label(alias))
    query = db.session.query(*args)
    query = query.group_by(*grp)

    return query


sub = prepare_aggregate_query(group_by=group_by, aggs=aggs)
sub = sub.join(dim, dim.c['deviceid'] == table.c['deviceid'])
query = build_aggregate_query(sub, group_by, aggs)

print(query)
