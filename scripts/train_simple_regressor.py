#!/usr/bin/python3
# *****************************************************************************
# © Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import sys
import getopt
import logging
import json
from iotfunctions import estimator
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions import pipeline as pp

import datetime as dt

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


def main(argv):

    # entityType = 'Clients04'
    entityTypeId = None
    featureC = 'pressure'
    targetC = 'temperature'
    predictC = 'predict'
    startTime = None
    endTime = None
    startTimeV = dt.datetime.utcnow()
    endTimeV = dt.datetime.utcnow()
    helpString = 'train.py -E <entityTypeId> -f <feature column> -o <target column> -p <prediction column> \
-s <starttime> -e <endtime>'

    try:
        opts, args = getopt.getopt(
            argv, "hf:t:p:s:e:E:", ["featureC=", "targetC=", "predictC=", "startTime=", "endTime=", "entityTypeId="])
    except getopt.GetoptError:
        print(helpString)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helpString)
            sys.exit()
        elif opt in ("-E", "--entityTypeId"):
            entityTypeId = int(arg)
        elif opt in ("-f", "--feature"):
            featureC = arg
        elif opt in ("-t", "--target"):
            targetC = arg
        elif opt in ("-p", "--predict"):
            predictC = arg
        elif opt in ("-s", "--starttime"):
            startTime = arg
        elif opt in ("-e", "--endtime"):
            endTime = arg
    print('EntityTypeId "', entityTypeId)
    print('Feature Column (X) "', featureC)
    print('Target Column (Y) "', targetC)
    print('Predictor Column "', predictC)
    print('StartTime "', startTime)
    print('EndTime "', endTime)

    if entityTypeId is None:
        print('entityTypeId is missing')
        print(helpString)
        sys.exit(3)

    # endTime == None means now

    if startTime == None:
        print('startTime is missing, please specify relative to endTime (-3 means 3 days before endTime)')
        print(helpString)
        sys.exit(4)
    else:
        startTimeV = dt.datetime.utcnow() - dt.timedelta(days=int(startTime))

    # db_schema = None
    db = Database(credentials=credentials)
    print(db)

    meta = db.get_entity_type(entityTypeId)

    logger.info('Connected to database')

    est = estimator.SimpleRegressor(features=[featureC], targets=[targetC], predictions=[predictC])
    est.delete_existing_models = True
    meta._functions = [est]

    logger.info('Created Regressor')

    # make sure the results of the python expression is saved to the derived metrics table
    meta._data_items.append({'columnName': predictC, 'columnType': 'NUMBER', 'kpiFunctionId': 22856,
                             'kpiFunctionDto': {'output': {'name': predictC}},
                             'name': predictC, 'parentDataItemName': None, 'sourceTableName': 'DM_CLIENTS04',
                             'tags': {}, 'transient': True, 'type': 'DERIVED_METRIC'})

    jobsettings = {'_production_mode': False,
                   '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
                   '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
                   '_db_schema': 'BLUADMIN',
                   'save_trace_to_file': True}

    logger.info('Instantiated training job')

    job = pp.JobController(meta, **jobsettings)
    job.execute()

    logger.info('Model trained')

    return


if __name__ == "__main__":
    main(sys.argv[1:])
