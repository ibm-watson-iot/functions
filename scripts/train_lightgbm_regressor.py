#!/usr/bin/python3

import sys
import getopt
import logging
import json
from iotfunctions import estimator
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
#from iotfunctions.anomaly import GBMRegressor
from mmfunctions.anomaly import GBMRegressor
from iotfunctions import pipeline as pp

import datetime as dt

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


def main(argv):

    # entityType = 'Clients04'
    entityType = ''
    featureC = 'pressure'
    targetC = 'temperature'
    predictC = 'predict'
    startTime = None
    endTime = None
    startTimeV = 0
    endTimeV = 0
    helpString = 'train.py -E <entityType> -f <feature column> -t <target column> -p <prediction column> \
-s <starttime> -e <endtime>'

    try:
        opts, args = getopt.getopt(
            argv, "hf:t:p:s:e:E:", ["featureC=", "targetC=", "predictC=", "startTime=", "endTime=", "entityType="])
    except getopt.GetoptError as ge:
        print(str(ge))
        print(helpString)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(helpString)
            sys.exit()
        elif opt in ("-E", "--entityType"):
            entityType = arg
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
    print('EntityType "', entityType)
    print('Feature Column (X) "', featureC)
    print('Target Column (Y) "', targetC)
    print('Predictor Column "', predictC)
    print('StartTime "', startTime)
    print('EndTime "', endTime)

    if entityType == '':
        print('entityType name is missing')
        print(helpString)
        sys.exit(3)

    # endTime == None means now
    if endTime == None:
        endTimeV = 0
    else:
        endTimeV = eval(endTime)

    if startTime == None:
        print('startTime is missing, please specify relative to endTime (3 means 3 days before endTime)')
        print(helpString)
        sys.exit(4)
    else:
        startTimeV = eval(startTime) + endTimeV

    # db_schema = None
    db = Database(credentials=credentials)
    print(db)

    meta = db.get_entity_type(entityType)

    logger.info('Connected to database')

    gbm = GBMRegressor(features=[featureC], targets=[targetC], threshold=2, predictions=[predictC])
                       #max_depth=20, num_leaves=40, n_estimators=4000, learning_rate=0.00001)

    gbm.delete_existing_models = True

    #gbm_fndef = db.load_catalog(function_list=['GBMRegressor'])
    #print (gbm_fndef)

    meta._functions = [gbm]

    logger.info('Created Regressor')

    # make sure the results of the python expression is saved to the derived metrics table
    meta._data_items.append({'columnName': predictC, 'columnType': 'NUMBER', 'kpiFunctionId': 22856,
                             'kpiFunctionDto': {'output': {'name': predictC}},
                             'name': predictC, 'parentDataItemName': None, 'sourceTableName': 'DM_CLIENTS04',
                             'tags': {}, 'transient': True, 'type': 'DERIVED_METRIC'})

    jobsettings = {'db': db,
                   '_production_mode': False,
                   '_start_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=startTimeV)),
                   '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=endTimeV)),
                   '_db_schema': 'BLUADMIN',
                   'save_trace_to_file': True}

    logger.info('Instantiated training job')

    job = pp.JobController(meta, **jobsettings)
    job.execute()

    logger.info('Model trained')

    return


if __name__ == "__main__":
    main(sys.argv[1:])
