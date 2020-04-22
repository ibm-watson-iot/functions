#!/usr/bin/python3

import sys
import getopt
import logging
import json
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.anomaly import GBMRegressor
# from mmfunctions.anomaly import GBMRegressor
from iotfunctions.dbtables import DBModelStore
from iotfunctions import pipeline as pp
from sqlalchemy import Column, Float

import datetime as dt
import ibm_db

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

db = None
db_connection = None
entityType = ''
featureC = 'pressure'
targetC = 'temperature'
predictC = 'predict'
metric = None
startTime = None
endTime = None
startTimeV = 0
endTimeV = 0
helpString = 'train.py -E <entityType> -f <feature column> -t <target column> -p <prediction column> -m <metric> \
-s <starttime> -e <endtime>'


def get_options(argv):
    global db, db_connection, entityType, featureC, targetC, predictC, metric, startTime, endTime, startTimeV, endTimeV, helpString
    try:
        opts, args = getopt.getopt(
            argv, "hf:t:p:m:s:e:E:", ["featureC=", "targetC=", "predictC=", "metric=", "startTime=", "endTime=", "entityType="])
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
        elif opt in ("-m", "--metric"):
            metric = arg
        elif opt in ("-s", "--starttime"):
            startTime = arg
        elif opt in ("-e", "--endtime"):
            endTime = arg
    print('EntityType "', entityType)
    print('Feature Column (X) "', featureC)
    print('Target Column (Y) "', targetC)
    print('Predictor Column "', predictC)
    print('Metric Name"', metric)
    print('StartTime "', startTime)
    print('EndTime "', endTime)

    return


def main(argv):
    global db, db_connection, entityType, featureC, targetC, predictC, metric, startTime, endTime, startTimeV, endTimeV, helpString
    get_options(argv)

    if entityType == '':
        print('entityType name is missing')
        print(helpString)
        sys.exit(3)

    if metric != '':
        print('ditch feature, target and predicted values and get it from definition - this is the safest way')

    # endTime == None means now
    if endTime is None:
        endTimeV = 0
    else:
        endTimeV = eval(endTime)

    if startTime is None:
        print('startTime is missing, please specify relative to endTime (3 means 3 days before endTime)')
        print(helpString)
        sys.exit(4)
    else:
        startTimeV = eval(startTime) + endTimeV

    # db_schema = None
    db = Database(credentials=credentials)
    print(db)

    # establish a native connection to db2 to store the model
    DB2ConnString = 'DATABASE=' + credentials['db2']['databaseName'] + \
                    ';HOSTNAME=' + credentials['db2']['host'] + \
                    ';PORT=' + str(credentials['db2']['port']) + \
                    ';PROTOCOL=TCPIP;UID=' + credentials['db2']['username'] + \
                    ';PWD=' + credentials['db2']['password']

    db_connection = ibm_db.connect(DB2ConnString, '', '')
    print(db_connection)

    model_store = DBModelStore(credentials['tenantId'], entityType, credentials['db2']['username'], db_connection, 'db2')
    db.model_store = model_store

    # with open('output.json', 'w+', encoding='utf-8') as G:
    #    json.dump(db.entity_type_metadata, G)

    logger.info('Connected to database - SQL alchemy and native')

    meta = None
    try:
        meta = db.get_entity_type(entityType)
        print(meta)
    except Exception:
        logger.error('Failed to retrieve information about entityType ' + str(entityType) + ' from the database')

    # make sure the results of the python expression is saved to the derived metrics table
    if metric == '':
        # take the first suitable choice if there is no metric
        sourceTableName = ''
        for di in meta['dataItemDto']:
            sourceTableName = di['sourceTableName']
            if len(sourceTableName) > 0:
                break
        if len(sourceTableName) > 0:
            meta._data_items.append({'columnName': predictC, 'columnType': 'NUMBER', 'kpiFunctionId': 22856,
                                     'kpiFunctionDto': {'output': {'name': predictC}},
                                     'name': predictC, 'parentDataItemName': None, 'sourceTableName': sourceTableName,
                                     'tags': {}, 'transient': True, 'type': 'DERIVED_METRIC'})
        else:
            logger.error('No suitable derived metric table found')
            return
    else:
        found = False
        try:
            for di in meta['dataItemDto']:
                if di.name == metric:
                    found = True
                    predictC = di.columnName
                    break
            if not found:
                logger.error('Metric does not exist')
                return
        except Exception:
            pass

    gbm = GBMRegressor(features=[featureC], targets=[targetC], predictions=[predictC])

    # max_depth=20, num_leaves=40, n_estimators=4000, learning_rate=0.00001)

    gbm.delete_existing_models = True

    jobsettings = {'db': db,
                   '_production_mode': False,
                   '_start_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=startTimeV)),
                   '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=endTimeV)),
                   '_db_schema': 'BLUADMIN',
                   'save_trace_to_file': True}

    if meta is not None:
        meta._functions = [gbm]
    else:
        meta = gbm._build_entity_type(columns = [Column(featureC, Float()), # Column('accel_power_1',Float()),
                                                 Column(predictC, Float())], **jobsettings)
        gbm._entity_type = meta

    logger.info('Created Regressor')

    logger.info('Instantiated training job')

    job = pp.JobController(meta, **jobsettings)
    job.execute()

    logger.info('Model trained')

    return


if __name__ == "__main__":
    main(sys.argv[1:])
    if db_connection is not None:
        db_connection.disconnect()
    if db is not None:
        db.close()
