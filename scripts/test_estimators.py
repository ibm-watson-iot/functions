import json
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
import iotfunctions.estimator as est

'''

This script tests the estimator functions locally

'''

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

db_schema = None
db = Database(credentials=credentials)

cols = [
]

samples = [
    est.SimpleRegressor(features=['x1', 'x2', 'x3'],
                        targets=['y1', 'y2'],
                        predictions=['y1_pred', 'y2_pred']
                        )
]

for s in samples:
    s.execute_local_test(db=db, columns=cols)
