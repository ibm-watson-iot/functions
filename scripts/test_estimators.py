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
                        ),
    est.SimpleAnomaly(features=['x1', 'x2', 'x3'],
                      targets=['y1','y2'],
                      threshold = 0.1,
                      predictions=['y1_pred', 'y2_pred'],
                      alerts = ['is_y1_anomaly', 'is_y2_anomaly'])
    ]

params = {
    'auto_train': True,
    'experiments_per_execution': 1,
    'parameter_tuning_iterations': 3,
    'test_size': 0.2,
    'stop_auto_improve_at': 0.85,
    'acceptable_score_for_model_acceptance': -1,
    'greater_is_better': True,
    'version_model_writes': False
}

for s in samples:
    s.execute_local_test(db=db, columns=cols, **params)
