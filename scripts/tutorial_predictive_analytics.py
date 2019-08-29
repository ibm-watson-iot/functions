import json
import logging
import numpy as np
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions import estimator
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
and functions for dependent target variables:

x0, x1 and x2 are independent random variables
y0 is also an independent random variable - it will not be possible to predict this variable
y1 is a direct linear function of x1 and x2 - it will be a breeze to predict
y2 is y1 with some added noise thrown in to make it more difficult to fit a model
y3 has a non linear relation to x1 and x2, but will be easy to predict with the right estimator
y4 is y3 with some noise

We will start by trying to predict the easy on: y1 using the SimpleRegressor function.

'''

entity_name = 'predict_test'                    # you can give your entity type a better nane
db = Database(credentials = credentials)
db_schema = None                                # set if you are not using the default
db.drop_table(entity_name)

fn_gen = bif.EntityDataGenerator(output_item='generator_ok')
fn_dep1 = bif.PythonExpression(  # linear relatoionship
    '5*df["x1"]-df["x2"]',
    'y1')
fn_dep2 = bif.PythonExpression(
    'df["x1"]*df["x1"]-df["x2"]',  # non-linear relationship
    'y3'
)
fn_noise = bif.RandomNoise(  # add noise to y1 and y3 to produce y2 and y4
    input_items=['y1', 'y3'],
    standard_deviation=.5,
    output_items=['y2', 'y4']
)

job_settings = {
    'delete_existing_models' : True,
}

entity = EntityType(entity_name,db,
                    Column('x1',Float()),
                    Column('x2',Float()),
                    Column('x3',Float()),
                    Column('y0',Float()),
                    fn_gen,
                    fn_dep1,
                    fn_dep2,
                    fn_noise,
                    estimator.SimpleRegressor(
                        features = ['x1','x2','x3'],
                        targets = ['y1'],
                        predictions = ['y1_predicted']),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date,**job_settings)

'''
When we execute the pipeline, it runs the preload generator, builds the target variables
and then executes the SimpleRegressor function on 30 days worth of data.

The first time the SimpleRegressor function executes, there is no trained model to 
predict y1, so the SimpleRegressor trains one and writes it in serialized form to COS.

It uses the trained model to predict y1. As expected, with y1 as a direct linear
relationship of x1 and x2, the model is good and the predictions spot on.

Here is a example of the results.

x1	            x3	            deviceid	x2	        _timestamp	        entitydatagenerator	    y1	            y3	            y2	            y4	            y1_predicted
-0.934393311	1.513619105	    73002	    0.571025693	2019/08/25 22:46	TRUE	                -5.242992247	0.302065166	    -5.691004953	-0.195822392	-5.242386625
0.012830963	    0.527282154	    73004	    1.957853419	2019/08/25 22:51	TRUE	                -1.893698605	-1.957688786	-2.566059999	-2.032410595	-1.893369331
0.552446078	    -0.909958249	73004	    1.443942632	2019/08/25 22:56	TRUE	                1.318287756	    -1.138745963	 0.384865548	-0.677763842	1.31826046
0.337931424	    1.107722852	    73004	    0.031767608	2019/08/25 23:01	TRUE	                1.657889509	    0.082430039	    1.404252135	    0.296344757	    1.657816656

The trace provides a lot of insight into the model training process:

The SimpleRegressor looks for an existing model in COS. To make sure that there was none,
we set the job parameter, "delete_existing_models" to True.

In the trace we see that there was no existing model for y1 so training is required.

"predicting target y1": "{'training_required': 'Training required because there is no existing model', 'use_existing_model': False}",

During training, the SimpleReggressor trained 5 models, evaluated each against a test dataset and kept the best model.

"Trained model: 0": "{'eval_metric_name': 'r2_score', 'col_name': 'y1_predicted', 'name': 'model.predict_test.SimpleRegressor.y1', 'eval_metric_test': 0.9986195070793537, 'eval_metric_train': 0.9995248209793295, 'evaluation_outcome': 'No prior model, first created is best', 'target': 'y1', 'params': {'learning_rate': 0.05, 'loss': 'ls', 'n_estimators': 100, 'min_samples_split': 5, 'max_depth': 4}, 'estimator_name': 'gradient_boosted_regressor', 'shelf_life_days': None, 'features': ['x1', 'x2', 'x3']}",
"Trained model: 1": "{'eval_metric_name': 'r2_score', 'col_name': 'y1_predicted', 'name': 'model.predict_test.SimpleRegressor.y1', 'eval_metric_test': 0.998773736716654, 'eval_metric_train': 0.9993632345825462, 'evaluation_outcome': 'Higher than previous best of 0.9986195070793537. New metric is 0.998773736716654', 'target': 'y1', 'params': {'learning_rate': 0.05, 'loss': 'ls', 'n_estimators': 250, 'min_samples_split': 9, 'max_depth': 2}, 'estimator_name': 'gradient_boosted_regressor', 'shelf_life_days': None, 'features': ['x1', 'x2', 'x3']}",
"Trained model: 2": "{'eval_metric_name': 'r2_score', 'col_name': 'y1_predicted', 'name': 'model.predict_test.SimpleRegressor.y1', 'eval_metric_test': 0.9999999904193875, 'eval_metric_train': 0.9999999905143194, 'evaluation_outcome': 'Higher than previous best of 0.998773736716654. New metric is 0.9999999904193875', 'target': 'y1', 'params': {'tol': 0.005, 'max_iter': 5000}, 'estimator_name': 'sgd_regressor', 'shelf_life_days': None, 'features': ['x1', 'x2', 'x3']}",
"Trained model: 3": "{'eval_metric_name': 'r2_score', 'col_name': 'y1_predicted', 'name': 'model.predict_test.SimpleRegressor.y1', 'eval_metric_test': 0.9992890097471945, 'eval_metric_train': 0.9999613533343915, 'target': 'y1', 'params': {'learning_rate': 0.05, 'loss': 'ls', 'n_estimators': 1000, 'min_samples_split': 2, 'max_depth': 4}, 'estimator_name': 'gradient_boosted_regressor', 'shelf_life_days': None, 'features': ['x1', 'x2', 'x3']}",
"Trained model: 4": "{'eval_metric_name': 'r2_score', 'col_name': 'y1_predicted', 'name': 'model.predict_test.SimpleRegressor.y1', 'eval_metric_test': 0.9999999912219784, 'eval_metric_train': 0.9999999912985721, 'evaluation_outcome': 'Higher than previous best of 0.9999999904193875. New metric is 0.9999999912219784', 'target': 'y1', 'params': {'tol': 0.001, 'max_iter': 10000}, 'estimator_name': 'sgd_regressor', 'shelf_life_days': None, 'features': ['x1', 'x2', 'x3']}",

The last model was the best so saved it to COS for later and used it for scoring.

If we execute again without deleting existing models, there will be no need to
retrain. Scoring will take place using the existing saved model.

'''

job_settings = {
    'delete_existing_models' : False,
}

entity = EntityType(entity_name,db,
                    Column('x1',Float()),
                    Column('x2',Float()),
                    Column('x3',Float()),
                    Column('y0',Float()),
                    fn_gen,
                    fn_dep1,
                    fn_dep2,
                    fn_noise,
                    estimator.SimpleRegressor(
                        features = ['x1','x2','x3'],
                        targets = ['y1'],
                        predictions = ['y1_predicted']),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date,**job_settings)

'''

The trace confirms that the SimpleRegressor reused the existing model.

"predicting target y1": "{'training_required': 'Existing model has not expired and eval metric is good', 'use_existing_model': True}",

We can confirm that the model is good by comparing the predictions with actuals on this newly regenerated data.

y1	            y1_predicted
-8.914096764	-8.888921485
-2.219113188	-2.203265514
1.968634163	    1.977106634
-0.959846407	-0.950027128
4.312884893	    4.312573849
8.264972582	    8.267702692
9.587129826	    9.586387549

The SimpleRegressor will continue using this model until its shelf life expiration date is reached.

'''

'''

We included y0 as a "bogus" target in the simulated data. It is random. Let's see what happens if we
try to predict it.

'''

fn_regression =    estimator.SimpleRegressor(
                        features = ['x1','x2','x3'],
                        targets = ['y0'],
                        predictions = ['y0_predicted'])

entity = EntityType(entity_name,db,
                    Column('x1',Float()),
                    Column('x2',Float()),
                    Column('x3',Float()),
                    Column('y0',Float()),
                    fn_gen,
                    fn_dep1,
                    fn_dep2,
                    fn_noise,
                    fn_regression,
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date)

'''
As expected, the results were less than spectacular. The estimator function still produced
a model, but it was a bad one. The r2 evaluation metric for the model on the test
dataset was 0.0078. 1 is a good value for r2. 0 is the really bad - the same as
random predictions. This score was close to zero. Since y0 is a random variable
with no relation to the features x1,x2 and x3, this is exactly as expected. 

y0	            y0_predicted
1.001255086	    0.327983859
-0.453189058	0.446344744
0.588423838     0.372199524
0.111952277     0.414435226
-0.099508104	0.358838611
1.14504302	    0.319323414
0.516061775	    0.272804999
1.060961476	    0.491862264
1.17796182	    0.479088163
1.229812989	    0.399665254
-0.227189551	0.312524269
1.784261783	    0.284606893
1.23220452	    0.364813048
-1.46457623	    0.407106247
0.792823309	    0.347049342
2.256637189	    0.471299716
-0.930970096	0.403213826
1.299849719	    0.353700911

The SimpleRegressor function has a couple of thresholds that you can set that
govern acceptable evaluation metrics. Let's start with setting
acceptable_score_for_model_acceptance. This is the minimal evaluation
metric value at which the model will actually be deloyed and used.
 
We will increase this to 0.5 and see what happens.

'''
job_settings = {
    'delete_existing_models' : True,
    'acceptable_score_for_model_acceptance' : 0.5
}

entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date,**job_settings)

'''
y_predict is null

This is expected behavior
The SimpleRegressor couln't fit a model with acceptable accuracy (r2 > 0.5) so it
did not perform scoring.

The trace provides confirmation of this:
"predicting target y0": "{'use_existing_model': False, 'training_required': 'Training required because there is no existing model'}"

You can also see the 5 unsuccessful attempts to train a model:

"Trained model: 0": "{'eval_metric_train': 0.027373912353378937, 'estimator_name': 'gradient_boosted_regressor', 'eval_metric_name': 'r2_score', 'evaluation_outcome': 'No prior model, first created is best', 'features': ['x1', 'x2', 'x3'], 'target': 'y0', 'shelf_life_days': None, 'col_name': 'y0_predicted', 'params': {'n_estimators': 250, 'loss': 'ls', 'learning_rate': 0.02, 'min_samples_split': 9, 'max_depth': 2}, 'name': 'model.predict_test.SimpleRegressor.y0', 'eval_metric_test': -0.002655277863766292}",
"Trained model: 1": "{'eval_metric_train': 0.004603787971566575, 'estimator_name': 'sgd_regressor', 'eval_metric_name': 'r2_score', 'evaluation_outcome': 'Higher than previous best of -0.002655277863766292. New metric is -0.0007227555842390654', 'features': ['x1', 'x2', 'x3'], 'target': 'y0', 'shelf_life_days': None, 'col_name': 'y0_predicted', 'params': {'tol': 0.002, 'max_iter': 1000}, 'name': 'model.predict_test.SimpleRegressor.y0', 'eval_metric_test': -0.0007227555842390654}",
"Trained model: 2": "{'eval_metric_train': 0.09419041443126963, 'estimator_name': 'gradient_boosted_regressor', 'eval_metric_name': 'r2_score', 'features': ['x1', 'x2', 'x3'], 'target': 'y0', 'shelf_life_days': None, 'col_name': 'y0_predicted', 'params': {'n_estimators': 500, 'loss': 'ls', 'learning_rate': 0.05, 'min_samples_split': 5, 'max_depth': 2}, 'name': 'model.predict_test.SimpleRegressor.y0', 'eval_metric_test': -0.013250721331356186}",
"Trained model: 3": "{'eval_metric_train': 0.04354194300816494, 'estimator_name': 'gradient_boosted_regressor', 'eval_metric_name': 'r2_score', 'features': ['x1', 'x2', 'x3'], 'target': 'y0', 'shelf_life_days': None, 'col_name': 'y0_predicted', 'params': {'n_estimators': 100, 'loss': 'ls', 'learning_rate': 0.02, 'min_samples_split': 5, 'max_depth': 4}, 'name': 'model.predict_test.SimpleRegressor.y0', 'eval_metric_test': -0.0029473827611010694}",
"Trained model: 4": "{'eval_metric_train': 0.2570767933935436, 'estimator_name': 'gradient_boosted_regressor', 'eval_metric_name': 'r2_score', 'features': ['x1', 'x2', 'x3'], 'target': 'y0', 'shelf_life_days': None, 'col_name': 'y0_predicted', 'params': {'n_estimators': 100, 'loss': 'ls', 'learning_rate': 0.02, 'min_samples_split': 9, 'max_depth': 10}, 'name': 'model.predict_test.SimpleRegressor.y0', 'eval_metric_test': -0.03436886346855683}",

The next time the pipeline runs, the SimpleRegressor will try to train the model again.

In this example you saw how the parameter  acceptable_score_for_model_acceptance governs
whether or not a model is suitable for scoring.

There is another important lifecyle management parameter: stop_auto_improve_at

This parameter also influences the decision about whether retraining is required.

Let's use these parameters to tell the SimpleRegressor that it can accept any model with a score
of better than 0, but that it should continue trying to improve until it gets at least 0.5.

'''

job_settings = {
    'delete_existing_models' : False,
    'acceptable_score_for_model_acceptance' : 0,
    'stop_auto_improve_at' : 0.5
}

entity.register(raise_error=True)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date,**job_settings)

#execute twice to confirm that retraining takes place again

entity.exec_local_pipeline(start_ts=start_date,**job_settings)

'''
During this execution we see that scores were produced on both
and training took place on both executions. The current model
is the best model selected from the 10 training attempts in 2 runs.


'''