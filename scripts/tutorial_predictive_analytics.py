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
entity.exec_local_pipeline(start_ts=start_date)

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

Let's see what happens if we try the same with predicting y0.

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