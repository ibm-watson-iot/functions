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
entity = EntityType(entity_name,db,
                    Column('x1',Float()),
                    Column('x2',Float()),
                    Column('x3',Float()),
                    Column('y_0',Float()),
                    bif.EntityDataGenerator(output_item='generator_ok'),
                    bif.PythonExpression(       # linear relatoionship
                        '5*df["x1"]-df["x2"]',
                        'y1'),
                    bif.PythonExpression(
                        'df["x1"]*df["x1"]-df["x2"]',   # non-linear relationship
                        'y3'
                    ),
                    bif.RandomNoise(                # add noise to y1 and y3 to produce y2 and y4
                        input_items = ['y1','y3'],
                        standard_deviation= .5,
                        output_items= ['y2','y4']
                    ),
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

The log provides a lot of insight as to what happened under the covers during model training.

2019-08-27T15:41:54.644 DEBUG iotfunctions.base.make_estimators Selected estimators ['gradient_boosted_regressor', 'gradient_boosted_regressor', 'sgd_regressor', 'gradient_boosted_regressor', 'sgd_regressor']
C:\Users\mike\Anaconda3\envs\functions\lib\site-packages\sklearn\model_selection\_split.py:1978: FutureWarning: The default value of cv will change from 3 to 5 in version 0.22. Specify it explicitly to silence this warning.
  warnings.warn(CV_WARNING, FutureWarning)
    
2019-08-27T15:41:56.549 DEBUG iotfunctions.base.fit_with_search_cv Used randomize search cross validation to find best hyper parameters for estimator RandomizedSearchCV
2019-08-27T15:41:56.560 DEBUG iotfunctions.base.find_best_model Trained estimator SimpleRegressor with an r2_score score of 0.9998844604767325
2019-08-27T15:41:56.564 INFO iotfunctions.metadata.test evaluated model model.predict_test.SimpleRegressor.y1 with evaluation metric value 0.9989645842025136
2019-08-27T15:41:56.564 DEBUG iotfunctions.base.find_best_model No prior model, first created is best
C:\Users\mike\Anaconda3\envs\functions\lib\site-packages\sklearn\model_selection\_split.py:1978: FutureWarning: The default value of cv will change from 3 to 5 in version 0.22. Specify it explicitly to silence this warning.
  warnings.warn(CV_WARNING, FutureWarning)
2019-08-27T15:41:57.371 DEBUG iotfunctions.base.fit_with_search_cv Used randomize search cross validation to find best hyper parameters for estimator RandomizedSearchCV
2019-08-27T15:41:57.384 DEBUG iotfunctions.base.find_best_model Trained estimator SimpleRegressor with an r2_score score of 0.9821804618200153
2019-08-27T15:41:57.388 INFO iotfunctions.metadata.test evaluated model model.predict_test.SimpleRegressor.y1 with evaluation metric value 0.9805138574145589
C:\Users\mike\Anaconda3\envs\functions\lib\site-packages\sklearn\model_selection\_split.py:1978: FutureWarning: The default value of cv will change from 3 to 5 in version 0.22. Specify it explicitly to silence this warning.
  warnings.warn(CV_WARNING, FutureWarning)
2019-08-27T15:41:57.424 DEBUG iotfunctions.base.fit_with_search_cv Used randomize search cross validation to find best hyper parameters for estimator RandomizedSearchCV
2019-08-27T15:41:57.425 DEBUG iotfunctions.base.find_best_model Trained estimator SimpleRegressor with an r2_score score of 0.9999999910107339
2019-08-27T15:41:57.426 INFO iotfunctions.metadata.test evaluated model model.predict_test.SimpleRegressor.y1 with evaluation metric value 0.999999990975172
2019-08-27T15:41:57.426 DEBUG iotfunctions.base.find_best_model Higher than previous best of 0.9989645842025136. New metric is 0.999999990975172
C:\Users\mike\Anaconda3\envs\functions\lib\site-packages\sklearn\model_selection\_split.py:1978: FutureWarning: The default value of cv will change from 3 to 5 in version 0.22. Specify it explicitly to silence this warning.
  warnings.warn(CV_WARNING, FutureWarning)
2019-08-27T15:42:00.912 DEBUG iotfunctions.base.fit_with_search_cv Used randomize search cross validation to find best hyper parameters for estimator RandomizedSearchCV
2019-08-27T15:42:00.919 DEBUG iotfunctions.base.find_best_model Trained estimator SimpleRegressor with an r2_score score of 0.9995330671315739
2019-08-27T15:42:00.921 INFO iotfunctions.metadata.test evaluated model model.predict_test.SimpleRegressor.y1 with evaluation metric value 0.9988003605416698
C:\Users\mike\Anaconda3\envs\functions\lib\site-packages\sklearn\model_selection\_split.py:1978: FutureWarning: The default value of cv will change from 3 to 5 in version 0.22. Specify it explicitly to silence this warning.
  warnings.warn(CV_WARNING, FutureWarning)
2019-08-27T15:42:00.949 DEBUG iotfunctions.base.fit_with_search_cv Used randomize search cross validation to find best hyper parameters for estimator RandomizedSearchCV
2019-08-27T15:42:00.950 DEBUG iotfunctions.base.find_best_model Trained estimator SimpleRegressor with an r2_score score of 0.9999999908477505
2019-08-27T15:42:00.951 INFO iotfunctions.metadata.test evaluated model model.predict_test.SimpleRegressor.y1 with evaluation metric value 0.999999990808704
2019-08-27T15:42:00.952 DEBUG iotfunctions.base.execute Trained model: {
 "eval_metric_name": "r2_score",
 "target": "y1",
 "features": [
  "x1",
  "x2",
  "x3"
 ],
 "name": "model.predict_test.SimpleRegressor.y1",
 "estimator_name": "sgd_regressor",
 "trained_date": "2019-08-27T22:41:57.425850",
 "eval_metric_train": 0.9999999910107339,
 "expiry_date": null,
 "eval_metric_test": 0.999999990975172
}



'''

