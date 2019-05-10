"""
This offline script will help you get familiar with the python expression syntax
used to build AS PythonExpressions and other AS Functions that accept
Python expressions as arguments.

Offline samples do not require installation of iotfunctions or connection
to an AS server. You can paste these examples into Watson Studio, a local
Jupyter Notebook or use them in a local python IDE. They have minimal
prereqs.

"""

import datetime as dt
import numpy as np
import pandas as pd
from collections import namedtuple


def test_simple_fn(function,df,parameters = None,output_item = 'output'):
    if parameters is None:
        parameters = {}
    df[output_item] = f(df,parameters)
    return df

'''
We will use offline test data to get familiar with simple python functions
This script will build a pandas dataframe for you representing data for
a list of entities. 

We will start with just a couple of data items: 'speed" and "travel_time". 

'''

row_count = 5
data_items = ["speed","travel_time"]
entities = ['XA01','XA02','XA03']
c = {'first_checkpoint': .3,
     'second_checkpoint' : 0.5,
     'range' : 500}

dfs = []
for e in entities:
    data = np.random.normal(100,10,(row_count,len(data_items)))
    df = pd.DataFrame(data=data,columns = data_items)
    df['id'] = e
    df['evt_timestamp'] = pd.date_range(
            end=dt.datetime.utcnow(),
            periods=row_count
            )
    df = df.set_index(['id','evt_timestamp'])
    dfs.append(df)

df = pd.concat(dfs)
print(df)

'''
You can paste a simple python function directly into the AS UI using the
PythonFunction function.

What makes it "simple" is that it only has two arguments (a dataframe and a
dictionary of parameters) as inputs and it must return an array or series
as output.

The body of the function is standard python so the sky is the limit for 
what you can do inside your simple function.

We will start with a random number generator.

This function returns a np array with the same number of rows as the
dataframe it receives as input.

'''

def f(df,parameters = None):
    #generate an 2-D array of random numbers
    
    output = np.random.normal(1,0.1,len(df.index))
    return output


print(test_simple_fn(f,df))

