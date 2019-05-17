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

# this simple test function mimics the behavior of PythonFunction locally

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
    #  generate an 2-D array of random numbers
    import numpy as np
    output = np.random.normal(1,0.1,len(df.index))
    return output


print(test_simple_fn(f,df))

'''
If you are wondering why bother using a function for this when you could have
acheieved the same results with an expression, you are quite right. This single
line expression could have been placed in an expression.

The benefit of a function is that you are not restricted to a single line.

Notice how the random number I am generating has a hardcoded mean and 
standard deviation. Let's add some more code to calibrate it from the data.

'''

def f(df,parameters = None):
    import numpy as np
    #  generate an 2-D array of random numbers
    mean = df['speed'].mean()
    sd = df['speed'].std()
    output = np.random.normal(mean,sd,len(df.index))
    return output


print(test_simple_fn(f,df))

'''
Technically speaking, the mean and standard deviation could have gone into
the expression too, hopefully this illustrates the difference between a 
single line expression and a multi-line function.

Of course you can also add control logic to a function.

'''

def f(df,parameters = None):
    #  generate an 2-D array of random numbers
    import numpy as np
    mean = df['speed'].mean()
    sd = df['speed'].std()
    if df['travel_time'].min() < 90:
        sd = sd * 3
    output = np.random.normal(mean,sd,len(df.index))
    return output

print(test_simple_fn(f,df))


'''
If you need to loop through the incoming dataframe and produce a new
output for every row you can do that too, but keep in mind that
vectorized pandas and numpy functions are a lot faster than looping.
'''

def f(df,parameters = None):
    #  loop to calculate distance the slow and cumbersome way
    output = []
    for index, row in df.iterrows():
        output.append(row['speed']*row['travel_time'])
    return output


print(test_simple_fn(f,df))

'''
You can pass parameters to a simple functions.

This function uses a parameter called 'rating'.

'''

def f(df,parameters = None):
    #  generate an 2-D array of random number
    import numpy as np
    mean = df['speed'].mean() * parameters.get('rating',1)
    sd = df['speed'].std()
    output = np.random.normal(mean,sd,len(df.index))
    return output

print(test_simple_fn(f,df,parameters={'rating':.25}))

'''
There are also a few system parameters that will be automatically added to
the parameters dictionary.

Since these can't be tested offline, we will cover them in another sample.

If the body of the function is too large to be pasted into to the UI or you
are going to reuse the function and don't want multiple versions of it 
pasted into the UI, you can store the function in Cloud Object Storage.

See sample_cos_function

'''


