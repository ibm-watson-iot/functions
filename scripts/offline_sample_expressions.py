"""
Offline samples will help you get familiar with the python expression syntax
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

PythonExpression = namedtuple('Exp', 'output expression')

'''
We will use offline test data to get familiar with python expressions.
This script will build a pandas dataframe for you representing data for
a list of entities. We will start with just a couple of data items:
'speed" and "time". 

'''

row_count = 5
data_items = ['speed','time']
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
Each AS Python Expression produces a new column in a Pandas DataFrame, ie:
a Pandas Series with the same index (number of rows and sort order) as the
input dataframe.

We will start with an expression to calculate distance from speed and time.

If you were to do this in python script, you would enter it as follows

'''

df["distance"] = df["speed"] * df["time"]
print(df)

'''
When using AS, you are not writing a python script. You are calling
python functions using metadata captured in the AS UI.

For the purposes of offline testing we will model AS expresions and their
metadata using a python tuple containing a new data item name and a string 
expression.

'''

ex1 = PythonExpression('distance','df["speed"]*df["time"]')

'''
Our tuple describes the column 'distance' as computed from the
input dataframe 'speed' column multipled by the input dataframe 'time'
column.

This is how to evaluate the expression defined above:
'''

df[ex1.output] = eval(ex1.expression)
print(df)

'''
Not all elements of your python expression have to dataframe columns.
You can also use constants. These will be broadcast to all rows of the
dataframe.

You can use the output of one expression in another too.

Let's calculate the half way mark.

'''

ex2 = PythonExpression('half_way_mark','df["distance"]/2')
df[ex2.output] = eval(ex2.expression)
print(df)

'''
Constants do not have to be hard-coded into your expression. You can
reference constants defined in the AS UI using a dictionary named c.

You have a AS constant called 'range' defined. You can use this to
calculated the number of refueling fuel stops.

Since the number of stops is an integer rather than a real number,
we will use the integer division operator //

'''

ex3 = PythonExpression('refuel_stops','df["distance"]//c["range"]')
df[ex3.output] = eval(ex3.expression)
print(df)

'''
You can also use numpy arrays.The array must have the same length as the
dataframe though.
'''

ex4 = PythonExpression('random_distance','df["distance"] + np.random.normal(10,1,row_count*len(entities))')
df[ex4.output] = eval(ex4.expression)
print(df)



    





