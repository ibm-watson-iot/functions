# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

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
a list of entities. 


We will start with just a couple of data items: 'speed" and "travel_time". 

'''

row_count = 5
data_items = ["speed", "travel_time"]
entities = ['XA01', 'XA02', 'XA03']
c = {'first_checkpoint': .3, 'second_checkpoint': 0.5, 'range': 500}

dfs = []
for e in entities:
    data = np.random.normal(100, 10, (row_count, len(data_items)))
    df = pd.DataFrame(data=data, columns=data_items)
    df['id'] = e
    df['evt_timestamp'] = pd.date_range(end=dt.datetime.utcnow(), periods=row_count)
    df = df.set_index(['id', 'evt_timestamp'])
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

df["distance"] = df["speed"] * df["travel_time"]
print(df)

'''
When using AS, you are not writing a python script. You are calling
python functions using metadata captured in the AS UI.

For the purposes of offline testing we will model AS expresions and their
metadata using a python tuple containing a new data item name and a string 
expression.

'''

ex1 = PythonExpression('distance', 'df["speed"]*df["travel_time"]')

'''
Our tuple describes the column 'distance' as computed from the
input dataframe 'speed' column multipled by the input dataframe 'time'
column.

This is how to evaluate the expression defined above:
'''

df[ex1.output] = eval(ex1.expression)
print(df)

'''
To use the expression in the PythonExpression function in the AS UI, paste
    
    df["speed"]*df["travel_time"]
    
as the expression and name the output of the expression. 

'''

'''
Not all elements of your python expression have to dataframe columns.
You can also use scalar values. These will be broadcast to all rows of the
dataframe.

You can use the output of one expression in another too.

Let's calculate the half way mark.

'''

ex2 = PythonExpression('half_way_mark', 'df["distance"]/2')
df[ex2.output] = eval(ex2.expression)
print(df)

'''
Scalars do not have to be hard-coded into your expression. You can
reference constants defined in the AS UI using a dictionary named c.

You have a AS constant called 'range' defined. You can use this to
calculated the number of refueling fuel stops.

Since the number of stops is an integer rather than a real number,
we will use the integer division operator //

'''

ex3 = PythonExpression('refuel_stops', 'df["distance"]//c["range"]')
df[ex3.output] = eval(ex3.expression)
print(df)

'''
You can also use numpy arrays.The array must have the same length as the
dataframe though.
'''

ex4 = PythonExpression('random_distance', 'df["distance"] + np.random.normal(10,1,row_count*len(entities))')
df[ex4.output] = eval(ex4.expression)
print(df)

'''
Since aggregate functions produce scalars, you can mix in aggregate functions
with vectorized parts of the expression.
'''

ex5 = PythonExpression('perc_total_distance', 'df["distance"] / df["distance"].sum()')
df[ex5.output] = eval(ex5.expression)

'''
So far all of our expressions involve the use of standard python operators.

Not all standard python operators work in vector expressions as they
will implicitly convert vectors to scalars and return a scalar results.
Sometimes you will need to call a vectorized function to replace an
operator. A good example is the logical operators and, or and not.

'''

'''
ex6 = PythonExpression(
        'far_and_fast_broken',
        'df["distance"]>200 and df["speed"]>200'
        )
df[ex6.output] = eval(ex6.expression)
print(df)
'''

'''

In the above example pandas is kind enough to return an error. "The truth
value for a Series ambiguous." In other cases the expression will execute,
but evaluate as scalars and then broadcast the scalar result back to the
rows of the dataframe, so you will always need to test your expression to
be sure that they are behaving as you intended.

In this case you can use you can use the & operator.

'''

ex6 = PythonExpression('far_and_fast', '(df["distance"]>105) & (df["speed"]>100)')
df[ex6.output] = eval(ex6.expression)
print(df)

'''
Generally here is no need to get frustrated trying to work these quirks out
by trial and error. There are so many new users of pandas asking lots of
questions online. I find that the search engines are really good at surfacing
valid answers. Search and you shall find!

You can also get familiar with the pandas documumentation for series and
dataframes. Pandas has all sorts of predefined functions for common analysis 
problems. 

Here are a few examples:

'''

ex7 = PythonExpression('is_moderate_speed', 'df["speed"].between(95,98)')
df[ex7.output] = eval(ex7.expression)
print(df)

'''
The next example reminds me of the pranks that the presenters of Top Gear
used to pull. Was I speeding? No never.
'''

ex8 = PythonExpression('speed_clipped', 'df["speed"].clip(92,100)')
df[ex8.output] = eval(ex8.expression)
print(df)

'''
After working through this script you should be comfortable writing and testing
PythonExpression and using them in the AS UI. You should understand the 
particular use cases that PythonExpression are suitable:
    --  transformations of data that produce a single output item
        in the form of a series or 2D array.
    --  can operate on vectors / broadcast scalar values
    --  do not add or remove rows of data.

'''
