import datetime as dt
import json
import pandas as pd
import numpy as np
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.base import BaseTransformer
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions import ui

'''

Basic storyline is that you are monitoring a crew of manufacturing robots
Sometimes errors in programming make robots follow roundabout paths to get their work done  

'''

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

'''

Use PythonFunction to calculate distance traveled

'''

from iotfunctions import PythonFunction
dist = PythonFunction(expression='df["speed"] * df["travel_time"] ')
dist.execute_local_test(dist)



'''

class MultiplyTwoItems(BaseTransformer):
    '''
    Multiply two input items together to produce output column
    '''
    
    def __init__(self, input_item_1, input_item_2, output_item):
        self.input_item_1 = input_item_1
        self.input_item_2 = input_item_2
        self.output_item = output_item
        super().__init__()


    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item_1] * df[self.input_item_2]
        return df

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingleItem(
                name = 'input_item_1',
                datatype=float,
                description = 'Input item 1'
                                              ))
        inputs.append(ui.UISingleItem(
                name = 'input_item_2',
                datatype=float,
                description = "Input item 2"
                                              ))
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(
                name = 'output_item',
                datatype=float,
                description='output data'
                ))
        return (inputs,outputs)
    
'''

This python file is a script as it is executable. Each time AS runs the
MultiplyTwoItems function, it will not be executing this script.
Instead it will be importing the MultiplyTwoItems class from a python module
inside a python package. To include functions in the AS catalog, you 
will need to build a python package and include this class in module of that
package.

I defined the above class in this script just for so that I could use it to 
show the structure of an AS function. To actually test this class, we will not
use the version of it that I copied into the script, we will use the official
version of it - the one that exists in the sample.py. 

'''

from iotfunctions.sample import MultiplyTwoItems

'''

We have just replaced the MultiplyToItems class with the official version
from the sample package. To understand the structure and content of the
sample package look at sample.py

To test MultiplyTowItems you will need to create an instance of it and
indicate for this instance, which two items will it multiply and what
will the result data item be called.

'''

fn = MultiplyTwoItems(
        input_item_1='x1',
        input_item_2='x2',
        output_item='y')

df = fn.execute_local_test(generate_days=1,to_csv=True)
print(df)

'''

This automated local test builds a client side entity type,
adds the function to the entity type, generates data items that match
the function arguments, generates a dataframe containing the columns
referenced in these arguments, executes the function on this data and
writes the execution results to a file.

Note: Not all functions can be tested locally like this as some require
a real entity type with real tables and a real connection to the AS service

'''

''' 

The automated test assumes that data items are numeric. You can also
specify datatypes by passing a list of SQL Alchemy column objects.

'''

cols = [
    Column('string_1', String(255))
        ]

df = fn.execute_local_test(generate_days = 1,to_csv=True,
                           columns = cols)

'''
Custom functions must be registered in the AS function catalog before
you can use them. To register a function:
    
'''

db.register_functions([MultiplyTwoItems])

'''
After registration has completed successfully the function is available for
use in the AS UI.

The register_functions() method allows you to register more than one function
at a time. You can also register a whole module file with all of its functions.


'''

from iotfunctions import bif
db.register_module(bif)


'''

Note: The 'bif' module is preinstalled, so these functions will not actually
be registered by register_module.

This script covers the complete process for authoring and registering custom 
functions. The subject of the sample function used in this script was really
basic. To get an idea some of the more interesting things you can do in
custom functions, we encourage you to browse through the sample module.

'''









    
    




