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

Once you have mastered PythonExpressions and SimpleFunctions and you are ready
to experience AS in all its glory, the next logical step is custom functions.

There are inherent limitations with both PythonExpressions and PythonFunctions.
PythonExpressions are a single line of code. PythonFunctins are complete code
blocks, but they may only produce a single output item.

Custom Functions elliminate these limitations and give you the ability to organise
your code in a more robust way as a python package.

At the heart of this package is one or python module containing custom
classes that you can register with the AS Function Catalog and use within
the UI.

This script introduces the basic workflow of building, testing and registering
custom functions.

When working with AS you will need to provide credentials. These credentials
are used to establish a connection to the database, COS and API server via an
AS Database object.

You should copy the credentials from the UI. They can be found in the Usage
page under section Watson IoT Platform Analytics 

Your credentials will look something like this:
    

credentials = {
  "tenantId": "AnalyticsServiceDev",
  "db2": {
    "username": "xxxxxxxxx",
    "password": "xxxxxxxxx",
    "databaseName": "BLUDB",
    "port": 50000,
    "httpsUrl": "xxxxxxxxx",
    "host": "xxxxxxxxx"
  },
  "iotp": {
    "url": "xxxxxxxxx",
    "orgId": "xxxxxxxxx",
    "host": "xxxxxxxxx",
    "port": xxxxxxxxx,
    "asHost": "xxxxxxxxx",
    "apiKey": "xxxxxxxxx",
    "apiToken": "xxxxxxxxx"
  },
  "messageHub": {
    "brokers": [
      "xxxxxxxxx",
      "xxxxxxxxx"
    ],
    "username": "xxxxxxxxx",
    "password": "xxxxxxxxx"
  },
  "objectStorage": {
    "region": "global",
    "url": "xxxxxxxxx",
    "username": "xxxxxxxxx",
    "password": "xxxxxxxxx"
  },
  "config": {
    "objectStorageEndpoint": "xxxxxxxxx",
    "bos_logs_bucket": "xxxxxxxxx",
    "bos_runtime_bucket": "xxxxxxxxx",
    "mh_topic_analytics_alerts": "xxxxxxxxx"
  }
}
  
You could define your credentials by setting the value of variable
to the contents of the dict from the UI, but to avoid the risk of 
inadvertently exposing credentials when sharing a script, place
the credentials in a file instead.

For this script I am using credentials located in a file called 
credentials_as_dev.json.

'''

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
The db_schema is the db2_schema name for your client database. If 
you are using the default schema of your user, there is no need to
provide a schema.
'''
db_schema = None


'''
Use the credentials to build an AS Database connection.
'''

db = Database(credentials=credentials)

'''

We will start with a simple custom function that multipies 2 numeric items.

This function performs data transformation. Data transformation functions
have an execute method that accepts a datafame as input and returns a 
modified dataframe.

The __init__ method describes the parameters of the function. This represents
the signature of the function and determines what configuration options
will be show for the function in the UI.

This sample function has three parameters:
    input_item_1, input_item_2 : The names of the input items to be multiplied
    output_item : The name of the output item produced

When producing functions that are going to be made available to AS users from
the AS catalog, your goal is make functions that are reusable on multiple
entity types. When building PythonExpression and PythonFunctions we 
could refer to specific data items in the function configuration because
we were describing a single instance of a function. With a custom catalog
function we avoid hardcoding anything specific to a particular entity type.
This function will be bound to specific items at the time of configuration.
When building the function we specify placeholders for data items.

AS will automatically generate a wizard to configure this function. To allow
AS to generate this wizard, you will need to describe a little bit more about
the arguments to the function. You do this by including a build_ui() class
method.

input_item_1 and input_item_2 are parameters that describe data items that
the function acts on. They are considered inputs to the function as they are
used by the function to produce another data item that serves as the output.

The build_ui() method returns a tuple containing two lists of arguments: 
the arguments that define the function inputs and the arguments that 
describe the output data items produced by the function.

Each function needs at least one input item and has to produce at least one
output item. The input need not be a data item, it could be a constant.
We will get to those later.

The ui module of iotfunctions contains a number of ui classes. For each 
input argument, choose an appropriate class. Since these inputs are
both single data items, we use th UISingleItem class.

There are only two output classes, UIFunctionOutSingle and UIFunctionOutMultiple.
Since the multiplication of two input data items produces a single 
output item (not an array of multiple data items), we use the 
UIFunctionOutSingle class.

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









    
    




