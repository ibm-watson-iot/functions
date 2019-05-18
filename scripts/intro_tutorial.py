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

Use PythonExpression to calculate distance traveled

'''

from iotfunctions.bif import PythonExpression
dist = PythonExpression(expression='df["speed"] * df["travel_time"] ',
                        output_name = 'distance')
dist.execute_local_test()

'''
Use PythonFunction to identify outliers

'''

fnstr = '''def f(df,parameters = None):
    import numpy as np
    threshold = df['distance'].mean() + 2 * df['distance'].std()
    output = np.where(df['distance']>threshold,True,None)
    return output
'''

from iotfunctions.bif import PythonFunction
dist = PythonFunction(
            function_code = fnstr,
            input_items = ['distance'],
            output_item = 'is_distance_high',
            parameters = None )
dist.execute_local_test(db=db)


'''
The speed and travel times must be adjusted to account for a delay
The following function is added to a repo so that it can be used in the catalog


class MultiplyByFactor(BaseTransformer):
    
    def __init__(self, input_items, factor, output_items):
                
        self.input_items = input_items
        self.output_items = output_items
        self.factor = float(factor)
        super().__init__()

    def execute(self, df):
        df = df.copy()
        for i,input_item in enumerate(self.input_items):
            df[self.output_items[i]] = df[input_item] * self.factor
        return df
    
    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(
                name = 'input_items',
                datatype=float,
                description = "Data items adjust",
                output_item = 'output_items',
                is_output_datatype_derived = True)
                      )        
        inputs.append(ui.UISingle(
                name = 'factor',
                datatype=float)
                      )
        outputs = []
        return (inputs,outputs)          
    
'''

from iotfunctions.sample import MultiplyByFactor
adj = MultiplyByFactor(
    input_items = ['speed','travel_time'],
    factor = 0.9,
    output_items = ['ajdusted_speed','adjusted_travel_time']
)
adj.execute_local_test()

db.register_functions(adj)

