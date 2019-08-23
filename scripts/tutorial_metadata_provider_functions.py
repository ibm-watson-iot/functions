import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions import base
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.base import BaseMetadataProvider
import datetime as dt

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Metadata provider functions
----------------------------

Most AS functions act on data. They are executed as part of a pipeline. They accept
pipeline data and contribute new data to the pipeline.

There is however a class of function that is not executable. All it does it capture
metadata in the form of its arguments onto the entity type and other functions. 

There are two built in functions that behave this way.
1) DropNull : Captures metadata that is used by a system process.
2) EntityFilter : Captures metadata that can be will be passed to all data source function

As a function author you can use a metadata provider function you would like to
capture a single piece of metadata that influences the way that multiple other
functions operate. They behave a lot like Entity Constants except that Entity Constants
are available to all Entity Types. Metadata providers apply only to entity types that
they are explicitly added to. 

In this tutorial we will create a metadata provider and use it in multiple functions.

'''

entity_name = 'metadata_provider_test'         # you can give your entity type a better name
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default

'''
First we will build our custom metadata provider function.
This one captures a single property called custom_metadata.
'''

class TestMetadataProvider(base.BaseMetadataProvider):

    def __init__(self,custom_metadata,output_item='custom_metadata_added'):

        self.custom_metadata = custom_metadata
        self._input_set = set()
        self._output_list = [output_item]

        # all keyword arguments will be treated as custom metadata
        kwargs = {'custom_metadata': custom_metadata}
        super().__init__(dummy_items=[],output_item=output_item,**kwargs)

'''
This class has no execute method and since it is only used for local testing
there is no build_ui class method. Instead the _input_set and _output list
are explicitly set during initialization so there is no need to infer using
UI registration metadata.

We will use this custom function to capture a value for the custom_metadata
property and then use this custom metadata in an a couple of expression.

When referring to the property in an expression, we can refer to it in two ways
1) self._entity_type.custom_property
2) c['custom_property']

AS builds a dictionary of entity type properties in all the build in functions
that act on expressions. This gives convenient access to all entrity type properties
using a local variable called c.

Developer Tip
-------------

To make this dictionary available in your own functions, use the following code
in your execute method

c = self._entity_type.get_attributes_dict()
  
'''

sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320},
    "data_item_sd": {'temp': 2,
                     'pressure': 5}
    }

entity = EntityType(entity_name,db,
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    Column('company_code',String(50)),
                    bif.EntityDataGenerator(
                        parameters= sim_parameters,
                        data_item = 'is_generated'
                            ),
                    TestMetadataProvider(108,'custom_metadata_added'),
                    bif.PythonExpression(
                        expression = 'df["temp"] + self._entity_type.custom_metadata',
                        output_name= 'adjusted_temp1'
                    ),
                    bif.PythonExpression(
                        expression='df["temp"] *2 + c["custom_metadata"]*2',
                        output_name='adjusted_temp2'
                    ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })

entity.exec_local_pipeline(start_ts = dt.datetime.utcnow() - dt.timedelta(days=30))

'''
Execution results

id	    evt_timestamp	temp	    deviceid	_timestamp	    entitydatagenerator	custom_metadata_added	adjusted_temp1	adjusted_temp2
73004	8/21/2019 20:50	24.46272725	73004	    8/21/2019 20:50	TRUE	            TRUE	                132.4627273	    264.9254545
73003	8/21/2019 20:55	22.28387595	73003	    8/21/2019 20:55	TRUE	            TRUE	                130.283876	    260.5677519
73001	8/21/2019 21:00	25.1225324	73001	    8/21/2019 21:00	TRUE	            TRUE	                133.1225324	    266.2450648
73000	8/21/2019 21:05	19.92432873	73000	    8/21/2019 21:05	TRUE	            TRUE	                127.9243287	    255.8486575
73000	8/21/2019 21:10	24.63826344	73000	    8/21/2019 21:10	TRUE	            TRUE	                132.6382634	    265.2765269


Notice how the value of 108 was used in both expressions.

'''





