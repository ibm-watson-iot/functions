import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.base import BasePreload
from iotfunctions import ui

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('../scripts/credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------

When creating a set of functions you can test how they these functions will
work together by creating a test pipeline. You can also connect the test
pipeline to real entity data so that you can what the actual results that the
function will deliver.

'''
    
'''
A database object is our connection to the mother ship
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

'''
To do anything with IoT Platform Analytics, you will need one or more entity type. 
You can create entity types through the IoT Platform or using the python API.

When defining an entity type, you can describe the raw input data items for the
entity type, as well as the functions , constants and granularities that apply to it.

The "widgets" entity type below has 3 input data items. Dataitems are denoted by the
SqlAlchemy column objects company_code, temp and pressure.

It also has a function EntityDataGenerator

The keyword args dict specifies extra properties. The database schema is only
needed if you are not using the default schema. You can also rename the timestamp.

'''

class BrokenPreload(BasePreload):
    """
    Preload function returns an error. Used to test error handling.
    """

    def __init__ (self, dummy_input = None,
                  output_item = 'broken_preload',
                  **parameters):
        super().__init__(dummy_items = [], output_item = output_item)
        self.dummy_input = dummy_input

    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):

        raise RuntimeError('BrokenPreload function failed as expected.')

        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name = 'dummy_input',
                               datatype=str,
                               description = 'Any input'
                                  )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs,outputs)



entity_name = 'test_broken_functions'
db_schema = None  # replace if you are not using the default schema
db.drop_table(entity_name, schema = db_schema)
entity = EntityType(entity_name,db,
                    Column('company_code',String(50)),
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    bif.EntityDataGenerator(
                        ids = ['A01','A02','B01'],
                        data_item = 'is_generated'
                            ),
                    BrokenPreload('dummy','broken_preload'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
'''
When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data

After creating an EntityType you will need to register it so that it visible in the UI.
To also register the functions and constants associated with the entity type, specify
'publish_kpis' = True.
'''
entity.register(raise_error=False)
'''
Entities can get pretty lonely without data. 

The EntityDataGenerator that we included on the entity will execute and add
a few rows of random data each time the AS pipeline runs (generally every 5 min).

You can also load some historical data using 'generate_data'

'''
entity.generate_data(days=0.5, drop_existing=True)

'''
To see the data you just loaded, ask the db object to read the database
table and produce a pandas dataframe.
'''
df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())

'''
To test the execution of kpi calculations defined for the entity type locally
use 'test_local_pipeline'.

A local test will not update the server job log or write kpi data to the AS data
lake. Instead kpi data is written to the local filesystem in csv form.


Test missing data item dependency for function

'''

entity = EntityType(entity_name,db,
                    Column('company_code',String(50)),
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    bif.EntityDataGenerator(
                        ids = ['A01','A02','B01'],
                        data_item = 'is_generated'
                            ),
                    bif.PythonExpression('df["pressure"]*df["missing_item"]','unable_to_execute'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })

entity.exec_local_pipeline(_abort_on_fail=False)

'''

Test broken expression

'''

entity = EntityType(entity_name,db,
                    Column('company_code',String(50)),
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    bif.EntityDataGenerator(
                        ids = ['A01','A02','B01'],
                        data_item = 'is_generated'
                            ),
                    bif.PythonExpression('a*b','broken_expression'),
                    bif.PythonExpression('df["broken_expression"].fillna(1)','next_expression'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })


'''

Execute with _abort_on_fail = False. The execution will continue after encountering the broken expression.
'next_expression' will calculate and deliver a value of 1

'''

entity.exec_local_pipeline(_abort_on_fail=False)

'''
Execute with _abort_on_fail = True. The execution will abort after encountering the broken expression. 
'''

entity.exec_local_pipeline(_abort_on_fail=True)