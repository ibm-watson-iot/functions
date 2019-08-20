import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
import datetime as dt

EngineLogging.configure_console_logging(logging.DEBUG)

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Simulation Tutorial
-------------------------

Don't have a live data feed? Need prototype an AS-based solution. You can use
simulation to build simple or elaborate data feeds for AS prototypes and demos.

The easiest way to get started with simulation is to use the  built in 
EntityDataGenerator function. You can use this function directly from the UI
and run it without config parameters. When used in this way, it will generate
basic numeric and categorical data items. It assumes that numeric data items
are continuous time series and that string data items are categorical time 
series. This tutorial will explain how to setup parameters to get more
realistic outputs.

The EntityDataGenerator is designed to mimic an IoT source. IoT sources feed
a time series table that contains input data for function processing. The
EntityDataGenerator operates in the same way.

We will start by looking at vanilla results without parameters. The first thing
that we will need is an EntityType with some numeric and string items.  

'''

entity_name = 'sim_test'                    # you can give your entity type a better nane
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default\
entity = EntityType(entity_name,db,
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    Column('company_code',String(50)),
                    Column('category_code',String(5)),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
entity.register(raise_error=True)

'''
To build historical data, you can use the entity's "generate_data" method.
'''

entity.generate_data(days=0.5, drop_existing=True)

'''
To see the data you just loaded, ask the db object to read the database
table and produce a pandas dataframe.
'''
df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())

'''
You should see a new database table - named the same way as your entity. This table
will be loaded with a half day's worth of data.

The numeric columns like 'temp' and 'pressure' will have timeseries data with a mean
of 0. Data will be generated with 5 devices.

temp                    pressure            devicid     evt-timetamp
-0.36397491820566885	0.26423994956819163	73003	    2019-08-19-08.10.23.860123
0.6568787527068098	    1.0140367243229087	73000	    2019-08-19-08.11.23.860123

The test entity type includes a "company_code". This is a special column with default
categorical values: ACME, JDI and ABC

The other string we created "category_code" will have random categorical data like "yc",
"ea". The categories are build from the column name so that they repeat over subsequent
executions of the EntityDataGenerator.

The fact that "temp" and "pressure" don't have realistic scaling detracts from there
value as demo aids. Let's scale them more appropriately. While we are at, it we will
define some better values for "category_code".

Want to change the frequency of generation? Set the value of freq. Use a valid pandas frequency string.

'''

sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320},
    "data_item_sd": {'temp': 2,
                     'pressure': 5},
    "data_item_domain" : {'category_code' : ['A','B','C']},
    "freq"  : '30S'    # 30 sec
}

entity.generate_data(days=0.5, drop_existing=True, **sim_parameters)

'''
Looking at the sim_test table, we now see:

temp                    pressure                deviceid         evt-timetamp                   category_code
22.60692494267075       318.4117321035006       73004           2019-08-19-10.03.44.721861      C
24.300509140926817      314.64777038989394      73004           2019-08-19-10.04.44.721861      A


You can try the same from the UI using the parameters above when configuring the EntityDataGenerator function.

Alternatively we can add the EntityDataGenerator to the entity type as follows:

Note: There are a few additional parameters available when operating this way: 
-- Need more entity types? Change the value of auto_entity_count
-- Want a different starting range for entity type ids? set start_entity_id

'''

sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320},
    "data_item_sd": {'temp': 2,
                     'pressure': 5},
    "data_item_domain" : {'category_code' : ['A','B','C']},
    "start_entity_id" : 1000,
    "auto_entity_count"  : 10,
    "freq"  : '30S'    # 30 sec
}

entity = EntityType(entity_name,db,
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    Column('company_code',String(50)),
                    Column('category_code',String(5)),
                    bif.EntityDataGenerator(
                        parameters= sim_parameters,
                        data_item = 'is_generated'
                            ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })


'''
When creating this entity type we included the EntityDataGenerator function and set
the the arguments "data_item" and "parameters". The "data_item" argument allows you to
name the dummy data item that the EntityDataGenerator adds that signifies that it has run.
The "parameters" argument is configured with the value of sim_parameters dictionary.

By adding the EntityDataGenerator to the entity type it will run each time the 
entity type's calc pipeline runs.

The EntityDataGenerator works differently from regular "transformer" functions. 
Transformer functions apply transformations to incoming entity data by creating new
derived data items. The EntityDataGenerator doesn't have an incoming entity data to
work with, so it builds its own. It loads this data directly into the entity's time
series input data where is can be read by transform functions.

To test the execution of kpi calculations defined for the entity type locally
use 'test_local_pipeline'.
'''

entity.exec_local_pipeline()



