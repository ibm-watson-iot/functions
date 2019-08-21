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

While we are at it, let's change the number of entity types we are simulating
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

'''
So far we have only looked at the ability to create numerical and categorical time
series data. You can also automatically generate dimensional attributes. 
'''

entity.make_dimension(
    'sim_test_dimension',
    Column('manufacturer', String(50)),
)

entity.register(raise_error=True)
entity.generate_data(days=0.5, drop_existing=True, **sim_parameters)

'''

Look at the data that was loaded into sim_test_dimension.

You should see something like this.

device_id   manufacturer
73004	    GHI Industries
73000	    GHI Industries
73003	    Rentech
73002	    GHI Industries
73001	    Rentech

"manufacturer" is another one of those magic column names that will use a default 
domain of values. 

Let's add additional numeric and non-numeric data to the dimension and set 
simulation parameters for them.

load_rating is a numeric dimension property with a mean of 500 and a default standard deviation
maintenance_org has a custom domain 

Important: When altering the dimension it is important to drop it first. The make_dimension
method attempts to reuse an existing table if there is one. In this case there is one and it
contains the wrong columns. We must drop it first so that make_dimension can create
a new dimension.

'''

db.drop_table('sim_test_dimension',schema=db_schema)

entity.make_dimension(
    'sim_test_dimension',
    Column('manufacturer', String(50)),
    Column('load_rating', Float()),
    Column('maintenance_org', String(50))
)

sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320,
                        'load_rating' : 500 },
    "data_item_sd": {'temp': 2,
                     'pressure': 5},
    "data_item_domain" : {'category_code' : ['A','B','C'],
                          'maintenance_org' : ['Lunar Parts','Relco']},
    "start_entity_id" : 1000,
    "auto_entity_count"  : 10,
    "freq"  : '30S'    # 30 sec
}

entity.register(raise_error=True)
entity.generate_data(days=0.5, drop_existing=True, **sim_parameters)

'''

This is what the new dimension looks like

devicid     manufacturer    load_rating         maintenance_org
1004	    GHI Industries	500.89830511533046	Relco
1005	    Rentech	        499.9407055104487	Lunar Parts
1008	    GHI Industries	500.79630237063606	Relco
1001	    Rentech	        500.00507522504734	Relco
1006	    Rentech	        498.48278537970765	Lunar Parts
1007	    GHI Industries	499.2788278282351	Relco
1002	    GHI Industries	501.1063668253522	Relco
1000	    GHI Industries	501.1359844685311	Relco
1003	    Rentech	        501.45477263284033	Relco
1009	    Rentech	        499.94029397932167	Lunar Parts


With what you have seen so far, generate_data and the AS function EntityDataGenerator allow 
for the simulation independent variables. Real world systems have a mix of independent and 
dependent variables. You can use AS functions to simulate dependent variables.

Consider and extension to this example where operating temperature is dependent ambient
temperature (temp) and load. We can model the relationship between these variables using
an AS function. In this example the relationship is simple enough to be modeled using a
PythonExpression. You could PythonFunctions or custom functions to model more complex
relationships.

We will also add some random noise to the result of the expression. This will allow our
simulation to retain some of the random variation typically seen in the real world.

'''

temp_function = bif.PythonExpression(
    expression = 'df["temp"]+df["pressure"]/300*5',
    output_name = 'operating_temperature_work'
)


entity = EntityType(entity_name,db,
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    Column('company_code',String(50)),
                    Column('category_code',String(5)),
                    bif.EntityDataGenerator(
                        parameters= sim_parameters,
                        data_item = 'is_generated'
                            ),
                    temp_function,
                    bif.RandomNoise(
                        input_items = ['operating_temperature_work'],
                        standard_deviation= 1,
                        output_items= ['operating_temperature']
                    ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })



entity.exec_local_pipeline()

'''
Note:  
entity.generate_data only writes simulated random data to the AS input table. It
does not retrieve this data and apply AS functions to it.

id	    evt_timestamp	temp	    pressure	deviceid	_timestamp	entitydatagenerator	operating_temperature_work	operating_temperature
1005	04:38.5	        22.82177094	327.0609021	1005	    04:38.5	    TRUE	            28.27278598	                27.9994426
1004	05:08.5	        22.78203275	321.8376423	1004	    05:08.5	    TRUE	            28.14599345	                27.95332118
1006	05:38.5	        23.77231385	313.3748436	1006	    05:38.5	    TRUE	            28.99522791	                30.04482662
1000	06:08.5	        24.23746302	329.5324336	1000	    06:08.5	    TRUE	            29.72967024	                27.95621538
1006	06:38.5	        24.26086898	321.2665546	1006	    06:38.5	    TRUE	            29.61531155	                29.18479368
1009	07:08.5	        26.14706462	321.1545257	1009	    07:08.5	    TRUE	            31.49964005	                32.32444398
1000	07:38.5	        20.27024524	313.5222948	1000	    07:38.5	    TRUE	            25.49561682	                25.74210273
 

When executing AS functions locally it is necessary to execute an AS pipeline as above

In this tutorial you learned:
-- How to load independent numeric and categorical values into AS input tables
-- You saw how this applied to both time series data and dimension data
-- You also saw how to model dependent variables using AS functions.

This tutorial showed how to model a very simple system. You can use the exaxt same
techniques to build realistic simulations of much more complex systems.

'''
