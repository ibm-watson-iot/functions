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
Slowly Changing Dimensions Tutorial
------------------------------------

Regular AS dimensions are not time variant. Although they may be updated at any time, AS is not aware
of any historical values so calculations performed on new data or historical data will use the current value
of a dimensional attribute.

Consider a device's owner. When first registered, device A01 is owned by Fred. Fred sells the device to John.
An AS summary records device usage stats by owner. Initially all stats are recorded against Fred.
After the change in ownership the stats for the device will be reflected under John.   

If at any time the summary is rebuilt, history will be rewritten. All usage stats will be recorded against the
new owner John. All traces of Fred will be lost from the system.

The above non time variant behavior makes sense for some dimensional attributes. In other cases it is
better to keep track of changes to dimensional attributes so that when recalculating data, history is
not rewritten. AS provides a built in function called SCD Lookup for this.

This tutorial demonstrates how to:
 -- create database tables to use as SCD lookups.
 -- use SCD lookups in calculations
 
When using a simulation to provide data to an entity type you can get the simulation to produce
the SCD table/s and sample data. To do this, add an scds element to the parameters dict. The
scd element is a dict too. It is keyed on the name of the scd property and contains a list of
possible values.

'''

entity_name = 'scd_test'                    # you can give your entity type a better name
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default


sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320},
    "data_item_sd": {'temp': 2,
                     'pressure': 5},
    "data_item_domain" : {'category_code' : ['A','B','C']},
    "scds": {'owner': [
                'Fred K',
                'Mary J',
                'Jane S',
                'John H',
                'Harry L',
                'Steve S']
    }
}

# when using the generation capabilities, the table name will be created
# automatically using the following naming convention

scd_name = '%s_scd_owner' %entity_name

# entity has an EntityDataGenerator function to generate data
# also has a SCDLookup function to retrieve data

entity = EntityType(entity_name,db,
                    Column('temp',Float()),
                    Column('pressure', Float()),
                    Column('company_code',String(50)),
                    Column('category_code',String(5)),
                    bif.EntityDataGenerator(
                        parameters= sim_parameters,
                        data_item = 'is_generated'
                            ),
                    bif.SCDLookup(table_name=scd_name,
                                  output_item='owner'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })


'''
Now that we have defined simulation parameters and attached them to an EntityDataGenerater
object on an EntityType we can run the AS calculation pipeline to execute the generator.

When running the calculation pipeline it is important to set a start_date in the past. Since
these dimensions change slowly, we will only see changes when looking a large enough
time window.

The default frequency for automatic generation of SCD data is 2 days. By starting the pipeline
30 days in the past we will see history. 
  
'''

db.drop_table(scd_name,schema=db_schema)
start_date = dt.datetime.utcnow() - dt.timedelta(days=30)
entity.exec_local_pipeline(start_ts=start_date)

'''
After running this, you can take a look at the structure and content of the SCD input
table that was generated. When building your own SCD table and populating it manually
you will need to conform to this structure.

deviceid    start_date                  end_date                    owner
73004	    2019-07-24-22.10.25.940354	2019-07-27-22.10.25.940353	Jane S
73004	    2019-07-27-22.10.25.940354	2019-08-14-22.10.25.940353	Steve S
73004	    2019-08-14-22.10.25.940354	2019-08-15-22.10.25.940353	Fred K
73004	    2019-08-15-22.10.25.940354	2019-08-20-22.10.25.940353	Jane S
73004	    2019-08-20-22.10.25.940354	2019-08-21-22.10.25.940353	John H
73004	    2019-08-21-22.10.25.940354	2262-04-11-23.47.16.854775	Harry L

Each SCD table only records the value of a single property, "owner" in this case.
It is keyed on device_id and start_date. The end_date should be a microsecond
less than the end date of the previous record. The current value should have an 
end_date in the future. There should be no gaps or overlaps in this data.

When doing using the SCDLookup function, AS finds a corresponding "owner"
for each incoming record of time series data. Here is an except of data for
73004 covering the timestamps 2019/08/20 21:05 to 2019/08/20 22:52. The
owner changed from Jane S to John H at 2019-08-20-22.10.25.940354. This
change is reflected in the output data produced by calculation pipeline 
execution.

id	    evt_timestamp	    deviceid	_timestamp	entitydatagenerator	owner
73004	2019/08/20 21:05	73004	    05:23.1	    TRUE	            Jane S
73004	2019/08/20 21:45	73004	    45:23.1	    TRUE	            Jane S
73004	2019/08/20 21:47	73004	    47:10.2	    TRUE	            Jane S
73004	2019/08/20 22:10	73004	    10:23.1	    TRUE	            Jane S
73004	2019/08/20 22:15	73004	    15:23.1	    TRUE	            John H
73004	2019/08/20 22:25	73004	    25:23.1	    TRUE	            John H
73004	2019/08/20 22:27	73004	    27:10.2	    TRUE	            John H
73004	2019/08/20 22:52	73004	    52:10.2	    TRUE	            John H

Note:
The SCD lookup always returns the last known value of the dimension as at the
time series timestamp. The end_dates provided are used when retrieving lookup data
but not when merging time series data - when merging only the start date is used.

If you need to handle gaps, you will need to explicitly provide a dimension
member of the gap, e.g.

deviceid    start_date                  end_date                    owner
73004	    2019-07-24-22.10.25.940354	2019-07-24-23.59        	Jane S
73004	    2019-07-27-22.10.25.940354	2019-08-14-22.10.25.940353	Steve S

This data has a gap from 2019-07-25 to 2019-07-27-22.10.25.940354

A time series entry occuring within that gap, e.g. 2019-07-26 will still carry
an owner of Jane S.

If instead you would like to see the gap, pre-process the input data to look like
this:

deviceid    start_date                  end_date                    owner
73004	    2019-07-24-22.10.25.940354	2019-07-24-23.59        	Jane S
73004       2019-07-24-23.59.00.000001  2019-07-27-22.10.25.940353  unknown
73004	    2019-07-27-22.10.25.940354	2019-08-14-22.10.25.940353	Steve S


Closing Comments:
-----------------

This tutorial demonstrated how to use the EntityDataGenerator to build a SCD
table and how to use the SCDLookup function to merge SCD data with time series
data.

Normally you would create your own SCD tables and populate them yourself. When
doing this, they should be created with the same set of mandatory columns:
deviceid, start_date, end_date along with a single column for the scd property
that the table holds.

'''
