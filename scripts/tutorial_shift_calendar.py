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
Using Custom Calendars in AS
----------------------------

AS is designed for the analysis of time series data. Time series' are always timestamped
using a Gregorian date, but sometimes data needs to be summarized by a non-Gregorian date,
e.g. rolling up to shifts in a production calendar.

The built-in function ShiftCalendar demonstrate how to build a lookup to a custom calendar.

When configuring the ShiftCalendar function you will need to describe the shifts along
with their start and end time. You do this by building a dict (json input in the UI) that
is keyed on the shift identifier and contains a tuple representing the number of hours after
midnight at which each shift started and ended. 

'''

shift_dict = {
        "1": (5.5, 14),
        "2": (14, 21),
        "3": (21, 29.5)
    }


'''
This shift_dict describes 3 shifts numbered 1 to 3. The first shift starts at 5:30 (5.5 hours
after midnight) and ends at 2pm (14 hours after midnight). The last shift starts at 9pm (21 hours 
after midnight) and ends at 5:30 the next morning (29.5 hours after midnight of the reference date
used to mark the start of the first shift) 

'''


entity_name = 'shift_calendar_test'         # you can give your entity type a better name
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default

'''
Build an entity type with some test metrics including a slowly changing dimension for owner.
'''

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
                    bif.ShiftCalendar(
                        shift_definition= shift_dict,
                        period_start_date= 'shift_start_date',
                        period_end_date= 'shift_end_date',
                        shift_day = 'shift_day',
                        shift_id = 'shift_id'
                    ),
                    bif.SCDLookup(table_name=scd_name,
                                  output_item='owner'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })

entity.exec_local_pipeline(start_ts = dt.datetime.utcnow() - dt.timedelta(days=30))

'''
Execution results

id	    evt_timestamp	            deviceid	devicetype	            logicalinterface_id	eventtype	format	updated_utc	category_code	company_code	pressure	temp	    _timestamp	        entitydatagenerator	owner	shift_day	    shift_id	shift_start_date	shift_end_date
73000	2019/08/21 18:44	        73000	    shift_calendar_test		vn			                                        C	            ACME	        318.4897967	23.61335373	2019/08/21 18:44	TRUE	            Jane S	2019/08/21 0:00	2	        2019/08/21 14:00	2019/08/21 21:00
73000	2019/08/21 18:54	        73000	    shift_calendar_test		ey			                                        B	            JDI	            319.978806	20.64978853	2019/08/21 18:54	TRUE	            Jane S	2019/08/21 0:00	2	        2019/08/21 14:00	2019/08/21 21:00
73000	2019/08/21 19:14	        73000	    shift_calendar_test		pe			                                        A	            JDI	            310.1259381	23.28227576	2019/08/21 19:14	TRUE	            Jane S	2019/08/21 0:00	2	        2019/08/21 14:00	2019/08/21 21:00

The shift lookup function produced the following data items:

shift_day:          2019/08/21
shift_id:           2
shift_start_date:   2019/08/21 14:00
shift_end_date:     2019/08/21 21:00

In this case, the shift day is the same as the Gregorian calendar day. This is not always the case though.
Consider a timestamp of 1am on Aug 22. Based on this shift calendar, this is the 3rd shift of Aug 21 so
the shift_day is different to the day of the timestamp.

Shift start and end dates are provided to use in other calculations, e.g you could use them to calculate the
time to shift end. 

Note for Function Developers 
----------------------------

Most AS functions serve a single purpose: to accept pipeline data, transform it and produce revised
pipeline data. The inclusion of a function doesn't generally change the entity type and each 
function operates independently - without understand what other functions are present on the 
Entity Type.
 
Custom Calendar functions actually change the entity type. When this function is added to the
entity type, the entity type is altered to include additional metadata which is now available to
other functions. This means that any function can be made aware of the existence of a custom
calendar as it may influence processing.

This special handling of custom calendars implies that there can only be one custom calendar
per entity type.

'''

print (entity.get_custom_calendar())

'''
The function object is returned when executing EntityType.get_custom_calendar(). You can
use this function object to get access to the shift definition or call its execute method
on new data.
  
'''





