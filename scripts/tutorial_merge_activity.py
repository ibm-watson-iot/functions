import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
import pandas as pd
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
Calculate Activity Durations
----------------------------

The built in function ActivityDuration is a special transformation function that
processes incoming data that describes the start and end date of activities to
compute a duration.

You may be thinking that durations are easy to compute, simply take the date
difference between the end and start timestamp for an activity. This is a valid
approach when the dimension values that further describe the activity remain
fixed for the duration of the activity. If however you need to reflect anything
that changes during that activity and apportion the duration between the
original value and new value, you can't correctly calculate the duration using
a date difference function. Here is an example:

Consider a maintenance activity that starts at 1:30pm on Aug 26 and ends at 1:30pm on
Aug 27. The duration of this activity is 24 hours right? Yes, as long as you are
not expecting to measure duration by other dimensions, like time. If you need to
know how much maintenance was done by shift, you need to divide up the 24 hour
period into shifts.

Consider a shift calendar where shift 1 ends at 2pm, shift 2 ends at 11pm and 
shift 3 ends at 5:30 am the next day, the 24 hours of maintenance duration should
be apportioned as follows:

Shift Day       Shift Id        Maintenance Duration
Aug 26          1               30 min
Aug 26          2               8 hours
Aug 26          3               8 hours
Aug 27          1               7.5 hours

In the tutorial that follows you will use the ActivityDuration built in function
to calculate activity durations and see how it apportions durations over changing
dimensions like shift and maintenance crew. You will also learn how ActivityDuration 
can handle cases where incoming activity data contains overlapping
time periods.

We will start by creating and entity type and some data. 
'''


entity_name = 'merge_test'                    # you can give your entity type a better name
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default


shift_dict = {
        "1": (5.5, 14),
        "2": (14, 21),
        "3": (21, 29.5)
}

sim_parameters = {
    "data_item_mean" : {'temp': 22,
                        'pressure' : 320},
    "data_item_sd": {'temp': 2,
                     'pressure': 5},
    "scds": {'crew': [
                'A',
                'B',
                'C']
            },
    "start_entity_id": 100,
    "auto_entity_count": 2

}

scd_name = '%s_scd_crew' %entity_name
activity_name = '%s_activity' %entity_name

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
                        shift_definition=shift_dict,
                        period_start_date='shift_start_date',
                        period_end_date='shift_end_date',
                        shift_day='shift_day',
                        shift_id='shift_id'
                    ),
                    bif.ActivityDuration(
                        table_name = activity_name,
                        activity_codes= ['maintenance'],
                        activity_duration= ['maintenance_duration']
                    ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })


start_date = dt.datetime.utcnow() - dt.timedelta(days=2)

'''

We will need some maintenance data. Activity tables have a predefined set of
columns. There are always only 4 columns a deviceid, start_date, end_date and
an activity_code. 

deviceid    start_date                  end_date                    activity
73003	    2018-11-18-23.09.20.325289	2018-11-19-00.06.38.195689	PM
73003	    2018-11-21-23.09.20.325289	2018-11-21-23.10.42.203689	UM

'''

activity_data = {
    'deviceid' : '100',
    'start_date' : dt.datetime.utcnow() - dt.timedelta(days=1),
    'end_date' : dt.datetime.utcnow(),
    'activity' : 'maintenance'
}

activity_df = pd.DataFrame(data=activity_data, index = [0])
#write activity data to a table
db.write_frame( df = activity_df,
                table_name=activity_name,
                if_exists = 'replace')

entity.exec_local_pipeline(start_ts=start_date)

'''

Here is an example of the outputs:

id	evt_timestamp	    deviceid	duration	maintenance_duration	shift_day	    shift_end_date	shift_id	shift_start_date
100	2019/25/8 19:29	    100	        90.13333333	90.13333333	            2019/25/8 0:00	2019/25/8 21:00	2	        2019/25/8 14:00
100	2019/25/8 21:00	    100	        510	        510	                    2019/25/8 0:00	2019/26/8 5:30	3	        2019/25/8 21:00
100	2019/26/8 5:30	    100	        510	        510	                    2019/26/8 0:00	2019/26/8 14:00	1	        2019/26/8 5:30
100	2019/26/8 14:00	    100	        329.8666667	329.8666667	            2019/26/8 0:00	2019/26/8 21:00	2	        2019/26/8 14:00

As per the example into the intro, the single maintenance task has been apportioned into 4 shifts.

Durations may be apportioned by changes in the values of other dimensions too.

Let's consider the maintenance crew by adding a SCD lookup function for crew. 
'''


db.drop_table(scd_name,schema=db_schema)

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
                        shift_definition=shift_dict,
                        period_start_date='shift_start_date',
                        period_end_date='shift_end_date',
                        shift_day='shift_day',
                        shift_id='shift_id'
                    ),
                    bif.SCDLookup(table_name=scd_name,
                                  output_item='crew'),
                    bif.ActivityDuration(
                        table_name=activity_name,
                        activity_codes=['maintenance'],
                        activity_duration=['maintenance_duration']
                    ),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })


entity.exec_local_pipeline(start_ts=start_date)                      

'''
Results are shown

id	evt_timestamp	deviceid	_auto_index_	duration	maintenance_duration	crew
100	2019/25/8 19:44	100	        4	            0.016666667	0.016666667	            C	    
100	2019/25/8 19:44	100	        5	            75.56666667	75.56666667	            B	
100	2019/25/8 21:00	100	        6	            510	        510	                    B	
100	2019/26/8 5:30	100	        7	            510	        510	                    B	
100	2019/26/8 14:00	100	        8	            344.4166667	344.4166667	            B	

Notice how the maintenance duration is now apportioned by the combination of the
crew and shift so any time there is either a crew change or shift change a new
row of data is generated and the full duration is now allocated to the combination
of dimensions.

The ActivityDuration function can also accommodate overlaps in activities. Consider the
case where "maintenance" activity is interrupted by a "break" activity.

'''

# two rows of activity data
# maintenance is interupted by an hour break

activity_data = {
    'deviceid' : ['100', '100'],
    'start_date' : [dt.datetime.utcnow() - dt.timedelta(days=1),dt.datetime.utcnow() - dt.timedelta(hours=12)],
    'end_date' : [dt.datetime.utcnow(),dt.datetime.utcnow() - dt.timedelta(hours=11)],
    'activity' : ['maintenance','break']
}

# ActivityDuration is configured to process both types of activity

fn_activity = bif.ActivityDuration(
                        table_name=activity_name,
                        activity_codes=['maintenance','break'],
                        activity_duration=['maintenance_duration','break_duration']
                    )

# ActivityDuration is configured to eliminate overlaps across both types of
# activity. Only one type of activity of any type can take place at any
# point in time.

fn_activity.remove_gaps = 'across_all'   # configure ActivityDuration to

entity.drop_tables(recreate=True)
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
                        shift_definition=shift_dict,
                        period_start_date='shift_start_date',
                        period_end_date='shift_end_date',
                        shift_day='shift_day',
                        shift_id='shift_id'
                    ),
                    bif.SCDLookup(table_name=scd_name,
                                  output_item='crew'),
                    fn_activity,
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })

activity_df = pd.DataFrame(data=activity_data, index = [0,1])
#write activity data to a table
db.write_frame( df = activity_df,
                table_name=activity_name,
                if_exists = 'replace')

entity.exec_local_pipeline(start_ts=start_date)

'''

Here are the results. Notice how the maintenance duration has been shorted by the break_duration.

evt_timestamp	    deviceid	activity	maintenance_duration	break_duration
2019/08/25 21:24	100	        maintenance	485.65	
2019/08/26 5:30	    100	        maintenance	234.35	
2019/08/26 9:24	    100	        break		                        60
2019/08/26 10:24	100	        maintenance	215.65	
2019/08/26 14:00	100	        maintenance	420	
2019/08/26 21:00	100	        maintenance	24.35	

If you set remove_gaps = 'within_single' the function would allow overlaps across multiple
activity types.

Wrapping up
------------

In this tutorial you have seen how to use the ActivityDuration function to calculate 
duration metrics from activity start and end dates. You saw it allocate durations
between slowly changing dimensions and shifts. You also saw how it can be configured
to not allow overlap in time allocation between different activities. 


Developer Notes
----------------

This is one of the more complex built in functions. The base class BaseDBActivityMerge
is more general as it supports reading data from multiple activity tables and reading
custom tables that don't conform to the standard column names via custom sql. If you are
developing other functions that need to be aware of a custom calendar or the presence of
slowly changing dimensions, BaseDBActivityMerge is a good reference.

'''
