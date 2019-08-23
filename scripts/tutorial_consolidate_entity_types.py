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
Consolidating Data from Multiple Entity Types
-----------------------------------------------

If you receive independent data feeds from multiple devices, but would like to
perform analysis across these devices, you can build a high level consolidated
entity type and use individual child devices as data sources for it. Consider this
example:

A work area in a building has a temperature sensor and motion sensor. You would like
to analyse comfort levels and estimate occupancy at a work area, room and building level.

In this tutorial we will simulate this scenario by building device level entity types
for temperature and motion sensors. We will create a work area entity type for
consolidation and use the GetEntityData built in function to get data from the child
devices.

'''

child1 = ('temp_data','temperature')               # (device name, metric name)
child2 = ('motion_data', 'motion')
consolidated = 'work_area_test'
device_count = 20

'''
Each device will be associated with a work area. It is the common "work_area" column that
we will use to consolidate data across devices.

Build a list of work_area ids for the simulation to run with.
'''
work_areas = ['w%s' %x for x in list(range(device_count))]

'''
Setup the simulation parameters and build the device entity types

We will assume that the devices are in a fixed location. This means
we need a device dimension table for each device with a work_area
on the dimension.

'''
db = Database(credentials = credentials)
db_schema = None                            # set if you are not using the default


sim_parameters = {
    "data_item_mean" : {'temperature': 22},
    "data_item_sd": {'temperature': 2 },
    "data_item_domain" : {'motion' : [0,1],
                          'work_area' : work_areas,
                          'zone_id' : ['N','S','E','W']},
    "auto_entity_count" : device_count
    }

# create entity types and execute the calc pipeline to get some test data

data_sources = []
start_device_id = 10000
for (data_source_name,metric_name) in [child1,child2]:

    sim_parameters['start_entity_id'] = start_device_id

    entity = EntityType(data_source_name,db,
                        Column(metric_name,Float()),
                        bif.EntityDataGenerator(
                            parameters= sim_parameters,
                            data_item = 'is_generated'
                                ),
                        **{
                          '_timestamp' : 'evt_timestamp',
                          '_db_schema' : db_schema
                          })
    dim = '%s_dim' %data_source_name
    entity.drop_tables([dim])
    entity.make_dimension(dim, #build name automatically
                          Column('work_area',String(50)))

    entity.exec_local_pipeline()
    data_sources.append(entity)

    start_device_id += 10000


'''

This is what the generated data looks like: Time series data for temperature:

DEVICEID	EVT_TIMESTAMP	            DEVICETYPE	    LOGICALINTERFACE_ID	EVENTTYPE	FORMAT	UPDATED_UTC	TEMPERATURE
10001	    2019-08-22-21.18.10.884128	temp_data		                    ep                  			21.602888736326957
10017	    2019-08-22-21.23.10.884128	temp_data		                    ye			                    23.517696994037884


The dimension contains the work_area

DEVICEID    WORK_AREA
10000	    w6
10001	    w3
10002	    w4
10003	    w10
10004	    w4
10005	    w14
10006	    w4
10007	    w1
10008	    w11
10009	    w4
10010	    w4
10011	    w15
10012	    w3
10013	    w4
10014	    w12
10015	    w2
10016	    w5
10017	    w3
10018	    w1
10019	    w4

This is what the generated data looks like for motion:

DEVICEID	EVT_TIMESTAMP	            DEVICETYPE	    LOGICALINTERFACE_ID	EVENTTYPE	FORMAT	UPDATED_UTC MOTION
20002	    2019-08-22-21.21.29.979137	motion_data		                    vy			                    1
20012	    2019-08-22-21.26.29.979137	motion_data		                    ee			                    1

DEVICEID    WORK_AREA
20000	    w13
20001	    w5
20002	    w3
20003	    w2
20004	    w4
20005	    w4
20006	    w18
20007	    w9
20008	    w6
20009	    w0
20010	    w1
20011	    w4
20012	    w4
20013	    w2
20014	    w15
20015	    w4
20016	    w12
20017	    w4
20018	    w3
20019	    w2

Notice that when building the device ids for each type of device we generated them in different ranges.
In the real world, different devices in the same location will not share the same deviceid so
it would be cheating to consolidate on a common deviceid.
  
Now that we have our device simulators set up, we can create aother entity type for "work_area". This
entity type has none of its own data columns, but it has two GetEntityData functions included: one for
each child device. 
 
  
'''

entity = EntityType(consolidated, db,
                    bif.GetEntityData(
                        source_entity_type_name = child1[0],
                        key_map_column = 'work_area',
                        input_items = child1[1],
                        output_items= child1[1]
                    ),
                    bif.GetEntityData(
                        source_entity_type_name=child2[0],
                        key_map_column='work_area',
                        input_items=child2[1],
                        output_items=child2[1]
                    ),
                    bif.PythonExpression(
                        expression='df["%s"]-21.5' %child1[1],
                        output_name='comfort_level'
                    ),
                    bif.PythonExpression(
                        expression='df["%s"]' % child2[1],
                        output_name='is_occupied'
                    ),
                    **{
                        '_timestamp': 'evt_timestamp',
                        '_db_schema': db_schema
                    })


'''
We also added two expression to the entity: comfort_level and is_occupied. These
are there to show how to work with child_entity_data. 
 
Execute the local pipeline. This will read the child entity type data and compute
comfort_level and is_occupied.
 
'''

entity.exec_local_pipeline()

'''

The outputs from pipeline execution are shown:

id	evt_timestamp	    deviceid	temperature	    evt_timestamp	motion	comfort_level	is_occupied
w19	2019/08/23 13:25	w19	        20.31762829	    25:00.9		            -1.18237171	
w2	2019/08/23 13:30	w2	        23.04951994	    30:00.9		            1.549519936	
w4	2019/08/23 13:25	w4		                    25:08.1	        0		                 0
w9	2019/08/23 13:30	w9		                    30:08.1     	0		                 0

By default the data from the various data sources are merged using a full outer join.
This means each row of data from each device is present in the merged data. The dataframe is
sparse as the timestamps the different data sources do not match.

If you attempted to build transformation functions directly on this merged dataset that involved
data from both datasources, the results would likely be rather meaningless because of all of the
null values in this non-time aligned data.
 
The easiest way to time align the data is aggregate it, e.g. to the hour level. You can
define meaningful functions acting on the dense aggregated data.

Closing Comments:

In this tutorial we build two device level entity types and learned to consolidate them 
under a single higher level construct - where that higher level construct is denoted
by a common dimension on each device. 

'''


