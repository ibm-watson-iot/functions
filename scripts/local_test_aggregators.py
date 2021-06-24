import logging
import pandas as pd
import numpy as np
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)

'''
Import and instantiate the functions to be tested
'''

from custom.sample_aggregator import XXXDemoSimpleAggregator, XXXDemoComplexAggregator

simple_fn = XXXDemoSimpleAggregator(source='speed', name='speed_aggregation')
complex_fn = XXXDemoComplexAggregator(source='speed', aggregations=['sum', 'max'], output=['speed_sum', 'speed_max'])

'''
USING DATA STORED IN A CSV
Call the execute method directly with a dataframe.

Read in the .csv file into a dataframe
The data must contain deviceid, and timestamp column. In sample_input_data.csv, we have deviceid column and 
evt_timestamp column representing deviceid, and timestamp columns respectively
'''
# read csv data in a dataframe
input_data = pd.read_csv('sample_input_data.csv', parse_dates=['evt_timestamp'])


# create hourly granularity
grain = [ 'deviceid',
          pd.Grouper(key='evt_timestamp', freq='H'),
        ]

# create aggragation groups
aggreagtion_groups = input_data.groupby(grain)

#apply the execute function on all the groups
#simple aggregators
output_data_simple_fn = aggreagtion_groups.agg({
    simple_fn.source: simple_fn.execute})

#complex_aggregators
output_data_complex_fn = aggreagtion_groups.apply(complex_fn.execute)

#print the dataframe returned by the execute method
logging.debug('SIMPLE AGGREGATION RESULT')
logging.debug(output_data_simple_fn)

logging.debug('COMPLEX AGGREGATION RESULT')
logging.debug(output_data_complex_fn)