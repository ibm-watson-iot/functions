import logging
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)

'''
Import and instantiate the functions to be tested
'''

from custom.hello_world import XXXHelloWorld

fn = XXXHelloWorld(name='Jones', greeting_col='greeting')

'''
USING DATA STORED IN A CSV
Call the execute method directly with a dataframe.

Read in the .csv file into a dataframe
The data must contain deviceid, and timestamp column. In sample_input_data.csv, we have id column and evt_timestamp 
column representing deviceid, and timestamp columns respectively
'''
import pandas as pd

# read csv data in a dataframe
input_data = pd.read_csv('sample_input_data.csv', parse_dates=['evt_timestamp'])
# index dataframe with [id, evt_timestamp] columns
input_data = input_data.set_index(['deviceid', 'evt_timestamp'])

output_data = fn.execute(input_data)

#print the dataframe returned by the execute method
logging.debug(output_data)