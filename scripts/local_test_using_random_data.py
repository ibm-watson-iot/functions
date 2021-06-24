import logging
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)

'''
Import and instantiate the functions to be tested
'''

from custom.hello_world import XXXHelloWorld

fn = XXXHelloWorld(name='Jones', greeting_col='greeting')


'''
USING RANDOMLY GENERATED DATA

The local test will generate data instead of using server data.
By default it will assume that the input data items are numeric.

Required data items will be inferred from the function inputs.

The function below executes an expression involving a column called x1
The local test function will generate data dataframe containing the column x1

By default test results are written to a file named df_test_entity_for_<function_name>
This file will be written to the working directory.
'''
output_data_on_randomly_generated_input = fn.execute_local_test(to_csv=True)
