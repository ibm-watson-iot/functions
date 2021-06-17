#  Licensed Materials - Property of IBM 
#  5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
#  (C) Copyright IBM Corp. 2020, 2021 All Rights Reserved.
#  US Government Users Restricted Rights - Use, duplication, or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp.

import pandas as pd
import numpy as np

"""
Import the new custom function object
FORMAT
custom.<python file that contains new custom function object> import <name of the new custom function object>
"""
from custom.functions import HelloWorld


"""
Define the function with inputs required in the __init__ 
FORMAT
fn = <name of aggregator class>(<input parameters of the aggregator class>)
"""
fn = HelloWorld(name='AS_Tester', greeting_col='greeting')


"""
Load your own data
"""

"""
Build the dataframe
-- custom data VS. random data?
-- can use iotfunctions.util functions to set index
Explain the input and output in regards to the pipeline?
Explain the index in the dataframe
"""
data_size_per_id = 100
time_axis = np.linspace(0, 20, data_size_per_id)
id_0_data = np.random.normal(0, 1, size=data_size_per_id) # white noise
id_1_data = np.sin(time_axis) # sin wave
id_2_data = np.ones(data_size_per_id) # constant value
id_3_data = np.zeros(data_size_per_id) # stuck at zero
id_4_data = np.add(id_0_data, id_1_data) # sin wave with white noise
id_5_data = [1]
data = {
    'id': np.concatenate(([6]*data_size_per_id, [1]*data_size_per_id, [2]*data_size_per_id,
                          [3]*data_size_per_id, [4]*data_size_per_id, [5])),
    'input': np.concatenate((id_0_data, id_1_data, id_2_data, id_3_data, id_4_data, id_5_data))
}


pipeline_data = pd.DataFrame(data)

"""
Different grouping in aggergators simple vs complex
"""
groups = pipeline_data.groupby('id')

# Simple Aggregators


# Complex Aggregators
out = groups.apply(fn.execute)


print(out)


exit(0)