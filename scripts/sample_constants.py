'''
Sample script demonstrates how to register new Analytic Service constants.

Constants are parameters that can be managed in the Analytic Service UI and
used within expressions and custom functions.

'''
#import open source libraries
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
import pandas as pd
import json

#supply credentials
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

# import classes needed from iotfunctions
from iotfunctions.db import Database
from iotfunctions.ui import UISingle,UIMulti

# Connect to Analytic Service
db = Database(credentials = credentials, tenant_id=credentials['tennant_id'])


'''
Constants are defined by identifying  the UI control that will manager them.
Constants may either be scalars or arrays. Single valued (scalars) will be
managed using single line edit, which is derived from the UISingle class.

'''
gamma = UISingle(name='gamma',
                 description= 'Sample single valued parameter',
                 datatype=float)

'''
Arrays are managed using a multi-select control
'''

zeta = UIMulti(name='zeta',
                 description= 'Sample multi-valued array',
                 values = ['A','B','C'],
                 datatype = str
            )

'''
Use the register_constants method on the Database object to make constants
available in the UI
'''
db.register_constants([gamma,zeta])




