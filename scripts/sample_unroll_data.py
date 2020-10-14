# *****************************************************************************
# © Copyright IBM Corp. 2018-2020.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

'''
The Built In Functions module contains customer specific helper functions
'''

import json
import datetime as dt
import pytz

import pandas as pd
import logging
import ast
import wiotp.sdk

from iotfunctions.base import (BaseTransformer)
from iotfunctions.ui import (UIMultiItem, UISingle)

logger = logging.getLogger(__name__)

# need to specify
#PACKAGE_URL = 'git+https://github.com/sedgewickmm18/mmfunctions.git'
#
_IS_PREINSTALLED = False


# On connect MQTT Callback.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code : " + str(rc))


# on publish MQTT Callback.
def on_publish(client, userdata, mid):
    print("Message Published.")

class UnrollData(BaseTransformer):
    '''
    Compute L2Norm of string encoded array
    '''
    def __init__(self, group1_in, group2_in, group1_out, group2_out):
        super().__init__()

        self.group1_in = group1_in
        self.group2_in = group2_in
        self.group1_out = group1_out
        self.group2_out = group2_out

    def execute(self, df):

        #
        c = self._entity_type.get_attributes_dict()
        try:
            auth_token = c['auth_token']
            print('Auth Token ' , str(auth_token))
        except Exception as ae:
            print('Auth Token missing ' , str(ae))

        i_am_device = False
        try:
            if 'auth' in auth_token:
                if 'identity' in auth_token.auth:
                    if 'orgId' in auth_token.auth.identity:
                        i_am_device = True
        except Exception:
            pass


        # ONE ENTITY FOR NOW
        # connect
        print('Unroll Data execute')
        client = None
        if i_am_device:
            client = wiotp.sdk.device.DeviceClient(config=auth_token, logHandlers=None)
        else:
            client = wiotp.sdk.application.ApplicationClient(config=auth_token, logHandlers=None)

        client.on_connect = on_connect  # On Connect Callback.
        client.on_publish = on_publish  # On Publish Callback.
        client.connect()

        Now = dt.datetime.now(pytz.timezone("UTC"))
        print(Now)

        # assume single entity
        for ix, row in df.iterrows():
            # columns with 15 elements
            #print(ix, row)
            device_id = ix[0].replace('Device','Shadow')

            vibx = ast.literal_eval(row['VibrationX'])
            viby = ast.literal_eval(row['VibrationY'])
            vibz = ast.literal_eval(row['VibrationZ'])

            # columns with 5 elements
            speed = ast.literal_eval(row['Speed'])
            power = ast.literal_eval(row['Power'])

            for i in range(15):
                jsin = {'evt_timestamp': (ix[1] + pd.Timedelta(seconds=20*i - 300)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z',
                        'vibx': vibx[i], 'viby': viby[i], 'vibz': vibz[i],
                        'speed': speed[i // 3], 'power': power[i // 3]}
                jsdump = json.dumps(jsin)
                js = json.loads(jsdump)
                print('sending ', js, ' to ', device_id)
                if i_am_device:
                    client.publishEvent(eventId="MMEventOutputType", msgFormat="json", data=js)
                else:
                    client.publishEvent(typeId="MMDeviceTypeShadow", deviceId=device_id, eventId="MMEventOutputType",
                                        msgFormat="json", data=js, qos=0)  # , onPublish=eventPublishCallback)

        msg = 'UnrollData'
        self.trace_append(msg)

        return (df)

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(
                name='group1_in',
                datatype=None,
                description='String encoded array of sensor readings, 15 readings per 5 mins',
                output_item='group1_out',
                is_output_datatype_derived=True, output_datatype=None
                ))
        inputs.append(UIMultiItem(
                name='group2_in',
                datatype=None,
                description='String encoded array of sensor readings, 5 readings per 5 mins',
                output_item='group2_out',
                is_output_datatype_derived=True, output_datatype=None
                ))

        # define arguments that behave as function outputs
        outputs = []
        return (inputs, outputs)
