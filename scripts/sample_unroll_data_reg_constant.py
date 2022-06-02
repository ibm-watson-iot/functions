#!/usr/bin/python3
# *****************************************************************************
# © Copyright IBM Corp. 2018 - 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import datetime as dt
import sys
import logging
import json
from iotfunctions.db import Database
from iotfunctions import bif
from iotfunctions.enginelog import EngineLogging

PACKAGE_URL = "https://github.com/sedgewickmm18/mmfunctions"

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())


db_schema = None
db = Database(credentials=credentials)


auth_token = {"identity": {"appId": "A:fillin:yourcred"},
                           "auth": {"key": "a-fillin-yourcred", "token": "FILLINYOURTKENHERE"}}

class AuthToken:

    def __init__(self, name, json):
        self.name = name
        self.datatype = 'JSON'
        self.description = 'AuthToken'
        self.value = json

    def to_metadata(self):
        meta = {'name': self.name, 'dataType': self.datatype, 'description': self.description,
                'value': self.value}
        return meta


#db.unregister_constants('auth_token')
db.register_constants([AuthToken('auth_token', auth_token)])
