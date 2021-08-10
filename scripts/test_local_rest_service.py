# *****************************************************************************
# © Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

'''
This script starts up a local web service to use when testing http functions

You will need web.py to run it

pip install web.py==0.40-dev1

Executing this script will start a server. view http://localhost:8080/ to test that it is running
'''

import web
import json

urls = ('/', 'index')


class index:

    def GET(self):
        response = {"deviceid": ["A101", "B102"], "temp": [37, 39], "pressure": [92, 89]}

        web.header('Content-Type', 'application/json')
        return json.dumps(response)


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
