import json
import logging

from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

'''
Specify the URL to your package here.
This URL must be accessible via pip install.
Example assumes the repository is private.
Replace XXXXXX with your personal access token.
After @ you must specify a branch.

Example:
PACKAGE_URL= 'git+https://abcd1234@github.com/jones/custom-functions.git@main'
'''

PACKAGE_URL = 'git+https://XXXXXX@github.com/<user_id>/<path_to_repository>@main'


'''
Supply credentials by pasting them from the usage section into the UI.
Place your credentials in a separate (json) file that you don't check into the repo.

This example assumes the credentials are in `credentials.json` file
'''

with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
db = Database(credentials=credentials)

'''
Import the functions to register
Register function so that you can see it in the UI
'''
from custom.hello_world import XXXHelloWorld
db.register_functions([XXXHelloWorld], url=PACKAGE_URL)
