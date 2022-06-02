#  Licensed Materials - Property of IBM
#  5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
#  (C) Copyright IBM Corp. 2020, 2022  All Rights Reserved.
#  US Government Users Restricted Rights - Use, duplication, or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp.

import logging
import unittest
import pandas as pd
from iotfunctions.bif import InvokeWMLModel
from iotfunctions.enginelog import EngineLogging

logger = logging.getLogger(__name__)


def test_invoke_watson_studio():
    EngineLogging.configure_console_logging(logging.DEBUG)

    # Function configuration for InvokeWMLModel
    invoke_model = InvokeWMLModel(input_items=FEATURE_NAMES, wml_auth=None, output_items=TARGET)
    invoke_model.deployment_id = DEPLOYMENT_ID
    invoke_model.apikey = API_KEY
    invoke_model.wml_endpoint = WML_ENDPOINT
    invoke_model.space_id = SPACE_ID

    if invoke_model.deployment_id == 'insert-watson-studio-deployment-id':
        logger.info('Testing without defined WML endpoint')
        return

    # Prepare Test data
    test_data = TEST_DATA
    test_df = pd.DataFrame(data=test_data, columns=FEATURE_NAMES)

    # Call test function
    test_results = invoke_model.execute(test_df)
    logger.debug(f'Test Results \n{test_results}')


"""
To use this test script you need to have a watson studio account and a model deployed in the deployment space.
Set the following global variables before calling the test function
1. Set the feature names and target to the same as deployed model
    FEATURE_NAMES = ['feature_1', 'feature_2', ...]
    TARGET = ['target_name']
2. Set wml credentials in function above
    DEPLOYMENT_ID = ***
    API_KEY = ***
    SPACE_ID = ***
    WML_ENDPOINT = ***
3. Set test data
    TEST_DATA is a list of list. Each inner list represents a row of data
    Example
        If we have two features defined as:
        FEATURE_NAMES  = ['feature_1', 'feature_2'],
        Then one row of features can be represented as a list
        ROW_1 = [24, 18]
        where feature_1 = 24 feature_2 = 18
        The test data is represented as a comma separated list of rows
        TEST_DATA = [ROW_1, ROW_2, ...]

    Example test data with two rows:
        TEST_DATA = [[24, 18], [25, 26]]
"""

FEATURE_NAMES = ['insert-one-or-more-feature-names']
TARGET = ['insert-target-name']

DEPLOYMENT_ID = 'insert-watson-studio-deployment-id'
API_KEY = 'insert-watson-studio-api-key'
SPACE_ID = 'insert-watson-studio-space-id'
WML_ENDPOINT = 'insert-watson-studio-endpoint-url'

TEST_DATA = ['insert-one-or-more-rows-of-test-data']

if __name__ == '__main__':
    test_invoke_watson_studio()
