import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from iotfunctions.base import BasePreload
from iotfunctions import ui

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/tests.git@'

class NoBuildUI(BasePreload):
    """
    Preload function has no build_ui method
    """

    def __init__ (self, dummy_input = None,
                  output_item = 'broken_preload',
                  **parameters):
        super().__init__(dummy_items = [], output_item = output_item)
        self.dummy_input = dummy_input

    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):

        return True

class ExtraArg(BasePreload):
    """
    Preload function has no
    """

    def __init__ (self, dummy_input = None,
                  output_item = 'broken_preload',
                  another_arg = 'another',
                  **parameters):
        super().__init__(dummy_items = [], output_item = output_item)
        self.dummy_input = dummy_input

    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):

        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name = 'dummy_input',
                               datatype=str,
                               description = 'Any input'
                                  )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs,outputs)

class ExtraMetadata(BasePreload):
    """
    Preload function has no
    """

    def __init__ (self, dummy_input = None,
                  output_item = 'broken_preload',
                  **parameters):
        super().__init__(dummy_items = [], output_item = output_item)
        self.dummy_input = dummy_input

    def execute(self,
                 df,
                 start_ts= None,
                 end_ts= None,
                 entities = None):

        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name = 'dummy_input',
                               datatype=str,
                               description = 'Any input'
                                  )
                    )
        inputs.append(ui.UISingle(name = 'another_input',
                               datatype=str,
                               description = 'Any input'
                                  )
                    )
        #define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name = 'output_item',
                                           datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs,outputs)