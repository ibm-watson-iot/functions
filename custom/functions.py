import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/<path_to_repository>@'


class HelloWorld(BaseTransformer):
    '''
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, name, greeting_col):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments

        self.name = name
        self.greeting_col = greeting_col
        super().__init__()

        # do not place any business logic in the __init__ method  # all business logic goes into the execute() method or methods called by the  # execute() method

    def execute(self, df):
        # the execute() method accepts a dataframe as input and returns a dataframe as output
        # the output dataframe is expected to produce at least one new output column

        df[self.greeting_col] = 'Hello %s' % self.name

        # If the function has no new output data, output a status_flag instead
        # e.g. df[<self.output_col_arg>> = True

        return df

    @classmethod
    def build_ui(cls):
        # Your function will UI built automatically for configuring it
        # This method describes the contents of the dialog that will be built
        # Account for each argument - specifying it as a ui object in the "inputs" or "outputs" list

        inputs = [ui.UISingle(name='name', datatype=str, description='Name of person to greet')]
        outputs = [
            ui.UIFunctionOutSingle(name='greeting_col', datatype=str, description='Output item produced by function')]
        return (inputs, outputs)
