import re
import logging
import datetime as dt
import math
import numpy as np
import pandas as pd

from iotfunctions.base import BaseSimpleAggregator
from iotfunctions.ui import (UIMultiItem,UIExpression)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/<user_id>/<path_to_repository>@starter_agg_package'

class HelloWorldAggregator(BaseSimpleAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, source=None, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.input_items = source
        self.expression = expression

    def execute(self, group):
        return eval(re.sub(r"\$\{GROUP\}", r"group", self.expression))

    @classmethod
    def build_ui(cls):
        inputs = []
        # Input variable name must be kept 'source'
        # Output variable name must be kept 'name'
        inputs.append(UIMultiItem(name='source', datatype=None, description=('Choose the data items'
                                                                            ' that you would like to'
                                                                                  ' aggregate'),
                                  output_item='name', is_output_datatype_derived=True))

        inputs.append(UIExpression(name='expression', description='Use ${GROUP} to reference the current grain.'
                                                    'All Pandas Series methods can be used on the grain.'
                                                    'For example, ${GROUP}.max() - ${GROUP}.min().'))
        return (inputs, [])
