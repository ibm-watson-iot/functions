import logging
import pandas as pd
import numpy as np

from iotfunctions import ui
from iotfunctions.base import BaseSimpleAggregator, BaseComplexAggregator

logger = logging.getLogger(__name__)


class XXXDemoSimpleAggregator(BaseSimpleAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, source, name):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments

        self.source = source
        self.output = name
        super().__init__()

        # do not place any business logic in the __init__ method
        # All business logic goes into the execute() method or methods called by the
        # execute() method

    def execute(self, group):
        # the execute() method accepts a dataframe as input and returns a aggregated result as output
        # the output dataframe is expected to produce at least one new output column
        return group.max() - group.min()


    @classmethod
    def build_ui(cls):
        # Your function will UI built automatically for configuring it
        # This method describes the contents of the dialog that will be built
        # Account for each argument - specifying it as a ui object in the "inputs" or "outputs" list

        inputs = [ui.UISingleItem(name='source', description='select a time-series signal')]
        outputs = [ui.UIFunctionOutSingle(name='name', datatype=float, description='Output item produced by function')]
        return inputs, outputs


class XXXDemoComplexAggregator(BaseComplexAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''
    ALL_AGGREGATIONS = ['sum', 'mean', 'min', 'max']
    NP_AGGREGATION_MAP = {
        'sum': np.sum,
        'mean': np.mean,
        'min': np.min,
        'max': np.max
    }

    def __init__(self, source, aggregations, output):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments
        super().__init__()
        self.source = source
        self.output = output
        self.agg_func = aggregations

        # do not place any business logic in the __init__ method
        # All business logic goes into the execute() method or methods called by the
        # execute() method

    def execute(self, group):
        # the execute() method accepts a dataframe as input and returns a aggregated result as output
        # the output dataframe is expected to produce at least one new output column
        ret_dict = {}

        for func, out in zip(self.agg_func, self.output):
            ret_dict[out] = group[self.source].agg(self.NP_AGGREGATION_MAP.get(func, np.nan))

        return pd.Series(ret_dict, index=self.output)


    @classmethod
    def build_ui(cls):
        # Your function will UI built automatically for configuring it
        # This method describes the contents of the dialog that will be built
        # Account for each argument - specifying it as a ui object in the "inputs" or "outputs" list

        inputs = [ui.UISingleItem(name='source', description='select a time-series signal'), 
                  ui.UIMulti(name='aggregations', datatype=str, description='Choose quality checks to run',
                             values=cls.ALL_AGGREGATIONS, output_item='output', required=True)]
        outputs = []
        return inputs, outputs