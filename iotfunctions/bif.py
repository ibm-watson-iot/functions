# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

"""
The Built In Functions module contains preinstalled functions
"""

import datetime as dt
import logging
import re
import time
import warnings
from collections import OrderedDict

import numpy as np
import scipy as sp
import pandas as pd
from sqlalchemy import String

from .base import (BaseTransformer, BaseEvent, BaseSCDLookup, BaseSCDLookupWithDefault, BaseMetadataProvider,
                   BasePreload, BaseDatabaseLookup, BaseDataSource, BaseDBActivityMerge, BaseSimpleAggregator)
from .loader import _generate_metadata
from .ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti, UIExpression,
                 UIText, UIParameters)
from .util import adjust_probabilities, reset_df_index, asList
from ibm_watson_machine_learning import APIClient


logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True


class ActivityDuration(BaseDBActivityMerge):
    """
    Merge data from multiple tables containing activities. An activity table
    must have a deviceid, activity_code, start_date and end_date. The
    function returns an activity duration for each selected activity code.
    """

    _is_instance_level_logged = False

    def __init__(self, table_name, activity_codes, activity_duration=None, additional_items=None,
                 additional_output_names=None):
        super().__init__(input_activities=activity_codes, activity_duration=activity_duration,
                         additional_items=additional_items, additional_output_names=additional_output_names)

        self.table_name = table_name
        self.activity_codes = activity_codes
        self.activities_metadata[table_name] = activity_codes
        self.activities_custom_query_metadata = {}

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='table_name', datatype=str, description='Source table name', ))
        inputs.append(UIMulti(name='activity_codes', datatype=str, description='Comma separated list of activity codes',
                              output_item='activity_duration', is_output_datatype_derived=False, output_datatype=float))
        inputs.append(UIMulti(name='additional_items', datatype=str, required=False,
                              description='Comma separated list of additional column names to retrieve',
                              output_item='additional_output_names', is_output_datatype_derived=True,
                              output_datatype=None))
        outputs = []

        return (inputs, outputs)


class AggregateWithExpression(BaseSimpleAggregator):
    """
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.

    Example:

    x.max() - x.min()

    """
    def __init__(self, source=None, expression=None, name=None):
        super().__init__()
        logger.info('AggregateWithExpression _init')

        self.source = source
        self.expression = expression
        self.name = name

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UIMultiItem(name='source', datatype=None,
                                  description=('Choose the data items that you would like to aggregate'),
                                  output_item='name', is_output_datatype_derived=True))
        inputs.append(UIExpression(name='expression', description='Paste in or type an AS expression'))
        return (inputs, [])

    def execute(self, x):
        logger.info('Execute AggregateWithExpression')
        logger.debug('Source=' + str(self.source) +  ', Expression=' +  str(self.expression) + ', Name=' + str(self.name))
        y = eval(self.expression)

        self.log_df_info(y, 'AggregateWithExpression evaluation')
        return y

class AggregateTimeInState(BaseSimpleAggregator):
    """
    Creates aggregation from the output of StateTimePreparation, a string
    encoded triple of a state change variable (-1 for leaving the state,
    0 for no change, 1 for entering the state) together with the current state
    and a unix epoch timestamp.
    It computes the overall number of seconds spent in a particular state.
    """

    def __init__(self, source=None, name=None):
        super().__init__()
        logger.info('AggregateTimeInState _init')

        self.source = source
        self.name = name
        print(dir(self))

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='source', datatype=None,
                                  description='Output of StateTimePreparation to aggregate over'))

        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='name', datatype=float,
                                description='Overall amount of seconds spent in a particular state'))

        return (inputs, outputs)

    def execute(self, group):
        logger.info('Execute AggregateTimeInState')

        lg = group.size
        if lg < 2:
            # We need at least two data points for function sp.interpolate.interp1d()
            logger.info(f'AggregateTimeInState no elements - returns 0 seconds, from {lg}')
            return 0.0

        # debug stuff
        #pd.set_option("display.max_rows", 50)
        #logger.info(str(group))

        df_group_exp = group.str.split(pat=',', n=3, expand=True)
        #logger.info(str(df_group_exp))

        gchange = None
        gstate = None
        gtime = None
        try:
            gchange = np.append(df_group_exp[0].values.astype(int), 0)
            gstate = np.append(df_group_exp[1].values.astype(int), 0)
            gtime = df_group_exp[2].values.astype(int)
        except Exception as esplit:
            logger.info('AggregateTimeInState elements with NaN- returns 0 seconds, from ' + str(gchange.size))
            return 0.0

        linear_interpolate = sp.interpolate.interp1d(np.arange(0, len(gtime)), gtime,
                                     kind='linear', fill_value='extrapolate')
        gtime = np.append(gtime, linear_interpolate(len(gtime)))


        # no statechange at all
        if not np.any(gchange):
            logger.debug('AggregateTimeInState: no state change at all in this aggregation, inject it')
            gchange[0] = gstate[0]
            gchange[-1] = -gstate[0]


        # look for state change 2 - start interval for StateTimePrep
        #
        # |  previous run  -1 ...  |2 ..  1 next run    |

        flag = 0
        index = 0
        with np.nditer(gchange, op_flags=['readwrite']) as it:
            for x in it:
                # apparently a StateTimePrep interval start, adjust
                if x == 2:
                    # we haven't seen a statechange yet, so state == statechange
                    if flag == 0:
                        x[...] = gstate[index]
                        x = gstate[index]
                    # we have seen a statechange before, check whether our state is different
                    elif gstate[index] != flag:
                        x[...] = -flag
                        x = -flag
                    # same state as before, just set to zero
                    else:
                        x[...] = 0
                        x = 0
                # no interval start but state change, so change the flag accordingly
                #   if x had been 2 before it is now corrected
                if x != 0:
                    flag = x
                index += 1

        # now reduce false statechange sequences like -1, 0, 0, -1, 0, 1
        #logger.info('HERE1: ' + str(gchange[0:400]))
        flag = 0
        with np.nditer(gchange, op_flags=['readwrite']) as it:
            for x in it:
                if flag == 0 and x != 0:
                    flag = x
                elif flag == x:
                    x[...] = 0
                elif flag == -x:
                    flag = x

        # adjust for intervals cut in half by aggregation
        '''
        +---------------------------- Interval ------------------------+
        0            1           -1           1           -1           0
          negative     positive     negative     positive     negative
          (ignore)       ADD        (ignore)      ADD         (ignore)

        0            1           -1           1                        0
          (ignore)       ADD        (ignore)      ADD


        0           -1            1          -1            1           0
           ADD         ignore         ADD        ignore       ADD

        0           -1            1          -1                        0
           ADD         ignore         ADD        (ignore)
        '''

        # first non zero index
        nonzeroMin = 0
        nonzeroMax = 0
        try:
            nonzeroMin = np.min(np.nonzero(gchange))
            nonzeroMax = np.max(np.nonzero(gchange))
        except Exception:
            logger.info('AggregateTimeInState all elements zero - returns 0 seconds, from ' + str(gchange.size))
            return 0.0
            pass

        if nonzeroMin > 0:
            #logger.info('YES1 ' + str(nonzeroMin) + ' ' + str(gchange[nonzeroMin]))
            if gchange[nonzeroMin] < 0:
                gchange[0] = 1
        else:
            #logger.info('NO 1 ' + str(nonzeroMin) + ' ' + str(gchange[nonzeroMin]))
            if gchange[0] < 0:
                gchange[0] = 0

        if nonzeroMax > 0:
            #logger.info('YES2 ' + str(nonzeroMax) + ' ' + str(gchange[nonzeroMax]))
            if gchange[nonzeroMax] > 0:
                gchange[-1] = -1
                # if nonzeroMax is last, ignore
                if gchange[nonzeroMax] < 0:
                    gchange[-1] = 0

        # we have odd
        #   -1     1    -1      -> gchange[0] = 0
        #    1    -1     1      -> gchange[-1] = 0
        #         even
        #   -1     1    -1     1   -> gchange[0] = 0 & gchange[-1] = 0
        #    1    -1     1    -1
        # small
        #   -1     1
        #    1    -1
        # smallest
        #   -1           -> gchange[0] = 0
        #    1           -> gchange[0] = 0

        siz = 0
        try:
            siz = np.count_nonzero(gchange)
            if siz == 1:
                gchange[0] = 0
            elif siz == 2 or siz == 0:
                print(2)
            elif siz % 2 != 0:
                # odd
                if gchange[0] == -1: gchange[0] = 0
                else: gchange[-1] = 0
            else:
                # even
                if gchange[0] == -1:
                    gchange[0] = 0
                    gchange[-1] = 0
        except Exception:
            logger.debug('AggregateTimeInState: no state change')
            pass

        #logger.debug('HERE2: ' + str(gchange[0:400]))
        logger.debug('AggregateTimeInState:  state changes ' + str(np.count_nonzero(gchange == 1)) +\
                     ' ' + str(np.count_nonzero(gchange == -1)))

        y = -(gchange * gtime).sum()
        #y = gtime.sum()
        logger.info(str(y))
        if y < 0:
            y = 0.0
        logger.info('AggregateTimeInState returns ' + str(y) + ' seconds, computed from ' + str(gchange.size))
        return y

class StateTimePreparation(BaseTransformer):
    '''
    Together with AggregateTimeInState StateTimePreparation
    calculates the amount of time a selected metric has been in a
    particular state.
    StateTimePreparation outputs an encoded triple of a state variable,
    a state change variable (-1 for leaving the state, 0 for no change,
     1 for entering the state) together with a unix epoch
    timestamp.
    The condition for the state change is given as binary operator
    together with the second argument, for example
    ">= 37"  ( for fever) or "=='running'" (for process states)
    '''
    def __init__(self, source=None, state_name=None, name=None):
        super().__init__()
        logger.info('StateTimePrep _init')

        self.source = source
        self.state_name = state_name
        self.name = name
        print(dir(self))

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='source', datatype=float,
                                  description='Data item to compute the state change array from'))
        inputs.append(UISingle(name='state_name', datatype=str,
                               description='Condition for the state change array computation'))
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='name', datatype=str, description='State change array output'))

        return (inputs, outputs)

    def _calc(self, df):
        logger.info('Execute StateTimePrep per entity')

        index_names = df.index.names
        ts_name = df.index.names[1]  # TODO: deal with non-standard dataframes (no timestamp)

        logger.info('Source: ' + self.source +  ', state_name ' +  self.state_name +  ', Name: ' + self.name +
                    ', Entity: ' + df.index[0][0])

        df_copy = df.reset_index()

        # pair of +- seconds and regular timestamp
        vstate = eval("df_copy[self.source] " + self.state_name).astype(int).values.astype(int)
        vchange = eval("df_copy[self.source] " + self.state_name).astype(int).diff().values.astype(int)

        logger.info(str(vstate))
        logger.info(str(vchange))

        #v1 = np.roll(v1_, -1)  # push the first element, NaN, to the end
        # v1[-1] = 0

        # first value is a NaN, replace it with special value for Aggregator
        vchange[0] = 2

        #logger.debug('HERE: ' + str(v1[0:600]))

        df_copy['__intermediate1__'] = vchange
        df_copy['__intermediate2__'] = vstate
        df_copy['__intermediate3__'] = (df_copy[ts_name].astype(int)// 1000000000)

        df_copy[self.name] = df_copy['__intermediate1__'].map(str) + ',' +\
                             df_copy['__intermediate2__'].map(str) + ',' +\
                             df_copy['__intermediate3__'].map(str) + ',' + df.index[0][0]

        df_copy.drop(columns=['__intermediate1__','__intermediate2__','__intermediate3__'], inplace=True)


        return df_copy.set_index(index_names)


class AlertExpression(BaseEvent):
    """
    Create alerts that are triggered when data values the expression is True
    """

    def __init__(self, expression, alert_name, **kwargs):
        self.expression = expression
        self.alert_name = alert_name
        super().__init__()

    def _calc(self, df):
        """
        unused
        """
        return df

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        df = df.copy()
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
            msg = 'Expression converted to %s. ' % expr
        else:
            expr = self.expression
            msg = 'Expression (%s). ' % expr
        self.trace_append(msg)
        df[self.alert_name] = np.where(eval(expr), True, None)
        return df

    def get_input_items(self):
        items = self.get_expression_items(self.expression)
        return items

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='expression',
                                   description="Define alert expression using pandas systax. Example: df['inlet_temperature']>50"))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)


class AlertExpressionWithFilter(BaseEvent):
    """
    Create alerts that are triggered when data values the expression is True
    """

    def __init__(self, expression, dimension_name, dimension_value, alert_name, **kwargs):
        self.dimension_name = dimension_name
        self.dimension_value = dimension_value
        self.expression = expression
        self.pulse_trigger = False
        self.alert_name = alert_name
        self.alert_end = None
        logger.info(
            'AlertExpressionWithFilter  dim: ' + str(dimension_name) + '  exp: ' + str(expression) + '  alert: ' + str(
                alert_name))
        super().__init__()

    # evaluate alerts by entity
    def _calc(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        df = df.copy()
        logger.info('AlertExpressionWithFilter  exp: ' + self.expression + '  input: ' + str(df.columns))

        expr = self.expression

        if '${' in expr:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", expr)
            msg = 'Expression converted to %s. ' % expr
        else:
            msg = 'Expression (%s). ' % expr

        self.trace_append(msg)

        expr = str(expr)
        logger.info('AlertExpressionWithFilter  - after regexp: ' + expr)

        try:
            evl = eval(expr)
            n1 = np.where(evl, 1, 0)
            if self.dimension_name is None or self.dimension_value is None or len(self.dimension_name) == 0 or len(
                    self.dimension_value) == 0:
                n2 = n1
                np_res = n1
            else:
                n2 = np.where(df[self.dimension_name] == self.dimension_value, 1, 0)
                np_res = np.multiply(n1, n2)

            # get time index
            ts_ind = df.index.get_level_values(self._entity_type._timestamp)

            if self.pulse_trigger:
                # walk through all subsequences starting with the longest
                # and replace all True with True, False, False, ...
                for i in range(np_res.size, 2, -1):
                    for j in range(0, i - 1):
                        if np.all(np_res[j:i]):
                            np_res[j + 1:i] = np.zeros(i - j - 1, dtype=int)
                            np_res[j] = i - j  # keep track of sequence length

                if self.alert_end is not None:
                    alert_end = np.zeros(np_res.size)
                    for i in range(np_res.size):
                        if np_res[i] > 0:
                            alert_end[i] = ts_ind[i]

            else:
                if self.alert_end is not None:
                    df[self.alert_end] = df.index[0]

            logger.info('AlertExpressionWithFilter  shapes ' + str(n1.shape) + ' ' + str(n2.shape) + ' ' + str(
                np_res.shape) + '  results\n - ' + str(n1) + '\n - ' + str(n2) + '\n - ' + str(np_res))
            df[self.alert_name] = np_res

        except Exception as e:
            logger.info('AlertExpressionWithFilter  eval for ' + expr + ' failed with ' + str(e))
            df[self.alert_name] = None
            pass

        return df

    def execute(self, df):
        """
        unused
        """
        return super().execute(df)

    def get_input_items(self):
        items = set(self.dimension_name)
        items = items | self.get_expression_items(self.expression)
        return items

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='dimension_name', datatype=str))
        inputs.append(UISingle(name='dimension_value', datatype=str, description='Dimension Filter Value'))
        inputs.append(UIExpression(name='expression', description="Define alert expression using pandas systax. \
                                                Example: df['inlet_temperature']>50. ${pressure} will be substituted \
                                                with df['pressure'] before evaluation, ${} with df[<dimension_name>]"))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)


class AlertExpressionWithFilterExt(AlertExpressionWithFilter):
    """
    Create alerts that are triggered when data values the expression is True
    """

    def __init__(self, expression, dimension_name, dimension_value, pulse_trigger, alert_name, alert_end, **kwargs):
        super().__init__(expression, dimension_name, dimension_value, alert_name, **kwargs)
        if pulse_trigger is None:
            self.pulse_trigger = True
        if alert_end is not None:
            self.alert_end = alert_end

        logger.info('AlertExpressionWithFilterExt  dim: ' + str(dimension_name) + '  exp: ' + str(
            expression) + '  alert: ' + str(alert_name) + '  pulsed: ' + str(pulse_trigger))

    def _calc(self, df):
        """
        unused
        """
        return df

    def execute(self, df):
        df = super().execute(df)
        logger.info('AlertExpressionWithFilterExt  generated columns: ' + str(df.columns))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='dimension_name', datatype=str))
        inputs.append(UISingle(name='dimension_value', datatype=str, description='Dimension Filter Value'))
        inputs.append(UIExpression(name='expression', description="Define alert expression using pandas systax. \
                                                Example: df['inlet_temperature']>50. ${pressure} will be substituted \
                                                with df['pressure'] before evaluation, ${} with df[<dimension_name>]"))
        inputs.append(
            UISingle(name='pulse_trigger', description="If true only generate alerts on crossing the threshold",
                     datatype=bool))

        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        outputs.append(
            UIFunctionOutSingle(name='alert_end', datatype=dt.datetime, description='End of pulse triggered alert'))
        return (inputs, outputs)


class AlertOutOfRange(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least one threshold.
    """

    def __init__(self, input_item, lower_threshold=None, upper_threshold=None, output_alert_upper=None,
                 output_alert_lower=None, **kwargs):

        self.input_item = input_item
        if lower_threshold is not None:
            lower_threshold = float(lower_threshold)
        self.lower_threshold = lower_threshold
        if upper_threshold is not None:
            upper_threshold = float(upper_threshold)
        self.upper_threshold = upper_threshold

        if output_alert_lower is None:
            self.output_alert_lower = 'output_alert_lower'
        else:
            self.output_alert_lower = output_alert_lower

        if output_alert_upper is None:
            self.output_alert_upper = 'output_alert_upper'
        else:
            self.output_alert_upper = output_alert_upper

        super().__init__()

    def _calc(self, df):
        """
        unused
        """

    def execute(self, df):
        # c = self._entity_type.get_attributes_dict()
        df = df.copy()
        df[self.output_alert_upper] = False
        df[self.output_alert_lower] = False

        if self.lower_threshold is not None:
            df[self.output_alert_lower] = np.where(df[self.input_item] <= self.lower_threshold, True, None)
        if self.upper_threshold is not None:
            df[self.output_alert_upper] = np.where(df[self.input_item] >= self.upper_threshold, True, None)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='lower_threshold', datatype=float,
                               description='Alert when item value is lower than this value', required=False, ))
        inputs.append(UISingle(name='upper_threshold', datatype=float,
                               description='Alert when item value is higher than this value', required=False, ))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_alert_lower', datatype=bool, description='Output of alert function'))
        outputs.append(
            UIFunctionOutSingle(name='output_alert_upper', datatype=bool, description='Output of alert function'))

        return (inputs, outputs)


class AlertHighValue(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold'.
    """

    def __init__(self, input_item, upper_threshold=None, alert_name=None, **kwargs):
        self.input_item = input_item
        self.upper_threshold = float(upper_threshold)
        if alert_name is None:
            self.alert_name = 'alert_name'
        else:
            self.alert_name = alert_name

        super().__init__()

    def _calc(self, df):
        """
        unused
        """

    def execute(self, df):
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item] >= self.upper_threshold, True, None)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='upper_threshold', datatype=float,
                               description='Alert when item value is higher than this value'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)

    def _getMetadata(self, df=None, new_df=None, inputs=None, outputs=None, constants=None):
        return self.build_ui()


class AlertLowValue(BaseEvent):
    """
    Fire alert when metric goes below a threshold'.
    """

    def __init__(self, input_item, lower_threshold=None, alert_name=None, **kwargs):
        self.input_item = input_item
        self.lower_threshold = float(lower_threshold)
        if alert_name is None:
            self.alert_name = 'alert_name'
        else:
            self.alert_name = alert_name

        super().__init__()

    def _calc(self, df):
        """
        unused
        """
        return df

    def execute(self, df):
        # c = self._entity_type.get_attributes_dict()
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item] <= self.lower_threshold, True, None)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=None, description='Item to alert on'))
        inputs.append(UISingle(name='lower_threshold', datatype=float,
                               description='Alert when item value is lower than this value'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='alert_name', datatype=bool, description='Output of alert function'))
        return (inputs, outputs)


class AutoTest(BaseTransformer):
    """
    Test the results of pipeline execution against a known test dataset.
    The test will compare calculated values with values in the test dataset.
    Discepancies will the written to a test output file.

    Note: This function is experimental
    """

    def __init__(self, test_datset_name, columns_to_test, result_col=None):
        super().__init__()

        self.test_datset_name = test_datset_name
        self.columns_to_test = columns_to_test
        if result_col is None:
            self.result_col = 'test_result'
        else:
            self.result_col = result_col

    def execute(self, df):
        db = self.get_db()
        # bucket = self.get_bucket_name()

        file = db.model_store.retrieve_model(self.test_datset_name)
        logger.debug('AutoTest executed - result in ' + str(file))

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = OrderedDict()
        inputs['test_datset_name'] = UISingle(name='test_datset_name', datatype=str,
                                              description=('Name of cos object containing'
                                                           ' test data. Object is a pickled '
                                                           ' dataframe. Object must be placed '
                                                           ' in the bos_runtime_bucket'))
        inputs['columns_to_test'] = UIMultiItem(name='input_items', datatype=None,
                                                description=('Choose the data items that'
                                                             ' you would like to compare'))
        outputs = OrderedDict()

        return (inputs, outputs)


class Coalesce(BaseTransformer):
    """
    Return first non-null value from a list of data items.
    """

    def __init__(self, data_items, output_item=None):
        super().__init__()
        self.data_items = data_items
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = df[self.data_items].bfill(axis=1).iloc[:, 0]

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('data_items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=float))

        return (inputs, outputs)


class CoalesceDimension(BaseTransformer):
    """
    Return first non-null value from a list of data items.
    """

    def __init__(self, data_items, output_item=None):
        super().__init__()
        self.data_items = data_items

        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = df[self.data_items].bfill(axis=1).iloc[:, 0]

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('data_items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=str, tags=['DIMENSION']))

        return (inputs, outputs)


class ConditionalItems(BaseTransformer):
    """
    Return null unless a condition is met.
    eg. if df["sensor_is_valid"]==True then deliver the value of df["temperature"] else deliver Null
    """

    def __init__(self, conditional_expression, conditional_items, output_items=None):

        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.conditional_items = conditional_items
        if output_items is None:
            output_items = ['conditional_%s' % x for x in conditional_items]
        self.output_items = output_items

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        df = df.copy()
        result = eval(self.conditional_expression)
        for i, o in enumerate(self.conditional_items):
            df[self.output_items[i]] = np.where(result, df[o], None)
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='conditional_expression',
                                   description="expression that returns a True/False value, eg. if df['sensor_is_valid']==True"))
        inputs.append(UIMultiItem(name='conditional_items', datatype=None,
                                  description='Data items that have conditional values, e.g. temp and pressure'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='output_items', cardinality_from='conditional_items', is_datatype_derived=False,
                               description='Function output items'))

        return (inputs, outputs)

    def get_input_items(self):
        items = self.get_expression_items(self.conditional_expression)
        return items


class DateDifference(BaseTransformer):
    """
    Calculate the difference between two date data items in days,ie: ie date_2 - date_1
    """

    def __init__(self, date_1, date_2, num_days=None):

        super().__init__()
        self.date_1 = date_1
        self.date_2 = date_2
        if num_days is None:
            self.num_days = 'num_days'
        else:
            self.num_days = num_days

    def execute(self, df):

        if self.date_1 is None or self.date_1 == self._entity_type._timestamp:
            ds_1 = self.get_timestamp_series(df)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]

        if self.date_2 is None or self.date_2 == self._entity_type._timestamp:
            ds_2 = self.get_timestamp_series(df)
            ds_2 = pd.to_datetime(ds_2)
        else:
            ds_2 = df[self.date_2]

        df[self.num_days] = (ds_2 - ds_1).dt.total_seconds() / (60 * 60 * 24)

        return df

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='date_1', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        inputs.append(UISingleItem(name='date_2', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='num_days', datatype=float, description='Number of days'))

        return (inputs, outputs)


class DateDifferenceConstant(BaseTransformer):
    """
    Calculate the difference between a data item and a constant_date,
    ie: ie constant_date - date_1
    """

    def __init__(self, date_1, date_constant, num_days=None):

        super().__init__()
        self.date_1 = date_1
        self.date_constant = date_constant
        if num_days is None:
            self.num_days = 'num_days'
        else:
            self.num_days = num_days

    def execute(self, df):

        if self.date_1 is None or self.date_1 == self._entity_type._timestamp:
            ds_1 = self.get_timestamp_series(df)
            ds_1 = pd.to_datetime(ds_1)
        else:
            ds_1 = df[self.date_1]

        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        constant_value = c[self.date_constant]
        ds_2 = pd.Series(data=constant_value, index=df.index)
        ds_2 = pd.to_datetime(ds_2)
        df[self.num_days] = (ds_2 - ds_1).dt.total_seconds() / (60 * 60 * 24)

        return df

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingleItem(name='date_1', datatype=dt.datetime, required=False,
                                   description=('Date data item. Use timestamp'
                                                ' if no date specified')))
        inputs.append(UISingle(name='date_constant', datatype=str, description='Name of datetime constant'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='num_days', datatype=float, description='Number of days'))

        return (inputs, outputs)


class DatabaseLookup(BaseDatabaseLookup):
    """
    Lookup columns from a database table. The lookup is time invariant. Lookup key column names
    must match data items names. Example: Lookup EmployeeCount and Country from a Company lookup
    table that is keyed on country_code.
    """

    # create the table and populate it using the data dict
    _auto_create_lookup_table = False

    def __init__(self, lookup_table_name, lookup_keys, lookup_items, parse_dates=None, output_items=None):
        super().__init__(lookup_table_name=lookup_table_name, lookup_keys=lookup_keys, lookup_items=lookup_items,
                         parse_dates=parse_dates, output_items=output_items)

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UISingle(name='lookup_table_name', datatype=str, description='Table name to perform lookup against')),
        inputs.append(UIMulti(name='lookup_keys', datatype=str, description='Data items to use as a key to the lookup'))
        inputs.append(UIMulti(name='lookup_items', datatype=str, description='columns to return from the lookup')),
        inputs.append(UIMulti(name='parse_dates', datatype=str, description='columns that should be converted to dates',
                              required=False))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutMulti(name='output_items', cardinality_from='lookup_items', is_datatype_derived=False,
                               description='Function output items', tags=['DIMENSION']))
        return (inputs, outputs)

    def get_item_values(self, arg, db):
        raise NotImplementedError('No items values available for generic database lookup function. \
                                   Implement a specific one for each table to define item values. ')


class DeleteInputData(BasePreload):
    """
    Delete data from time series input table for entity type
    """

    def __init__(self, dummy_items, older_than_days, output_item=None):
        super().__init__(dummy_items=dummy_items)
        self.older_than_days = older_than_days
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):
        entity_type = self.get_entity_type()
        self.get_db().delete_data(table_name=entity_type.name, schema=entity_type._db_schema,
                                  timestamp=entity_type._timestamp, older_than_days=self.older_than_days)
        msg = 'Deleted data for %s' % (self._entity_type.name)
        logger.debug(msg)
        return True

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, description='Dummy data items'))
        inputs.append(
            UISingle(name='older_than_days', datatype=float, description='Delete data older than this many days'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class DropNull(BaseMetadataProvider):
    """
    Drop any row that has all null metrics
    """

    def __init__(self, exclude_items, drop_all_null_rows=True, output_item=None):
        if output_item is None:
            output_item = 'drop_nulls'

        kw = {'_custom_exclude_col_from_auto_drop_nulls': exclude_items, '_drop_all_null_rows': drop_all_null_rows}
        super().__init__(dummy_items=exclude_items, output_item=output_item, **kw)
        self.exclude_items = exclude_items
        self.drop_all_null_rows = drop_all_null_rows

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='exclude_items', datatype=None,
                                  description='Ignore non-null values in these columns when dropping rows'))
        inputs.append(
            UISingle(name='drop_all_null_rows', datatype=bool, description='Enable or disable drop of all null rows'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class EntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    Time series columns defined on the entity data table will be populated
    with random data.

    Optional parameters:

    freq: pandas frequency string. Time series frequency.
    scd_frequency: pandas frequency string.  Dimension change frequency.
    activity_frequency: pandas frequency string. Activity frequency.
    activities = dict keyed on activity name containing list of activities codes
    scds = dict keyes on scd property name containing list of string domain items
    data_item_mean: dict keyed by data item name. Mean value.
    data_item_sd: dict keyed by data item name. Standard deviation.
    data_item_domain: dictionary keyed by data item name. List of values.
    drop_existing: bool. Drop existing input tables and generate new for each run.

    """

    is_data_generator = True
    freq = '5min'
    scd_frequency = '1D'
    activity_frequency = '3D'
    start_entity_id = 73000  # used to build entity ids
    auto_entity_count = 5  # default number of entities to generate data for
    data_item_mean = None
    data_item_sd = None
    data_item_domain = None
    activities = None
    scds = None
    drop_existing = False

    # ids of entities to generate. Change the value of the range() function to change the number of entities

    def __init__(self, ids=None, output_item=None, parameters=None, **kw):

        if output_item is None:
            output_item = 'entity_data_generator'

        if parameters is None:
            parameters = {}
        parameters = {**kw, **parameters}
        self.parameters = parameters
        self.set_params(**parameters)
        super().__init__(dummy_items=[], output_item=output_item)
        if ids is None:
            ids = self.get_entity_ids()
        self.ids = ids

        if self.data_item_mean is None:
            self.data_item_mean = {}
        if self.data_item_sd is None:
            self.data_item_sd = {}
        if self.data_item_domain is None:
            self.data_item_domain = {}
        if self.activities is None:
            self.activities = {}
        if self.scds is None:
            self.scds = {}

    def execute(self, df, start_ts=None, end_ts=None, entities=None):

        # Define simulation related metadata on the entity type

        if entities is None:
            entities = self.ids

        # Add scds
        for key, values in list(self.scds.items()):
            self._entity_type.add_slowly_changing_dimension(key, String(255))
            self.data_item_domain[key] = values

        # Add activities metadata to entity type
        table_name_prefix = self._entity_type.name
        if self._entity_type._metric_table_name is not None:
            table_name_prefix = self._entity_type._metric_table_name
        for key, codes in list(self.activities.items()):
            name = '%s_%s' % (table_name_prefix, key)
            self._entity_type.add_activity_table(name, codes)

        # Generate data

        if start_ts is not None:
            seconds = (dt.datetime.utcnow() - start_ts).total_seconds()
        else:
            seconds = pd.to_timedelta(self.freq).total_seconds()

        df = self._entity_type.generate_data(entities=entities, days=0, seconds=seconds, freq=self.freq,
                                             scd_freq=self.scd_frequency, write=True,
                                             data_item_mean=self.data_item_mean, data_item_sd=self.data_item_sd,
                                             data_item_domain=self.data_item_domain, drop_existing=self.drop_existing)

        self.usage_ = len(df.index)

        return True

    def get_entity_ids(self):
        """
        Generate a list of entity ids
        """
        ids = [str(self.start_entity_id + x) for x in list(range(self.auto_entity_count))]
        return (ids)

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMulti(name='ids', datatype=str, description='Comma separate list of entity ids, e.g: X902-A01,X902-A03'))
        inputs.append(UIParameters())
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class EntityFilter(BaseMetadataProvider):
    """
    Filter data retrieval queries to retrieve only data for the entity ids
    included in the filter
    """

    def __init__(self, entity_list, output_item=None):
        if output_item is None:
            output_item = 'is_filter_set'

        dummy_items = ['deviceid']
        kwargs = {'_entity_filter_list': entity_list}
        super().__init__(dummy_items, output_item=output_item, **kwargs)
        self.entity_list = entity_list

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name='entity_list', datatype=str, description='comma separated list of entity ids'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool,
                                           description='Returns a status flag of True when executed'))

        return (inputs, outputs)


class PythonExpression(BaseTransformer):
    """
    Create a new item from an expression involving other items
    """

    def __init__(self, expression, output_name):
        self.output_name = output_name
        super().__init__()
        # convert single quotes to double
        self.expression = self.parse_expression(expression)
        # registration
        self.constants = ['expression']
        self.outputs = ['output_name']

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        df = df.copy()
        requested = list(self.get_input_items())
        msg = self.expression + ' .'
        self.trace_append(msg)
        msg = 'Function requested items: %s . ' % ','.join(requested)
        self.trace_append(msg)
        df[self.output_name] = eval(self.expression)
        return df

    def get_input_items(self):
        items = self.get_expression_items(self.expression)
        return items

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='expression',
                                   description="Define alert expression using pandas systax. Example: df['inlet_temperature']>50"))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_name', datatype=float, description='Output of expression'))

        return (inputs, outputs)


class GetEntityData(BaseDataSource):
    """
    Get time series data from an entity type. Provide the table name for the entity type and
    specify the key column to use for mapping the source entity type to the destination.
    e.g. Add temperature sensor data to a location entity type by selecting a location_id
    as the mapping key on the source entity type

    Note: This function is experimental
    """
    is_deprecated = True

    merge_method = 'outer'
    allow_projection_list_trim = False

    def __init__(self, source_entity_type_name, key_map_column, input_items, output_items=None):
        warnings.warn('GetEntityData is deprecated.', DeprecationWarning)
        self.source_entity_type_name = source_entity_type_name
        self.key_map_column = key_map_column
        super().__init__(input_items=input_items, output_items=output_items)

    def get_data(self, start_ts=None, end_ts=None, entities=None):
        db = self.get_db()
        target = self.get_entity_type()
        # get entity type metadata from the AS API
        source = db.get_entity_type_by_name(self.source_entity_type_name)
        source._checkpoint_by_entity = False
        source._pre_aggregate_time_grain = target._pre_aggregate_time_grain
        source._pre_agg_rules = target._pre_agg_rules
        source._pre_agg_outputs = target._pre_agg_outputs
        cols = [self.key_map_column, source._timestamp]
        cols.extend(self.input_items)
        renamed_cols = [target._entity_id, target._timestamp]
        renamed_cols.extend(self.output_items)
        df = source.get_data(start_ts=start_ts, end_ts=end_ts, entities=entities, columns=cols)
        df = self.rename_cols(df, cols, renamed_cols)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='source_entity_type_name', datatype=str,
                               description="Enter the name of the entity type that you would like to retrieve data from"))
        inputs.append(UISingle(name='key_map_column', datatype=str, description="Enter the name of the column on the source entity type that represents the map \
                                            to the device id of this entity type"))
        inputs.append(UIMulti(name='input_items', datatype=str,
                              description="Comma separated list of data item names to retrieve from the source entity type",
                              output_item='output_items', is_output_datatype_derived=True))
        outputs = []

        return (inputs, outputs)

class IfThenElse(BaseTransformer):
    """
    Set the value of the output_item based on a conditional expression.
    When the conditional expression returns a True value, return the value of the true_expression.

    Example:
    conditional_expression: df['x1'] > 5 * df['x2']
    true expression: df['x2'] * 5
    false expression: 0
    """

    def __init__(self, conditional_expression, true_expression, false_expression, output_item=None):
        super().__init__()
        self.conditional_expression = self.parse_expression(conditional_expression)
        self.true_expression = self.parse_expression(true_expression)
        self.false_expression = self.parse_expression(false_expression)
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        df = df.copy()
        df[self.output_item] = np.where(eval(self.conditional_expression), eval(self.true_expression),
                                        eval(self.false_expression))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIExpression(name='conditional_expression', description="expression that returns a True/False value, \
                                                eg. if df['temp']>50 then df['temp'] else None"))
        inputs.append(UIExpression(name='true_expression', description="expression when true, eg. df['temp']"))
        inputs.append(UIExpression(name='false_expression', description='expression when false, eg. None'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)

    def get_input_items(self):
        items = self.get_expression_items([self.conditional_expression, self.true_expression, self.false_expression])
        return items


class PackageInfo(BaseTransformer):
    """
    Show the version of a list of installed packages. Optionally install packages that are not installed.
    """

    def __init__(self, package_names, add_to_trace=True, install_missing=True, version_output=None):

        self.package_names = package_names
        self.add_to_trace = add_to_trace
        self.install_missing = install_missing
        if version_output is None:
            version_output = ['%s_version' % x for x in package_names]
        self.version_output = version_output
        super().__init__()

    def execute(self, df):
        import importlib
        entity_type = self.get_entity_type()
        df = df.copy()
        for i, p in enumerate(self.package_names):
            ver = ''
            try:
                installed_package = importlib.import_module(p)
            except (BaseException):
                if self.install_missing:
                    entity_type.db.install_package(p)
                    try:
                        installed_package = importlib.import_module(p)
                    except (BaseException):
                        ver = 'Package could not be installed'
                    else:
                        try:
                            ver = 'installed %s' % installed_package.__version__
                        except AttributeError:
                            ver = 'Package has no __version__ attribute'
            else:
                try:
                    ver = installed_package.__version__
                except AttributeError:
                    ver = 'Package has no __version__ attribute'
            df[self.version_output[i]] = ver
            if self.add_to_trace:
                msg = '( %s : %s)' % (p, ver)
                self.trace_append(msg)

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMulti(name='package_names', datatype=str, description='Comma separate list of python package names',
                    output_item='version_output', is_output_datatype_derived=False, output_datatype=str))
        inputs.append(UISingle(name='install_missing', datatype=bool))
        inputs.append(UISingle(name='add_to_trace', datatype=bool))
        # define arguments that behave as function outputs
        outputs = []

        return (inputs, outputs)


class PythonFunction(BaseTransformer):
    """
    Execute a paste-in function. A paste-in function is python function declaration
    code block. The function must be called 'f' and accept two inputs:
    df (a pandas DataFrame) and parameters (a dict that you can use
    to externalize the configuration of the function).

    The function can return a DataFrame,Series,NumpyArray or scalar value.

    Example:
    def f(df,parameters):
        #  generate an 2-D array of random numbers
        output = np.random.normal(1,0.1,len(df.index))
        return output

    Function source may be pasted in or retrieved from Cloud Object Storage.

    PythonFunction is currently experimental.
    """

    function_name = 'f'

    def __init__(self, function_code, input_items, output_item, parameters=None):

        self.function_code = function_code
        self.input_items = input_items
        self.output_item = output_item
        super().__init__()
        if parameters is None:
            parameters = {}

        function_name = parameters.get('function_name', None)
        if function_name is not None:
            self.function_name = function_name

        self.parameters = parameters

    def execute(self, df):

        # function may have already been serialized to cos

        kw = {}

        if not self.function_code.startswith('def '):
            bucket = self.get_bucket_name()
            fn = self._entity_type.db.model_store.retrieve_model(self.function_code)
            kw['source'] = 'cos'
            kw['filename'] = self.function_code
            if fn is None:
                msg = (' Function text does not start with "def ". '
                       ' Function is assumed to located in COS'
                       ' Cant locate function %s in cos. Make sure this '
                       ' function exists in the %s bucket' % (self.function_code, bucket))
                raise RuntimeError(msg)

        else:
            fn = self._entity_type.db.make_function(function_name=self.function_name, function_code=self.function_code)
            kw['source'] = 'paste-in code'
            kw['filename'] = None

        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        kw['input_items'] = self.input_items
        kw['output_item'] = self.output_item
        kw['entity_type'] = self._entity_type
        kw['db'] = self._entity_type.db
        kw['c'] = c
        kw['logger'] = logger
        self.trace_append(msg=self.function_code, log_method=logger.debug, **kw)

        result = fn(df=df, parameters={**kw, **self.parameters})

        df[self.output_item] = result

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        inputs.append(UIText(name='function_code', description='Paste in your function definition'))
        inputs.append(UISingle(name='parameters', datatype=dict, required=False,
                               description='optional parameters specified in json format'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item', datatype=float))

        return (inputs, outputs)


class RaiseError(BaseTransformer):
    """
    Halt execution of the pipeline raising an error that will be shown. This function is
    useful for testing a pipeline that is running to completion but not delivering the expected results.
    By halting execution of the pipeline you can view useful diagnostic information in an error
    message displayed in the UI.
    """

    def __init__(self, halt_after, abort_execution=True, output_item=None):
        super().__init__()
        self.halt_after = halt_after
        self.abort_execution = abort_execution
        if output_item is None:
            self.output_item = 'pipeline_exception'
        else:
            self.output_item = output_item

    def execute(self, df):
        msg = self.log_df_info(df, 'Prior to raising error')
        self.trace_append(msg)
        msg = 'The calculation was halted deliberately by the IoTRaiseError function. Remove the IoTRaiseError \
               function or disable "abort_execution" in the function configuration. '
        if self.abort_execution:
            raise RuntimeError(msg)

        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='halt_after', datatype=None, description='Raise error after calculating items'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class RandomNoise(BaseTransformer):
    """
    Add random noise to one or more data items
    """

    def __init__(self, input_items, standard_deviation, output_items):
        super().__init__()
        self.input_items = input_items
        self.standard_deviation = standard_deviation
        self.output_items = output_items

    def execute(self, df):
        for i, item in enumerate(self.input_items):
            output = self.output_items[i]
            random_noise = np.random.normal(0, self.standard_deviation, len(df.index))
            df[output] = df[item] + random_noise
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='standard_deviation', datatype=float, description="Standard deviation of noise"))
        inputs.append(
            UIMultiItem(name='input_items', description="Chose data items to add noise to", output_item='output_items',
                        is_output_datatype_derived=True))
        outputs = []

        return (inputs, outputs)


class RandomUniform(BaseTransformer):
    """
    Generate a uniformally distributed random number.
    """

    def __init__(self, min_value, max_value, output_item=None):
        super().__init__()
        self.min_value = min_value
        self.max_value = max_value
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.uniform(self.min_value, self.max_value, len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='min_value', datatype=float))
        inputs.append(UISingle(name='max_value', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class RandomNormal(BaseTransformer):
    """
    Generate a normally distributed random number.
    """

    def __init__(self, mean, standard_deviation, output_item=None):
        super().__init__()
        self.mean = mean
        self.standard_deviation = standard_deviation
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.normal(self.mean, self.standard_deviation, len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='mean', datatype=float))
        inputs.append(UISingle(name='standard_deviation', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class RandomNull(BaseTransformer):
    """
    Occassionally replace random values with null values for selected items.
    """

    def __init__(self, input_items, output_items):
        super().__init__()
        self.input_items = input_items
        self.output_items = output_items

    def execute(self, df):
        for counter, item in enumerate(self.input_items):
            choice = np.random.choice([True, False], len(df.index))
            df[self.output_items[counter]] = np.where(choice, None, df[item])

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMultiItem(name='input_items', datatype=None, description='Select items to apply null replacement to',
                        output_item='output_items', is_output_datatype_derived=True, output_datatype=None))
        outputs = []
        return (inputs, outputs)


class RandomChoiceString(BaseTransformer):
    """
    Generate random categorical values.
    """

    def __init__(self, domain_of_values, probabilities=None, output_item=None):
        super().__init__()
        self.domain_of_values = domain_of_values
        self.probabilities = adjust_probabilities(probabilities)
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.choice(a=self.domain_of_values, p=self.probabilities, size=len(df.index))
        return df

    @classmethod
    def build_ui(cls):
        #  define arguments that behave as function inputs

        inputs = []
        inputs.append(UIMulti(name='domain_of_values', datatype=str, required=True))
        inputs.append(UIMulti(name='probabilities', datatype=float, required=False))
        #  define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=str, description='Random output', tags=['DIMENSION']))

        return (inputs, outputs)


class RandomDiscreteNumeric(BaseTransformer):
    """
    Generate random discrete numeric values.
    """

    def __init__(self, discrete_values, probabilities=None, output_item=None):
        super().__init__()
        self.discrete_values = discrete_values
        self.probabilities = adjust_probabilities(probabilities)
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = np.random.choice(a=self.discrete_values, p=self.probabilities, size=len(df.index))

        return df

    @classmethod
    def build_ui(cls):
        #  define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMulti(name='discrete_values', datatype=float))
        inputs.append(UIMulti(name='probabilities', datatype=float, required=False))
        #  define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float, description='Random output'))

        return (inputs, outputs)


class SaveCosDataFrame(BaseTransformer):
    """
    Serialize dataframe to COS
    """

    def __init__(self, filename=None, columns=None, output_item=None):

        super().__init__()
        if filename is None:
            self.filename = 'job_output_df'
        else:
            self.filename = filename

        self.columns = columns

        if output_item is None:
            self.output_item = 'save_df_result'
        else:
            self.output_item = output_item

    def execute(self, df):

        if self.columns is not None:
            sf = df[self.columns]
        else:
            sf = df
        db = self.get_db()
        bucket = self.get_bucket_name()
        db.cos_save(persisted_object=sf, filename=self.filename, bucket=bucket, binary=True)
        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='filename', datatype=str))
        inputs.append(UIMultiItem(name='columns'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=str, description='Result of save operation'))

        return (inputs, outputs)


class SCDLookup(BaseSCDLookup):
    """
    Lookup an slowly changing dimension property from a scd lookup table containing:
    Start_date, end_date, device_id and property. End dates are not currently used.
    Previous lookup value is assumed to be valid until the next.
    """

    def __init__(self, table_name, output_item=None):
        self.table_name = table_name
        super().__init__(table_name=table_name, output_item=output_item)


class IoTSCDLookupWithDefault(BaseSCDLookupWithDefault):
    """
    Look up an scd property from a scd lookup table containing columns for:
    start_date, end_date, device_id and dimension property.
    If the table does not provide a value for a given time
    the default value is taken.
    """

    def __init__(self, table_name, dimension_name, entity_name, start_name, end_name, default_value, output_item=None):
        super().__init__(table_name=table_name, output_item=output_item, default_value=default_value,
                         dimension_name=dimension_name, entity_name=entity_name, start_name=start_name,
                         end_name=end_name)


class ShiftCalendar(BaseTransformer):
    """
    Generate data for a shift calendar using a shift_definition in the form of a dict keyed on shift_id
    Dict contains a tuple with the start and end hours of the shift expressed as numbers. Example:
          {
               "1": [5.5, 14],
               "2": [14, 21],
               "3": [21, 29.5]
           },
    """

    is_custom_calendar = True
    auto_conform_index = True

    def __init__(self, shift_definition=None, period_start_date=None, period_end_date=None, shift_day=None,
                 shift_id=None):

        if shift_definition is None:
            self.shift_definition = {"1": [5.5, 14], "2": [14, 21], "3": [21, 29.5]}
        else:
            self.shift_definition = shift_definition

        if period_start_date is None:
            self.period_start_date = 'shift_start_date'
        else:
            self.period_start_date = period_start_date

        if period_end_date is None:
            self.period_end_date = 'shift_end_date'
        else:
            self.period_end_date = period_end_date

        if shift_day is None:
            self.shift_day = 'shift_day'
        else:
            self.shift_day = shift_day

        if shift_id is None:
            self.shift_id = 'shift_id'
        else:
            self.shift_id = shift_id

        super().__init__()

    def get_data(self, start_date, end_date):
        if start_date is None:
            raise ValueError('Start date is required when building data for a shift calendar')
        if end_date is None:
            raise ValueError('End date is required when building data for a shift calendar')

        # Subtract a day from start_date and add a day to end_date to provide shift information for the full
        # calendar days at left and right boundary.
        # Example: shift1 = [22:00,10:00], shift2 = [10:00, 22:00], data point = '2019-11-22 23:01:00' ==> data point
        # falls into shift_day '2019-11-23', not '2019-11-22'
        one_day = pd.DateOffset(days=1)
        start_date = start_date.date() - one_day
        end_date = end_date.date() + one_day
        dates = pd.date_range(start=start_date, end=end_date, freq='1D').tolist()
        dfs = []
        for shift_id, start_end in list(self.shift_definition.items()):
            data = {}
            data[self.shift_day] = dates
            data[self.shift_id] = shift_id
            data[self.period_start_date] = [x + dt.timedelta(hours=start_end[0]) for x in dates]
            data[self.period_end_date] = [x + dt.timedelta(hours=start_end[1]) for x in dates]
            dfs.append(pd.DataFrame(data))
        df = pd.concat(dfs)
        df[self.period_start_date] = pd.to_datetime(df[self.period_start_date])
        df[self.period_end_date] = pd.to_datetime(df[self.period_end_date])
        df.sort_values([self.period_start_date], inplace=True)
        return df

    def get_empty_data(self):
        col_types = {self.shift_day: 'datetime64[ns]', self.shift_id: 'float64',
                     self.period_start_date: 'datetime64[ns]', self.period_end_date: 'datetime64[ns]'}

        df = pd.DataFrame(columns=col_types.keys())
        df = df.astype(dtype=col_types)

        return df

    def execute(self, df):

        df = reset_df_index(df, auto_index_name=self.auto_index_name)
        entity_type = self.get_entity_type()
        (df, ts_col) = entity_type.df_sort_timestamp(df)
        start_date = df[ts_col].min()
        end_date = df[ts_col].max()

        if len(df.index) > 0:
            calendar_df = self.get_data(start_date=start_date, end_date=end_date)
            df = pd.merge_asof(left=df, right=calendar_df, left_on=ts_col, right_on=self.period_start_date,
                               direction='backward')

            df = self._entity_type.index_df(df)

        return df

    def get_period_end(self, date):

        df = self.get_data(date, date)
        result = df[self.period_end_date].max()

        return result

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='shift_definition', datatype=dict, description=''))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='period_start_date', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='period_end_date', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='shift_day', datatype=dt.datetime, tags=['DIMENSION']))
        outputs.append(UIFunctionOutSingle(name='shift_id', datatype=int, tags=['DIMENSION']))

        return (inputs, outputs)


class Sleep(BaseTransformer):
    """
    Wait for the designated number of seconds
    """

    def __init__(self, sleep_after, sleep_duration_seconds=None, output_item=None):
        super().__init__()
        self.sleep_after = sleep_after

        if sleep_duration_seconds is None:
            self.sleep_duration_seconds = 30
        else:
            self.sleep_duration_seconds = sleep_duration_seconds

        if output_item is None:
            self.output_item = 'sleep_status'
        else:
            self.output_item = output_item

    def execute(self, df):
        msg = 'Sleep duration: %s. ' % self.sleep_duration_seconds
        self.trace_append(msg)
        time.sleep(self.sleep_duration_seconds)
        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            UIMultiItem(name='sleep_after', datatype=None, required=False, description='Sleep after calculating items'))
        inputs.append(UISingle(name='sleep_duration_seconds', datatype=float))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class TraceConstants(BaseTransformer):
    """
    Write the values of available constants to the trace
    """

    def __init__(self, dummy_items=None, output_item=None):
        super().__init__()

        self.dummy_items = dummy_items
        if output_item is None:
            self.output_item = 'trace_written'
        else:
            self.output_item = output_item

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        msg = 'entity constants retrieved'
        self.trace_append(msg, **c)

        if c is None or len(c) == 0:
            logger.info("No constants have been defined")
        else:
            logger.info("The list of available constants (name: value):")
            for name, value in c.items():
                logger.info(f"{name:>30}: {str(value)}")

        df[self.output_item] = True
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, required=False, description='Not required'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class TimestampCol(BaseTransformer):
    """
    Deliver a data item containing the timestamp
    """

    def __init__(self, dummy_items=None, output_item=None):
        super().__init__()
        self.dummy_items = dummy_items
        if output_item is None:
            self.output_item = 'timestamp_col'
        else:
            self.output_item = output_item

    def execute(self, df):
        ds_1 = self.get_timestamp_series(df)
        ds_1 = pd.to_datetime(ds_1)
        df[self.output_item] = ds_1

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name='dummy_items', datatype=None, required=False, description='Not required'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=dt.datetime, description='Timestamp column name'))

        return (inputs, outputs)


# Renamed functions
IoTExpression = PythonExpression
IoTRandomChoice = RandomChoiceString
IoTRandonNormal = RandomNormal
IoTActivityDuration = ActivityDuration
IoTSCDLookup = SCDLookup
IoTShiftCalendar = ShiftCalendar
IoTAlertHighValue = AlertHighValue
IoTAlertLow = AlertLowValue
IoTAlertExpression = AlertExpression
IoTAlertOutOfRange = AlertOutOfRange
IoTAutoTest = AutoTest
IoTConditionalItems = ConditionalItems
IoTDatabaseLookup = DatabaseLookup
IoTDeleteInputData = DeleteInputData
IoTDropNull = DropNull
IoTEntityFilter = EntityFilter
IoTIfThenElse = IfThenElse
IoTPackageInfo = PackageInfo
IoTRaiseError = RaiseError
IoTSaveCosDataFrame = SaveCosDataFrame
IoTSleep = Sleep
IoTTraceConstants = TraceConstants


# Deprecated functions
class IoTEntityDataGenerator(BasePreload):
    """
    Automatically load the entity input data table using new generated data.
    Time series columns defined on the entity data table will be populated
    with random data.
    """

    is_deprecated = True

    def __init__(self, ids=None, output_item=None):
        self.ids = ids
        if output_item is None:
            self.output_item = 'entity_data_generator'
        else:
            self.output_item = output_item

    def get_replacement(self):
        new = EntityDataGenerator(ids=self.ids, output_item=self.output_item)

        return new


class IoTCalcSettings(BaseMetadataProvider):
    """
    Overide default calculation settings for the entity type
    """

    is_deprecated = True

    def __init__(self, checkpoint_by_entity=False, pre_aggregate_time_grain=None, auto_read_from_ts_table=None,
                 sum_items=None, mean_items=None, min_items=None, max_items=None, count_items=None, sum_outputs=None,
                 mean_outputs=None, min_outputs=None, max_outputs=None, count_outputs=None, output_item=None):

        warnings.warn('IoTCalcSettings is deprecated. Use entity type constants instead of a '
                      'metadata provider to set entity type properties', DeprecationWarning)

        if auto_read_from_ts_table is None:
            auto_read_from_ts_table = True

        if output_item is None:
            output_item = 'output_item'

        # metadata for pre-aggregation:
        # pandas aggregate dict containing a list of aggregates for each item
        self._pre_agg_rules = {}
        # dict containing names of aggregate items produced for each item
        self._pre_agg_outputs = {}
        # assemble these metadata structures
        self._apply_pre_agg_metadata('sum', items=sum_items, outputs=sum_outputs)
        self._apply_pre_agg_metadata('mean', items=mean_items, outputs=mean_outputs)
        self._apply_pre_agg_metadata('min', items=min_items, outputs=min_outputs)
        self._apply_pre_agg_metadata('max', items=max_items, outputs=max_outputs)
        self._apply_pre_agg_metadata('count', items=count_items, outputs=count_outputs)
        # pass metadata to the entity type
        kwargs = {'_checkpoint_by_entity': checkpoint_by_entity, '_pre_aggregate_time_grain': pre_aggregate_time_grain,
                  '_auto_read_from_ts_table': auto_read_from_ts_table, '_pre_agg_rules': self._pre_agg_rules,
                  '_pre_agg_outputs': self._pre_agg_outputs}
        super().__init__(dummy_items=[], output_item=output_item, **kwargs)

    def _apply_pre_agg_metadata(self, aggregate, items, outputs):
        """
        convert UI inputs into a pandas aggregate dictionary and
        a separate dictionary containing names of aggregate items
        """
        if items is not None:
            if outputs is None:
                outputs = ['%s_%s' % (x, aggregate) for x in items]
            for i, item in enumerate(items):
                try:
                    self._pre_agg_rules[item].append(aggregate)
                    self._pre_agg_outputs[item].append(outputs[i])
                except KeyError:
                    self._pre_agg_rules[item] = [aggregate]
                    self._pre_agg_outputs[item] = [outputs[i]]
                except IndexError:
                    msg = 'Metadata for aggregate %s is not defined correctly. Outputs array should match \
                           length of items array.' % aggregate
                    raise ValueError(msg)

        return None

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UISingle(name='auto_read_from_ts_table', datatype=bool, required=False,
                               description='By default, data retrieved is from the designated input table. Use this setting to disable.', ))
        inputs.append(
            UISingle(name='checkpoint_by_entity', datatype=bool, required=False, description='By default a single '))
        inputs.append(UISingle(name='pre_aggregate_time_grain', datatype=str, required=False, description='By default, data is retrieved at the input grain. Use this setting to preaggregate \
                                            data and reduce the volumne of data retrieved',
                               values=['1min', '5min', '15min', '30min', '1H', '2H', '4H', '8H', '12H', 'day', 'week',
                                       'month', 'year']))
        inputs.append(UIMultiItem(name='sum_items', datatype=float, required=False,
                                  description='Choose items that should be added when aggregating',
                                  output_item='sum_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='mean_items', datatype=float, required=False,
                                  description='Choose items that should be averaged when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='min_items', datatype=float, required=False,
                                  description='Choose items that the system should find the smallest value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='max_items', datatype=float, required=False,
                                  description='Choose items that the system should find the largest value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        inputs.append(UIMultiItem(name='count_items', datatype=float, required=False,
                                  description='Choose items that the system should count the value when aggregating',
                                  output_item='mean_outputs', is_output_datatype_derived=True))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

        return (inputs, outputs)


class IoTCosFunction(BaseTransformer):
    """
    Execute a serialized function retrieved from cloud object storage.
    Function returns a single output.

    Function is replaced by PythonFunction

    """

    is_deprecated = True

    def __init__(self, function_name, input_items, output_item=None, parameters=None):

        warnings.warn('IoTCosFunction is deprecated. Use PythonFunction.', DeprecationWarning)

        # the function name may be passed as a function object or function name (string)
        # if a string is provided, it is assumed that the function object has already been serialized to COS
        # if a function onbject is supplied, it will be serialized to cos
        self.input_items = input_items
        if output_item is None:
            self.output_item = 'output_item'
        else:
            self.output_item = output_item
        super().__init__()
        # get the cos bucket
        # if function object, serialize and get name
        self.function_name = function_name
        # The function called during execution accepts a single dictionary as input
        # add all instance variables to the parameters dict in case the function needs them
        if parameters is None:
            parameters = {}
        parameters = {**parameters, **self.__dict__}
        self.parameters = parameters

    def execute(self, df):
        db = self.get_db()
        bucket = self.get_bucket_name()
        # first test execution could include a fnction object
        # serialize it
        if callable(self.function_name):
            db.cos_save(persisted_object=self.function_name, filename=self.function_name.__name__, bucket=bucket,
                        binary=True)
            self.function_name = self.function_name.__name__
        # retrieve
        function = db.cos_load(filename=self.function_name, bucket=bucket, binary=True)
        # execute
        df = df.copy()
        rf = function(df, self.parameters)
        # rf will contain the orginal columns along with a single new output column.
        return rf

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem('input_items'))
        inputs.append(UISingle(name='function_name', datatype=float,
                               description='Name of function object. Function object must be serialized to COS before you can use it'))
        inputs.append(UISingle(name='parameters', datatype=dict, required=False,
                               description='Parameters required by the function are provides as json.'))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(UIFunctionOutSingle('output_item'))


# All of below functions are moved from calc.py file in Analytics Service.
class Alert:

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create alerts that are triggered when data values reach a particular range.', 'input': [
                {'name': 'sources', 'description': 'Select one or more data items to build your alert.',
                 'type': 'DATA_ITEM', 'required': True, 'dataType': 'ARRAY',
                 'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "sources",
                                "type": "array", "minItems": 1, "items": {"type": "string"}}}, {'name': 'expression',
                                                                                                'description': 'Build the expression for your alert by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                                                                                                'type': 'CONSTANT',
                                                                                                'required': True,
                                                                                                'dataType': 'LITERAL'}],
            'output': [{'name': 'name', 'description': 'The name of the new alert.', 'dataType': 'BOOLEAN',
                        'tags': ['ALERT', 'EVENT']}], 'tags': ['EVENT']})

    def __init__(self, name=None, sources=None, expression=None):
        warnings.warn('Alert function is deprecated. Use AlertExpression.', DeprecationWarning)
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.name = name
        self.expression = expression
        self.sources = sources

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None

        sources_not_in_column = df.index.names
        df = df.reset_index()

        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('alert_expression=%s' % str(expr))

        df[self.name] = np.where(eval(expr), True, None)

        self.logger.debug('alert_name {}'.format(self.name))
        if 'test' in self.name:
            self.logger.debug('alert_dataframe {}'.format(df[self.name]))

        df = df.set_index(keys=sources_not_in_column)

        return df


class NewColFromCalculation(BaseTransformer):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Create a new data item by expression.', 'input': [
            {'name': 'sources', 'description': 'Select one or more data items to be used in the expression.',
             'type': 'DATA_ITEM', 'required': True, 'dataType': 'ARRAY',
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "sources", "type": "array",
                            "minItems": 1, "items": {"type": "string"}}}, {'name': 'expression',
                                                                           'description': 'Build the expression by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                                                                           'type': 'CONSTANT', 'required': True,
                                                                           'dataType': 'LITERAL'}],
                                        'output': [{'name': 'name', 'description': 'The name of the new data item.'}],
                                        'tags': ['EVENT', 'JUPYTER']})

    def __init__(self, name=None, sources=None, expression=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.name = name
        self.expression = expression
        self.sources = sources

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None
        sources_not_in_column = df.index.names
        df = df.reset_index()

        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('new_column_expression=%s' % str(expr))

        df[self.name] = eval(expr)

        df = df.set_index(keys=sources_not_in_column)

        return df


class Filter(BaseTransformer):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Filter data by expression.', 'input': [
            {'name': 'sources', 'description': 'Select one or more data items to be used in the expression.',
             'type': 'DATA_ITEM', 'required': True, 'dataType': 'ARRAY',
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "sources", "type": "array",
                            "minItems": 1, "items": {"type": "string"}}}, {'name': 'expression',
                                                                           'description': 'Build the filtering expression by using Python script. To reference a data item, use the format ${DATA_ITEM}.',
                                                                           'type': 'CONSTANT', 'required': True,
                                                                           'dataType': 'LITERAL'},
            {'name': 'filtered_sources',
             'description': 'Data items to be kept when expression is evaluated to be true.', 'type': 'DATA_ITEM',
             'required': True, 'dataType': 'ARRAY',
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "filtered_sources",
                            "type": "array", "minItems": 1, "items": {"type": "string"}}}], 'output': [
            {'name': 'names', 'description': 'The names of the new data items.', 'dataTypeFrom': 'filtered_sources',
             'cardinalityFrom': 'filtered_sources'}], 'tags': ['EVENT', 'JUPYTER']})

    def __init__(self, names=None, filtered_sources=None, sources=None, expression=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if names is not None and isinstance(names, str):
            names = [n.strip() for n in names.split(',') if len(n.strip()) > 0]

        if names is None or not isinstance(names, list):
            raise RuntimeError("argument names must be provided and must be a list")
        if filtered_sources is None or not isinstance(filtered_sources, list) or len(filtered_sources) != len(names):
            raise RuntimeError(
                "argument filtered_sources must be provided and must be a list and of the same length of names")
        if filtered_sources is not None and not set(names).isdisjoint(set(filtered_sources)):
            raise RuntimeError("argument filtered_sources must not have overlapped items with names")
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.names = {}
        for name, source in list(zip(names, filtered_sources)):
            self.names[source] = name
        self.expression = expression
        self.sources = sources
        self.filtered_sources = filtered_sources

    def execute(self, df):
        try:
            c = self._entity_type.get_attributes_dict()
        except Exception:
            c = None

        # Make index levels available as columns
        sources_not_in_column = df.index.names
        df = df.reset_index()

        # remove conflicting column names
        cleaned_names = {}
        for name, new_name in self.names.items():
            if name in df.columns:
                if new_name not in df.columns:
                    cleaned_names[name] = new_name
                else:
                    self.logger.warning('The filter cannot be applied to column %s because the destination column %s '
                                        'already exists in the dataframe. Available columns in the dataframe are %s' % (
                                            name, new_name, list(df.columns)))
            else:
                self.logger.warning('The filter cannot be applied to column %s because this column is not available '
                                    'in the dataframe. Therefore column %s cannot be calculated. Available columns '
                                    'in the dataframe are %s' % (name, new_name, list(df.columns)))

        # execute given expression
        expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
        self.logger.debug('filter_expression=%s' % str(expr))
        mask = eval(expr)

        # copy columns and apply mask
        for name, new_name in self.names.items():
            df[new_name] = df[name].where(mask)

        df.set_index(keys=sources_not_in_column, drop=True, inplace=True)

        return df


class NewColFromSql(BaseTransformer):

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Create new data items by joining SQL query result.', 'input': [
            {'name': 'sql', 'description': 'The SQL query.', 'type': 'CONSTANT', 'required': True,
             'dataType': 'LITERAL'}, {'name': 'index_col',
                                      'description': 'Columns in the SQL query result to be joined (multiple items are comma separated).',
                                      'type': 'CONSTANT', 'required': True, 'dataType': 'ARRAY',
                                      'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#",
                                                     "title": "index_col", "type": "array", "minItems": 1,
                                                     "items": {"type": "string"}}}, {'name': 'parse_dates',
                                                                                     'description': 'Columns in the SQL query result to be parsed as dates (multiple items are comma separated).',
                                                                                     'type': 'CONSTANT',
                                                                                     'required': True,
                                                                                     'dataType': 'ARRAY',
                                                                                     'jsonSchema': {
                                                                                         "$schema": "http://json-schema.org/draft-07/schema#",
                                                                                         "title": "parse_dates",
                                                                                         "type": "array", "minItems": 1,
                                                                                         "items": {"type": "string"}}},
            {'name': 'join_on', 'description': 'Data items to join the query result to.', 'type': 'DATA_ITEM',
             'required': True, 'dataType': 'ARRAY',
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "join_on", "type": "array",
                            "minItems": 1, "items": {"type": "string"}}}], 'output': [
            {'name': 'names', 'description': 'The names of the new data items.'}], 'tags': ['JUPYTER']})

    def __init__(self, names=None, sql=None, index_col=None, parse_dates=None, join_on=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if names is not None and isinstance(names, str):
            names = [n.strip() for n in names.split(',') if len(n.strip()) > 0]
        if index_col is not None and isinstance(index_col, str):
            index_col = [n.strip() for n in index_col.split(',') if len(n.strip()) > 0]
        if parse_dates is not None and isinstance(parse_dates, str):
            parse_dates = [n.strip() for n in parse_dates.split(',') if len(n.strip()) > 0]

        if names is None or not isinstance(names, list):
            raise RuntimeError("argument names must be provided and must be a list")
        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')
        if index_col is None or not isinstance(index_col, list):
            raise RuntimeError('argument index_col must be provided and must be a list')
        if join_on is None:
            raise RuntimeError('argument join_on must be given')
        if parse_dates is not None and not isinstance(parse_dates, list):
            raise RuntimeError('argument parse_dates must be a list')

        self.names = names
        self.sql = sql
        self.index_col = index_col
        self.parse_dates = parse_dates
        self.join_on = asList(join_on)

    def execute(self, df):
        df_sql = self._get_dms().db.read_sql_query(self.sql, index_col=self.index_col, parse_dates=self.parse_dates)

        if len(self.names) > len(df_sql.columns):
            raise RuntimeError(
                'length of names (%d) is larger than the length of query result (%d)' % (len(self.names), len(df_sql)))

        # in case the join_on is in index, reset first then set back after join
        sources_not_in_column = df.index.names
        df = df.reset_index()
        df = df.merge(df_sql, left_on=self.join_on, right_index=True, how='left')
        df = df.set_index(keys=sources_not_in_column)

        renamed_cols = {df_sql.columns[idx]: name for idx, name in enumerate(self.names)}
        df = df.rename(columns=renamed_cols)

        return df


class NewColFromScalarSql(BaseTransformer):

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create a new data item from a scalar SQL query returning a single value.', 'input': [
                {'name': 'sql', 'description': 'The SQL query.', 'type': 'CONSTANT', 'required': True,
                 'dataType': 'LITERAL'}], 'output': [{'name': 'name', 'description': 'The name of the new data item.'}],
            'tags': ['JUPYTER']})

    def __init__(self, name=None, sql=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError('argument name must be given')
        if sql is None or not isinstance(sql, str) or len(sql) == 0:
            raise RuntimeError('argument sql must be given as a non-empty string')

        self.name = name
        self.sql = sql

    def execute(self, df):
        df_sql = self._get_dms().db.read_sql_query(self.sql)
        if df_sql.shape != (1, 1):
            raise RuntimeError(
                'the scalar sql=%s does not return single value, but the shape=%s' % (len(self.sql), len(df_sql.shape)))
        df[self.name] = df_sql.iloc[0, 0]
        return df


class Shift(BaseTransformer):
    def __init__(self, name, start, end, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        self.name = name
        self.ranges = [start, end]
        self.cross_day_to_next = cross_day_to_next
        self.cross_day = (start > end)
        if self.cross_day:
            self.ranges.insert(1, dt.time(0, 0, 0))
            self.ranges.insert(1, dt.time(23, 59, 59, 999999))
        self.ranges = list(pairwise(self.ranges))

    def within(self, datetime):
        if isinstance(datetime, dt.datetime):
            date = dt.date(datetime.year, datetime.month, datetime.day)
            time = dt.time(datetime.hour, datetime.minute, datetime.second, datetime.microsecond)
        elif isinstance(datetime, dt.time):
            date = None
            time = datetime
        else:
            logger.debug('unknown datetime value type::%s' % datetime)
            raise ValueError('unknown datetime value type')

        for idx, range in enumerate(self.ranges):
            if range[0] <= time and time < range[1]:
                if self.cross_day and date is not None:
                    if self.cross_day_to_next and idx == 0:
                        date += dt.timedelta(days=1)
                    elif not self.cross_day_to_next and idx == 1:
                        date -= dt.timedelta(days=1)
                return (date, True)

        return False

    def start_time(self, shift_day=None):
        if shift_day is None:
            return self.ranges[0][0]
        else:
            if self.cross_day and self.cross_day_to_next:
                shift_day -= dt.timedelta(days=1)
            return dt.datetime.combine(shift_day, self.ranges[0][0])

    def end_time(self, shift_day=None):
        if shift_day is None:
            return self.ranges[-1][-1]
        else:
            if self.cross_day and not self.cross_day_to_next:
                shift_day += dt.timedelta(days=1)
            return dt.datetime.combine(shift_day, self.ranges[-1][-1])

    def __eq__(self, other):
        return self.name == other.name and self.ranges == other.ranges

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "%s: (%s, %s)" % (self.name, self.ranges[0][0], self.ranges[-1][1])


class ShiftPlan(BaseTransformer):
    def __init__(self, shifts, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        shifts = {shift: [dt.time(*tuple(time)) for time in list(pairwise(time_range))] for shift, time_range in
                  shifts.items()}

        # validation: shifts cannot overlap, gaps are allowed though

        self.shifts = []
        for shift, time_range in shifts.items():
            self.shifts.append(Shift(shift, time_range[0], time_range[1], cross_day_to_next=cross_day_to_next))

        self.shifts.sort(key=lambda x: x.ranges[0][0])

        if cross_day_to_next and self.shifts[-1].cross_day:
            self.shifts.insert(0, self.shifts[-1])
            del self.shifts[-1]
        self.cross_day_to_next = cross_day_to_next

        self.logger.debug("ShiftPlan: shifts=%s, cross_day_to_next=%s" % (self.shifts, self.cross_day_to_next))

    def get_shift(self, datetime):
        for shift in self.shifts:
            ret = shift.within(datetime)
            if ret:
                return (ret[0], shift)

        return None

    def next_shift(self, shift_day, shift):
        shift_idx = None

        for idx, shft in enumerate(self.shifts):
            if shift == shft:
                shift_idx = idx
                break

        if shift_idx is None:
            logger.debug("unknown shift: %s" % str(shift))
            raise ValueError("unknown shift: %s" % str(shift))

        shift_idx = shift_idx + 1
        if shift_idx >= len(self.shifts):
            shift_idx %= len(self.shifts)
            shift_day += dt.timedelta(days=1)

        return (shift_day, self.shifts[shift_idx])

    def get_real_datetime(self, shift_day, shift, time):
        if shift.cross_day == False:
            return dt.datetime.combine(shift_day, time)

        if self.cross_day_to_next and time > shift.ranges[-1][-1]:
            # cross day shift the part before midnight
            return dt.datetime.combine(shift_day - dt.timedelta(days=1), time)
        elif self.cross_day_to_next == False and time < shift.ranges[0][0]:
            # cross day shift the part after midnight
            return dt.datetime.combine(shift_day + dt.timedelta(days=1), time)
        else:
            return dt.datetime.combine(shift_day, time)

    def split(self, start, end):
        start_shift = self.get_shift(start)
        end_shift = self.get_shift(end)

        if start_shift is None:
            raise ValueError("starting time not fit in any shift: start_shift is None")
        if end_shift is None:
            raise ValueError("ending time not fit in any shift: end_shift is None")

        if start > end:
            logger.warning('starting time must not be after ending time %s %s. Ignoring end date.' % (start, end))
            return [(start_shift, start, start)]

        if start_shift == end_shift:
            return [(start_shift, start, end)]

        splits = []
        shift_day, shift = start_shift
        splits.append((start_shift, start, self.get_real_datetime(shift_day, shift, shift.ranges[-1][-1])))
        start_shift = self.next_shift(shift_day, shift)
        while start_shift != end_shift:
            shift_day, shift = start_shift
            splits.append((start_shift, self.get_real_datetime(shift_day, shift, shift.ranges[0][0]),
                           self.get_real_datetime(shift_day, shift, shift.ranges[-1][-1])))
            start_shift = self.next_shift(shift_day, shift)
        shift_day, shift = end_shift
        splits.append((end_shift, self.get_real_datetime(shift_day, shift, shift.ranges[0][0]), end))

        return splits

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.shifts)


class IdentifyShiftFromTimestamp(BaseTransformer):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the shift that was active when data was received by using the timestamp on the data.',
            'input': [{'name': 'timestamp',
                       'description': 'Specify the timestamp data item on which to base your calculation.',
                       'type': 'DATA_ITEM', 'required': True, 'dataType': 'TIMESTAMP'}, {'name': 'shifts',
                                                                                         'description': 'Specify the shift plan in JSON syntax. For example, {"1": [7, 30, 16, 30]} Where 1 is the shift ID, 7 is the start hour, 30 is the start minutes, 16 is the end hour, and 30 is the end minutes. You can enter multiple shifts separated by commas.',
                                                                                         'type': 'CONSTANT',
                                                                                         'required': True,
                                                                                         'dataType': 'JSON'},
                      {'name': 'cross_day_to_next',
                       'description': 'If a shift extends past midnight, count it as the first shift of the next calendar day.',
                       'type': 'CONSTANT', 'required': False, 'dataType': 'BOOLEAN'}], 'output': [{'name': 'shift_day',
                                                                                                   'description': 'The staring timestamp of a day, as identified by the timestamp and the shift plan.',
                                                                                                   'dataType': 'TIMESTAMP',
                                                                                                   'tags': [
                                                                                                       'DIMENSION']},
                                                                                                  {'name': 'shift_id',
                                                                                                   'description': 'The shift ID, as identified by the timestamp and the shift plan.',
                                                                                                   'dataType': 'LITERAL',
                                                                                                   'tags': [
                                                                                                       'DIMENSION']}, {
                                                                                                      'name': 'shift_start',
                                                                                                      'description': 'The starting time of the shift, as identified by the timestamp and the shift plan.',
                                                                                                      'dataType': 'TIMESTAMP',
                                                                                                      'tags': [
                                                                                                          'DIMENSION']},
                                                                                                  {'name': 'shift_end',
                                                                                                   'description': 'The ending time of the shift, as identified by the timestamp and the shift plan.',
                                                                                                   'dataType': 'TIMESTAMP',
                                                                                                   'tags': [
                                                                                                       'DIMENSION']},
                                                                                                  {'name': 'hour_no',
                                                                                                   'description': 'The hour of the day, as identified by the timestamp and the shift plan.',
                                                                                                   'dataType': 'NUMBER',
                                                                                                   'tags': [
                                                                                                       'DIMENSION']}],
            'tags': ['JUPYTER']})

    def __init__(self, shift_day=None, shift_id=None, shift_start=None, shift_end=None, hour_no=None,
                 timestamp="timestamp", shifts=None, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if shift_day is None:
            raise RuntimeError("argument shift_day must be provided: shift_day is None")
        if shift_id is None:
            raise RuntimeError("argument shift_id must be provided: shift_id is None")
        if shift_start is not None and not isinstance(shift_start, str):
            raise RuntimeError("argument shift_start must be a string")
        if shift_end is not None and not isinstance(shift_end, str):
            raise RuntimeError("argument shift_end must be a string")
        if hour_no is not None and not isinstance(hour_no, str):
            raise RuntimeError("argument hour_no must be a string")
        if timestamp is None:
            raise RuntimeError("argument timestamp must be provided: timestamp is None")
        if shifts is None or not isinstance(shifts, dict) and len(shifts) > 0:
            raise RuntimeError("argument shifts must be provided and is a non-empty dict")

        self.shift_day = shift_day
        self.shift_id = shift_id
        self.shift_start = shift_start if shift_start is not None and len(shift_start.strip()) > 0 else None
        self.shift_end = shift_end if shift_end is not None and len(shift_end.strip()) > 0 else None
        self.hour_no = hour_no if hour_no is not None and len(hour_no.strip()) > 0 else None
        self.timestamp = timestamp

        self.shifts = ShiftPlan(shifts, cross_day_to_next=cross_day_to_next)

    def execute(self, df):
        generated_values = {self.shift_day: [], self.shift_id: [], 'self.shift_start': [], 'self.shift_end': [],
                            'self.hour_no': [], }
        df[self.shift_day] = self.shift_day
        df[self.shift_id] = self.shift_id
        if self.shift_start is not None:
            df[self.shift_start] = self.shift_start
        if self.shift_end is not None:
            df[self.shift_end] = self.shift_end
        if self.hour_no is not None:
            df[self.hour_no] = self.hour_no

        timestampIndex = df.index.names.index(self.timestamp) if self.timestamp in df.index.names else None
        if timestampIndex is not None:
            # Timestamp is a index level
            for idx in df.index:
                t = idx[timestampIndex]
                if isinstance(t, str):
                    t = pd.to_datetime(t)

                ret = self.shifts.get_shift(t)
                if ret is None:
                    continue

                shift_day, shift = ret

                generated_values[self.shift_day].append(
                    pd.Timestamp(year=shift_day.year, month=shift_day.month, day=shift_day.day))
                generated_values[self.shift_id].append(shift.name)
                generated_values['self.shift_start'].append(shift.start_time(shift_day))
                generated_values['self.shift_end'].append(shift.end_time(shift_day))
                generated_values['self.hour_no'].append(t.hour)
        else:
            # Timestamp is a column
            for idx, value in df[self.timestamp].items():
                t = value
                if isinstance(t, str):
                    t = pd.to_datetime(t)

                ret = self.shifts.get_shift(t)
                if ret is None:
                    continue

                shift_day, shift = ret

                generated_values[self.shift_day].append(
                    pd.Timestamp(year=shift_day.year, month=shift_day.month, day=shift_day.day))
                generated_values[self.shift_id].append(shift.name)
                generated_values['self.shift_start'].append(shift.start_time(shift_day))
                generated_values['self.shift_end'].append(shift.end_time(shift_day))
                generated_values['self.hour_no'].append(t.hour)

        df[self.shift_day] = generated_values[self.shift_day]
        df[self.shift_id] = generated_values[self.shift_id]
        if self.shift_start is not None:
            df[self.shift_start] = generated_values['self.shift_start']
        if self.shift_end is not None:
            df[self.shift_end] = generated_values['self.shift_end']
        if self.hour_no is not None:
            df[self.hour_no] = generated_values['self.hour_no']

        return df


class SplitDataByActiveShifts(BaseTransformer):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the shift that was active when data was received by using the timestamp on the data.',
            'input': [{'name': 'start_timestamp',
                       'description': 'Specify the timestamp data item on which the data to be split must be based.',
                       'type': 'DATA_ITEM', 'required': True, 'dataType': 'TIMESTAMP'}, {'name': 'end_timestamp',
                                                                                         'description': 'Specify the timestamp data item on which the data to be split must be based.',
                                                                                         'type': 'DATA_ITEM',
                                                                                         'required': True,
                                                                                         'dataType': 'TIMESTAMP'},
                      {'name': 'shifts',
                       'description': 'Specify the shift plan in JSON syntax. For example, {"1": [7, 30, 16, 30]} Where 1 is the shift ID, 7 is the start hour, 30 is the start minutes, 16 is the end hour, and 30 is the end minutes. You can enter multiple shifts separated by commas.',
                       'type': 'CONSTANT', 'required': True, 'dataType': 'JSON'}, {'name': 'cross_day_to_next',
                                                                                   'description': 'If a shift extends past midnight, count it as the first shift of the next calendar day.',
                                                                                   'type': 'CONSTANT',
                                                                                   'required': False,
                                                                                   'dataType': 'BOOLEAN'}], 'output': [
                {'name': 'shift_day',
                 'description': 'The staring timestamp of a day, as identified by the timestamp and the shift plan.',
                 'dataType': 'TIMESTAMP', 'tags': ['DIMENSION']},
                {'name': 'shift_id', 'description': 'The shift ID, as identified by the timestamp and the shift plan.',
                 'dataType': 'LITERAL', 'tags': ['DIMENSION']}, {'name': 'shift_start',
                                                                 'description': 'The starting time of the shift, as identified by the timestamp and the shift plan.',
                                                                 'dataType': 'TIMESTAMP', 'tags': ['DIMENSION']},
                {'name': 'shift_end',
                 'description': 'The ending time of the shift, as identified by the timestamp and the shift plan.',
                 'dataType': 'TIMESTAMP', 'tags': ['DIMENSION']}], 'tags': ['JUPYTER']})

    def __init__(self, start_timestamp, end_timestamp, ids='id', shift_day=None, shift_id=None, shift_start=None,
                 shift_end=None, shifts=None, cross_day_to_next=True):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if ids is None:
            raise RuntimeError("argument ids must be provided")
        if start_timestamp is None:
            raise RuntimeError("argument start_timestamp must be provided")
        if end_timestamp is None:
            raise RuntimeError("argument end_timestamp must be provided")
        if shift_day is None:
            raise RuntimeError("argument shift_day must be provided")
        if shift_id is None:
            raise RuntimeError("argument shift_id must be provided")
        if shift_start is not None and not isinstance(shift_start, str):
            raise RuntimeError("argument shift_start must be a string")
        if shift_end is not None and not isinstance(shift_end, str):
            raise RuntimeError("argument shift_end must be a string")
        if shifts is None or not isinstance(shifts, dict) and len(shifts) > 0:
            raise RuntimeError("argument shifts must be provided and is a non-empty dict")

        self.ids = ids
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.shift_day = shift_day
        self.shift_id = shift_id
        self.shift_start = shift_start if shift_start is not None and len(shift_start.strip()) > 0 else None
        self.shift_end = shift_end if shift_end is not None and len(shift_end.strip()) > 0 else None

        self.shifts = ShiftPlan(shifts, cross_day_to_next=cross_day_to_next)

    def execute(self, df):
        generated_rows = []
        generated_values = {self.shift_day: [], self.shift_id: [], 'self.shift_start': [], 'self.shift_end': [], }
        append_generated_values = {self.shift_day: [], self.shift_id: [], 'self.shift_start': [],
                                   'self.shift_end': [], }
        df[self.shift_day] = self.shift_day
        df[self.shift_id] = self.shift_id
        if self.shift_start is not None:
            df[self.shift_start] = self.shift_start
        if self.shift_end is not None:
            df[self.shift_end] = self.shift_end

        # self.logger.debug('df_index_before_move=%s' % str(df.index.to_frame().dtypes.to_dict()))
        indexes_moved_to_columns = df.index.names
        df = df.reset_index()
        # self.logger.debug('df_index_after_move=%s, df_columns=%s' % (str(df.index.to_frame().dtypes.to_dict()), str(df.dtypes.to_dict())))

        # Remember positions of columns in dataframe (position starts with 1 because df_row will contain index of
        # dataframe at position 0)
        position_column = {}
        for pos, col_name in enumerate(df.columns, 1):
            position_column[col_name] = pos

        cnt = 0
        cnt2 = 0
        for df_row in df.itertuples(index=True, name=None):
            idx = df_row[0]
            if cnt % 1000 == 0:
                self.logger.debug('%d rows processed, %d rows added' % (cnt, cnt2))

            cnt += 1
            row_start_timestamp = df_row[position_column[self.start_timestamp]]
            row_end_timestamp = df_row[position_column[self.end_timestamp]]
            if pd.notna(row_start_timestamp) and pd.notna(row_end_timestamp):
                result_rows = self.shifts.split(pd.to_datetime(row_start_timestamp), pd.to_datetime(row_end_timestamp))
            elif pd.notna(row_start_timestamp):
                shift_day, shift = self.shifts.get_shift(pd.to_datetime(row_start_timestamp))
                generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                generated_values[self.shift_id].append(shift.name)
                generated_values['self.shift_start'].append(shift.start_time(shift_day))
                generated_values['self.shift_end'].append(shift.end_time(shift_day))
                continue
            else:
                generated_values[self.shift_day].append(None)
                generated_values[self.shift_id].append(None)
                generated_values['self.shift_start'].append(None)
                generated_values['self.shift_end'].append(None)
                continue
            # self.logger.debug(result_rows)

            for i, result_row in enumerate(result_rows):
                shift_day, shift = result_row[0]
                start_timestamp = result_row[1]
                end_timestamp = result_row[2]

                if i == 0:
                    # accessing original row must not be through the itterrows's row since that's a copy
                    # but accessing by loc slicing is really slow, so we only do it when needed, and it is
                    # assumed cross-shift is relatively rare
                    if len(result_rows) > 1:
                        df.loc[idx, self.start_timestamp] = start_timestamp
                        df.loc[idx, self.end_timestamp] = end_timestamp

                    generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                    generated_values[self.shift_id].append(shift.name)
                    generated_values['self.shift_start'].append(shift.start_time(shift_day))
                    generated_values['self.shift_end'].append(shift.end_time(shift_day))
                else:
                    cnt2 += 1
                    new_row = pd.Series(df_row[1:], index=df.columns)
                    new_row[self.start_timestamp] = start_timestamp
                    new_row[self.end_timestamp] = end_timestamp
                    generated_rows.append(new_row)

                    append_generated_values[self.shift_day].append(pd.to_datetime(shift_day.strftime('%Y-%m-%d')))
                    append_generated_values[self.shift_id].append(shift.name)
                    append_generated_values['self.shift_start'].append(shift.start_time(shift_day))
                    append_generated_values['self.shift_end'].append(shift.end_time(shift_day))

        self.logger.debug('original_rows=%d, rows_added=%d' % (cnt, cnt2))
        if len(generated_rows) > 0:
            # self.logger.debug('df_shape=%s' % str(df.shape))
            df = df.append(generated_rows, ignore_index=True)
            self.logger.debug('df_shape=%s' % str(df.shape))

            generated_values[self.shift_day].extend(append_generated_values[self.shift_day])
            generated_values[self.shift_id].extend(append_generated_values[self.shift_id])
            generated_values['self.shift_start'].extend(append_generated_values['self.shift_start'])
            generated_values['self.shift_end'].extend(append_generated_values['self.shift_end'])

        self.logger.debug('length_generated_values=%s, length_generated_rows=%s' % (
            len(generated_values[self.shift_day]), len(generated_rows)))

        df[self.shift_day] = generated_values[self.shift_day]
        df[self.shift_id] = generated_values[self.shift_id]
        if self.shift_start is not None:
            df[self.shift_start] = generated_values['self.shift_start']
        if self.shift_end is not None:
            df[self.shift_end] = generated_values['self.shift_end']

        df = df.set_index(keys=indexes_moved_to_columns, drop=True, append=False)

        return df


class MergeByFirstValid(BaseTransformer):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create alerts that are triggered when data values reach a particular range.', 'input': [
                {'name': 'sources', 'description': 'Select one or more data items to be merged.', 'type': 'DATA_ITEM',
                 'required': True, 'dataType': 'ARRAY',
                 'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "sources",
                                "type": "array", "minItems": 1, "items": {"type": "string"}}}], 'output': [
                {'name': 'name', 'description': 'The new data item name for the merge result to create.',
                 'dataTypeFrom': 'sources'}], 'tags': ['EVENT', 'JUPYTER']})

    def __init__(self, name=None, sources=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if name is None or not isinstance(name, str):
            raise RuntimeError("argument name must be provided and must be a string")

        self.name = name
        self.sources = sources

    def execute(self, df):
        sources_not_in_column = df.index.names
        df = df.reset_index()

        df[self.name] = df[self.sources].bfill(axis=1).iloc[:, 0]
        msg = 'MergeByFirstValid %s' % df[self.name].unique()[0:50]
        self.logger.debug(msg)

        msg = 'Null merge key: %s' % df[df[self.name].isna()].head(1).transpose()
        self.logger.debug(msg)

        # move back index
        df = df.set_index(keys=sources_not_in_column)

        return df


class InvokeWMLModel(BaseTransformer):
    '''
    Pass multivariate data in input_items to a regression function deployed to
    Watson Machine Learning. The results are passed back to the univariate
    output_items column.
    Credentials for the WML endpoint representing the deployed function are stored
    as pipeline constants, a name to lookup the WML credentials as JSON document.
    Example: 'my_deployed_endpoint_wml_credentials' referring to
    {
	    "apikey": "<my api key",
	    "url": "https://us-south.ml.cloud.ibm.com",
	    "space_id": "<my space id>",
	    "deployment_id": "<my deployment id">
    }
    This name is passed to InvokeWMLModel in wml_auth.
    '''
    def __init__(self, input_items, wml_auth, output_items):
        super().__init__()

        logger.debug(input_items)

        self.whoami = 'InvokeWMLModel'

        self.input_items = input_items

        if isinstance(output_items, str):
            self.output_items = [output_items]    # regression
        else:
            self.output_items = output_items      # classification

        self.wml_auth = wml_auth

        self.deployment_id = None
        self.apikey = None
        self.wml_endpoint = None
        self.space_id = None

        self.client = None

        self.logged_on = False


    def __str__(self):
        out = self.__class__.__name__
        try:
            out = out + 'Input: ' + str(self.input_items) + '\n'
            out = out + 'Output: ' + str(self.output_items) + '\n'

            if self.wml_auth is not None:
                out = out + 'WML auth: ' + str(self.wml_auth) + '\n'
            else:
                #out = out + 'APIKey: ' + str(self.apikey) + '\n'
                out = out + 'WML endpoint: ' + str(self.wml_endpoint) + '\n'
                out = out + 'WML space id: ' + str(self.space_id) + '\n'
                out = out + 'WML deployment id: ' + str(self.deployment_id) + '\n'
        except Exception:
            pass
        return out


    def login(self):

        # only do it once
        if self.logged_on:
            return

        # retrieve WML credentials as constant
        #    {"apikey": api_key, "url": 'https://' + location + '.ml.cloud.ibm.com'}
        c = None
        if isinstance(self.wml_auth, dict):
            wml_credentials = self.wml_auth
        elif self.wml_auth is not None:
            try:
                c = self._entity_type.get_attributes_dict()
            except Exception:
                c = None
            try:
                wml_credentials = c[self.wml_auth]
            except Exception as ae:
                raise RuntimeError("No WML credentials specified")
        else:
            wml_credentials = {'apikey': self.apikey , 'url': self.wml_endpoint, 'space_id': self.space_id}

        try:
            self.deployment_id = wml_credentials['deployment_id']
            self.space_id = wml_credentials['space_id']
            logger.info('Found credentials for WML')
        except Exception as ae:
            raise RuntimeError("No valid WML credentials specified")

        # get client and check credentials
        self.client = APIClient(wml_credentials)
        if self.client is None:
            #logger.error('WML API Key invalid')
            raise RuntimeError("WML API Key invalid")

        # set space
        self.client.set.default_space(wml_credentials['space_id'])

        # check deployment
        deployment_details = self.client.deployments.get_details(self.deployment_id, 1)
        # ToDo - test return and error msg
        logger.debug('Deployment Details check results in ' + str(deployment_details))

        self.logged_on = True


    def execute(self, df):

        logger.info('InvokeWML exec')

        # Create missing columns before doing group-apply
        df = df.copy().fillna('')
        missing_cols = [x for x in (self.output_items) if x not in df.columns]
        for m in missing_cols:
            df[m] = None

        self.login()

        return super().execute(df)


    def _calc(self, df):

        if len(self.input_items) >= 1:
            index_nans = df[df[self.input_items].isna().any(axis=1)].index
            rows = df.loc[~df.index.isin(index_nans), self.input_items].values.tolist()
            scoring_payload = {
                'input_data': [{
                    'fields': self.input_items,
                    'values': rows}]
            }
        else:
            logging.error("no input columns provided, forwarding all")
            return df

        results = self.client.deployments.score(self.deployment_id, scoring_payload)

        if results:
            # Regression
            if len(self.output_items) == 1:
                df.loc[~df.index.isin(index_nans), self.output_items] = \
                    np.array(results['predictions'][0]['values']).flatten()
            # Classification
            else:
                arr = np.array(results['predictions'][0]['values'])
                df.loc[~df.index.isin(index_nans), self.output_items[0]] = arr[:,0].astype(int)
                arr2 = np.array(arr[:,1].tolist())
                df.loc[~df.index.isin(index_nans), self.output_items[1]] = arr2.T[0]

        else:
            logging.error('error invoking external model')

        return df


    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'input_items', datatype=float,
                                  description = "Data items adjust", is_output_datatype_derived = True))
        inputs.append(UISingle(name='wml_auth', datatype=str,
                               description='Endpoint to WML service where model is hosted', tags=['TEXT'], required=True))

        # define arguments that behave as function outputs
        outputs=[]
        outputs.append(UISingle(name='output_items', datatype=float))
        return (inputs, outputs)


class InvokeWMLClassifier(InvokeWMLModel):
    '''
    Pass multivariate data in input_items to a classification function deployed to
    Watson Machine Learning. The results are passed back to the univariate
    output_items column.
    Credentials for the WML endpoint representing the deployed function are stored
    as pipeline constants, a name to lookup the WML credentials as JSON document.
    Example: 'my_deployed_endpoint_wml_credentials' referring to
    {
	    "apikey": "<my api key",
	    "url": "https://us-south.ml.cloud.ibm.com",
	    "space_id": "<my space id>",
	    "deployment_id": "<my deployment id">
    }
    This name is passed to InvokeWMLModel in wml_auth.
    '''
    def __init__(self, input_items, wml_auth, output_items, confidence):
        super().__init__(input_items, wml_auth, [output_items, confidence])

        self.whoami = 'InvokeWMLClassifier'


    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(UIMultiItem(name = 'input_items', datatype=float,
                                  description = "Data items adjust", is_output_datatype_derived = True))
        inputs.append(UISingle(name='wml_auth', datatype=str,
                               description='Endpoint to WML service where model is hosted', tags=['TEXT'], required=True))

        # define arguments that behave as function outputs
        outputs=[]
        outputs.append(UISingle(name='output_items', datatype=float))
        outputs.append(UISingle(name='confidence', datatype=float))
        return (inputs, outputs)


def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)
