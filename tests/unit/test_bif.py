# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import pytest
from unittest.mock import MagicMock, patch
import os
import json
import pandas as pd
import numpy as np

from iotfunctions import bif
from iotfunctions import metadata

def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )

EXPRESSION = "df['col1'] > 3"

# object for mocking
class Object():
    pass

@pytest.fixture()
def empty_df():
    return pd.DataFrame()

@pytest.fixture()
def sample_df():
    return func_sample_df()

def func_sample_df():
    d = {'col1': [1, 3, 4], 'col2': [1, 4, 2], 'evt_timestamp': ["2019-09-26 18:08:11.262975", "2019-09-26 18:08:11.262975", "2019-09-26 18:08:11.262975"], "id": [1, 2, 3]}
    df = pd.DataFrame(d)
    # TODO: make 'evt_timestamp' and 'id' into a multiindex (similar to pipeline)
    return df


class TestAlertExpression():
    ALERT_NAME = "test_alert"

    params = {
        "test_init": [dict(expression=EXPRESSION, alert_name=ALERT_NAME)],
        "test_calc": [{}],
        "test_execute": [dict(expression=EXPRESSION, text_log="Expression (df['col1'] > 3). "),
                         dict(expression="df['col2'] < 3", text_log="Expression (df['col2'] < 3). ")]
    }

    def test_init(self, expression, alert_name):
        alert = bif.AlertExpression(expression=expression, alert_name=alert_name)
        assert alert.expression == expression
        assert alert.alert_name == alert_name

    def test_calc(self, sample_df):
        '''
        unused function, shouldn't do anything
        '''
        alert_expression = bif.AlertExpression(expression=EXPRESSION, alert_name=self.ALERT_NAME)
        same_df = alert_expression._calc(sample_df)
        assert sample_df.equals(sample_df)

    def test_execute(self, sample_df, expression, text_log):
        alert_expression = bif.AlertExpression(expression=expression, alert_name=self.ALERT_NAME)
        alert_expression._entity_type = MagicMock()
        sample_df_copy = sample_df.copy()
        alert_expression.trace_append = MagicMock(return_value=None)

        df = alert_expression.execute(sample_df)

        assert alert_expression._entity_type.get_attributes_dict.call_count == 1
        assert self.ALERT_NAME in list(df.columns)
        assert sample_df_copy.equals(sample_df)
        alert_expression.trace_append.assert_called_once_with(text_log)
        assert df[self.ALERT_NAME].fillna(False).equals(eval(expression))

class TestCoalesce():
    params = {
        "test_init": [dict(data_items=["col1", "col2"], output_item="coalesce_output_item", expected_output="coalesce_output_item"),
                      dict(data_items=["col1", "col2"], output_item=None, expected_output="output_item")],
        "test_execute": [dict(data_items=["col1", "col2"], input_df=func_sample_df(), expected_output=[1,3,4]),
                         dict(data_items=["col2", "col1"], input_df=func_sample_df(), expected_output=[1,4,2]),
                         dict(data_items=["col1", "col2"], input_df=pd.DataFrame({'col1': [1, None, 4], 'col2': [1, 4, 2],  'evt_timestamp': ["2019-09-26 18:08:11.262975", "2019-09-26 18:08:11.262975", "2019-09-26 18:08:11.262975"], "id": [1, 2, 3]}), expected_output=[1,4,4])]
    }

    def test_init(self, data_items, output_item, expected_output):
        coalesce = bif.Coalesce(data_items=data_items, output_item=output_item)
        assert coalesce.data_items == data_items
        assert coalesce.output_item == expected_output

    def test_execute(self, input_df, data_items, expected_output):
        coalesce = bif.Coalesce(data_items=data_items, output_item="coalesce_output_item")

        df = coalesce.execute(input_df)

        assert coalesce.output_item in list(df.columns)
        assert df[coalesce.output_item].array == expected_output

class TestDateDifference():
    params = {
        "test_init": [dict(date_1="evt_timestamp_dim", date_2="evt_timestamp_dim", num_days="dd_num_days", expected_num_days="dd_num_days"),
                      dict(date_1="evt_timestamp_dim", date_2="evt_timestamp_dim", num_days=None, expected_num_days="num_days")],
        "test_execute": [{}]
    }
    def test_init(self, date_1, date_2, num_days, expected_num_days):
        dd = bif.DateDifference(date_1=date_1, date_2=date_2, num_days=num_days)
        assert dd.date_1 == date_1
        assert dd.date_2 == date_2
        assert dd.num_days == expected_num_days

    def test_execute(self):
        # TODO
        pass


class TestDeleteInputData():
    params = {
        "test_init": [dict(dummy_items=["col1", "col2"], older_than_days=5, output_item="did_output_item", expected_output="did_output_item"),
                      dict(dummy_items=["col1", "col2"], older_than_days=4, output_item=None, expected_output="output_item")],
        "test_execute": [{}]
    }
    def test_init(self, dummy_items, older_than_days, output_item, expected_output):
        did = bif.DeleteInputData(dummy_items=dummy_items, older_than_days=older_than_days, output_item=output_item)
        assert did.older_than_days == older_than_days
        assert did.dummy_items == dummy_items
        assert did.output_item == expected_output

    def test_execute(self):
        # TODO
        pass

class TestPythonExpression():
    params = {
        "test_init": [{}],
        "test_execute": [dict(expression=EXPRESSION, input_items=['col1'], expected_output=[False, False, True]),
                         dict(expression="df['col1'] < df['col2']", input_items=['col1', 'col2'], expected_output=[False, True, False]),
                         dict(expression="df['col2'] * 0.5", input_items=['col2'], expected_output=[0.5, 2.0, 1.0])]
    }

    @patch('iotfunctions.bif.PythonExpression.parse_expression')
    def test_init(self, mocked_parse_expression):
        test_output_name = "some_output"
        mocked_parse_expression.return_value = EXPRESSION

        py_exp = bif.PythonExpression(expression=EXPRESSION, output_name=test_output_name)

        mocked_parse_expression.assert_called_once_with(EXPRESSION)
        assert py_exp.output_name == test_output_name
        assert py_exp.expression == EXPRESSION
        assert py_exp.constants == ['expression']
        assert py_exp.outputs == ['output_name']


    @patch('iotfunctions.bif.PythonExpression.trace_append')
    @patch('iotfunctions.bif.PythonExpression.get_input_items')
    def test_execute(self, mocked_input_items, mocked_trace_append, expression, sample_df, input_items, expected_output):
        output_name = "test_output_name"
        mocked_input_items.return_value = input_items
        sample_df_copy = sample_df.copy()
        py_exp = bif.PythonExpression(expression=expression, output_name=output_name)
        py_exp._entity_type = MagicMock()

        df = py_exp.execute(sample_df)

        assert py_exp._entity_type.get_attributes_dict.call_count == 1
        assert mocked_trace_append.call_count == 2
        assert output_name in list(df.columns)
        assert sample_df_copy.equals(sample_df)
        assert df[output_name].equals(eval(expression))
        assert df[output_name].array == expected_output

