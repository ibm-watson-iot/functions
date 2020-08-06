# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
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

# empty object for mocks
class Object():
    pass

EXPRESSION = "df['col1'] > 3"
ALERT_NAME = "test_alert"

@pytest.fixture()
def alert_expression():
    expr = bif.AlertExpression(expression=EXPRESSION, alert_name=ALERT_NAME)
    expr._entity_type = Object()
    return expr

@pytest.fixture()
def empty_df():
    return pd.DataFrame()

@pytest.fixture()
def sample_df():
    return func_sample_df()

def func_sample_df():
    d = {'col1': [1, 3, 4], 'col2': [1, 4, 2]}
    return pd.DataFrame(d)


class TestAlertExpression():
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

    def test_calc(self, alert_expression, sample_df):
        '''
        unused function, shouldn't do anything
        '''
        same_df = alert_expression._calc(sample_df)
        assert sample_df.equals(sample_df)

    def test_execute(self, sample_df, expression, text_log):
        alert_expression = bif.AlertExpression(expression=expression, alert_name=ALERT_NAME)
        alert_expression._entity_type = Object()
        sample_df_copy = sample_df.copy()
        alert_expression._entity_type.get_attributes_dict = MagicMock(return_value={})
        alert_expression.trace_append = MagicMock(return_value=None)

        df = alert_expression.execute(sample_df)

        assert ALERT_NAME in list(df.columns)
        assert sample_df_copy.equals(sample_df)
        alert_expression._entity_type.get_attributes_dict.assert_called_once()
        alert_expression.trace_append.assert_called_once_with(text_log)
        assert df[ALERT_NAME].fillna(False).equals(eval(expression))

class TestCoalesce():
    params = {
        "test_init": [dict(data_items=["col1", "col2"], output_item="coalesce_output_item", expected_output="coalesce_output_item"),
                      dict(data_items=["col1", "col2"], output_item=None, expected_output="output_item")],
        "test_execute": [dict(data_items=["col1", "col2"], input_df=func_sample_df(), expected_output=[1,3,4]),
                         dict(data_items=["col2", "col1"], input_df=func_sample_df(), expected_output=[1,4,2]),
                         dict(data_items=["col1", "col2"], input_df=pd.DataFrame({'col1': [1, None, 4], 'col2': [1, 4, 2]}), expected_output=[1,4,4])]
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
        pass

class PythonExpression():
    params = {
        "test_init": [{}],
        "test_execute": [{}]
    }

    @patch('bif.PythonExpression.parse_expression')
    def test_init(self, mocked):
        py_exp = bif.PythonExpression(expression="", output_name="")
        mocked.assert_called_once() 
        pass

    def test_execute(self):
        pass