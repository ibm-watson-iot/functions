# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import re
import logging
from collections import defaultdict

import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_ns_dtype, is_object_dtype
import numpy as np
import ibm_db

import iotfunctions.metadata as md
from iotfunctions.base import (BaseAggregator, BaseFunction)
from iotfunctions.dbhelper import check_sql_injection
from iotfunctions.util import log_data_frame, rollback_to_interval_boundary, UNIQUE_EXTENSION_LABEL, \
    rollforward_to_interval_boundary, find_frequency_from_data_item
from iotfunctions import dbhelper


def asList(x):
    if not isinstance(x, list):
        x = [x]
    return x


def add_simple_aggregator_execute(cls, func_name):
    def fn(self, group):
        return self.execute(group)

    setattr(cls, func_name, fn)
    fn.__name__ = func_name


class Aggregation(BaseFunction):

    """
    Keyword arguments:
    ids -- the names of entity id columns, can be a string or a list of
           string. aggregation is always done first by entities unless
           this is not given (None) and it is not allowed to include any
           of entity id columns in sources
    """
    def __init__(self, dms, ids=None, timestamp=None, granularity=None, columns_with_scope=None, simple_aggregators=None,
                 complex_aggregators=None, direct_aggregators=None):

        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        self.dms = dms

        self.simple_aggregators = simple_aggregators if simple_aggregators is not None else []
        self.complex_aggregators = complex_aggregators if complex_aggregators is not None else []
        self.direct_aggregators = direct_aggregators if direct_aggregators is not None else []

        self.ids = ids
        if self.ids is not None:
           self.ids = asList(self.ids)
        else:
            self.ids = list()

        self.timestamp = timestamp

        self.logger.debug('aggregation_input_granularity=%s' % str(granularity))

        if granularity is not None and not isinstance(granularity, tuple):
            raise RuntimeError('argument granularity must be a tuple')

        self.frequency = None
        self.groupby = None
        self.entityFirst = True
        if granularity is not None:
            self.frequency, self.groupby, self.entityFirst, dummy = granularity

        if self.groupby is None:
            self.groupby = ()

        if self.groupby is not None and not isinstance(self.groupby, tuple):
            raise RuntimeError('argument granularity[1] must be a tuple')

        self.columns_with_scope = columns_with_scope if columns_with_scope is not None else {}

        self.logger.debug(f'aggregation_ids={str(self.ids)}, aggregation_timestamp={str(self.timestamp)}, '
                          f'simple_aggregators={str(self.simple_aggregators)}, '
                          f'complex_aggregators={str(self.complex_aggregators)}, '
                          f'aggregation_direct_aggregators={str(self.direct_aggregators)}, '
                          f'aggregation_frequency={str(self.frequency)}, aggregation_groupby={str(self.groupby)}, '
                          f'aggregation_entityFirst={str(self.entityFirst)}, '
                          f'columns_with_scope={str(columns_with_scope)}')

    def _agg_dict(self, df, simple_aggregators):
        numerics = df.select_dtypes(include=[np.number, bool]).columns.values

        agg_dict = defaultdict(list)
        for srcs, scope_sources, agg, name, scope, filtered_sources in simple_aggregators:

            for src in srcs:
                if src in numerics:
                    agg_dict[src].append(agg)
                else:
                    numeric_only_funcs = {'sum', 'mean', 'std', 'var', 'prod', Sum, Mean, StandardDeviation, Variance,
                                          Product}
                    if agg not in numeric_only_funcs:
                        agg_dict[src].append(agg)

        return agg_dict

    def execute(self, df, start_ts=None, end_ts=None, entities=None, offset=None):

        # Make index of dataframe available as columns
        for index_name in df.index.names:
            if index_name == 'id':
                df['entity_id'] = df.index.get_level_values('id')
            else:
                df[index_name] = df.index.get_level_values(index_name)

        # Gather columns which are used to determine the groups
        group_base = []
        group_base_names = []
        if len(self.ids) > 0 and self.entityFirst:
            group_base.extend(self.ids)
            group_base_names.extend(self.ids)

        if self.timestamp is not None and self.frequency is not None:
            if self.frequency == 'W':
                # 'W' by default use right label and right closed
                if offset is not None:
                    group_base.append(pd.Grouper(level=self.timestamp, freq=self.frequency,
                                                 label='left', closed='left', offset=-offset))
                else:
                    group_base.append(pd.Grouper(level=self.timestamp, freq=self.frequency,
                                                 label='left', closed='left'))
            else:
                # other alias seems to not needing to special handle
                if offset is not None:
                    group_base.append(pd.Grouper(level=self.timestamp, freq=self.frequency, offset=-offset))
                else:
                    group_base.append(pd.Grouper(level=self.timestamp, freq=self.frequency))

            group_base_names.append(self.timestamp)

        if self.groupby is not None and len(self.groupby) > 0:
            group_base.extend(self.groupby)
            group_base_names.extend(self.groupby)

        self.logger.debug(f'group_base={str(group_base)}, '
                          f'group_base_names={str(group_base_names)}')

        # Handle scopes: Duplicate source column and apply the scope to the duplicate
        log_messages = []
        for input_col_name, scope_and_new_col in self.columns_with_scope.items():
            for scope, new_input_col_name in scope_and_new_col:
                df[new_input_col_name] = self._apply_scope(df, input_col_name, scope)
                log_messages.append(f"({input_col_name} --> {new_input_col_name})")

        self.logger.info(f"A scope has been applied on the following columns: {str(log_messages)}")

        # Get list of all input columns (simple and complex aggregator only)
        all_input_col_names = set()
        for aggregators in [self.simple_aggregators, self.complex_aggregators]:
            for func, input_col_names, output_col_names in aggregators:
                all_input_col_names.update(input_col_names)

        self.logger.debug(
            f"The following columns are required for simple and complex aggregators: {str(all_input_col_names)}")

        # Check for missing columns
        missing_col_names = all_input_col_names.difference(df.columns)
        if len(missing_col_names) > 0:
            raise RuntimeError(f"The following columns are required as input for the simple and complex aggregation "
                               f"functions but are missing in the data frame: {str(list(missing_col_names))}. "
                               f"Available columns in data frame: {str(list(df.columns))}")

        # Circumvent issue with pandas' Grouper:
        # Groupers with frequency 'W', 'MS' and 'AS'/'YS' do not take care about hours/minutes/seconds/microseconds
        # /nanoseconds in the timestamps. They leave them unaffected. Therefore, we have to generate the group labels
        # by ourselves and to replace the Groupers by the group labels. This makes any aggregator which deploys
        # group_base on any other dataframe (= data frame with different index) as df fail.
        trouble_makers = {'W-SUN', 'MS', 'AS-JAN'}
        corrected_group_base = []
        for item in group_base:
            if isinstance(item, pd.Grouper):
                freq_string = item.freq.freqstr
                if freq_string in trouble_makers:
                    freq_date_offset = pd.tseries.frequencies.to_offset(freq_string)
                    time_reset = pd.DateOffset(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)

                    # Roll back timestamp to, for example, begin of week, set time to 00:00:00.000000000 (and subtract
                    # offset of timezone) to get timestamp indicating the begin of aggregation interval
                    if offset is not None:
                        group_labels = df.index.get_level_values(item.level).to_series().transform(lambda x: freq_date_offset.rollback(x + offset) + time_reset - offset)
                    else:
                        group_labels = df.index.get_level_values(item.level).to_series().transform(lambda x: freq_date_offset.rollback(x) + time_reset)

                    corrected_group_base.append(pd.DatetimeIndex(group_labels.array, name=group_labels.name))

                else:
                    corrected_group_base.append(item)

            else:
                corrected_group_base.append(item)

        group_base = corrected_group_base

        # Split data frame into groups
        groups = df.groupby(group_base)

        all_dfs = []

        ###############################################################################
        # simple aggregators
        ###############################################################################

        # Define named aggregations
        named_aggregations = {}
        log_messages = []
        for func, input_col_names, output_col_names in self.simple_aggregators:
            for input_col_name, output_col_name in zip(input_col_names, output_col_names):
                named_aggregations[output_col_name] = pd.NamedAgg(column=input_col_name, aggfunc=func)
                log_messages.append(f"(input={input_col_name} --> agg_func={func.__name__}() --> output={output_col_name})")
        self.logger.info(f"input/output relationship for simple aggregators: {str(log_messages)}")

        # Aggregate
        if len(named_aggregations) > 0:
            agg_df_simple = groups.agg(**named_aggregations)
            if agg_df_simple.empty:
                # Corrective action for unexpected behaviour of pandas: The user-defined (from the pandas perspective)
                # aggregation functions like our 'Count' function are never called when the corresponding dataframe is
                # empty. Although this behaviour (which was introduced in pandas 1.5.x) helps to avoid exceptions for
                # aggregation functions which are not designed to handle empty dataframes properly it causes issues
                # for function like 'Count':
                # The Count function returns a result of type 'int64' independent of the input type. This exchange of
                # type does not happen when our count function is never called. Therefore, pandas returns a (empty)
                # result column with the type of the input instead of type 'int64'. By the way, pandas build-in count()
                # function does not show this issue!
                # We take corrective action for all simple aggregators by enforcing the correct result type in case of
                # an empty dataframe

                # Determine the output metrics whose type must be changed. Then assign to the corresponding columns in
                # dataframe a dummy value of the expected type to enforce the conversion of type. This approach is much
                # simpler in case of an empty dataframe than using dataframe.astype() which throws an exception,
                # for example, when a datetime64[ns] is converted to a float64.
                for output_metric_name in agg_df_simple.columns:
                    output_metric = self.dms.data_items.get(output_metric_name)
                    if output_metric is not None:
                        output_metric_type = output_metric.get('columnType')
                        column_type = agg_df_simple[output_metric_name].dtype
                        new_type = None
                        if output_metric_type == "BOOLEAN":
                            if not is_bool_dtype(column_type):
                                new_type = 'boolean'
                                agg_df_simple[output_metric_name] = True
                        elif output_metric_type == "NUMBER":
                            if not is_numeric_dtype(column_type):
                                new_type = 'float64'
                                agg_df_simple[output_metric_name] = 1.0
                        elif output_metric_type == "LITERAL":
                            if not is_string_dtype(column_type):
                                new_type = 'str'
                                agg_df_simple[output_metric_name] = ""
                        elif output_metric_type == "TIMESTAMP":
                            if not is_datetime64_ns_dtype(column_type):
                                new_type = 'datetime64[ns]'
                                agg_df_simple[output_metric_name] = pd.TIMESTAMP(0)
                        elif output_metric_type == "JSON":
                            if not is_object_dtype(column_type):
                                new_type = 'object'
                                agg_df_simple[output_metric_name] = ""
                        else:
                            self.logger.warning(f"The output type could not be enforced for metric {output_metric_name} "
                                                f"because it has an unexpected type {output_metric_type}.")

                        if new_type is not None:
                            self.logger.debug(f"Type was changed from {column_type} to {new_type} for "
                                              f"output metric {output_metric_name}")
                    else:
                        self.logger.warning(f"The output type could not be enforced for metric {output_metric_name} "
                                            f"because it was not found in the list of defined metrics.")

            log_data_frame('Data frame after application of simple aggregators', agg_df_simple)

            all_dfs.append(agg_df_simple)

        ###############################################################################
        # complex aggregators
        ###############################################################################

        for func, input_col_names, output_col_names in self.complex_aggregators:

            self.logger.info('executing complex aggregation function - sources %s' % str(input_col_names))
            self.logger.info('executing complex aggregation function - output %s' % str(output_col_names))
            df_apply = groups.apply(func)

            # Some aggregation functions return None instead of an empty data frame. Therefore, the result of
            # groups.apply() can be an empty data frame without columns and without index when all function calls
            # returned None. We take corrective action and build a new empty dataframe with the expected columns and
            # the expected index including level names
            if df_apply.empty and df_apply.columns.empty:

                # Build empty index with correct level names
                if len(group_base_names) > 1:
                    tmp_array = [[] for i in range(len(group_base_names))]
                    new_index = pd.MultiIndex.from_arrays(tmp_array, names=group_base_names)
                else:
                    new_index = pd.Index([], name=group_base_names[0])

                # Build data frame with index and expected columns
                df_apply = pd.DataFrame([], columns=output_col_names, index=new_index)

                # Cast columns in data frame to the expected type
                column_types = {}
                for name in output_col_names:
                    source_metadata = self.dms.data_items.get(name)

                    if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                        tmp_type = float
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                        tmp_type = bool
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                        tmp_type = 'datetime64[ns]'
                    else:
                        tmp_type = str

                    column_types[name] = tmp_type

                df_apply = df_apply.astype(dtype=column_types, copy=False)

            all_dfs.append(df_apply)

            log_data_frame('func=%s, aggregation_df_apply' % str(func), df_apply)

        ###############################################################################
        # direct aggregators
        ###############################################################################
        for func, input_col_names, output_col_names in self.direct_aggregators:

            self.logger.info('executing direct aggregation function - sources %s' % str(input_col_names))
            self.logger.info('executing direct aggregation function - output %s' % str(output_col_names))

            df_direct = func(df=df, group_base=group_base, group_base_names=group_base_names, start_ts=start_ts, end_ts=end_ts, entities=entities, offset=offset)
            if df_direct.empty and df_direct.columns.empty:
                for name in output_col_names:
                    df_direct[name] = None

                    source_metadata = self.dms.data_items.get(name)
                    if source_metadata is None:
                        continue

                    if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                        df_direct = df_direct.astype({name: float})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                        df_direct = df_direct.astype({name: bool})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                        df_direct = df_direct.astype({name: 'datetime64[ns]'})
                    else:
                        df_direct = df_direct.astype({name: str})

            all_dfs.append(df_direct)

            log_data_frame('func=%s, aggregation_df_direct' % str(func), df_direct)

        ###############################################################################
        # concat all results
        ###############################################################################
        df = pd.concat(all_dfs, axis=1, sort=True)

        # Corrective action: pd.concat() removes name from Index when we only have one level. There is no issue for
        # MultiIndex which is used for two and more levels
        if len(group_base_names) == 1:
            df.index.names = group_base_names

        log_data_frame('aggregation_final_df', df)

        return df

    def _apply_scope(self, df, input_col_name, scope):
        eval_expression = ""
        if scope.get('type') == 'DIMENSIONS':
            self.logger.debug('Applying Dimensions Scope')
            dimension_count = len(scope.get('dimensions'))
            for dimension_filter in scope.get('dimensions'):
                dimension_name = dimension_filter['name']
                dimension_value = dimension_filter['value']
                dimension_count -= 1
                if isinstance(dimension_value, str):
                    dimension_value = [dimension_value]
                else:
                    # Convert to list explicitly to guarantee subsequent 'str(dimension_value)' returns a proper
                    # string. Counter example: str(dict.values()) returns "dict_values([...])"
                    dimension_value = list(dimension_value)
                eval_expression += 'df[\'' + dimension_name + '\'].isin(' + str(dimension_value) + ')'
                eval_expression += ' & ' if dimension_count != 0 else ''
        else:
            self.logger.debug('Applying Expression Scope')
            eval_expression = scope.get('expression')
            if eval_expression is not None and '${' in eval_expression:
                eval_expression = re.sub(r"\$\{(\w+)\}", r"df['\1']", eval_expression)
        self.logger.debug('Final Scope Mask Expression {}'.format(eval_expression))

        scope_mask = eval(eval_expression)

        return df[input_col_name].where(scope_mask)


def _general_aggregator_input():
    return {'name': 'source', 'description': 'Select the data item that you want to use as input for your calculation.',
            'type': 'DATA_ITEM', 'required': True, }.copy()

'''
# How to run

# import stuff in this file
from iotfunctions.aggregate import (Aggregation, add_simple_aggregator_execute)

# choose aggrator function
func = bif.AggregateWithExpression

# set up parameter list
params_dict = {}
params_dict['input_items'] = 'score'  # input column
params_dict['output_items'] = 'score_out'  # output_column
params_dict['expression'] = 'x.max()'

# set name for the function pandas grouper should run
func_name = 'execute_AggregateWithExpression'
# add func_name as alias for the aggregate method
add_simple_aggregator_execute(func, func_name)
# turn func into a closure
func_clos = getattr(func(**params_dict), func_name)

# instatiate an Aggregation object with entity index, timestamp index,
# desired granularity and a (short) chain of aggregators
# granularity is a quadruple with frequency, list of columns to group, boolean whether to go by entity and
#   entity id (which seems to be ignored)
# simple aggregator is a list of triplets with column name, closure as above and a name
agg_fct = Aggregation(None, ids=['entity'], timestamp='timestamp', granularity=('T', ('score',), True, 16623),
                    simple_aggregators=[('score', func_clos, 'AggWithExpr')])

# Execute the aggregator
jobsettings = { 'db': db, '_db_schema': 'public', 'save_trace_to_file' : True}
et = agg_fct._build_entity_type(columns = [Column('score',Float())], **jobsettings)
df_agg = agg_fct.execute(df=df_input)


'''


def _number_aggregator_input():
    input_item = _general_aggregator_input()
    input_item['dataType'] = 'NUMBER'
    return input_item


def _no_datatype_aggregator_output():
    return {'name': 'name',
            'description': 'Enter a name for the data item that is produced as a result of this calculation.'}.copy()


def _general_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataTypeFrom'] = 'source'
    return output_item


def _number_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataType'] = 'NUMBER'
    return output_item


def _generate_metadata(cls, metadata):
    common_metadata = {'name': cls.__name__, 'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
                       'category': 'AGGREGATOR', 'input': [_general_aggregator_input()],
                       'output': [_general_aggregator_output()]}
    common_metadata.update(metadata)
    return common_metadata


class Aggregator(BaseAggregator):
    def __init__(self):
        super().__init__()


class SimpleAggregator(Aggregator):
    is_simple_aggregator = True

    def __init__(self):
        super().__init__()


class ComplexAggregator(Aggregator):
    is_complex_aggregator = True

    def __init__(self):
        super().__init__()

class DirectAggregator(Aggregator):
    is_direct_aggregator = True

    def __init__(self):
        super().__init__()

class Sum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the sum of the values in your data set.',
                                        'input': [_number_aggregator_input(), {'name': 'min_count',
                                                                               'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                               'type': 'CONSTANT', 'required': False,
                                                                               'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()]})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        return group.sum(min_count=self.min_count)


class Minimum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Identifies the minimum value in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.min(skipna=True)


class Maximum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Identifies the maximum value in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.max()


class Mean(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the mean of the values in your data set.',
                                        'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        super().__init__()

    def execute(self, group):
        return group.mean()


class Median(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Indentifies the median of the values in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.median()


class Count(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Counts the number of values in your data set.',
                                        'input': [_general_aggregator_input(), {'name': 'min_count',
                                                                                'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                                'type': 'CONSTANT', 'required': False,
                                                                                'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()], 'tags': ['EVENT']})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        # group is expected to be a pandas' series but in case of an empty dataframe groupby.agg() hands over an empty
        # data frame. This is an erroneous behaviour in pandas, but we have to take it into account: Group.count()
        # returns a series instead of integer when group is a data frame instead of series. When cnt is a series the
        # subsequent cnt >= self.min_count raises an ambiguous truth exception - at least for some panda versions
        # like 1.4.3 but not for 1.3.4 and 1.5.4
        if not group.empty:
            cnt = group.count()
        else:
            cnt = 0
        return cnt if cnt >= self.min_count else None


class DistinctCount(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Counts the number of distinct values in your data set.',
                                        'input': [_general_aggregator_input(), {'name': 'min_count',
                                                                                'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                                'type': 'CONSTANT', 'required': False,
                                                                                'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()], 'tags': ['EVENT']})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        # group is expected to be a pandas' series but in case of an empty dataframe groupby.agg() hands over an empty
        # data frame. This is an erroneous behaviour in pandas, but we have to take it into account: Do not deploy
        # unique() on group when it is a dataframe because unique() does only exist for series.
        if not group.empty:
            cnt = len(group.dropna().unique())
        else:
            cnt = 0
        return cnt if cnt >= self.min_count else None


class StandardDeviation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Calculates the standard deviation of the values in your data set.',
                                   'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        pass

    def execute(self, group):
        return group.std()


class Variance(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the variance of the values in your data set.',
                                        'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        pass

    def execute(self, group):
        return group.var()


class Product(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the product of the values in your data set.',
                                        'input': [_number_aggregator_input(), {'name': 'min_count',
                                                                               'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                               'type': 'CONSTANT', 'required': False,
                                                                               'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()]})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        return group.prod(min_count=self.min_count)


class First(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Identifies the first value in your data set.', 'tags': ['EVENT']})

    def __init__(self):
        pass

    def execute(self, group):

        first = None
        group = group.dropna()

        if not group.empty:
            if isinstance(group, pd.Series):
                first = group.iat[0]
            elif isinstance(group, pd.DataFrame):
                first = group.iat[0, 0]

        return first


class Last(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Identifies the last value in your data set.', 'tags': ['EVENT']})

    def __init__(self):
        pass

    def execute(self, group):

        last = None
        group = group.dropna()

        if not group.empty:
            if isinstance(group, pd.Series):
                last = group.iat[-1]
            elif isinstance(group, pd.DataFrame):
                last = group.iat[-1, 0]

        return last


class AggregateWithCalculation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Create aggregation using expression on a data item.',
                                        'input': [_general_aggregator_input(), {'name': 'expression',
                                                                                'description': 'The expression. Use ${GROUP} to reference the current grain. All Pandas Series methods can be used on the grain. For example, ${GROUP}.max() - ${GROUP}.min().',
                                                                                'type': 'CONSTANT', 'required': True,
                                                                                'dataType': 'LITERAL'}],
                                        'output': [_no_datatype_aggregator_output()], 'tags': ['EVENT', 'JUPYTER']})

    def __init__(self, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.expression = expression

    def execute(self, group):
        return eval(re.sub(r"\${GROUP}", r"group", self.expression))


class MsiOccupancyCount(DirectAggregator):

    KPI_FUNCTION_NAME = "OccupancyCount"

    BACKTRACK_IMPACTING_PARAMETER = "start_of_calculation"

    def __init__(self, raw_occupancy_count, start_of_calculation=None, name=None):
        super().__init__()
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if raw_occupancy_count is not None and len(raw_occupancy_count) > 0:
            self.raw_occupancy_count = raw_occupancy_count
        else:
            raise RuntimeError(f"Function {self.KPI_FUNCTION_NAME} requires the parameter raw_occupancy_count "
                               f"but it is empty: raw_occupancy_count={raw_occupancy_count}")

        if start_of_calculation is not None:
            try:
                self.start_of_calculation = pd.Timestamp(start_of_calculation, unit='ms')
            except Exception as ex:
                raise RuntimeError(f"The optional parameter start_of_calculation of function {self.KPI_FUNCTION_NAME} "
                                   f"cannot be converted to a timestamp: start_of_calculation={start_of_calculation}") from ex
        else:
            self.start_of_calculation = None

        if name is not None and len(name) > 0:
            self.output_name = name
        else:
            raise RuntimeError(f"No name was provided for the metric which is calculated by function {self.KPI_FUNCTION_NAME}: name={name}")

    def execute(self, df, group_base, group_base_names, start_ts=None, end_ts=None, entities=None, offset=None):

        # If entities is None define list of entities by data frame. The list of entities can then be incomplete!
        if entities is None or len(entities) == 0:
            raise RuntimeError(f'The framework does not provide a list of entities to function {self.__class__.__name__} '
                               f'for offset {offset}: entities={entities}')

        if offset is None:
            raise RuntimeError(f'The framework does not provide an offset to function {self.__class__.__name__}.')

        # Schema name and table name of result table for sql statement
        sql_schema_name = check_sql_injection(self.dms.schema)
        sql_quoted_schema_name = dbhelper.quotingSchemaName(sql_schema_name, self.dms.is_postgre_sql)

        sql_output_table_name = check_sql_injection(self.dms.entity_type_obj._data_items.get(self.output_name).get('sourceTableName'))
        sql_quoted_output_table_name = dbhelper.quotingTableName(sql_output_table_name, self.dms.is_postgre_sql)

        # Find data item representing the result of this KPI function
        result_data_item = self.dms.entity_type_obj._data_items.get(self.output_name)

        # Find granularity_set/frequency of this aggregation
        agg_frequency = find_frequency_from_data_item(result_data_item, self.dms.granularities)

        # Find time range covered by this pipeline run (align start and end date with grain boundaries)
        aligned_run_end = rollback_to_interval_boundary(self.dms.launch_date, agg_frequency)
        if self.dms.previous_launch_date is not None:
            aligned_run_start = rollback_to_interval_boundary(self.dms.previous_launch_date, agg_frequency)
        else:
            # No previous execution date available (first pipeline run for this resource type). Fall back to the earliest
            # data event in input data frame
            time_min = df.index.get_level_values(level=group_base_names[1]).min()
            if pd.notna(time_min):
                aligned_run_start = rollback_to_interval_boundary(time_min, agg_frequency)
            else:
                # Empty data frame: Nothing to process, i.e. time range covered by this pipeline is zero
                aligned_run_start = aligned_run_end

        # Find any explicitly defined backtrack for this KPI function
        # backtrack = result_data_item.get('kpiFunctionDto').get('backtrack')
        # if backtrack is not None and len(backtrack) > 0:
        #     backtrack_start = aligned_run_end - pd.tseries.offsets.DateOffset(**backtrack)
        # else:
        #     backtrack_start = None

        # Check if recalculation of OccupancyCount is configured
        if self.start_of_calculation is not None:
            backtrack_start = rollback_to_interval_boundary(self.start_of_calculation + offset, agg_frequency) - offset
        else:
            backtrack_start = None

        # Determine the following:
        #   1) Time range [aligned_cycle_start, aligned_cycle_end]: aligned start and end of this cycle
        #   2) aligned_calc_start: Point in time from which OccupancyCount is supposed to be calculated (it will be aligned with grain boundaries).
        if self.dms.running_with_backtrack:
            # Pipeline runs in BackTrack mode.
            # Hint: start_ts is already aligned with grain boundaries (including offset)
            #       end_ts is aligned with grain boundaries (including offset) except for the last cycle
            aligned_cycle_start = rollback_to_interval_boundary(start_ts + offset, agg_frequency) - offset
            aligned_cycle_end = rollback_to_interval_boundary(end_ts + offset, agg_frequency) - offset
            if backtrack_start is None:
                aligned_calc_start = aligned_run_start
            else:
                aligned_calc_start = backtrack_start

        else:
            # Pipeline runs in CheckPoint mode (There is only one cycle)
            aligned_cycle_start = aligned_run_start
            aligned_cycle_end = aligned_run_end
            aligned_calc_start = aligned_cycle_start

        self.logger.debug(f"aligned_cycle_start = {aligned_cycle_start}, aligned_cycle_end = {aligned_cycle_end}, "
                          f"aligned_calc_start = {aligned_calc_start}, "
                          f"self.dms.running_with_backtrack = {self.dms.running_with_backtrack}, "
                          f"self.start_of_calculation = {self.start_of_calculation}, "
                          f"agg_frequency = {agg_frequency}")

        s_agg_result = None
        entities_placeholders = ', '.join(['?' for x in entities])
        if aligned_cycle_start < aligned_cycle_end:
            # When aligned_calc_start <= aligned_cycle_start then we calculate all OccupancyCount values in this cycle. But
            # when aligned_calc_start > aligned_cycle_start then we **do not** calculate all OccupancyCount values in this
            # cycle. Fetch those non-calculated OccupancyCount values from output table to complete the internal data frame
            # which might be used later on by other KPI functions
            s_missing_result_values = None
            if aligned_calc_start > aligned_cycle_start:
                # Load missing OccupancyCount values from output table (this only happens in BackTrack mode)
                sql_statement = f'SELECT "VALUE_N", "TIMESTAMP", "ENTITY_ID" FROM {sql_quoted_schema_name}.{sql_quoted_output_table_name} ' \
                                f'WHERE "KEY" = ? AND "TIMESTAMP" >= ? AND "TIMESTAMP" < ? AND ' \
                                f'"ENTITY_ID" IN ({entities_placeholders}) ORDER BY "ENTITY_ID", "TIMESTAMP" ASC'

                stmt = ibm_db.prepare(self.dms.db_connection, sql_statement)
                try:
                    ibm_db.bind_param(stmt, 1, self.output_name)
                    ibm_db.bind_param(stmt, 2, aligned_cycle_start)
                    ibm_db.bind_param(stmt, 3, min(aligned_calc_start, aligned_cycle_end))
                    for position, device in enumerate(entities, 4):
                        ibm_db.bind_param(stmt, position, device)
                    ibm_db.execute(stmt)
                    row = ibm_db.fetch_tuple(stmt)
                    result_data = []
                    result_index = []
                    while row is not False:
                        result_data.append(row[0])
                        result_index.append((row[2], row[1]))
                        row = ibm_db.fetch_tuple(stmt)

                    if len(result_data) > 0:
                        s_missing_result_values = pd.Series(data=result_data, index=pd.MultiIndex.from_tuples(tuples=result_index, names=group_base_names), name=self.output_name)
                        self.logger.debug(f"Number of missing results: {len(s_missing_result_values)}")
                    else:
                        self.logger.debug(f"No missing results available.")
                except Exception as ex:
                    raise Exception(
                        f'Retrieval of result values failed with sql statement "{sql_statement}"') from ex
                finally:
                    ibm_db.free_result(stmt)

            # Because we only get a data event when the raw metric changes, but we want to provide values for all time
            # units of the derived metric we have to fetch the latest available OccupancyCount values from
            # input table to fill gaps at the beginning. This step is not required for cycles in which we do not
            # calculate any OccupancyCount values.
            if aligned_calc_start < aligned_cycle_end:
                s_start_result_values = None

                # Read just one result value per device right before aligned_calc_start. Distinguish input data item
                # between raw metric and derived metric:
                input_data_item = self.dms.entity_type_obj._data_items.get(self.raw_occupancy_count)
                if input_data_item.get('type') == 'METRIC':
                    input_data_item_is_raw = True

                    # Raw metric table as input source
                    sql_quoted_input_table_name = dbhelper.quotingTableName(check_sql_injection(self.dms.eventTable),
                                                                            self.dms.is_postgre_sql)
                    sql_quoted_timestamp_col_name = dbhelper.quotingColumnName(check_sql_injection(self.dms.eventTimestampColumn), self.dms.is_postgre_sql)
                    sql_quoted_device_id_col_name = dbhelper.quotingColumnName(check_sql_injection(self.dms.entityIdColumn), self.dms.is_postgre_sql)
                    sql_quoted_input_data_item_col_name = dbhelper.quotingColumnName(check_sql_injection(input_data_item['columnName']), self.dms.is_postgre_sql)

                    sql_statement = f'WITH PREVIOUS_VALUES AS ' \
                                        f'(SELECT {sql_quoted_input_data_item_col_name}, {sql_quoted_device_id_col_name}, ' \
                                        f'ROW_NUMBER() OVER ( PARTITION BY {sql_quoted_device_id_col_name} ORDER BY {sql_quoted_timestamp_col_name} DESC) ROW_NUM ' \
                                        f'FROM {sql_quoted_schema_name}.{sql_quoted_input_table_name} ' \
                                        f'WHERE {sql_quoted_timestamp_col_name} < ? AND {sql_quoted_device_id_col_name} IN ({entities_placeholders})) ' \
                                    f'SELECT {sql_quoted_input_data_item_col_name}, {sql_quoted_device_id_col_name} FROM PREVIOUS_VALUES WHERE ROW_NUM = 1'
                else:
                    input_data_item_is_raw = False

                    # Output table as input source
                    sql_quoted_input_table_name = dbhelper.quotingTableName(check_sql_injection(input_data_item.get('sourceTableName')),
                                                                            self.dms.is_postgre_sql)

                    sql_statement = f'WITH PREVIOUS_VALUES AS ' \
                                        f'(SELECT "VALUE_N", "ENTITY_ID", ' \
                                        f'ROW_NUMBER() OVER ( PARTITION BY "ENTITY_ID" ORDER BY "TIMESTAMP" DESC) ROW_NUM ' \
                                        f'FROM {sql_quoted_schema_name}.{sql_quoted_input_table_name} ' \
                                        f'WHERE "KEY" = ? AND "TIMESTAMP" < ? AND "ENTITY_ID" IN ({entities_placeholders})) ' \
                                    f'SELECT "VALUE_N", "ENTITY_ID" FROM PREVIOUS_VALUES WHERE ROW_NUM = 1'

                stmt = ibm_db.prepare(self.dms.db_connection, sql_statement)
                try:
                    position = 1
                    if input_data_item_is_raw is False:
                        ibm_db.bind_param(stmt, position, self.raw_occupancy_count)
                        position = position + 1
                    ibm_db.bind_param(stmt, position, max(aligned_calc_start, aligned_cycle_start))
                    position = position + 1
                    for pos, device in enumerate(entities, position):
                        ibm_db.bind_param(stmt, pos, device)
                    ibm_db.execute(stmt)
                    row = ibm_db.fetch_tuple(stmt)
                    result_data = []
                    result_index = []
                    while row is not False:
                        result_data.append(row[0] if pd.notna(row[0]) and row[0] > 0 else 0.0)
                        result_index.append(row[1])
                        row = ibm_db.fetch_tuple(stmt)
                    if len(result_data) > 0:
                        s_start_result_values = pd.Series(data=result_data, index=pd.Index(result_index, name=group_base_names[0]))
                        self.logger.debug(f"Number of start results: {len(s_start_result_values)}")
                    else:
                        self.logger.debug(f"No start results available.")
                except Exception as ex:
                    raise Exception(
                        f'Retrieval of previous result value failed with sql statement "{sql_statement}"') from ex
                finally:
                    ibm_db.free_result(stmt)

                # We only want to calculate OccupancyCount between aligned_calc_start and aligned_cycle_end. Shrink data
                # frame to required time range.
                s_calc = df[self.raw_occupancy_count].copy()
                tmp_timestamps = s_calc.index.get_level_values(level=group_base_names[1])
                s_calc = s_calc[(tmp_timestamps >= max(aligned_calc_start, aligned_cycle_start)) & (tmp_timestamps < aligned_cycle_end)]

                # Cast column self.raw_occupancy_count to float because we allow it to be of type string for convenience
                s_calc = s_calc.astype(float)

                # Explicitly replace all negative raw occupancy counts by zero. We interpret negative counts which can occur in the input data as zero-counts.
                s_calc.mask(s_calc < 0, 0.0, inplace=True)

                # Aggregate new column to get result metric. Result metric has name self.raw_output_name in data frame df_agg_result.
                # Columns in group_base_names go into index of df_agg_result. We search for the last raw occupancy count
                # in every aggregation interval.
                # df_agg_result = s_calc.groupby(group_base).agg(func=['max','last'])
                # Replace previous line by the following lines in an attempt to avoid internal bug in pandas
                df_agg_result = s_calc.groupby(group_base).agg(func='max')
                df_agg_result.name = 'max'
                df_agg_result = df_agg_result.to_frame()
                df_agg_result['last'] = s_calc.groupby(group_base).agg(func='last')

                # Rename column 'max' in df_agg_result to self.output_name
                df_agg_result.rename(columns={'max': self.output_name}, inplace=True)

                # df_agg_result only holds values for aggregation intervals for which we had data events. Therefore,
                # create data frame with an index which holds entries for each aggregation interval between
                # aligned_calc_start (including) and aligned_cycle_end (excluding)
                time_index = pd.date_range(start=max(aligned_calc_start, aligned_cycle_start), end=(aligned_cycle_end - pd.Timedelta(value=1, unit='ns')), freq=agg_frequency)
                full_index = pd.MultiIndex.from_product([entities, time_index], names=group_base_names)
                tmp_col_name = self.output_name + UNIQUE_EXTENSION_LABEL
                full_df = pd.DataFrame(data={tmp_col_name: np.nan}, index=full_index)
                df_agg_result = full_df.join(df_agg_result, how='left')
                df_agg_result.drop(columns=[tmp_col_name], inplace=True)

                if s_start_result_values is not None:
                    # Add previous result(s) to first value(s) in df_agg_result when first value is np.nan
                    df_agg_result['last'] = df_agg_result['last'].groupby(level=group_base_names[0], sort=False).transform(self.add_to_first, s_start_result_values)

                # Use the last data event for the forward fill instead of maximum data event because the last count can
                # substantially deviate from the maximum in the same aggregation interval
                df_agg_result['last'].ffill(inplace=True)
                df_agg_result[self.output_name].mask(df_agg_result[self.output_name].isna(), df_agg_result['last'], inplace=True)
                s_agg_result = df_agg_result[self.output_name]

            # Add result values which has not been calculated in this run but were taken from output table
            if s_missing_result_values is not None:
                if s_agg_result is None or s_agg_result.empty:
                    s_agg_result = s_missing_result_values
                else:
                    s_agg_result = pd.concat([s_missing_result_values, s_agg_result], verify_integrity=True)

            if s_agg_result is not None:
                s_agg_result.sort_index(ascending=True, inplace=True)

        else:
            # Nothing to do because range in which OccupancyCount must be calculated is zero or negative.
            pass

        if s_agg_result is None:
            s_agg_result = pd.Series([], index=pd.MultiIndex.from_arrays([[], pd.DatetimeIndex([])], names=group_base_names), name=self.output_name, dtype='int64')

        return s_agg_result.to_frame()

    @classmethod
    def add_to_first(self, sub_s, value_map):
        if pd.isna(sub_s.iat[0]) and sub_s.name in value_map.index:
            sub_s.iat[0] = value_map.at[sub_s.name]
        return sub_s


class MsiOccupancy(DirectAggregator):

    KPI_FUNCTION_NAME = "OccupancyDuration"

    def __init__(self, occupancy_count, occupancy=None):
        super().__init__()
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if occupancy_count is not None and len(occupancy_count) > 0:
            self.occupancy_count = occupancy_count
        else:
            raise RuntimeError(f"Function {self.KPI_FUNCTION_NAME} requires the parameter occupancy_count "
                               f"but parameter occupancy_count is empty: occupancy_count={occupancy_count}")

        if occupancy is not None and len(occupancy) > 0:
            self.output_name = occupancy
        else:
            raise RuntimeError(f"No name was provided for the metric which is calculated by function "
                               f"{self.KPI_FUNCTION_NAME}: occupancy={occupancy}")

    def execute(self, df, group_base, group_base_names, start_ts=None, end_ts=None, entities=None, offset=None):

        # Find data item representing the result of this KPI function
        result_data_item = self.dms.entity_type_obj._data_items.get(self.output_name)

        # Determine destination frequency of this aggregation
        agg_frequency = find_frequency_from_data_item(result_data_item, self.dms.granularities)

        if not df.empty:

            # Get a copy of input data frame with group base in index
            df_copy = df[[self.occupancy_count]].copy()

            # Copy timestamps from index back into columns; use a different name to avoid ambiguities
            timestamp_level_name = group_base_names[1]
            timestamp_col_name = timestamp_level_name + UNIQUE_EXTENSION_LABEL
            df_copy[timestamp_col_name] = df_copy.index.get_level_values(level=timestamp_level_name)

            # Determine duration of (forward-directed) time gaps between each row for each aggregation interval. The
            # duration of last time gap is always np.nan for all aggregation intervals because of the missing successor.
            # Use output column name as temporary column name because we can be sure it is not used in data frame.
            groupby = df_copy.groupby(group_base)
            df_copy[self.output_name] = groupby[timestamp_col_name].diff(-1) * -1

            # Fill in duration of last time gap for each aggregation interval with the length of source granularity
            input_data_item = self.dms.entity_type_obj._data_items.get(self.occupancy_count)
            input_frequency = find_frequency_from_data_item(input_data_item, self.dms.granularities)
            input_frequency_duration = None
            if input_frequency is not None:
                input_frequency_duration = pd.Timedelta(value=1, unit=input_frequency)
                df_copy_diff_isna = df_copy[self.output_name].isna()
                df_copy[self.output_name].mask(df_copy_diff_isna, input_frequency_duration, inplace=True)

            # Remove a time gap when its corresponding occupancy count is zero
            df_copy[self.output_name].where(df_copy[self.occupancy_count] > 0, pd.NaT, inplace=True)

            # Sum up the time gaps for each aggregation interval
            s_occupancy = groupby[self.output_name].sum()

            # Convert pd.Timedelta to float64 depending on the input frequency
            if input_frequency_duration is not None:
                s_occupancy = s_occupancy / input_frequency_duration
            else:
                s_occupancy = s_occupancy.dt.total_seconds()

        else:
            s_occupancy = pd.Series([], index=pd.MultiIndex.from_arrays([[], pd.DatetimeIndex([])], names=group_base_names), name=self.output_name, dtype='float64')

        return s_occupancy.to_frame()


class MsiOccupancyLocation(DirectAggregator):

    KPI_FUNCTION_NAME = "OccupancyLocation"

    def __init__(self, x_pos, y_pos, name=None):

        super().__init__()
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if x_pos is not None and len(x_pos) > 0:
            self.x_pos = x_pos
        else:
            raise RuntimeError(f"Function {self.KPI_FUNCTION_NAME} requires the parameter x_pos "
                               f"but parameter x_pos is empty: x_pos={x_pos}")

        if y_pos is not None and len(y_pos) > 0:
            self.y_pos = y_pos
        else:
            raise RuntimeError(f"Function {self.KPI_FUNCTION_NAME} requires the parameter y_pos "
                               f"but parameter y_pos is empty: y_pos={y_pos}")

        if name is not None and len(name) > 0:
            self.output_name = name
        else:
            raise RuntimeError(f"No name was provided for the metric which is calculated by function "
                               f"{self.KPI_FUNCTION_NAME}: name={name}")

    def execute(self, df, group_base, group_base_names, start_ts=None, end_ts=None, entities=None, offset=None):

        if df.shape[0] > 0:
            df_clean = df[[self.x_pos, self.y_pos]].dropna(how='any')
            count_col_name = f'count{UNIQUE_EXTENSION_LABEL}'
            df_clean[count_col_name] = 1
            s_json = df_clean.groupby(group_base).apply(self.calc_locations, self.x_pos, self.y_pos, count_col_name)
        else:
            # No rows in data frame
            s_json = pd.Series(data=[], index=df.index)

        s_json.name = self.output_name
        if s_json.dtype != object:
            s_json = s_json.astype(object)

        return s_json.to_frame()

    def calc_locations(self, df, x_pos, y_pos, count_col_name):
        s_count = df.groupby([x_pos, y_pos])[count_col_name].count()
        x_y_count = []
        for (x, y), count in s_count.items():
            x_y_count.append({"x": x, "y": y, "count": count})

        return {"data": x_y_count}
