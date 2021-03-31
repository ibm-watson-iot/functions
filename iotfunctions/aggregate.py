# Licensed Materials - Property of IBM
# 5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
# (C) Copyright IBM Corp. 2020 All Rights Reserved.
# US Government Users Restricted Rights - Use, duplication, or disclosure
# restricted by GSA ADP Schedule Contract with IBM Corp.
import re
import logging
from collections import defaultdict

import pandas as pd
import numpy as np

import iotfunctions.metadata as md
from iotfunctions.base import (BaseAggregator, BaseFunction)
from iotfunctions.util import log_data_frame


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
    def __init__(self, dms, ids=None, timestamp=None, granularity=None, simple_aggregators=None,
                 complex_aggregators=None, direct_aggregators=None):

        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        self.dms = dms

        if simple_aggregators is None:
            simple_aggregators = []

        if complex_aggregators is None:
            complex_aggregators = []

        if direct_aggregators is None:
            direct_aggregators = []

        self.simple_aggregators = simple_aggregators
        self.complex_aggregators = complex_aggregators
        self.direct_aggregators = direct_aggregators

        self.ids = ids
        if self.ids is not None:
            # if not set(self.ids).isdisjoint(set([sa[0] for sa in self.simple_aggregators])):
            #     raise RuntimeError('sources (%s) must not include any columns in ids (%s), use grain entityFirst attribute to have that' % (str(self.sources), str(self.ids)))
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

        self.logger.debug(
            'aggregation_ids=%s, aggregation_timestamp=%s, aggregation_simple_aggregators=%s, aggregation_complex_aggregators=%s, aggregation_direct_aggregators=%s, aggregation_frequency=%s, aggregation_groupby=%s, aggregation_entityFirst=%s' % (
                str(self.ids), str(self.timestamp), str(self.simple_aggregators), str(self.complex_aggregators),
                str(self.direct_aggregators), str(self.frequency), str(self.groupby), str(self.entityFirst)))

    def _agg_dict(self, df, simple_aggregators):
        numerics = df.select_dtypes(include=[np.number, bool]).columns.values

        agg_dict = defaultdict(list)
        for srcs, agg, name in simple_aggregators:
            srcs = asList(srcs)
            for src in srcs:
                if src in numerics:
                    agg_dict[src].append(agg)
                else:
                    numeric_only_funcs = {'sum', 'mean', 'std', 'var', 'prod', Sum, Mean, StandardDeviation, Variance,
                                          Product}
                    if agg not in numeric_only_funcs:
                        agg_dict[src].append(agg)

        return agg_dict

    def execute(self, df):
        agg_dict = self._agg_dict(df, self.simple_aggregators)

        self.logger.debug('aggregation_column_methods=%s' % dict(agg_dict))

        df = df.reset_index()

        # The index has been moved to the columns and the index levels ('id', event timestamp, dimensions) can now be
        # used as a starting point of an aggregation. Provide index level 'id' - if it exists - as 'entity_id' as well.
        if 'id' in df.columns:
            df['entity_id'] = df['id']

        group_base = []
        if len(self.ids) > 0 and self.entityFirst:
            group_base.extend(self.ids)

        if self.timestamp is not None and self.frequency is not None:
            if self.frequency == 'W':
                # 'W' by default use right label and right closed
                group_base.append(pd.Grouper(key=self.timestamp, freq=self.frequency, label='left', closed='left'))
            else:
                # other alias seems to not needing to special handle
                group_base.append(pd.Grouper(key=self.timestamp, freq=self.frequency))

        if self.groupby is not None and len(self.groupby) > 0:
            group_base.extend(self.groupby)

        self.logger.debug('aggregation_groupbase=%s' % str(group_base))

        groups = df.groupby(group_base)

        all_dfs = []

        # simple aggregators

        df_agg = groups.agg(agg_dict)

        log_data_frame('aggregation_after_groupby_df_agg', df_agg.head())

        renamed_cols = {}
        for srcs, agg, names in self.simple_aggregators:
            for src, name in zip(asList(srcs), asList(names)):
                func_name = agg.func.__name__ if hasattr(agg, 'func') else agg.__name__ if hasattr(agg,
                                                                                                   '__name__') else agg
                renamed_cols['%s|%s' % (src, func_name)] = name if name is not None else src
        for name in (
                set(agg_dict.keys()) - set([item for sa in self.simple_aggregators for item in asList(sa[0])])):
            for agg in agg_dict[name]:
                func_name = agg.func.__name__ if hasattr(agg, 'func') else agg.__name__ if hasattr(agg,
                                                                                                   '__name__') else agg
                renamed_cols['%s|%s' % (name, func_name)] = name

        self.logger.info('after simple aggregation function - sources %s' % str(renamed_cols))

        new_columns = []
        for col in df_agg.columns:
            if len(col[-1]) == 0:
                new_columns.append('|'.join(col[:-1]))
            else:
                new_columns.append('|'.join(col))
        df_agg.columns = new_columns

        if len(renamed_cols) > 0:
            df_agg.rename(columns=renamed_cols, inplace=True)

        all_dfs.append(df_agg)

        log_data_frame('aggregation_df_agg', df_agg.head())

        # complex aggregators

        for srcs, func, names in self.complex_aggregators:

            self.logger.info('executing complex aggregation function - sources %s' % str(srcs))
            self.logger.info('executing complex aggregation function - output %s' % str(names))
            df_apply = groups.apply(func)

            if df_apply.empty and df_apply.columns.empty:
                for name in names:
                    df_apply[name] = None

                    source_metadata = self.dms.data_items.get(name)
                    if source_metadata is None:
                        continue

                    if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                        df_apply = df_apply.astype({name: float})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                        df_apply = df_apply.astype({name: bool})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                        df_apply = df_apply.astype({name: 'datetime64[ns]'})
                    else:
                        df_apply = df_apply.astype({name: str})

            all_dfs.append(df_apply)

            log_data_frame('func=%s, aggregation_df_apply' % str(func), df_apply.head())

        # direct aggregators

        for srcs, func, names in self.direct_aggregators:

            self.logger.info('executing direct aggregation function - sources %s' % str(srcs))
            self.logger.info('executing direct aggregation function - output %s' % str(names))

            df_direct = func(df=df, group_base=group_base)
            if df_direct.empty and df_direct.columns.empty:
                for name in names:
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

            log_data_frame('func=%s, aggregation_df_direct' % str(func), df_direct.head())

        # concat all results
        df = pd.concat(all_dfs, axis=1)

        # Adding entity_id column in the aggregate df by default
        id_idx = 'id'
        entity_id_col = 'entity_id'
        if id_idx in df.index.names and entity_id_col not in df.columns and self.entityFirst:
            df[entity_id_col] = df.index.get_level_values(id_idx)

        log_data_frame('aggregation_final_df', df.head())

        return df


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


class DirectAggregator(Aggregator):
    is_direct_aggregator = True


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
        cnt = group.count()
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
        cnt = len(group.dropna().unique())
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
