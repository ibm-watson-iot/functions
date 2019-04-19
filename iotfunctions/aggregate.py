import re

from .catalog import CATEGORY_AGGREGATOR
from iotfunctions.base import BaseAggregator

CATALOG_CATEGORY = CATEGORY_AGGREGATOR


def _general_aggregator_input():
    return {
        'name': 'source',
        'description': 'Select the data item that you want to use as input for your calculation.',
        'type': 'DATA_ITEM',
        'required': True,
    }.copy()

def _number_aggregator_input():
    input_item = _general_aggregator_input()
    input_item['dataType'] = 'NUMBER'
    return input_item

def _no_datatype_aggregator_output():
    return {
        'name': 'name',
        'description': 'Enter a name for the data item that is produced as a result of this calculation.'
    }.copy()

def _general_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataTypeFrom'] = 'source'
    return output_item

def _number_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataType'] = 'NUMBER'
    return output_item

def _generate_metadata(cls, metadata):
    common_metadata = {
        'name': cls.__name__,
        'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
        'category': CATALOG_CATEGORY,
        'input': [
            _general_aggregator_input()
        ],
        'output': [
            _general_aggregator_output()
        ]
    }
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
        return _generate_metadata(cls, {
            'description': 'Calculates the sum of the values in your data set.',
            'input': [
                _number_aggregator_input(),
                {
                    'name': 'min_count',
                    'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'NUMBER'
                }
            ],
            'output': [
                _number_aggregator_output()
            ]
        })

    def __init__(self, min_count=1):
        self.min_count = min_count

    def execute(self, group):
        return group.sum(min_count=self.min_count)


class Minimum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the minimum value in your data set.'
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.min(skipna=True)


class Maximum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the maximum value in your data set.'
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.max()


class Mean(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Calculates the mean of the values in your data set.',
            'input': [
                _number_aggregator_input()
            ],
            'output': [
                _number_aggregator_output()
            ]
        })

    def __init__(self):
        super().__init__()

    def execute(self, group):
        return group.mean()


class Median(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Indentifies the median of the values in your data set.'
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.median()


class Count(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Counts the number of values in your data set.', 
            'input': [
                _general_aggregator_input(),
                {
                    'name': 'min_count',
                    'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'NUMBER'
                }
            ],
            'output': [
                _number_aggregator_output()
            ],
            'tags': [
                'EVENT'
            ] 
        })

    def __init__(self, min_count=1):
        self.min_count = min_count

    def execute(self, group):
        cnt = group.count()
        return cnt if cnt >= self.min_count else None


class DistinctCount(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Counts the number of distinct values in your data set.', 
            'input': [
                _general_aggregator_input(),
                {
                    'name': 'min_count',
                    'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'NUMBER'
                }
            ],
            'output': [
                _number_aggregator_output()
            ],
            'tags': [
                'EVENT'
            ] 
        })

    def __init__(self, min_count=1):
        self.min_count = min_count

    def execute(self, group):
        cnt = len(group.dropna().unique())
        return cnt if cnt >= self.min_count else None


class StandardDeviation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Calculates the standard deviation of the values in your data set.',
            'input': [
                _number_aggregator_input()
            ],
            'output': [
                _number_aggregator_output()
            ]
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.std()


class Variance(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Calculates the variance of the values in your data set.',
            'input': [
                _number_aggregator_input()
            ],
            'output': [
                _number_aggregator_output()
            ]
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.var()


class Product(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Calculates the product of the values in your data set.', 
            'input': [
                _number_aggregator_input(),
                {
                    'name': 'min_count',
                    'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                    'type': 'CONSTANT',
                    'required': False,
                    'dataType': 'NUMBER'
                }
            ],
            'output': [
                _number_aggregator_output()
            ]
        })

    def __init__(self, min_count=1):
        self.min_count = min_count

    def execute(self, group):
        return group.prod(min_count=self.min_count)


class First(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the first value in your data set.',
            'tags': [
                'EVENT'
            ]
        })

    def __init__(self):
        pass

    def execute(self, group):
        group = group.dropna()
        try:
            first = group.iloc[0]
        except IndexError:
            first = None
        return first


class Last(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Identifies the last value in your data set.',
            'tags': [
                'EVENT'
            ]
        })

    def __init__(self):
        pass

    def execute(self, group):
        return group.dropna().tail(1)


class AggregateWithCalculation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create aggregation using expression on a data item.', 
            'input': [
                _general_aggregator_input(),
                {
                    'name': 'expression',
                    'description': 'The expression. Use ${GROUP} to reference the current grain. All Pandas Series methods can be used on the grain. For example, ${GROUP}.max() - ${GROUP}.min().',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                }
            ],
            'output': [
                _no_datatype_aggregator_output()
            ],
            'tags': [
                'EVENT',
                'JUPYTER'
            ] 
        })

    def __init__(self, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.expression = expression
        
    def execute(self, group):
        return eval(re.sub(r"\$\{GROUP\}", r"group", self.expression))

