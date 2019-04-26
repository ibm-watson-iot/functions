import re
import logging
from iotfunctions.base import BaseSimpleAggregator, BaseComplexAggregator
from .ui import UISingle,UIMultiItem,UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti, UIMulti

CATALOG_CATEGORY = 'AGGREGATOR'

logger = logging.getLogger(__name__)


class AggregateItems(BaseSimpleAggregator):

    '''
    Use common aggregation methods to aggregate one or more data items
    
    '''
    
    def __init__(self,input_items,aggregation_function,output_items=None):
        
        super().__init__()
        
        self.input_items = input_items
        self.aggregation_function = aggregation_function
        
        if output_items is None:
            output_items = ['%s_%s' %(x,aggregation_function) for x in self.input_items]
        
        self.output_items = output_items
        
    def get_aggregation_method(self):
        
        out = self.get_available_methods().get(self.aggregation_function,None)
        if out is None:
            raise ValueError('Invalid aggregation function specified: %s'
                             %self.aggregation_function)
        
        return out 
        
    @classmethod
    def build_ui(cls):
        
        inputs = []
        inputs.append(UIMultiItem(name = 'input_items',
                                  datatype= None,
                                  description = ('Choose the data items'
                                                 ' that you would like to'
                                                 ' aggregate'),
                                  output_item = 'output_items',
                                  is_output_datatype_derived = True
                                          ))
                                  
        aggregate_names = list(cls.get_available_methods().keys())
                                  
        inputs.append(UISingle(name = 'aggregation_function',
                               description = 'Choose aggregation function',
                               values = aggregate_names))
        
        return (inputs,[])
    
    @classmethod
    def count_distinct(cls,series):
        
        return len(series.dropna().unique())                                  
        
    @classmethod
    def get_available_methods(cls):
        
        return {
                'sum' : 'sum',
                'count' : 'count',
                'count_distinct' : cls.count_distinct,
                'min' : 'min',
                'max' : 'max',
                'mean' : 'mean',
                'median' : 'median',
                'std' : 'std',
                'var' : 'var',
                'first': 'first',
                'last': 'last',
                'product' : 'product'
                }


class AggregateWithCalculation(BaseSimpleAggregator):
    
    '''
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.
    
    Example:
        
    x.max() - x(max)
    
    '''
    
    def __init__(self,input_items,expression,output_items):
    
        super().__init__()
        
        self.input_items = input_items
        self.expression = expression    
        self.output_items = output_items

    @classmethod
    def build_ui(cls):
        
        inputs = []
        inputs.append(UIMultiItem(name = 'input_items',
                                  datatype= None,
                                  description = ('Choose the data items'
                                                 ' that you would like to'
                                                 ' aggregate'),
                                  output_item = 'output_items',
                                  is_output_datatype_derived = True
                                          ))
                                  
        inputs.append(UISingle(name = 'expression',
                               description = 'Paste in or type an AS expression',
                               datatype = str))
        
        return (inputs,[])
        
    def aggregate(self, x):
        
        # for compatibility
        # replace {GROUP}.max() with series.max
        expression = re.sub(r"\$\{GROUP\}", r"x", self.expression)
        
        return eval(expression)


'''
Individual aggregation functions have been replaced by the AggregateItems function.
'''        

class Sum():
    
    is_deprecated = True
    aggregation_function = 'sum'

    def __init__(self, source,name,min_count=1):
        self.source = source
        self.name = name
        self.min_count = min_count
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        
        if self.min_count > 1:
            self.trace_append(
                msg = ('Possible incompatibility when replacing %s with '
                       ' %s. The replacement function does not use the'
                       ' "min_count" property.' %(self.__class__.__name__,
                        new.__class__.__name__)),
                log_method = logger.debug
                )
        return new

class Minimum():
    
    is_deprecated = True
    aggregation_function = 'min'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new


class Maximum():
    
    is_deprecated = True
    aggregation_function = 'max'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new

class Mean():
    
    is_deprecated = True
    aggregation_function = 'mean'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new


class Median():
    
    is_deprecated = True
    aggregation_function = 'median'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        
        return new


class Count():
    
    is_deprecated = True
    aggregation_function = 'count'

    def __init__(self, source,name,min_count=1):
        self.source = source
        self.name = name
        self.min_count = min_count
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        
        if self.min_count > 1:
            self.trace_append(
                msg = ('Possible incompatibility when replacing %s with '
                       ' %s. The replacement function does not use the'
                       ' "min_count" property.' %(self.__class__.__name__,
                        new.__class__.__name__)),
                log_method = logger.debug
                )
        return new


class DistinctCount():
    
    is_deprecated = True
    aggregation_function = 'count_distinct'

    def __init__(self, source,name,min_count=1):
        self.source = source
        self.name = name
        self.min_count = min_count
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        
        if self.min_count > 1:
            self.trace_append(
                msg = ('Possible incompatibility when replacing %s with '
                       ' %s. The replacement function does not use the'
                       ' "min_count" property.' %(self.__class__.__name__,
                        new.__class__.__name__)),
                log_method = logger.debug
                )
        return new

class StandardDeviation():
    
    is_deprecated = True
    aggregation_function = 'std'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new
    
class Variance():
    
    is_deprecated = True
    aggregation_function = 'var'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new    

class Product():
    
    is_deprecated = True
    aggregation_function = 'product'

    def __init__(self, source,name,min_count=1):
        self.source = source
        self.name = name
        self.min_count = min_count
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        
        if self.min_count > 1:
            self.trace_append(
                msg = ('Possible incompatibility when replacing %s with '
                       ' %s. The replacement function does not use the'
                       ' "min_count" property.' %(self.__class__.__name__,
                        new.__class__.__name__)),
                log_method = logger.debug
                )
        return new

class First():
    
    is_deprecated = True
    aggregation_function = 'first'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new

class Last():
    
    is_deprecated = True
    aggregation_function = 'last'

    def __init__(self, source,name):
        self.source = source
        self.name = name
        
    def get_replacement(self):
        
        new = AggregateItems(
            input_item = self.source,
            output_item = self.name,
            aggregation_function = self.aggregation_function
                )
        return new




