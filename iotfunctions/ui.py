# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
import logging
import datetime as dt

logger = logging.getLogger(__name__)


class BaseUIControl(object):
    is_ui_control = True

    def convert_datatype(self, from_datatype):
        conversions = {bool: 'BOOLEAN', str: 'LITERAL', float: 'NUMBER', int: 'NUMBER', dict: 'JSON',
                       dt.datetime: 'TIMESTAMP', None: None}
        try:
            return conversions[from_datatype]
        except KeyError:
            msg = 'couldnt convert type %s ' % from_datatype
            raise TypeError(msg)

    def convert_schema_datatype(self, from_datatype):
        conversions = {bool: 'boolean', str: 'string', float: 'number', int: 'number', dt.datetime: 'number',
                       None: None}
        try:
            return conversions[from_datatype]
        except KeyError:
            msg = 'couldnt convert type %s ' % from_datatype
            raise TypeError(msg)


class UIFunctionOutSingle(BaseUIControl):
    '''
    Single output item
    
    Parameters
    -----------
    name : str
        Name of function argument
    datatype: python datatype object 
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    '''

    type_ = 'OUTPUT_DATA_ITEM'

    def __init__(self, name, datatype=None, description=None, tags=None):

        self.name = name
        self.datatype = datatype
        if description is None:
            description = 'Choose an item name for the function output'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags

    def to_metadata(self):
        meta = {'name': self.name, 'dataType': self.convert_datatype(self.datatype), 'description': self.description,
                'tags': self.tags}
        return meta


class UIFunctionOutMulti(BaseUIControl):
    '''
    Array of multiple outputs
    
    Parameters
    -----------
    name : str
        Name of function argument
    cardinality_from: str
        Name of input argument that defines the number of items to expect from this array output. Specify an array input.
    is_datatype_derived: bool
        Specify true when the output datatypes are the same as the datatypes of the input array that drives this output array.
    datatype: python datatype object
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    '''

    type_ = 'OUTPUT_DATA_ITEM'

    def __init__(self, name, cardinality_from, is_datatype_derived=False, datatype=None, description=None, tags=None,
                 output_item=None):

        self.name = name
        self.cardinality_from = cardinality_from
        self.is_datatype_derived = is_datatype_derived
        if description is None:
            description = 'Provide names and datatypes for output items'
        self.description = description
        if datatype is not None:
            datatype = self.convert_datatype(datatype)
        self.datatype = datatype
        if tags is None:
            tags = []
        self.tags = tags

    def to_metadata(self):

        if not self.datatype is None:
            datatype = [self.datatype]
        else:
            datatype = None

        meta = {'name': self.name, 'cardinalityFrom': self.cardinality_from, 'dataTypeForArray': datatype,
                'description': self.description, 'tags': self.tags,
                'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "type": "array",
                               "items": {"type": "string"}}}

        if self.is_datatype_derived:
            meta['dataTypeFrom'] = self.cardinality_from

        return meta


class UISingleItem(BaseUIControl):
    '''
    Choose a single item as a function argument
    
    Parameters
    -----------
    name : str
        Name of function argument
    datatype: python datatype object 
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    required: bool
        Specify True when this argument is mandatory
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    '''

    type_ = 'DATA_ITEM'

    def __init__(self, name, datatype=None, description=None, required=True, tags=None):

        self.name = name
        self.datatype = datatype
        self.required = required
        if description is None:
            description = 'Choose one or more data item to use as a function input'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags

    def to_metadata(self):

        if self.datatype is None:
            datatype = None
        else:
            datatype = self.convert_datatype(self.datatype)

        meta = {'name': self.name, 'type': self.type_, 'dataType': datatype, 'required': self.required,
                'description': self.description, 'tags': self.tags}

        return meta


class UIMultiItem(BaseUIControl):
    '''
    Multi-select list of data items
    
    Parameters
    -----------
    name : str
        Name of function argument
    datatype: python datatype object 
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    required: bool
        Specify True when this argument is mandatory
    min_items: int
        The minimum number of items that must be selected
    max_items: int
        The maximum number of items that can be selected
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT'] 
    '''

    type_ = 'DATA_ITEM'

    def __init__(self, name, datatype=None, description=None, required=True, min_items=None, max_items=None, tags=None,
                 output_item=None, is_output_datatype_derived=False, output_datatype=None):

        self.name = name
        self.datatype = datatype
        self.required = required
        if description is None:
            description = 'Choose one or more data item to use as a function input'
        self.description = description
        if min_items is None:
            if self.required:
                min_items = 1
            else:
                min_items = 0
        self.min_items = min_items
        self.max_items = max_items
        if tags is None:
            tags = []
        self.tags = tags
        # the following metadata is optional
        # used to create an array output for this input
        self.output_item = output_item
        self.is_output_datatype_derived = is_output_datatype_derived
        self.output_datatype = output_datatype

    def to_metadata(self):

        if self.datatype is None:
            datatype = None
        else:
            datatype = [self.convert_datatype(self.datatype)]

        meta = {'name': self.name, 'type': self.type_, 'dataType': 'ARRAY', 'dataTypeForArray': datatype,
                'required': self.required, 'description': self.description, 'tags': self.tags,
                'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "type": "array",
                               "minItems": self.min_items, "maxItems": self.max_items, "items": {"type": "string"}}}
        return meta

    def to_output_metadata(self):

        if self.output_item is not None:
            if not self.output_datatype is None:
                datatype = [self.convert_datatype(self.output_datatype)]
            else:
                datatype = None

            meta = {'name': self.output_item, 'cardinalityFrom': self.name, 'dataTypeForArray': datatype,
                    'description': self.description, 'tags': self.tags,
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "type": "array",
                                   "items": {"type": "string"}}}

            if self.is_output_datatype_derived:
                meta['dataTypeFrom'] = self.name

            return meta
        else:
            return None


class UIMulti(BaseUIControl):
    '''
    Multi-select list of constants
    
    Parameters
    -----------
    name : str
        Name of function argument
    datatype: python datatype object 
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    required: bool
        Specify True when this argument is mandatory
    min_items: int
        The minimum number of values that must be entered/selected
    max_items: int
        The maximum number of values that can be entered/selected
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    values: list
        Values to display in UI picklist        
    '''

    type_ = 'CONSTANT'

    def __init__(self, name, datatype, description=None, required=True, min_items=None, max_items=None, tags=None,
                 values=None, output_item=None, is_output_datatype_derived=False, output_datatype=None):

        self.name = name
        self.datatype = datatype
        self.required = required
        if description is None:
            description = 'Enter a list of comma separated values'
        self.description = description
        if min_items is None:
            if self.required:
                min_items = 1
            else:
                min_items = 0
        self.min_items = min_items
        self.max_items = max_items
        if tags is None:
            tags = []
        self.tags = tags
        self.values = values
        # the following metadata is optional
        # used to create an array output for this input
        self.output_item = output_item
        self.is_output_datatype_derived = is_output_datatype_derived
        self.output_datatype = output_datatype

    def to_metadata(self):

        if self.datatype is None:
            msg = 'Datatype is required for multi constant array input %s' % self.name
            raise ValueError(msg)
        else:
            datatype = [self.convert_datatype(self.datatype)]
            schema_datatype = self.convert_schema_datatype(self.datatype)

        meta = {'name': self.name, 'type': self.type_, 'dataType': 'ARRAY', 'dataTypeForArray': datatype,
                'required': self.required, 'description': self.description, 'tags': self.tags, 'values': self.values,
                'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "type": "array",
                               "minItems": self.min_items, "maxItems": self.max_items,
                               "items": {"type": schema_datatype}}}
        return meta

    def to_output_metadata(self):

        if self.output_item is not None:
            if self.output_datatype is not None:
                datatype = [self.convert_datatype(self.output_datatype)]
                schema_type = self.convert_schema_datatype(self.output_datatype)
            else:
                datatype = None
                schema_type = None

            meta = {'name': self.output_item, 'cardinalityFrom': self.name, 'dataTypeForArray': datatype,
                    'description': self.description, 'tags': self.tags,
                    'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "type": "array",
                                   "items": {"type": schema_type}}}

            if self.is_output_datatype_derived:
                meta['dataTypeFrom'] = self.name

            return meta
        else:
            return None


class UISingle(BaseUIControl):
    '''
    Single valued constant
    
    Parameters
    -----------
    name : str
        Name of function argument
    datatype: python datatype object 
        Used to validate UI input. e.g. str, float, dt.datetime, bool
    required: bool
        Specify True when this argument is mandatory
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    values: list
        Values to display in UI picklist        
    '''

    type_ = 'CONSTANT'

    def __init__(self, name, datatype=None, description=None, tags=None, required=True, values=None, default=None):

        self.name = name
        self.datatype = datatype
        if description is None:
            description = 'Enter a constant value'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags
        self.required = required
        self.values = values
        self.default = default

    def to_metadata(self):
        meta = {'name': self.name, 'type': self.type_, 'dataType': self.convert_datatype(self.datatype),
                'description': self.description, 'tags': self.tags, 'required': self.required, 'values': self.values}

        if self.default is not None:
            if isinstance(self.default, dict):
                meta['value'] = self.default
            else:
                meta['value'] = {'value': self.default}

        return meta


class UIStatusFlag(UIFunctionOutSingle):
    '''
    Output a boolean value indicating that function was executed
    '''

    def __init__(self, name='is_executed'):
        super().__init__(name=name, datatype=bool, description=('Function returns as boolean status flag indicating'
                                                                ' successful execution'), )


class UIText(UISingle):
    '''
    UI control that allows entering multiple lines of text
    
    Parameters
    -----------
    name : str
        Name of function argument
    required: bool
        Specify True when this argument is mandatory
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    default: str
        Optional default
    '''

    def __init__(self, name='expression', description=None, tags=None, required=True, default=None):

        if description is None:
            description = 'Enter text'
        self.description = description
        if tags is None:
            tags = ['TEXT']
        else:
            tags.append('TEXT')

        super().__init__(name=name, description=description, tags=tags, required=required, default=default,
                         datatype=str)


class UIParameters(UISingle):
    '''
    UI control for capturing a json input

    Parameters
    ----------
    name : str (optional)
        Name of function argument. Default is "parameters"
    '''

    def __init__(self, name='parameters', description='enter json parameters', tags=None, required=False, default=None):
        super().__init__(name=name, description=description, tags=tags, required=required, default=default,
                         datatype=dict)


class UIExpression(UIText):
    '''
    UI control that allows entering a python expression
    
    Parameters
    -----------
    name : str
        Name of function argument
    required: bool
        Specify True when this argument is mandatory
    description: str
        Help text to display in UI
    tags: list of strs
        Optional tags, e.g. ['DIMENSION', 'EVENT', 'ALERT']
    default: str
        Optional default
    '''

    def __init__(self, name='expression', description=None, tags=None, required=True, default=None):

        if description is None:
            description = 'Enter a python expression'
        self.description = description
        if tags is None:
            tags = ['EXPRESSION']
        else:
            tags.append('EXPRESSION')

        super().__init__(name=name, description=description, tags=tags, required=required, default=default)
