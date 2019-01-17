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
    '''
    Base class for UI
    '''
    def convert_datatype(self,from_datatype):
        conversions = {bool: 'BOOLEAN',
                       str: 'LITERAL',
                       float: 'NUMBER',
                       int: 'NUMBER',
                       dt.datetime: 'TIMESTAMP'
                       }
        try:
            return conversions[from_datatype]
        except KeyError:
            msg = 'couldnt convert type %s. will attempt to use type supplied '
            logger.warning(msg)
            return from_datatype
                
class UIFunctionOutSingle(BaseUIControl):
    '''
    Single output item
    '''
    def __init__(self,name, datatype=None, description=None, tags = None):
        
        self.name = name
        self.datatype = datatype
        if description is None:
            description = 'Choose an item name for the function output'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags
        
    def to_metadata(self):
        meta = {
                'name' : self.name,
                'dataType' : self.convert_datatype(self.datatype),
                'description' : self.description,
                'tags' : self.tags
                }
        return meta


class UIFunctionOutMulti(BaseUIControl):
    '''
    Array of multiple outputs
    '''
    def __init__(self,name, cardinality_from,
                 is_datatype_derived = False,
                 datatype = None,
                 description=None,
                 tags = None):
        
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
            datatype= None
                    
        meta = {
                'name' : self.name,
                'cardinalityFrom' : self.cardinality_from,
                'dataTypeForArray' : datatype,
                'description' : self.description,
                'tags' : self.tags,
                'jsonSchema' : {
                                "$schema" : "http://json-schema.org/draft-07/schema#",
                                "type" : "array",
                                "items" : {"type": "string"}
                                }
                }
                
        if self.is_datatype_derived:
            meta['dataTypeFrom'] = self.cardinality_from
                
        return meta
    
class UISingleItem(BaseUIControl):
    '''
    Choose a single item as a function argument
    '''
    def __init__(self,name, datatype=None, description=None, required = True,
                 tags = None):
        
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
            datatype = [self.convert_datatype(self.datatype)]
        
        meta = {
                'name' : self.name,
                'type' : 'DATA_ITEM' ,
                'dataType' : datatype,
                'required' : self.required,
                'description' : self.description,
                'tags' : self.tags
                }
        return meta     
    
class UIMultiItem(BaseUIControl):
    '''
    Multi-select list of data items
    '''
    def __init__(self,name, datatype=None, description=None, required = True,
                 min_items = None, max_items = None, tags = None):
        
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
        
    def to_metadata(self):
        
        if self.datatype is None:
            datatype = None
        else:
            datatype = [self.convert_datatype(self.datatype)]
        
        meta = {
                'name' : self.name,
                'type' : 'DATA_ITEM' ,
                'dataType' : 'ARRAY',
                'dataTypeForArray' : datatype,
                'required' : self.required,
                'description' : self.description,
                'tags' : self.tags,
                'jsonSchema' : {
                                "$schema" : "http://json-schema.org/draft-07/schema#",
                                "type" : "array",
                                "minItems" : self.min_items,
                                "maxItems" : self.max_items,
                                "items" : {"type": "string"}
                                }
                }
        return meta          
    
class UISingle(BaseUIControl):
    '''
    Single valued constant
    '''
    def __init__(self,name, datatype=None, description=None, tags = None, required = True):
        
        self.name = name
        self.datatype = datatype
        if description is None:
            description = 'Enter a constant value'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags
        self.required = required
        
    def to_metadata(self):
        meta = {
                'name' : self.name,
                'type' : 'CONSTANT',
                'dataType' : self.convert_datatype(self.datatype),
                'description' : self.description,
                'tags' : self.tags,
                'required' : self.required
                }
        return meta
    
class UISingleItem(BaseUIControl):
    '''
    Multi-select list of data items
    '''
    def __init__(self,name, datatype=None, description=None, required = True,
                 min_items = None, max_items = None, tags = None):
        
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
        
    def to_metadata(self):
        
        meta = {
                'name' : self.name,
                'type' : 'DATA_ITEM' ,
                'dataType' : self.convert_datatype(self.datatype),
                'required' : self.required,
                'description' : self.description,
                'tags' : self.tags
                }
        return meta
        
    
    