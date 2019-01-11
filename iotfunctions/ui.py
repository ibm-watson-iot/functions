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
        
        meta = {
                'name' : self.name,
                'type' : 'DATA_ITEM' ,
                'dataType' : self.convert_datatype(self.datatype),
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
    
class UISingle(BaseUIControl):
    '''
    Single valued constant
    '''
    def __init__(self,name, datatype=None, description=None, tags = None):
        
        self.name = name
        self.datatype = datatype
        if description is None:
            description = 'Enter a constant value'
        self.description = description
        if tags is None:
            tags = []
        self.tags = tags
        
    def to_metadata(self):
        meta = {
                'name' : self.name,
                'type' : 'CONSTANT',
                'dataType' : self.convert_datatype(self.datatype),
                'description' : self.description,
                'tags' : self.tags
                }
        return meta    
    
    