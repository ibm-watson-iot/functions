# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import numpy as np
import re
import pandas as pd
import logging
import iotfunctions as iotf
from .preprocessor import BaseTransformer, BaseEvent


logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'

class IoTAlertExpression(BaseEvent):
    '''
    Create alerts that are triggered when data values reach a particular range.
    '''
    def __init__(self, input_items, expression , alert_name):
        self.input_items = input_items
        self.expression = expression
        self.alert_name = alert_name
        super().__init__()
        # registration metadata
        self.inputs = ['input_items', 'expression']
        self.constants = ['expression']
        self.outputs = ['alert_name']
        self.tags = ['EVENT']
        
    def _calc(self,df):
        '''
        unused
        '''
        return df
        
    def execute(self, df):
        df = df.copy()
        if '${' in self.expression:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", self.expression)
            msg = 'expression converted to %s' %expr
        else:
            expr = self.expression
            msg = 'expression was not in the form "${item}" so it will evaluated asis (%s)' %expr
        self.trace_append(msg)
        df[self.alert_name] = np.where(eval(expr), True, np.nan)
        return df
    
    
class IoTAlertOutOfRange(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold or drops below a lower_theshold. Specify at least one threshold.
    """
    
    optionalItems = ['lower_threshold','upper_threshold'] 
    
    def __init__ (self,input_item, lower_threshold=None, upper_threshold=None,
                  output_alert_upper = 'output_alert_upper', output_alert_lower = 'output_alert_lower'):
        
        self.input_item = input_item
        if not lower_threshold is None:
            lower_threshold = float(lower_threshold)
        self.lower_threshold = lower_threshold
        if not upper_threshold is None:
            upper_threshold = float(upper_threshold)
        self.upper_threshold = upper_threshold
        self.output_alert_lower = output_alert_lower
        self.output_alert_upper = output_alert_upper
        super().__init__()
        
    def _calc(self,df):
        '''
        unused
        '''        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.output_alert_upper] = False
        df[self.output_alert_lower] = False
        
        if not self.lower_threshold is None:
            df[self.output_alert_lower] = np.where(df[self.input_item]<=self.lower_threshold,True,False)
        if not self.upper_threshold is None:
            df[self.output_alert_upper] = np.where(df[self.input_item]>=self.upper_threshold,True,False)
            
        return df  
    
    
class IoTAlertHighValue(BaseEvent):
    """
    Fire alert when metric exceeds an upper threshold'.
    """
    
    def __init__ (self,input_item,  upper_threshold=None,
                  alert_name = 'alert_name', ):
        self.input_item = input_item
        self.upper_threshold = float(upper_threshold)
        self.alert_name = alert_name
        super().__init__()
        
    def _calc(self,df):
        '''
        unused
        '''        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item]>=self.upper_threshold,True,False)
            
        return df     
    
class IoTAlertLowValue(BaseEvent):
    """
    Fire alert when metric goes below a threshold'.
    """
    
    def __init__ (self,input_item,  lower_threshold=None,
                  alert_name = 'alert_name', ):
        self.input_item = input_item
        self.lower_threshold = float(lower_threshold)
        self.alert_name = alert_name
        super().__init__()
        
    def _calc(self,df):
        '''
        unused
        '''        
        
    def execute(self,df):
        
        df = df.copy()
        df[self.alert_name] = np.where(df[self.input_item]<=self.lower_threshold,True,False)
            
        return df
    
class IoTPackageInfo(BaseTransformer):
    """
    Show the version number 
    """
    
    def __init__ (self, dummy_item, package_url = 'package_url', module = 'module', version = 'version'):
        
        self.dummy_item = dummy_item
        self.package_url = package_url
        self.module = module
        self.version = version
        super().__init__()
        
    def execute(self,df):
        
        df = df.copy()
        df[self.package_url] = self.url
        df[self.module] = self.__module__
        df[self.version] = iotf.__version__
        
        return df

   