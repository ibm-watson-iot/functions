# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
from ibm_db_sa.base import DOUBLE
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

def register(module,db):
    '''
    Automatically register all functions in a module. To be regiserable, a function
    must have an class variable containing a dictionary of arguments (auto_register_args)
    '''

    for (name,cls) in inspect.getmembers(module):
        try:
            kwargs = cls.auto_register_args
        except AttributeError:
            kwargs = None
        
        if not kwargs is None:
            args,varargs,keywords,defaults = (inspect.getargspec(cls.__init__))
            args = args[1:]
            stmt = 'mod.%s(**%s)' %(name,kwargs)
            instance = eval(stmt)
            instance.db = db
            df = instance.get_test_data()
            instance.register(df=df)
        
                
class TimeSeriesGenerator(object):

    ''' 
    Generate random sample time series with trend and seasonality
    
    Parameters
    ----------
    metrics : list of strings
        Names of numeric items to generate data for
    dates : list of strings
        Names of other date items (not the timestamp of the timeseries)
    categoricals: list of strings
        Names of categorical items to generate data for
    ids: list of strings
        identifiers a the entity that this data pertains to
    days: number 
        Number of days back from current date to generate data for
    seconds: number
        Number of seconds back from current date to gerate data for
    freq : str
        Pandas frequency string
    increase_per_day : number
        scaling parameter for the trend component    
    noise : number
        scaling parameter for the noise component
    day_harmonic = number
        scaling parameter for the seasonal component
    day_of_week_harmonic = number
        scaling parameter for the seasonal component        
    '''

    ref_date = dt.datetime(2018, 1, 1, 0, 0, 0, 0)
    _timestamp = 'evt_timestamp'
    _entity_id = 'deviceid'

    def __init__(self,metrics=None,
                      ids=None,
                      days=0,
                      seconds=300,
                      freq='1min',
                      categoricals = None,
                      dates=None,
                      increase_per_day = 0.0001,
                      noise = 0.1 ,
                      day_harmonic = 0.2,
                      day_of_week_harmonic = 0.2
                      ):
    
        if metrics is None:
            metrics = ['x1','x2','x3']
        
        if categoricals is None:
            categoricals = []
            
        if dates is None:
            dates = []
            
        self.metrics = metrics
        self.categoricals = categoricals
        
        dates = [x for x in dates if x != self._timestamp]
        
        self.dates = dates
        
        if ids is None:
            ids = ['sample_%s' %x for x in list(range(10))]
        self.ids = ids
        
        self.days = days
        self.seconds = seconds
        self.freq = freq
        #optionally scale using dict keyed on metric name
        self.mean = {}
        self.sd = {}
        
        self.increase_per_day = increase_per_day
        self.noise = noise
        self.day_harmonic = day_harmonic
        self.day_of_week_harmonic = day_of_week_harmonic
        
        #domain is a dictionary of lists keyed on categorical item names
        #the list contains that values of the categoricals 
        self.domain = {}
 
        
    def get_data(self,start_ts=None,end_ts=None,entities=None):
        
        skewed_domain = {}
        for d in self.categoricals:
            try:
                self.domain[d]
            except KeyError:
                self.domain[d] = self._generate_domain_values(d)
            skewed_domain[d] = self.domain[d]
            max_items = min([5,len(skewed_domain[d])])
            for item_number, item in enumerate(skewed_domain[d][:max_items]):
                skewed_domain[d].extend([item for x in list(range(item_number))])
            
        
        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=self.days)
        start = start - dt.timedelta(seconds=self.seconds)
        
        ts = pd.date_range(end=end,start=start,freq=self.freq)
        y_cols = []
        y_cols.extend(self.metrics)
        y_cols.extend(self.dates)
        y_count = len(y_cols)
        rows = len(ts)
        noise = np.random.normal(0,1,(rows,y_count))
        
        df = pd.DataFrame(data=noise,columns=y_cols)
        df[self._entity_id] = np.random.choice(self.ids, rows)
        df[self._timestamp] = ts
        
        days_from_ref = (df[self._timestamp] - self.ref_date).dt.total_seconds() / (60*60*24)
        day = df[self._timestamp].dt.day
        day_of_week = df[self._timestamp].dt.dayofweek
        
        for m in y_cols:
            try:
                df[m] = df[m] * self.sd[m]
            except KeyError:
                pass            
            df[m] = df[m] + days_from_ref * self.increase_per_day
            df[m] = df[m] + np.sin(day*4*math.pi/364.25) * self.day_harmonic
            df[m] = df[m] + np.sin(day_of_week*2*math.pi/6) * self.day_of_week_harmonic
            try:
                df[m] = df[m] + self.mean[m]
            except KeyError:
                pass
            
        for d in self.categoricals:
            df[d] = np.random.choice(skewed_domain[d], len(df.index))
            
        for t in self.dates:
            df[t] = dt.datetime.utcnow() + pd.to_timedelta(df[t],unit = 'D')
            df[t] = pd.to_datetime(df[t])
            
        df.set_index([self._entity_id,self._timestamp])
        msg = 'Generated %s rows of time series data from %s to %s' %(rows,start,end)
        logger.debug(msg)
        
        return df
    
    def execute(self,df=None):
        df = self.get_data()
        return df
    
    def _generate_domain_values(self,item_name):
        
        if item_name in ['company','company_id','company_code']:
            return ['ABC','ACME','JDI']
        elif item_name in ['country','country_id','country_code']:
            return ['US','CA','UK','DE']
        else:
            #build a domain from the characters contained in the name
            #this way the domain will be consistent between executions
            domain = []
            for d in list(item_name):
                domain.extend(['%s%s'%(d,x) for x in item_name])
            return  domain
        
    
    def set_mean(self,metric,mean):
        '''
        Set the mean value of generated numeric item
        '''        
        self.mean[metric] = mean
        
    def set_sd(self,metric,sd):
        '''
        Set the standard deviation of a generated numeric item
        '''                
        self.sd[metric] = sd
        
    def set_domain(self,item_name,values):
        '''
        Set the values for a generated categorical item
        '''
        self.domain[item_name] = values
    



