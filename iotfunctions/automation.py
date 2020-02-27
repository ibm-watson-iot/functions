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
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def register(module, db):
    '''
    Automatically register all functions in a module. To be regiserable, a function
    must have an class variable containing a dictionary of arguments (auto_register_args)
    '''

    for (name, cls) in inspect.getmembers(module):
        try:
            kwargs = cls.auto_register_args
        except AttributeError:
            kwargs = None

        if not kwargs is None:
            stmt = 'mod.%s(**%s)' % (name, kwargs)
            instance = eval(stmt)
            instance.db = db
            df = instance.get_test_data()
            instance.register(df=df)


class CategoricalGenerator(object):
    '''
    Generate categorical values.
    
    Parameters
    ----------
    name: str
        name of categorical item
    categories: list of strings (optional)
        domain of values. if none give, will use defaults or generate random
    '''

    # most commonly occuring value
    _mode = 5

    def __init__(self, name, categories=None, weights=None):

        self.name = name
        if categories is None:
            categories = self.get_default_categories()
        self.categories = categories
        if weights is None:
            weights = np.random.normal(1, 0.1, len(self.categories))
            mode = min(self._mode, len(self.categories))
            for i in list(range(mode)):
                weights[i] = weights[i] + i
            weights = weights / np.sum(weights)
        self.weights = weights

    def get_default_categories(self):
        '''
        Return sample categoricals for a few predefined item names or generate
        '''

        if self.name in ['company', 'company_id', 'company_code']:
            return ['ABC', 'ACME', 'JDI']
        if self.name in ['plant', 'plant_id', 'plant_code']:
            return ['Zhongshun', 'Holtsburg', 'Midway']
        elif self.name in ['country', 'country_id', 'country_code']:
            return ['US', 'CA', 'UK', 'DE']
        elif self.name in ['firmware', 'firmware_version']:
            return ['1.0', '1.12', '1.13', '2.1']
        elif self.name in ['manufacturer']:
            return ['Rentech', 'GHI Industries']
        elif self.name in ['zone']:
            return ['27A', '27B', '27C']
        elif self.name in ['status', 'status_code']:
            return ['inactive', 'active']
        elif self.name in ['operator', 'operator_id', 'person', 'employee']:
            return ['Fred', 'Joe', 'Mary', 'Steve', 'Henry', 'Jane', 'Hillary', 'Justin', 'Rod']
        else:
            # build a domain from the characters contained in the name
            # this way the domain will be consistent between executions
            domain = []
            for d in list(self.name):
                domain.extend(['%s%s' % (d, x) for x in self.name])
            return domain

    def get_data(self, rows):
        '''
        generate array of random categorical values
        '''
        return np.random.choice(self.categories, rows, p=self.weights)


class DateGenerator(object):
    '''
    Generate a array of random dates
    
    '''
    _default_future_days = 0
    _default_past_days = 30

    def __init__(self, name=None, start_date=None, end_date=None):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date

    def get_data(self, rows):
        days = list(np.random.uniform(-self._default_past_days, self._default_future_days, rows))
        dates = [dt.datetime.utcnow() + dt.timedelta(days=x) for x in days]
        return dates


class MetricGenerator(object):
    '''
    Generate a array of random numbers
    
    Will support predefined names in the future with named ranges in case
    you are wondering why this is needed
    
    '''
    _default_future_days = 0
    _default_past_days = 30

    def __init__(self, name=None, mean=0, sd=1):
        self.name = name
        self.mean = mean
        self.sd = sd

    def get_data(self, rows):
        metrics = np.random.normal(self.mean, self.sd, rows)

        return metrics


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
    datasource = df
        Pandas dataframe as source
    datasourcemetrics : list of strings
        Names of numeric items to take from source
    '''

    ref_date = dt.datetime(2018, 1, 1, 0, 0, 0, 0)
    _timestamp = 'evt_timestamp'
    _entity_id = 'deviceid'

    def __init__(self, metrics=None, ids=None, days=0, seconds=300, freq='1min', categoricals=None, dates=None,
                 increase_per_day=0.0001, noise=0.1, day_harmonic=0.2, day_of_week_harmonic=0.2, timestamp=None,
                 domains=None, datasource=None, datasourcemetrics=None):

        if timestamp is not None:
            self._timestamp = timestamp

        if metrics is None:
            metrics = ['x1', 'x2', 'x3']

        if categoricals is None:
            categoricals = []

        if dates is None:
            dates = []

        self.metrics = metrics
        self.categoricals = categoricals

        dates = [x for x in dates if x != self._timestamp]

        self.dates = dates

        if ids is None:
            ids = ['sample_%s' % x for x in list(range(10))]
        self.ids = ids

        self.days = days
        self.seconds = seconds
        self.freq = freq
        # optionally scale using dict keyed on metric name
        self.data_item_mean = {}
        self.data_item_sd = {}

        self.increase_per_day = increase_per_day
        self.noise = noise
        self.day_harmonic = day_harmonic
        self.day_of_week_harmonic = day_of_week_harmonic

        # domain is a dictionary of lists keyed on categorical item names
        # the list contains that values of the categoricals
        if domains is None:
            domains = {}
        self.domain = domains

        self.datasource = datasource
        self.datasourcemetrics = datasourcemetrics

    def get_data(self, start_ts=None, end_ts=None, entities=None):

        end = dt.datetime.utcnow()
        start = end - dt.timedelta(days=self.days)
        start = start - dt.timedelta(seconds=self.seconds)

        if self.datasource is not None:
            if isinstance(self.datasource.index.dtype, pd.DatetimeIndex):
                ts = self.datasource.index.copy()
            elif self._timestamp in self.datasource.columns:
                self.datasource = self.datasource.set_index(self._timestamp).sort_index()
                ts = self.datasource.index
            else:
                ts = pd.date_range(end=end, freq=self.freq, periods=self.datasource.index.size)
        else:
            ts = pd.date_range(end=end, start=start, freq=self.freq)

        y_cols = []
        y_cols.extend(self.metrics)
        y_cols.extend(self.dates)
        y_count = len(y_cols)
        rows = len(ts)
        noise = np.random.normal(0, 1, (rows, y_count))

        df = pd.DataFrame(data=noise, columns=y_cols)

        df[self._entity_id] = np.random.choice(self.ids, rows)
        df[self._timestamp] = ts
        df[self._timestamp] = df[self._timestamp].astype('datetime64[ms]')

        days_from_ref = (df[self._timestamp] - self.ref_date).dt.total_seconds() / (60 * 60 * 24)
        day = df[self._timestamp].dt.day
        day_of_week = df[self._timestamp].dt.dayofweek

        for m in y_cols:
            try:
                df[m] = df[m] * self.data_item_sd[m]
            except KeyError:
                pass
            if self.datasourcemetrics is not None and (m in self.datasourcemetrics):
                try:
                    df[m] = self.datasource[m].to_numpy()
                    print('assigned column ' + m)
                except KeyError:
                    df[m] = 0
                    pass
            else:
                df[m] = df[m] + days_from_ref * self.increase_per_day
                df[m] = df[m] + np.sin(day * 4 * math.pi / 364.25) * self.day_harmonic
                df[m] = df[m] + np.sin(day_of_week * 2 * math.pi / 6) * self.day_of_week_harmonic
                try:
                    df[m] = df[m] + self.data_item_mean[m]
                except KeyError:
                    pass

        for d in self.categoricals:
            try:
                categories = self.domain[d]
            except KeyError:
                categories = None
            gen = CategoricalGenerator(d, categories=categories)
            df[d] = gen.get_data(len(df.index))

        for t in self.dates:
            df[t] = dt.datetime.utcnow() + pd.to_timedelta(df[t], unit='D')
            df[t] = pd.to_datetime(df[t])

        msg = 'Generated %s rows of time series data from %s to %s' % (rows, start, end)
        logger.debug(msg)

        return df

    def execute(self):
        df = self.get_data()
        return df

    def set_mean(self, metric, mean):
        '''
        Set the mean value of generated numeric item
        '''
        self.data_item_mean[metric] = mean

    def set_sd(self, metric, sd):
        '''
        Set the standard deviation of a generated numeric item
        '''
        self.data_item_sd[metric] = sd

    def set_domain(self, item_name, values):
        '''
        Set the values for a generated categorical item
        '''
        self.domain[item_name] = values

    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key, value in list(params.items()):
            setattr(self, key, value)
        return self
