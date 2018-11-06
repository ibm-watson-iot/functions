# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import datetime as dt
import pandas as pd

class ShiftCalendar(object):
    '''
    Generate data for a shift calendar using a shift_definition in the form of a dict keyed on shift_id
    Dict contains a tuple with the start and end hours of the shift expressed as numbers. Example:
          {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 5.5)
           },    
    '''
    def __init__ (self,shift_definition=None):
        if shift_definition is None:
            shift_definition = {
               "1": (5.5, 14),
               "2": (14, 21),
               "3": (21, 5.5)
           }
        self.shift_definition = shift_definition
    
    def get_data(self,start_date,end_date):
        start_date = start_date.date()
        end_date = end_date.date()
        dates = pd.DatetimeIndex(start=start_date,end=end_date,freq='1D').tolist()
        dfs = []
        for shift_id,start_end in list(self.shift_definition.items()):
            data = {}
            data['shift_day'] = dates
            data['shift_id'] = shift_id
            data['end_date'] = [x+dt.timedelta(hours=start_end[1]) for x in dates]
            index = [x+dt.timedelta(hours=start_end[0]) for x in dates]
            dfs.append(pd.DataFrame(data, index = index))
        df = pd.concat(dfs)
        df.index = pd.to_datetime(df.index)
        df['end_date'] = pd.to_datetime(df['end_date'])
        return df
            
            
            
    
    