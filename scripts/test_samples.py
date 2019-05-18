import json
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
import iotfunctions.sample as sample

'''

This script tests the sample functions locally

'''

with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

db_schema = None
db = Database(credentials=credentials)

cols = [
    Column('company',String),
    Column('status',String)
]

samples = [
    sample.CompanyFilter(company_code='company', company='ACME', status_flag='is_filtered'),
    sample.MultiplyTwoItems(input_item_1='qty', input_item_2='unit_price', output_item='price'),
    sample.MultiplyColumns(input_items=['quantity', 'unit_price', 'discount']),
    sample.MergeSampleTimeSeries(input_items=['temp', 'pressure', 'velocity'],
                                 output_items=['temp', 'pressure', 'velocity']),
    sample.MultiplyByFactor(input_items=['load', 'rating'], factor=0.9,
                            output_items=['adjusted_load', 'adjusted_rating']),
    sample.StatusFilter(status_input_item='status',include_only='active',output_item='is_filtered')
]

for s in samples:
    s.execute_local_test(db=db, columns=cols)
