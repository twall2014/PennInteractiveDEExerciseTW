import pytest
import os
import boto3
from botocore.config import Config
from nhldata.app import NHLApi, StorageKey, Storage, Crawler
from datetime import datetime

api = NHLApi()
s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
dest_bucket = os.environ.get('DEST_BUCKET', 'output')
storage = Storage(dest_bucket, s3client)
crawler = Crawler(api, storage)

# TODO feel free to add tests for anything as you work things out

def test_single_day_load():
    # test to see how the API handles single days.
    startDate = datetime(2022,1,1)
    endDate = datetime(2022,1,1)
    api_query = api.schedule(startDate,endDate)
    assert(len(api_query['dates']) == 1)

def test_multi_day_load():
    # test to see how the API handles multi-day date ranges.
    startDate = datetime(2022,1,1)
    endDate = datetime(2022,1,3)
    api_query = api.schedule(startDate,endDate)
    assert(len(api_query['dates']) == 3)

def test_longer_multi_day_load():
    # test to see how the API handles date ranges beyond 2 weeks.
    startDate = datetime(2022,1,1)
    endDate = datetime(2022,1,21)
    api_query = api.schedule(startDate,endDate)
    assert(len(api_query['dates']) == 21)

def test_blank_dates_load():
    # tests to see if app tries to load date ranges in which there are 0 games. 
    startDate = datetime(2022,2,3)
    endDate = datetime(2022,2,4)
    api_query = api.schedule(startDate,endDate)
    for date in api_query['dates']:
        assert(len(date['games'] == 0))

def test_invalid_date_range():
    startDate = datetime(2022,1,3)
    endDate = datetime(2022,1,1)
    api_query = api.schedule(startDate,endDate)
    assert(len(api_query['dates']) > 200) # it does this when dates are reversed for some reason?

