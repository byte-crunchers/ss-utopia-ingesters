import boto3
import os
import json
from stock_ingester import upload_dir

def count_objects(bucket, prefix):
    count_obj = 0
    for i in bucket.objects.filter(Prefix=prefix):
        count_obj = count_obj + 1
    return count_obj

def test_upload_dir():
    f = open('awskey.json', 'r')
    key = json.load(f)      
    session = boto3.session.Session(aws_access_key_id=key['access_key'], aws_secret_access_key=key['secret_key'])
    s3 = session.resource('s3')
    bucket = s3.Bucket('data-datalake-1')
    bucket.objects.filter(Prefix="stock/test/").delete()
    assert count_objects(bucket, "stock/test/") == 0
    upload_dir('test/')
    assert count_objects(bucket, "stock/test/") == 40
    bucket.objects.filter(Prefix="stock/test/").delete()
    assert count_objects(bucket, "stock/test/") == 0
    
    
