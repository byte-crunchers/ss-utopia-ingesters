import boto3
import os
import json

def upload_dir(subdir=''):
    f = open('awskey.json', 'r')
    key = json.load(f)      
    s3 = boto3.client('s3', aws_access_key_id=key['access_key'], aws_secret_access_key=key['secret_key'])
    stock_dir = './dummy_data/stock folder'
    for entry in os.scandir(stock_dir):
        s3.upload_file(entry.path, "data-datalake-1", 'stock/{}{}'.format(subdir, entry.name))
        #print (entry.path)
        #print (entry.name)
        #print()

if __name__ == "__main__":
    upload_dir('')