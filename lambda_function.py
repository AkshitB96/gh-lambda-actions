import json
from csv import reader 
import csv
import boto3
import os
import datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    datestamp = dt.datetime.now().strftime("%Y/%m/%d")
    timestamp = dt.datetime.now().strftime("%s")
    
    filename_json = "/tmp/file_{ts}.json".format(ts=timestamp)
    filename_csv = "/tmp/file_{ts}.csv".format(ts=timestamp)
    keyname_parquet = "{ts}.parquet".format(ds=datestamp, ts=timestamp)
    keyname_s3 = "{ts}.json".format(ds=datestamp, ts=timestamp)
    
    json_data = []
    uid = []
    today = dt.datetime.now().strftime("%x")
    new_bucket_name = "emaildatajson"

    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key_name = record['s3']['object']['key']
        
    s3_object = s3.get_object(Bucket=bucket_name, Key=key_name)
    data = s3_object['Body'].read()
    contents = data.decode('utf-8')
    
    # resp = s3.get_object(Bucket=bucket_name, Key=key_name)
    # data = resp['Body'].read().decode('utf-8')
    
    # df = pd.read_csv(s3_object)
    # print(df.to_parquet())
    
    # # df = pd.read_csv(list(reader(data)))
    # print(data)
    
   
    
    with open(filename_csv, 'a') as csv_data:
        csv_data.write(contents)
        # print(contents)
    
    with open(filename_csv) as csv_data:
        csv_reader = csv.DictReader(csv_data)
        for csv_row in csv_reader:
            json_data.append(csv_row)
            uid.append(csv_row['Number'])
           
    
    i = 0
    
    with open(filename_json, 'w') as json_file:
        json_file.write(json.dumps(json_data))
        
        df = pd.DataFrame({'Partner_Name': key_name,
                      			'Date': today,
                        		'UID': uid,
                                'Data': json_data
        })
       
        # print(uid)
    
    
    # with open(filename_json, 'r') as json_file_contents:
    #     response = s3.put_object(Bucket=new_bucket_name, Key=keyname_s3, Body=json_file_contents.read())
        
    """ Write a dataframe to a Parquet on S3 """
    # print("Writing {} records to {}".format(len(df), filename))
    output_file = f"s3://{new_bucket_name}/{keyname_parquet}"
    # df.to_parquet(output_file)
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file, compression='snappy')
    
    # buffer = io.BytesIO()
    # s = boto3.resource('s3')
    # object = s.Object('emaildatajson','1648106453.parquet')
    # object.download_fileobj(buffer)
    # dr = pd.read_parquet(buffer)

    # print(dr.head())
    
    os.remove(filename_csv)
    os.remove(filename_json)
    
    # print(df)

    return {
        'statusCode': 200,
        'body': json.dumps('CSV converted to Parquet and available at: {bucket}/{key}'.format(bucket=new_bucket_name,key=keyname_parquet))
    }