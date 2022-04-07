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
from io import StringIO
from io import BytesIO
import mysql.connector
from mysql.connector import Error

try:
    mydb = mysql.connector.connect(
    host="aapop8rg6szuzt.cfcfombhbl6n.ap-south-1.rds.amazonaws.com",
    user="onecode",
    password="Asdx#123",
    database="ebdb" 
    )
    print("MySQL Database connection successful")
except Error as err:
    print(f"Error: '{err}'")



# print(mydb)

# cur = mydb.cursor()

# cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ebdb'")

# for table in [tables[0] for tables in cur.fetchall()]:
#     print(table)

# s3 = boto3.client('s3')
# s3_resource = boto3.resource('s3')

# def lambda_handler(event, context):
    
datestamp = dt.datetime.now().strftime("%Y/%m/%d")
timestamp = dt.datetime.now().strftime("%s")
    
#     try:
#         for record in event['Records']:
#             bucket_name = record['s3']['bucket']['name']
#             keyName = record['s3']['object']['key']
#     except Exception:
#         raise Exception
        
csv_fileName = "Paytm Money_Report - sheet 1.csv"
partnerID = ""
pcategories = []
partnerName = csv_fileName.split("_")[0]

# print("SELECT id FROM partner WHERE fullname = %s",(partnerName))
mycursor = mydb.cursor()
pIDQuery = """SELECT id FROM partner WHERE fullname=%s"""
partnerName = (partnerName,)
mycursor.execute(pIDQuery,partnerName)
partnerID = mycursor.fetchall()



# partnerID = mydb.ebdb




#     key_name_space = keyName.replace("+"," ")
#     key_name = keyName.lower().replace(" ","").split("-")[0]

#     keyname_parquet = "{kn}/{ds}/{ts}.parquet".format(ds=datestamp, ts=timestamp, kn = key_name)
    
#     uid = []
    
#     today = dt.datetime.now().strftime("%Y/%m/%d")
#     new_bucket_name = "emaildatajson"
    
#     obj = s3.get_object(Bucket=bucket_name, Key=key_name_space)
#     data = pd.read_csv(obj['Body'])
    
#     js = data.to_json(orient='records', lines=True).splitlines()
    
    
#     uid = pd.DataFrame(data)
    
#     df = pd.DataFrame({
#         'data' : js,
#         'Partner_Name' : key_name,
#         'Date' : today
#     })

#     df["UID"] = uid.Mobile
    
#     output_file = f"s3://{new_bucket_name}/{keyname_parquet}"
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, output_file, compression='snappy')
    
#     print('CSV converted to Parquet and available at: {bucket}/{key}'.format(bucket=new_bucket_name,key=keyname_parquet))

#     return {
#         'statusCode': 200,
#         'body': json.dumps('CSV converted to Parquet and available at: {bucket}/{key}'.format(bucket=new_bucket_name,key=keyname_parquet))
#     }