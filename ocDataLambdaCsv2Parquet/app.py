import json
import boto3
import datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lib import db_config as db
import numpy as np
import logging

# setting up logger
log = logging.getLogger()
log.setLevel(logging.INFO)

# connecting to s3
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    
    datestamp = dt.datetime.now().strftime("%Y/%m/%d")
    timestamp = dt.datetime.now().strftime("%s")
    today = dt.datetime.now().strftime("%Y/%m/%d")
    new_bucket_name = "emaildatajson"
    uid = []
    key_name_space = ""
    key_name = ""
    chunk_list = []
    partnerID = ""
    
    try:
        # connection to DB
        try:
            conn = db.mydb
            log.info('connected to db')
        except Exception as e:
            log.error(e)
            raise Exception("Connection Not established with DB");
            
            
        # Getting s3 bucket and file name
        try:
            for record in event['Records']:
                bucket_name = record['s3']['bucket']['name']
                keyName = record['s3']['object']['key']
                log.info('%s is being processed',keyName)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception("Failed to get Bucket or File name")
        
        try:
            key_name_space = keyName.replace("+"," ")
            key_path_name = keyName.lower().split("-")[0]
            key_path_name_parquet = keyName.lower().replace(" ","").split("-")[0]
            key_name = key_path_name.split("/")[2].replace("+"," ")
            keyname_parquet = "{kn}/{ds}/{ts}.parquet".format(ds=datestamp, ts=timestamp, kn = key_path_name_parquet)
            
            log.info('%s partner name passed to query',key_name)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception('Failed to get proper filenames for %s where these are file names %s --> %s --> %s',keyName,key_name_space,key_path_name,key_name)
        
        # Query to get Partner ID by Name
        try:
            partnerName = key_name
            # partnerName = "dhan"
            mycursor = db.mydb.cursor()
            pIDQuery = """SELECT id FROM partner WHERE fullname=%s"""
            mycursor.execute(pIDQuery,partnerName)
            pID = mycursor.fetchall()
            partnerID = pID[0][0]
            log.info('partner id is %s for the file %s',partnerID,keyName)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception("Fail to get partner ID")
        
        # query to get unique column for csv from partner meta data
        try:
            pSchemaQuery = """SELECT attr_name,attr_code FROM partner p
                        JOIN partner_category pc ON p.id = pc.partner_id
                        JOIN partner_report_metadata prm ON prm.partner_category_id = pc.id
                        WHERE p.id=%s AND should_match = 1"""
            mycursor.execute(pSchemaQuery,partnerID)
            partnerMetaData = mycursor.fetchall()
            csvUniqueColName = partnerMetaData[0][0]
            uniqueAttrCode = partnerMetaData[0][1]
            log.info('Unique col name and code %s, %s for file %s',csvUniqueColName,uniqueAttrCode,keyName)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception("Fail to get unique column name")
        
        # get csv data and to json
        obj = s3.get_object(Bucket=bucket_name, Key=key_name_space)
        data = pd.read_csv(obj['Body'], chunksize=10000)
        
        try:
            # dividing csv data in chunks
            for chunk in data:
                
                # removing duplicates from chunk
                chunk1 = chunk.drop_duplicates(subset = csvUniqueColName).sort_values(by=[csvUniqueColName], ascending=True)
                
                # converting chunk data to json to store in data
                js = chunk1.to_json(orient='records', lines=True).splitlines()
                
                # temp dataframe to get UID column
                uid = pd.DataFrame(chunk1)
                
                # creating data for query
                uniqueColDataFromCSV = uid[csvUniqueColName].astype(str).values.flatten().tolist()
                queryFormatedData = ",".join(uniqueColDataFromCSV)
                
                # queries to get user id and cx id
                try:
                    if uniqueAttrCode == "phnNo":
                        cxAgentDetailsQuery = "SELECT DISTINCT c.id, c.user_id, c.phone_number FROM oc_customer c JOIN oc_user_form ouf ON c.id = ouf.oc_customer_id JOIN oc_partner_form opf ON ouf.oc_partner_form_id = opf.id WHERE c.phone_number IN ({}) AND opf.partner_id = {}".format(queryFormatedData,partnerID)
                        mycursor.execute(cxAgentDetailsQuery)
                        da = mycursor.fetchall()
                        dq = np.asarray(da)
                        
                    elif uniqueAttrCode == "id":
                        cxAgentDetailsQuery = "SELECT DISTINCT c.id,c.user_id,phone_number FROM oc_user_form uf JOIN oc_customer c on uf.oc_customer_id = c.id WHERE uf.id IN ({})".format(queryFormatedData)
                        mycursor.execute(cxAgentDetailsQuery)
                        da = mycursor.fetchall()
                        dq = np.asarray(da)
                        
                    elif uniqueAttrCode == "email":
                        colquery = "email"
                        queryFormatedData = ','.join(map(repr,uniqueColDataFromCSV))
                        cxAgentDetailsQuery = "SELECT DISTINCT ouf.oc_customer_id, ouf.user_id, ocv.value FROM oc_form ocf JOIN oc_field_group ocfg ON ocf.id = ocfg.oc_form_id JOIN oc_field ocfd ON ocfg.id = ocfd.oc_field_group_id JOIN oc_field_value ocv ON ocfd.id = ocv.oc_field_id JOIN oc_user_form ouf ON ocv.oc_user_form_id = ouf.id JOIN oc_customer oc ON ouf.oc_customer_id = oc.id WHERE ocf.name = '{}' AND ocfd.name = '{}' AND ocv.value IN ({})".format(partnerName,colquery,queryFormatedData)
                        mycursor.execute(cxAgentDetailsQuery)
                        da = mycursor.fetchall()
                        dq = np.asarray(da)
                    else:
                        print('Not specified query for the partner')
                except Exception as e:
                    log.error("For file %s error is %s",keyName,e)
                    raise Exception("Unable to get cx_id and user_id")
                    
                try:
                    # dataframe for csv rows
                    df = pd.DataFrame({
                    'data' : js,
                    'Partner_Name' : key_name,
                    'Date' : today,
                    'UID' : uid[csvUniqueColName]
                    })
                    
                    # dataframe for cx and user id for the particualr partner
                    dg = pd.DataFrame({
                        'cx_id' : dq[:,0],
                        'user_id' : dq[:,1],
                        'UID' : dq[:,2]
                    })
                    
                    # merging both dataframes
                    fda = pd.merge(df,dg,how = 'outer', on = 'UID')
                    chunk_list.append(fda)
                except Exception as e:
                    log.error("For file %s error is %s",keyName,e)
                    raise Exception("Failed to write to dataframe")
            log.info('got chunk data for file %s',keyName)
            log.info('dataframe created for the file %s',keyName)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception("Fail to convert to JSON")
        df_concat = pd.concat(chunk_list)

        log.info('complete dataframe concatenated for file %s',keyName)
        
            
        # writing a new file and storing in S3 in parquet form
        try:
            output_file = f"s3://{new_bucket_name}/{keyname_parquet}"
            table = pa.Table.from_pandas(df_concat)
            pq.write_table(table, output_file, compression='snappy')
            log.info('Output parquet file %s written for %s',output_file,keyName)
        except Exception as e:
            log.error("For file %s error is %s",keyName,e)
            raise Exception("fail to write parquet file to s3 bucket")
        
        print('CSV converted to Parquet and available at: {bucket}/{key}'.format(bucket=new_bucket_name,key=keyname_parquet))
        
        return {
            'statusCode': 200,
            'body': json.dumps('CSV converted to Parquet and available at: {bucket}/{key}'.format(bucket=new_bucket_name,key=keyname_parquet))
        }  
    except Exception as e:
        log.error("For file %s error is %s",keyName,e)
        return {
            'statusCode': 500,
            'body': "internal server error"
        } 
