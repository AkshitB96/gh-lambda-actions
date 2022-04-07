import pymysql
import sys
import os

#rds settings
rds_host  = "analyticsdb010422.cfcfombhbl6n.ap-south-1.rds.amazonaws.com"
name = "dummy"
password = "Localhost@1"
db_name = "ebdb"

try:
    mydb = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except Exception as e:
    raise Exception("DB connection not established")
    sys.exit()

print("SUCCESS: Connection to RDS MySQL instance succeeded")


