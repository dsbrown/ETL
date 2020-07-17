#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import config  # Global Settings
import boto.rds
import MySQLdb
import time


# Connect to RDS 
conn = boto.rds.connect_to_region("us-west-2")
instances = conn.get_all_dbinstances()

instance = conn.get_all_dbinstances(config.db_instance)
db = instance[0]
endpoint,port = db.endpoint

print "Connecting to MySQL using MySQLdb: ", "host="+endpoint+","+"user="+config.db_user+","+"passwd="+config.db_password+","+"db="+config.db_name+","+"port="+str(port)
mydb=MySQLdb.connect(host=endpoint,user=config.db_user,passwd=config.db_password,db=config.db_name,port=port)
print "Connected to MySQL: "
cursor = mydb.cursor()

query = "SELECT * FROM SiteTbl"

cursor.execute(query)
print "SiteTbl Table"
row = cursor.fetchone()
while row is not None:
    print "------------------------------------------------------------------"
    print (row)
    row = cursor.fetchone()

print "======================================================================"
print "Assessment Table"
query = "SELECT * FROM AssessmentResultsTbl"
cursor.execute(query)
print "AssessmentResultsTbl Table"
row = cursor.fetchone()
while row is not None:
    print "------------------------------------------------------------------"
    print (row)
    row = cursor.fetchone()

cursor.close()
mydb.close()
conn.close()
