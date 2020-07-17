#!/usr/local/bin/python
# -*- coding: utf-8 -*-

###################################################################################
#
#                          CSV to RDS SQL Program
#
# Description:
# Given a CSV file with the first (header) row being keys and the following rows
# being the values that correspond to the keys. Create corresponding records in 
# a mysql database on RDS and insert the data into a prior defined database.
# The key value pairs are inserted into dicts and we create an array of dicts
# for corresponding to each row of the csv spread sheet.
#
# Known Problems:
# This program, when its working will be fine for a first insert, but it doesn't
# check to see if a site or assesment is pre-existing and update it, it just 
# happily duplicates the records.
# 
#
# Author: David S. Brown
# v1.0  DSB     Create csv2sql
#
####################################################################################

import argparse
import sys
import csv
import config  # Global Settings
import uuid
import boto.rds
import MySQLdb
import time
import logging, logging.config
from datetime import date, datetime, timedelta
logging.config.dictConfig(config.LOGGING)


parser = argparse.ArgumentParser(
    description='Create records in a mysql database on RDS and insert the data from'\
                'a predefined and constructed database'
    )

####################################################################################
#
#                               Create Arguments
#
####################################################################################

# Count of verbose flags such as: arg_parse.py -v, arg_parse.py -vv, arg_parse.py -vvv, etc
parser.add_argument("-v", "--verbose", action="count", default=0, help="increase output verbosity, >= 3=Debug")

# Files to open, one by one as in csvparse.py -f foo.txt bar.txt fu.txt 
parser.add_argument("-f", "--files", nargs="+", dest="mfiles", help="file names to open separated by spaces")

# You may optionally change the delimeter and quote character
parser.add_argument('-d', '-delimiter', default=",", help="Optionally specify the field separation delimiter used, the default is ','")
parser.add_argument('-q', '-quote_delimiter', default='"', help="Optionally specify the quote delimiter that is being used, the default is \"")

parser.add_argument('--overwrite_site', action='store_true', default=False, help="If it encounters a duplicate site entry it will overwrite it with the data in the csv file. This is dangerious use with care.")
parser.add_argument('--overwrite_assessment', action='store_true', default=False, help="If it encounters a duplicate assessment entry it will overwrite it with the data in the csv file. This is dangerious use with care.")

# Prints help if no arguments
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)
####################################################################################
#
#                                Functions
#
####################################################################################

# is_it_a_number - Looks at a string and tries to interpret if it is an INT, FLOAT or STR
def is_it_a_number(text):
    try:
        int(text)
        return 'INT'
    except ValueError:
        pass
    try:
        float(text)
        return 'FLOAT'
    except ValueError:
        pass
    return 'STR'

# sql_action_statement - build SQL statement to put data into table
#   table is the SQL table to be effected
#   d data is a dictionary hash, walk through it and build SQL statement
#   action is 'INSERT' or 'REPLACE' depending on the desired side effect
#   INSERT is for new entries and will error if the entry exists
#   REPLACE will UPDATE an existing entry by replacing all matching fields
def sql_action_statement(table,d,action):
    sql_keys =action+" INTO "+table+" ("
    sql_data = "Values ("
    for key in d.keys():
        sql_keys+=key   
        sql_data+="'"+str(d[key])+"'"
        sql_keys+=", "
        sql_data+=", "
    sql_keys=sql_keys[:-2] # removes the final ", " 
    sql_data=sql_data[:-2]
    sql_data+=") "  
    sql_keys+=") "
    return sql_keys+sql_data

# valid_dicts -This validates the fields and the data in d (array of
# ditionaries)  and hands back another   array of type d with only field names
# that match the database field names  and data that is actually valid for the
# database field type  it silently throws away fields that don't match and THIS
# IS IMPORTAINT  it adds the SiteUUID to all records it returns, this is so we
# can later  reconnect the entries if we need to, otherwise we lose the
# connection  between the data.  field definitions are in config

def valid_dicts(d,field_desc): #  (array of dicts, dict of valid fields and types)
    dreturn = []
    valid = field_desc.keys()

    for record in d:  #Again with the list of dictonaries ...
        e = {}
        if not record['SiteUUID'] : 
            e['SiteUUID'] = record['SiteUUID'] 
            print("Preserved SiteUUID")
        for key in record.keys():
            if key in valid:
                #examine and validate data in each field
                if field_desc[key] ==  'INT':
                    if str(record[key]) in ('x','X','1'):
                        e[key]='1'
                    elif str(record[key]) in ('','0'):
                        e[key]='0'
                    elif is_it_a_number(record[key]) in ('INT','FLOAT'):
                        e[key]=str(record[key])
                    else:
                        e[key]='1'
                        logger.warning("Warning, INT: "+str(record[key])+" converted to 1")
                elif field_desc[key] in ('DOUBLE', 'FLOAT'):
                    if is_it_a_number(record[key]) in ('INT','FLOAT'):
                        e[key]=str(record[key])
                    else:
                        logger.warning( "Warning, FLOAT: "+str(record[key])+" wants to be a number, but cant be")
                elif field_desc[key] in ('DATETIME'):  # date format typicaly: 5/6/2015
                    mydate = time.strptime(record[key], '%m/%d/%y')
                    #YYYY-MM-DD HH:MM:SS
                    e[key] = time.strftime("%Y-%m-%d %H:%M:%S",mydate)
                else:
                    e[key]=record[key]
        dreturn.append(e)
    return dreturn

# build_query - builds a SQL search query based on the search_fiels passed
#   to it and getting values from the dictionry record
def build_query(record,search_fields): #Builds a SQL search query
    c=0
    query=""
    for i in search_fields:
        if record.get(i):   #.get - doesn't error if if doesn't exist
            if c:
                query+=" AND "
            c+=1
            query+=i+" = '"+record[i]+"'"
    return query

# write_record - Executes cursor on the SQL statment
#    the record to the database
#   takes s the SQL statement to do the insert 
#   returns 1 if success and sets record_id to the record id written,
#   returns 0 if failure and sets record_id = -1 
def write_record(s,record_id):
    try:
        logger.debug("SQL Statement: %s", s)
        affected_count = cursor.execute(s)
        mydb.commit()
        logger.info ("Wrote %s records ",str(affected_count))
        record_id = cursor.lastrowid
        return 1
    except MySQLdb.IntegrityError:
        logger.error( "failed to insert values, IntegrityError")
        record_id = -1
        return 0


####################################################################################
#
#                                   Main
#
####################################################################################
#
# Slurp up the csv files into an array of dicts
# Add a Site UUID if one doesn't exist already
# 

args = parser.parse_args()
logger = logging.getLogger(__name__)

if args.verbose == 1:
    logging.basicConfig(level=logging.INFO)
elif args.verbose > 1:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logger.debug("args: %s", args)

# Slurp up the csv files into an array of dicts
filesProcessed = []
data = []
if args.mfiles:
    for mfile in args.mfiles:
    
        logger.debug("filename: %s", mfile)

        o = open(mfile,'rU')    #It will throw an error if not opened up in U mode
        reader = csv.DictReader(o)
        for row in reader:
            if not row['SiteUUID']  :
                row['SiteUUID'] = uuid.uuid4()   # Add a SiteUUID for every dict in the array according to RFC 4122, uuid4 is more secure than uuid1
                logger.debug("creating UUID")
            else
                logger.debug("skipping UUID")
            data.append(row)
            #logger.debug("Row %s",row)
    filesProcessed.append(mfile)
logger.info("Files procesed: " + str(filesProcessed))

# Validate fields per table, only  fields that are part 
# of this table, toss all others out

SiteTbl_dicts=valid_dicts(data,write_record)
#logger.debug("Site Dictionary: " + str(SiteTbl_dicts))
    
AssessmentResultsTbl_dicts=valid_dicts(data,config.AssessmentResultsTbl_desc)
#logger.debug("Assessment Dictionary: " + str(AssessmentResultsTbl_dicts))

# AuditResultsTbl Not implemented because no csv data at this time this will probably will be a seperate program

# Connect to RDS 
conn = boto.rds.connect_to_region("us-west-2")
instances = conn.get_all_dbinstances()
logger.debug( "Instances: " + str(instances))
instance = conn.get_all_dbinstances(config.db_instance)
db = instance[0]
endpoint,port = db.endpoint
logger.debug(  "endpoint [" + str(endpoint) + " " + str(port) + "]")
logger.debug(  "Connecting to MySQL using MySQLdb: "+ "host="+endpoint+","+"user="+config.db_user+","+"passwd="+config.db_password+","+"db="+config.db_name+","+"port="+str(port) )
mydb=MySQLdb.connect(host=endpoint,user=config.db_user,passwd=config.db_password,db=config.db_name,port=port)
cursor = mydb.cursor()


# UNDER CONSTRUCTION!
# Write the Site(s) to the database   
# Process: 

#   For each record in the list of records, see if the site exists already if it
#   does don't update it unless the overwrite flag is set further, update the
#   SiteUUID data and the PK row number in the audit record if its in the
#   database to be what is reflected in the database.


# turn list of dictionaries into a SQL statement and write/update it to the SiteTbl database
# Ensure successful write and then get row number
# Future: should check and see if Site exists already before writing it!
# First Look for duplicate site records 
# if we find it, we will correct the SiteUUID
#



for record in SiteTbl_dicts:
    record_id=-1
    # Query the database determine if the site already exists:
    query = "SELECT SiteId_tbl, SiteUUID FROM SiteTbl WHERE "
    search_fields1 =('VendorSiteLatitude', 'VendorSiteLongitude')
    search_fields2 =('VendorName','VendorSiteName','VendorSiteAddress','VendorSiteCity','VendorSiteState','VendorSiteCountry')
    query += "( "+build_query(record,search_fields1)+" ) OR ( "+build_query(record,search_fields2)+" )"
    
    previous_SiteUUID = record['SiteUUID']
    cursor.execute(query)
    row_results = cursor.fetchone()
    if row_results  #The SiteUUID can never be replaced, must use existing one
        (SiteId_tbl_val, SiteUUID_val) = row_results
        if SiteUUID_val:        # If Site previously exists, replace its UUID and PK row number in the data structures 
            logger.info("Found site has been previously defined not adding it, use --overwrite_site to override ")
            #reset the SiteUUID to be the UUID we found in the database and get the Primary Key
            logger.debug("SiteUUID was: %s changing it to: %s",previous_SiteUUID,SiteUUID_val)
            record['SiteUUID'] = SiteUUID_val
            record_id = SiteId_tbl_val
            logger.debug("Row Number will be : %s",SiteId_tbl_val)

            # Loop through the other table(s) and update the SiteUUID and Primary Key where appropriate
            for record2 in AssessmentResultsTbl_dicts:
                if str(record2['SiteUUID']) == str(previous_SiteUUID):
                    record2['SiteUUID'] = SiteUUID_val
                    record2['SiteId'] = SiteId_tbl_val
        if args.overwrite_site:
          
            else:
                raise ValueError("Fatal: cursor (database) returns value but its not SiteUUID, this is very confusing")
                sys.exit(10)
        else # overwrite site info including UUID 
            logger.info( "Overwriting site info")
            s = sql_action_statement("SiteTbl",record,"REPLACE")
            if write_record(s,record_id) == 0:   #s is the SQL statement, returns record_id the row number effected
                raise ValueError("Fatal: cant write Site Record")
                sys.exit(10)
        else:
            logger.info( "Site doesnt exist in the site table, adding it")
            # Create site record
            s = sql_action_statement("SiteTbl",record,"INSERT")
            if write_record(s,record_id) == 0:
                raise ValueError("Fatal: cant write Site Record")
                sys.exit(10)
        # Loop through the other table(s) and update the row number 
        if record_id >= 0:
            for record2 in AssessmentResultsTbl_dicts:
                if str(record2['SiteUUID']) == str(previous_SiteUUID):
                    record2['SiteId'] = record_id
        else:
                #On inital write, we exercise this section of code we should never be in - why?
                # record_id must be -1 
                # The Site table is getting written and it doesn't fatal
                #
                logger.warning("This is wierd, in SiteTbl section went to update AssessmentResultsTbl_dicts but record_id = -1 ")
    cursor.fetchall() # the docs say you need to fetch all rows before doing another query ->void

# Use row number above to write record below
# turn list of dictionaries into SQL statement and write it
# AssessmentResultsTbl

# Bug check: It seems like it fails to write the Assemssment the first time but it works the second
# need to check flow ...
for record in AssessmentResultsTbl_dicts:
    #First check and see if the Assessment is already in the database
    query = "SELECT AssessmentGUID FROM AssessmentResultsTbl WHERE "
    search_fields = ('AssessmentGUID',)
    query += build_query(record,search_fields)
    logger.debug("Assessment Query: "+query)

    cursor.execute(query)
    row_results = cursor.fetchone() 
    if row_results and not args.overwrite_assessment:
        (AssessmentGUID) = row_results
        logger.info("Asessment exists, not updating use --overwrite_assessment to override")
    else:
        if args.overwrite_assessment:
            logger.info("Overwriting Assessment data ...")
            s = sql_action_statement("AssessmentResultsTbl",record,"REPLACE")
            if write_record(s,record_id) == 0:
                raise ValueError("Fatal: cant overwrite Assessment Record")
        else:
            logger.info("Writing asessment data ...")            
            s = sql_action_statement("AssessmentResultsTbl",record,"INSERT")
            if write_record(s,record_id) == 0:
                raise ValueError("Fatal: cant write Assessment Record")
                sys.exit(11)
    cursor.fetchall()

cursor.close()
mydb.close()
conn.close()
