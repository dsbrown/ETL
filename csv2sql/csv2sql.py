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
# v1.0  DSB      7 Mar 2015     Create csv2sql
# v1.1  DSB     18 Mar 2015     Modified to be simplier, doesn't use as complex data structure
# v1.2 DSB     25 Mar 2015     Modified to actualy work - Yea! Now meets all original requirements
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

# is_it_a_number - Looks at a string and tries to interpret if it is an TUPLE, INT, FLOAT or STR
def is_it_a_number(text):
    if isinstance(text, int) or isinstance(text, float) or isinstance(text, long) : 
        return 'INT'
    
    if isinstance(text, (list, tuple)):
        return 'TUPLE'

    if isinstance(text, basestring):
        try:
            int(text)
            return 'INT'
        except ValueError:
            pass
        try:
            long(text)
            return 'INT'
        except ValueError:
            pass
        try:
            float(text)
            return 'FLOAT'
        except ValueError:
            pass
    else:
        return 'STR'


# sql_action_statement - build SQL statement to put data into table
#   table is the SQL table to be effected
#   d data is a dictionary hash, walk through it and build SQL statement
#   action is 'INSERT' or 'REPLACE' depending on the desired side effect
#   INSERT is for new entries and will error if the entry exists
#   REPLACE will UPDATE an existing entry by replacing all matching fields
#sql_action_statement("SiteTbl","REPLACE",config.SiteTbl_desc,row)
def sql_action_statement(table,action,tbl_desc,d):
    e=valid_dictionary(d,tbl_desc)

    sql_preamble = ""
    sql_postscript = ""
    sql_keys =" ( "
    sql_data = " Values ("
    for key in e.keys():
        sql_keys+=key
        sql_data+="'"+str(e[key])+"'"
        sql_keys+=", "
        sql_data+=", "
    sql_keys=sql_keys[:-2] # removes the final ", " 
    sql_data=sql_data[:-2]
    sql_data+=") "  
    sql_keys+=") "

    if action in ('INSERT',):
        sql_preamble = "INSERT INTO "+table
    if action in ('REPLACE',):
        sql_preamble = "REPLACE INTO "+table
    if action in ('DUPLICATE',):
        sql_preamble = "INSERT INTO "+table
        sql_postscript = " ON DUPLICATE KEY UPDATE "
        for key in e.keys():
            sql_postscript+=" %s='%s', " % (key,e[key])
        sql_postscript=sql_postscript[:-2]

    return sql_preamble+sql_keys+sql_data+sql_postscript

#This accepts a date string of the form 5/6/2015 or 5/6/15
def TryDateParse(datestring):
    dateformats = ("%m/%d/%y","%m/%d/%Y")
    succes = False
    for aformat in dateformats:
        try:
            mydate = time.strptime(datestring,aformat)
            succes = True
            break
        except:
            pass
    if not succes:
        return 0
    else:
        return mydate

# valid_dictionary - Given a dictionary d of {field_name : field_value,,,} and
# a dictionary field_desc of {field_name : field_type,,} return a third
# dictionary which contains only field_names in field_desc and  which contains
# data from d that is actually valid for the database field type as described in
# field_desc.

def valid_dictionary(d,field_desc): #  (d dictionary of data, dictionary of valid fields and types)
    valid = field_desc.keys()
    e = {}
    for key in d.keys():
        if key in valid:
            #examine and validate data in each field
            type_of_var = is_it_a_number(d[key]) 
            if type_of_var in ('TUPLE',):
                e[key] = d[key][0]
                logger.warning("valid_dictionary: field: "+key+" with value: "+d[key]+" is a tuple, truncated to first element")

            if field_desc[key] ==  'INT':
                if str(d[key]) in ('1','y','Y','x','X'):
                    e[key]='1'
                elif str(d[key]) in ('','0','n','N'):
                    e[key]='0'
                elif type_of_var in ('INT','FLOAT'): 
                    e[key]=d[key]
                else:
                    e[key]=d[key]
                    logger.warning("Warning, INT: "+str(d[key])+" wants to be a number, but isn't drawn that way")
            elif field_desc[key] in ('DOUBLE', 'FLOAT'):
                if type_of_var in ('INT','FLOAT'):
                    e[key]=str(d[key])
                else:
                    e[key]=d[key]
                    logger.warning( "Warning, FLOAT: "+str(d[key])+" wants to be a number, but can't be")
            elif field_desc[key] in ('DATETIME'):  # date format typicaly: 5/6/15
                mydate = TryDateParse(d[key])
                if not mydate:
                    raise ValueError("Fatal: can't convert date "+str(d[key])+" to date")
                    sys.exit(110)
                    #YYYY-MM-DD HH:MM:SS
                e[key] = time.strftime("%Y-%m-%d %H:%M:%S",mydate)
                #print "Formated Time %s" % (e[key])
            else:
                e[key]=d[key]
    return e

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
            query+=i+" = '"+str(record[i])+"'"
    return query

# write_record - Execute s the SQL statement typically an INSERT or REPLACE 
#   returns >=0 if success
#   returns -1 if failure 

def write_record(s):
    try:
        logger.debug("SQL Statement: %s", s)
        affected_count = cursor.execute(s)
        mydb.commit()
        logger.info ("Wrote %s records ",str(affected_count))
        return cursor.lastrowid
    except MySQLdb.IntegrityError:
        logger.error( "failed to insert values, IntegrityError")
        return -1


####################################################################################
#
#                                   Main
#
####################################################################################
# Setup Logging 
args = parser.parse_args()
logger = logging.getLogger(__name__)

if args.verbose == 1:
    logging.basicConfig(level=logging.INFO)
elif args.verbose > 1:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logger.debug("args: %s", args)

######################
#   Connect to RDS 
######################
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

# Slurp up the csv files 

# into an array of dicts
# Add a Site UUID if one doesn't exist already
#
# Slurp up the csv files into a dictionary one at a time
# then look to see if the site already is in the database
# if it is, get the SiteUUID and PK SiteId_tbl, if not assign new ones
# Vaidate the data
filesProcessed = []

if args.mfiles:
    for mfile in args.mfiles:
        logger.debug("filename: %s", mfile)
        o = open(mfile,'rU')    #It will throw an error if not opened up in U mode
        reader = csv.DictReader(o)
        for row in reader:
            ####################################
            # Process Site Informaton
            # Query the database and try to find a Site in the Site Table that matches this one
            # If found get the SiteUUID and PK, if required, overwrite the Site Data
            ####################################
            query = "SELECT SiteId_tbl, SiteUUID FROM SiteTbl WHERE "
            search_fields1 =('VendorSiteLatitude', 'VendorSiteLongitude')
            search_fields2 =('VendorName','VendorSiteName','VendorSiteAddress','VendorSiteCity','VendorSiteState','VendorSiteCountry')
            query += "( "+build_query(row,search_fields1)+" ) OR ( "+build_query(row,search_fields2)+" )"
            cursor.execute(query)
            row_results = cursor.fetchone()
            if row_results:  #We found the site, interesting fact: the SiteUUID can never be replaced, must use existing one
                (SiteId_tbl_val, SiteUUID_val) = row_results

                 # If Site previously exists, replace its UUID  in the data structure
                if SiteUUID_val:       
                    if not args.overwrite_site:
                        print "Found site has been previously defined not adding it, use --overwrite_site to override "
                    #Set the SiteUUID to be the UUID we found in the database and get the Primary Key
                    logger.debug("SiteUUID was found, changing it to: %s",SiteUUID_val)
                    row['SiteUUID'] = SiteUUID_val
                else:
                    raise ValueError("Fatal: cursor (database) returns value but its not SiteUUID, this is very confusing")
                    sys.exit(10)

                # If Site previously exists, replace its PK row number in the data structure SiteId for Site, SiteId_tbl for Assessment
                if SiteId_tbl_val:   
                    row['SiteId_tbl'] = SiteId_tbl_val
                    row['SiteId'] = SiteId_tbl_val
                    logger.debug("Row Number will be : %s",SiteId_tbl_val)
                else:
                    raise ValueError("Fatal: cursor (database) returns value but its not SiteId_tbl, this is very confusing")
                    sys.exit(10)

                # The site exists, but we need to overwrite the site info
                if args.overwrite_site: 
                    logger.info( "Overwriting site info")
                    s = sql_action_statement("SiteTbl","DUPLICATE",config.SiteTbl_desc,row)
                    record_id = write_record(s) 
                    if record_id < 0 :
                        raise ValueError("Fatal: cant write Site Record")
                        sys.exit(10)
                    # else:
                    #     row['SiteId_tbl'] = record_id  
                    #     row['SiteId'] = record_id

            # Site doesn't exisit in the database, create entry for site in the database in SiteTbl      
            else: 
                row['SiteUUID'] = uuid.uuid4()   # Add a SiteUUID for every dict in the array according to RFC 4122, uuid4 is more secure than uuid1
                logger.debug("creating UUID")
                s = sql_action_statement("SiteTbl","INSERT",config.SiteTbl_desc,row)
                record_id = write_record(s) 
                if record_id < 0:   
                    raise ValueError("Fatal: cant write Site Record")
                    sys.exit(10)
                # else:
                #     row['SiteId_tbl'] = record_id
                #     row['SiteId'] = record_id

            cursor.fetchall() # Docs say you have to fetch all rows from the previous query before you do a new one 


            ####################################
            #
            # Process Assessment Informaton
            #
            ####################################
            SiteId_tbl_val = -1 # initialize with an invalid value

            # Query the database and try to find the Site mentioned actually exists in the Site Table 
            # If found get the SiteID_tbl PK <- SiteID FK, 
            query = "SELECT SiteId_tbl FROM SiteTbl WHERE "
            search_fields1 =('SiteUUID',)
            query += "( "+build_query(row,search_fields1)+" )"
            logger.debug("Looking in Site Table for Site Info: "+query)
            cursor.execute(query)
            row_results = cursor.fetchone()
            if row_results:  #We found the site
                SiteId_tbl_val = row_results[0]
            else:
                raise ValueError("Fatal: cant find SiteUUID: "+row['SiteUUID']+" in database. You must define the Site before the assessment can be entered")
                sys.exit(10)
            
            # See if the assessment is already in the database by matching the AssessmentGUID
            # If we find a matching assessment get the primary key of the match
            query = "SELECT idAssessmentTable FROM AssessmentResultsTbl WHERE " 
            search_fields = ('AssessmentGUID',)
            query += str(build_query(row,search_fields))
            logger.debug("Assessment Lookup Query: "+query)
            cursor.execute(query)
            row_results = cursor.fetchone() 

            # We found an assessment that matches the AssessmentGUID
            #
            if row_results:
                idAssessmentTable_val = row_results[0]
                logger.debug( "PK:"+str(idAssessmentTable_val))
            
                # if we are authorized to overwrite the record
                if args.overwrite_assessment:
                    row['idAssessmentTable'] = idAssessmentTable_val  #Primary Key
                    logger.info("Overwriting Assessment data @ "+str(idAssessmentTable_val)+"s ...") 
                    s = sql_action_statement("AssessmentResultsTbl","DUPLICATE",config.AssessmentResultsTbl_desc,row)

                    record_id = write_record(s)
                    if record_id < 0:   
                        raise ValueError("Fatal: cant overwrite Assessment Record")
                        sys.exit(10)
                    else:
                        logger.info("Overwrote asessment data ...")  
                else:
                    print "Assessment exists, not updating use --overwrite_assessment to override"

            # No matching asssesment, write the new one        
            else: 
                row['SiteId'] =  SiteId_tbl_val
                logger.debug("SiteId: %d" % row['SiteId'])
                logger.info("Writing new asessment data ...")            
                s = sql_action_statement("AssessmentResultsTbl","INSERT",config.AssessmentResultsTbl_desc,row)
                record_id = write_record(s) 
                if record_id < 0:
                    raise ValueError("Fatal: cant write new Assessment Record")
                    sys.exit(11)
                else:
                    logger.info("Wrote asessment data ...")  
            cursor.fetchall() # the docs say you need to fetch all rows before doing another query ->void
    filesProcessed.append(mfile)
logger.info("Files procesed: " + str(filesProcessed))

cursor.close()
mydb.close()
conn.close()
