#!/usr/local/bin/python
# -*- coding: utf-8 -*-

####################################################################################
# columns missing
# Short program to tell me what columns are missing from a CSV file
#
# Author: David S. Brown
# v1.0  DSB     Create col_missing.py
#
####################################################################################

import argparse
import sys
import csv
import config  # Global Settings
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

# Should program print a detailed table?
parser.add_argument("-t", "--table",  help="Print detailed table")


# Files to open, one by one as in csvparse.py -f foo.txt bar.txt fu.txt 
parser.add_argument("-f", "--files", nargs="+", dest="mfiles", help="file names to open separated by spaces")


# You may optionally change the delimeter and quote character
parser.add_argument('-d', '-delimiter', default=",", help="Optionally specify the field separation delimiter used, the default is ','")
parser.add_argument('-q', '-quote_delimiter', default='"', help="Optionally specify the quote delimiter that is being used, the default is \"")

# Prints help if no arguments
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)
####################################################################################
#
#                                Functions
#
####################################################################################



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
        o = open(mfile,'rU')    #It will throw an error if not opened up in U mode
        reader = csv.DictReader(o)
        for row in reader:
            #row = reader[0]
            csv_cols = set(row.keys())
            site_cols = config.SiteTbl_desc.keys()
            assessment_cols = config.AssessmentResultsTbl_desc.keys()
            exp_cols = set(site_cols) | set(assessment_cols)
            all_cols = set(csv_cols) | exp_cols
            
            if  args.table :
                print "%20s\t%3s\t%8s" % ("Key is in", "CSV", "Database")
                for i in sorted(all_cols):
                    if i in csv_cols : 
                        csve = "X"
                    else:
                        csve = ""
                    if i in exp_cols : 
                        expe = "X"
                    else:
                        expe = ""
                        
                    print "%20s\t%1s\t%1s" % (i, csve, expe)
            else:
                diff_cols = set(csv_cols).symmetric_difference(exp_cols)
                print "Columns Not in Both Sets:"
                print sorted(diff_cols)
    filesProcessed.append(mfile)
logger.info("Files procesed: " + str(filesProcessed))

