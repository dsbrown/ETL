#!/usr/bin/env python 
# -*- coding: utf-8 -*-

###################################################################################
#
# 							CSV To SQL Conversion Program
# Description:
# Open and parse csv files
#
# Author: David S. Brown
# V1.0  10 March 2015     First Draft
#
####################################################################################

import argparse
import sys
import csv

parser = argparse.ArgumentParser(description='Parse CSV files',
                                )

####################################################################################
#
# 									 Flags
#
####################################################################################

# Count of verbose flags such as: arg_parse.py -v, arg_parse.py -vv, arg_parse.py -vvv, etc
parser.add_argument("-v", "--verbose", action="count", default=0, help="increase output verbosity")
# a numeric debug level as in arg_parse.py -D 3
parser.add_argument("-D", "--debug", type=int, default=0, help="Set integer debug level from 0-9 as: -D 3")

# Files to open, one by one as in csvparse.py -f foo.txt bar.txt fu.txt 
parser.add_argument("-f", "--files", nargs="+", dest="mfiles", help="file names to open separated by spaces")

# You may optionally change the delimeter and quote character
parser.add_argument('-d', '-delimiter', default=",", help="Optionally specify the field separation delimiter used, the default is ','")
parser.add_argument('-q', '-quote_delimiter', default='"', help="Optionally specify the quote delimiter that is being used, the default is \"")


####################################################################################
#
# 									Main
#
####################################################################################

args = parser.parse_args()

if args.debug >= 2: print("args: ", args)

filesProcessed = []
filesSkipped = []
for mfile in args.mfiles:
	if (args.debug >= 1) or (args.verbose >=1):
		print("filename:{}".format(mfile))
	f = open(mfile, 'rU')
	line = f.readline()
	line=line.strip().split(",")
	if (args.debug >= 1) or (args.verbose >=1):
		print("First line of the file '" + str(mfile) + "' has " + str(len(line)) + " columns which are:")
		print("{}".format(line))
		print "processing"
	#
	#Open CSV file
	f.seek(0)
	csv_f = csv.reader(f)
	for row in csv_f:
	  print row[2]

	filesProcessed.append(mfile)

if args.interactive >= 1:
	print("Files procesed: " + str(filesProcessed))
	print("Files skipped: " + str(filesSkipped))

