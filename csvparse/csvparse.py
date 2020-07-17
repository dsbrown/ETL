#!/usr/bin/env python 
# -*- coding: utf-8 -*-

###################################################################################
#
# 							CSV Parsing Program
#							Early Demonstration
# Description:
# Open and parse csv files
#
# Author: David S. Brown
# Last Major Change: 15 November 2014
#
####################################################################################

import argparse
import sys
import csv

parser = argparse.ArgumentParser(description='Parse CSV files',
	#epilog="to be used only for good",
	#prefix_chars='-+',
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
parser.add_argument('-i', '-interactive', dest="interactive", action="store_true", default=False, 
	help="This will output helpful information about the file its processing and ask if you want to continue, the default is non-interactive",
	)

# Files to open, one by one as in csvparse.py -f foo.txt bar.txt fu.txt 
parser.add_argument("-f", "--files", nargs="+", dest="mfiles", help="file names to open separated by spaces")

# You may optionally change the delimeter and quote character
parser.add_argument('-d', '-delimiter', default=",", help="Optionally specify the field separation delimiter used, the default is ','")
parser.add_argument('-q', '-quote_delimiter', default='"', help="Optionally specify the quote delimiter that is being used, the default is \"")


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.
    
    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
    valid = {"yes":"yes",   "y":"yes",  "ye":"yes",
             "no":"no",     "n":"no"}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while 1:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return default
        elif choice in valid.keys():
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")



####################################################################################
#
# 									Main
#
####################################################################################

args = parser.parse_args()

if args.debug >= 2: print("args: ", args)

#
# Open files and if interactive examine the file to see if the header is present
#
filesProcessed = []
filesSkipped = []
for mfile in args.mfiles:
	if (args.debug >= 1) or (args.verbose >=1):
		print("filename:{}".format(mfile))
	f = open(mfile, 'rU')
	line = f.readline()
	#line=line.strip()
	#line=line.split(",")
	line=line.strip().split(",")
	#s="First line of the file '" + str(mfile) + "' has " + str(len(line)) + " columns which are:"
	#print(s)

	if args.interactive >= 1:
		print("First line of the file '" + str(mfile) + "' has " + str(len(line)) + " columns which are:")
		print("{}".format(line))
		answer=query_yes_no("Is this the csv file header?",default="yes")
		if args.debug >= 2: print("{}".format(answer))
		if answer is "no":
			print("skipping")
			filesSkipped.append(mfile)
			continue

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

