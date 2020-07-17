#!/usr/bin/env python 
# -*- coding: utf-8 -*-

###################################################################################
#
# 						XLS Bulk convert xls xlsx
#							Early Demonstration
# Description:
# Open and parse all xls files in a directory, creata a dictionary of dictionaries
# keyed on Agile number 
#
# This is a series of programs to investigate openpyxl spreadsheet module
#
# Author: David S. Brown
# Last Major Change: 7 August 2015
#
####################################################################################

import argparse
import sys
from os import listdir
from os.path import isfile, join, splitext
#from openpyxl.workbook import Workbook
#from openpyxl.compat import range
#from openpyxl import cell, load_workbook
import xlrd



TO_FORMAT = '.xlsx'
FROM_FORMAT = '.xls'

parser = argparse.ArgumentParser( description='''
						Convert xls to xlsx
						''',
						)

####################################################################################
#
#                               Create Arguments
#
####################################################################################

# a numeric debug level as in arg_parse.py -D 3
parser.add_argument("-D", "--debug", type=int, default=0, help="Set integer debug level from 0-9 as: -D 3")


# File to open, and optional worksheet to read
parser.add_argument("-d", "--dir", nargs="?", dest="mdir", required=True, 
					help="directory to find Excel Workbooks")


# Prints help if no arguments
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()

####################################################################################
#
#                                Functions
#
####################################################################################

####################################################################################
#
# 									Main
#
####################################################################################

if args.debug >= 2: print "args: "+str(args)
if args.debug >= 1:	print "dir:{%s}" % args.mdir

for name in listdir(args.mdir):
	fullname = join(args.mdir, name)
	if isfile(fullname):
		(root,ext) = splitext(name)
		if ext == FROM_FORMAT:
			print "Found xls: %s" % fullname
			book = xlrd.open_workbook(fullname)
			index = 0
			nrows, ncols = 0, 0
			while nrows * ncols == 0:
				sheet = book.sheet_by_index(index)
				nrows = sheet.nrows
				ncols = sheet.ncols
				index += 1
			# prepare a xlsx sheet
			book1 = Workbook()
			sheet1 = book1.get_active_sheet()

			for row in xrange(0, nrows):
				for col in xrange(0, ncols):
					sheet1.cell(row=row, column=col).value = sheet.cell_value(row, col)
