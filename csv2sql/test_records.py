#!/usr/local/bin/python
# -*- coding: utf-8 -*-

#
import sys
import config  # Global Settings
import uuid


mySiteDict=config.SiteTbl_desc
    print("Site Dictionary: " + str(SiteTbl_dicts))
    print

myAssessmentDict=config.AssessmentResultsTbl_desc
    print("Assessment Dictionary: " + str(SiteTbl_dicts))
    print
 
 for record in mySiteDict:
    record_id=-1

    row['SiteUUID'] = uuid.uuid4()

    # Look and see if the site already exists, if it does don't add it again
    # build query of site info
    query += "( "+build_query(record,search_fields1)+" ) OR ( "+build_query(record,search_fields2)+" )"
    print query
    previous_SiteUUID = record['SiteUUID']

    cursor.execute(query)
    row_results = cursor.fetchone() 
    if row_results:
        (SiteId_tbl_val, SiteUUID_val) = row_results
        if SiteUUID_val:
            # If Site previously exists, use its UUID in both tables
            print "Found site has been previously defined not adding it "
            #print " SiteId_tbl_val :"+str(SiteId_tbl_val)+" SiteUUID_val "+str(SiteUUID_val) 
            
            record['SiteUUID'] = SiteUUID_val
            record_id = SiteId_tbl_val

            # Loop through the other table(s) and update the SiteUUID and row number where appropriate
            for record2 in AssessmentResultsTbl_dicts:
                if record2['SiteUUID'] == previous_SiteUUID:
                    record2['SiteUUID'] = SiteUUID_val
                    #record2['SiteId'] = SiteId_tbl_val
                    record2['SiteId'] = "foo"
                print "record2: "+str(record2)
        else:
            print "Fatal: cursor returns value but its not SiteUUID"
            sys.exit(10)
    else:
        print "Site doesnt exist in the site table ,adding it"
        # Site doesn't exists, so create it
        s = sql_insert_statement("SiteTbl",record)
        if args.verbose >=3 : print s
        if write_record(s,record_id) == 0:
            # Loop through the other table(s) and update the row number 
            for record2 in AssessmentResultsTbl_dicts:
                if record2['SiteUUID'] == previous_SiteUUID:
                    record2['SiteId'] = record_id
        else:
            print "Fatal: cant write Site Record"
            sys.exit(10)

    cursor.fetchall # the docs say you need to fetch all rows before doing another query ->void

# Use row number above to write record below
# turn list of dictionaries into SQL statement and write it
# AssessmentResultsTbl
for record in AssessmentResultsTbl_dicts:
    print "Record: "+str(record)
    s = sql_insert_statement("AssessmentResultsTbl",record)
    print "SQL statement: "+s
    if write_record(s,record_id) == 0:
        print "Fatal: cant write Assessment Record"
        sys.exit(11)
 
#ToDo - why doesn't Assessment Record write?

cursor.close()
mydb.close()
conn.close()
