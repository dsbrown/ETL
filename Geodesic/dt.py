#!/usr/local/bin/python

''' 
Distance Tool

2019  David Brown GPL

Converted to Python 3

The distance tool determines distance statistics between locations this has been rewritten from a tool used to
provide base data to Tableau. The original which belongs to the company I wrote it for did a lot of complex
relationships which was beyond what Tableau could do so it had to be done externally and then added to the
visualization. This is the simplified and modified version, which I rewrote and refactored as an example of:

1) how to pick up a file from an S3 bucket and process it and then write it
  back for further processing using boto3
2) do complex set combinatorics
3) compute great circle distances with haversine
4) manage complex records

This was launched with Lambda and in this case another ETL loaded the data to a database.

In this mock example, we have an aircraft parts company, it provides services to airports It has a main site near the
airport and two alternate sites some miles away, they have names based on the airport code plus a number to
distinguish between the three local sites. We want to get the actual, minimum, maximum and average distance between
the airport and the supply sites and each other. In this way leadership can understand things like how long it will
take to resupply the airport when they are out of engine parts or the like.

'''

import argparse
import copy
import datetime
import logging
import math
import os
import uuid
from math import radians, cos, sin, asin, sqrt
import boto3
import botocore
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AVG_EARTH_RADIUS = 6371  # in km
COUNTRIES = ['IT-25', 'IT-21', 'IT-42', 'IT-34', 'IT-62']

#############################################
#  haversine - Compute distances on a globe
#############################################

def haversine(point1, point2, miles=False):
    """ Calculate the great-circle distance between two points on the Earth surface.

    :input: two 2-tuples, containing the latitude and longitude of each point
    in decimal degrees.

    Example: haversine((45.7597, 4.8422), (48.8567, 2.3508))

    :output: Returns the distance bewteen the two points.
    The default unit is kilometers. Miles can be returned
    if the ``miles`` parameter is set to True.

    """
    # unpack latitude/longitude
    lat1, lng1 = point1
    lat2, lng2 = point2

    # convert all latitudes/longitudes from decimal degrees to radians
    lat1, lng1, lat2, lng2 = list(map(radians, (lat1, lng1, lat2, lng2)))

    # calculate haversine
    lat = lat2 - lat1
    lng = lng2 - lng1
    d = sin(lat * 0.5) ** 2 + cos(lat1) * cos(lat2) * sin(lng * 0.5) ** 2
    h = 2 * AVG_EARTH_RADIUS * asin(sqrt(d))
    if miles:
        return h * 0.621371  # in miles
    else:
        return h  # in kilometers

def geodesic_float(xxx_todo_changeme, xxx_todo_changeme1):
    (origin_lat, origin_long) = xxx_todo_changeme
    (dest_lat, dest_long) = xxx_todo_changeme1
    try:
        success = True
        origin_lat  = float(origin_lat)
        origin_long = float(origin_long)
        dest_lat    = float(dest_lat)
        dest_long   = float(dest_long)
    except:
        print("Failed to convert to float")
        success = False
    return (success, (origin_lat, origin_long), (dest_lat, dest_long))

def geodesic_validate(xxx_todo_changeme2, xxx_todo_changeme3):
    (origin_lat, origin_long) = xxx_todo_changeme2
    (dest_lat, dest_long) = xxx_todo_changeme3
    return not (math.isnan(origin_lat) or math.isnan(origin_long) or math.isnan(dest_lat) or math.isnan(dest_long))

def geodesic_valid_miles(xxx_todo_changeme4, xxx_todo_changeme5):
    (origin_lat, origin_long) = xxx_todo_changeme4
    (dest_lat, dest_long) = xxx_todo_changeme5
    (valid, (origin_lat, origin_long), (dest_lat, dest_long)) = geodesic_float((origin_lat, origin_long),(dest_lat, dest_long))
    if valid:
        if geodesic_validate((origin_lat,origin_long),(dest_lat,dest_long)):
            # __out = haversine((origin_lat,origin_long),(dest_lat,dest_long),miles=True)
            # print (origin_lat,origin_long),(dest_lat,dest_long)
            # print __out
            return haversine((origin_lat,origin_long),(dest_lat,dest_long),miles=True)
        else:
            return(float('nan'))

def geodesic_valid_kilometers(xxx_todo_changeme6, xxx_todo_changeme7):
    (origin_lat, origin_long) = xxx_todo_changeme6
    (dest_lat, dest_long) = xxx_todo_changeme7
    #if args.debug: print(("origin_lat: %s, origin_long: %s, dest_lat: %s, dest_long: %s"%(origin_lat,origin_long,dest_lat,dest_long)))
    (valid, (origin_lat, origin_long),(dest_lat, dest_long)) = geodesic_float((origin_lat, origin_long),(dest_lat, dest_long))
    if valid:
        if geodesic_validate((origin_lat,origin_long),(dest_lat,dest_long)):
            return haversine((origin_lat,origin_long),(dest_lat,dest_long))
            #return geodesic((origin_lat,origin_long),(dest_lat,dest_long)).kilometers
        else:
            return(float('nan'))


#############################################
#  Output Records to CSV
#############################################

def create_row(site_origin_obj, site_dest_obj, ap_sites):
    output_row = {}
    #output_row['ID'] = str(uuid.uuid4().hex()[0:18])
    output_row['ID'] = str(uuid.uuid4().hex[0:18])
    site_origin_df = site_origin_obj.site_df
    site_dest_df = site_dest_obj.site_df


    # record origin data into the output row
    site_origin = site_origin_df.to_dict('index')    
    site_origin_dict = list(site_origin.values())[0]
    output_row['MAIN_NAME'] = site_origin_dict['MAIN.NAME']
    output_row['AIRPORT_NAME'] = site_origin_dict['AIRPORT.NAME']
    output_row['SITE_ORIGIN_NAME'] = site_origin_dict['NAME']
    output_row['SITE_ORIGIN_GEOLOCATION__LONGITUDE__S'] = site_origin_dict['SITE_LOCATION__LONGITUDE__S']
    output_row['SITE_ORIGIN_GEOLOCATION__LATITUDE__S'] = site_origin_dict['SITE_LOCATION__LATITUDE__S'] 
    output_row['ORIGIN_SITE_SERVICES__C'] = site_origin_dict['SITE_SERVICES__C']     
    output_row['ORIGIN_SITE_TYPE__C'] = site_origin_dict['LOCATION']         

    # record destination data into the output row
    site_dest = site_dest_df.to_dict('index')    
    site_dest_dict = list(site_dest.values())[0]
    output_row['MAIN_NAME'] = site_dest_dict['MAIN.NAME']
    output_row['AIRPORT_NAME'] = site_dest_dict['AIRPORT.NAME']
    output_row['SITE_DESTINATION_NAME'] = site_dest_dict['NAME']
    output_row['SITE_DESTINATION_GEOLOCATION__LONGITUDE__S'] = site_dest_dict['SITE_LOCATION__LONGITUDE__S']
    output_row['SITE_DESTINATION_GEOLOCATION__LATITUDE__S'] = site_dest_dict['SITE_LOCATION__LATITUDE__S'] 
    output_row['DESTINATION_SITE_SERVICES__C'] = site_dest_dict['SITE_SERVICES__C']     
    output_row['DESTINATION_SITE_TYPE__C'] = site_dest_dict['LOCATION']         

    # geodesic((lat, long),(lat, long))
    origin_lat  = site_origin_dict['SITE_LOCATION__LATITUDE__S']
    origin_long = site_origin_dict['SITE_LOCATION__LONGITUDE__S']
    dest_lat    = site_dest_dict['SITE_LOCATION__LATITUDE__S']
    dest_long   = site_dest_dict['SITE_LOCATION__LONGITUDE__S']
    output_row['DISTANCE_MILES__C'] = geodesic_valid_miles((origin_lat,origin_long),(dest_lat,dest_long))
    output_row['DISTANCE_KM__C']    = geodesic_valid_kilometers((origin_lat,origin_long),(dest_lat,dest_long))

    if site_dest_obj.name in ap_sites:
        if site_origin_obj.name == site_dest_obj.name:
            return()
        output_row_site = copy.copy(output_row)         # Return a shallow copy of output_row
        return([output_row_site])
    else:
        output_row_ap   = copy.copy(output_row)
        output_row_site = copy.copy(output_row)
        return([output_row_ap,output_row_site])

#############################################
#             Record Management
#############################################

class AirportCode(object):
    all_zone_objs = []
    global_max_km = float('nan')
    global_max_miles = float('nan')
    global_min_km = float('nan')
    global_min_miles = float('nan')
    global_total_km = float('nan')
    global_total_miles = float('nan')
    global_count_km = 0
    global_count_miles = 0

    # df is the entire data frame, airport is a single airport name
    def __init__(self, df, airport_name):
        self.name = airport_name                                                         # Name of the airport a string
        self.df_airport_name = df[df['AIRPORT.NAME'] == airport_name]                    # df of this airport only
        self.zones = set(self.df_airport_name['MAIN.NAME'].tolist())
        self.zones_objs = self.setzone_objs(self.df_airport_name, self.zones)
        AirportCode.all_zone_objs.extend(self.zones_objs)
        self.peer_sites_obj = []

    def setzone_objs(self,df,zones):
        zone_objs = []
        for zone in zones:
            zone_obj = AirPort(df, zone)
            zone_objs.append(zone_obj)
        return zone_objs

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)

class AirPort(object):
    all_site_objs = []
    # global_max_km = float('nan')
    # global_max_miles = float('nan')
    # global_min_km = float('nan')
    # global_min_miles = float('nan')
    # global_total_km = float('nan')
    # global_total_miles = float('nan')
    # global_count_km = 0
    # global_count_miles = 0

    def __init__(self, df, zone):
        self.name = zone
        self.df_zone = df[df['MAIN.NAME'] == zone]
        self.sites = set(self.df_zone['NAME'].tolist())
        self.site_objs = self.setsite_objs(self.df_zone,self.sites)
        AirPort.all_site_objs.extend(self.site_objs)

        self.max_km_within_cluster = float('nan')
        self.max_miles_within_cluster = float('nan')
        self.min_km_within_cluster = float('nan')
        self.min_miles_within_cluster = float('nan')
        self.total_km_within_cluster = float('nan')
        self.total_miles_within_cluster = float('nan')
        self.count_km_within_cluster = 0
        self.count_miles_within_cluster = 0

    def setsite_objs(self,df,sites):
        site_objs = []
        for site in sites:
            site_obj = Sites(df,site)
            site_objs.append(site_obj)
        return site_objs

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)

class Sites(object):
    def __init__(self, df, site):
        self.name = site
        self.site_df = df[df['NAME'] == site]
        self.site_peers = []
        self.site_permutations = []

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)

#############################################
#          Sum & Update Statistics
#############################################

def update_max(record,value):
    if math.isnan(record) or record < value:
        return value
    return record

def update_min(record,value):
    if math.isnan(record) or record >= value:
        return value
    return record

def update_avg(total,count,value):
    if math.isnan(total):
        total = value
    else:
        total += value
    if math.isnan(count):
        count = 1
    else:
        count += 1
    if args.debug: print(("update_avg / Total: {} Count: {} Value: {}".format(total,count,value)))
    return(total,count)

def update_statistics(output, airport_obj, ap_obj, site_obj):
    for row in output:
        miles = float(row['DISTANCE_MILES__C'])
        km =    float(row['DISTANCE_KM__C'])
        ap_obj.max_km_within_cluster=update_max(ap_obj.max_km_within_cluster, km)
        ap_obj.max_miles_within_cluster=update_max(ap_obj.max_miles_within_cluster, miles)
        ap_obj.min_km_within_cluster=update_min(ap_obj.min_km_within_cluster, km)
        ap_obj.min_miles_within_cluster=update_min(ap_obj.min_miles_within_cluster, miles)
        (ap_obj.total_km_within_cluster, ap_obj.count_km_within_cluster)=update_avg(ap_obj.total_km_within_cluster, ap_obj.count_km_within_cluster, km)
        (ap_obj.total_miles_within_cluster, ap_obj.count_miles_within_cluster)=update_avg(ap_obj.total_miles_within_cluster, ap_obj.count_miles_within_cluster, miles)
    return

#############################################
#                   main
#############################################
if __name__ == "__main__":

    DEFAULT_BUCKET = "dsb001"
    REMOTE_INPUT_FILE = 'source/location.csv'
    LOCAL_INPUT_FILE = 'location.csv'    
    PRIMARY_OUTPUT_FILE = "location_distance.csv"

    WITHIN_AIRPORT_SUMMARY__OUTPUT_FILE = "airport_distance_summary.csv"  # Maximum Minimum for sites that match INCL_WITHIN_AZ

    parser = argparse.ArgumentParser(description="Distance Tool")
    parser.add_argument("-s", "--s3_bucket", 
                        nargs="?",  
                        dest="s3_bucket", 
                        required=False, 
                        default = DEFAULT_BUCKET,
                        help="The bucket containing the site information")
    parser.add_argument("-f", "--s3_file", 
                        nargs="?",  
                        dest="s3_file", 
                        required=False, 
                        default = REMOTE_INPUT_FILE,
                        help="The file path (key) containing the site information")
    parser.add_argument("-i", "--input_file", 
                        nargs="?",  
                        dest="input_file", 
                        required=False, 
                        help="The local file containing the site information")
    parser.add_argument('--remote',
                        required = False,
                        action = 'store_true',
                        default = False,
                        help = "Use S3 for input and output")
    parser.add_argument("-o", "--output_file",
                        nargs="?",
                        dest="output_file",
                        required=True,
                        default=PRIMARY_OUTPUT_FILE,
                        help="The csv file to write to")
    parser.add_argument('--debug',
                        required = False,
                        action = 'store_true',
                        default = False,
                        help = "Debug statements")
    
    args = parser.parse_args()

    if not args.input_file:
        s3 = boto3.resource('s3')
        try:
            s3.Bucket(args.s3_bucket).download_file(args.s3_file, LOCAL_INPUT_FILE)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
                raise
        df = pd.read_csv(LOCAL_INPUT_FILE,index_col='ID')

    else:
        s3=False
        df = pd.read_csv(args.input_file,index_col='ID')


    
    # Valid Data With Location filter sites by this criteria:
    df = df[(df['ISDELETED'] == False)]                     # Remove all the sites that are deleted
    df = df[(df['SITE_STATUS__C'] != 'Closed')]
    df = df.dropna(subset=['SITE_LOCATION__LATITUDE__S','SITE_LOCATION__LONGITUDE__S'])

    #Remove countries in the countries list we want to ignore
    df = df[~df['LOCATION'].isin(COUNTRIES)]
    if args.debug: print(df.shape)
    if args.debug: print(df.head)

    # Walk through the data frame and build the hierarchical tree
    airport_code_set = set(df['AIRPORT.NAME'].tolist())
    airport_code = {}
    for airport in airport_code_set:
        airport_code[airport] = AirportCode(df, airport)
 
    # Now build the combinations of sites

    ############################################################################################################
    #      1) build up the list of peer site objects in the airport make that available in the airport record
    ############################################################################################################
    if args.debug: print("-----------------------------------------------------------------------------------------------")
    for airport in airport_code:
        for ap in airport_code[airport].zones_objs:
            airport_code[airport].peer_sites_obj.extend(ap.site_objs)
        if args.debug: print("AirportCode: %s\nPeers:\t%s"%(airport,airport_code[airport].peer_sites_obj))

    output = []   # This is the primary output of Case 1 and Case 2

    ############################################################################################################    
    #       2) for each site record, build up a list of its peers,combinations with peers, remove all sibling sites and itself
    ############################################################################################################
    for airport in airport_code:
        for ap in airport_code[airport].zones_objs:
            ap_sites = str(ap.site_objs)
            for site_obj in ap.site_objs:
                _lat = site_obj.site_df.iloc[0,site_obj.site_df.columns.get_loc("SITE_LOCATION__LATITUDE__S")]
                _lng = site_obj.site_df.iloc[0,site_obj.site_df.columns.get_loc("SITE_LOCATION__LONGITUDE__S")]
                #geo_average(ap, _lat, _lng)
                # this is including the other sites in the AZ need to eliminate!!!
                for peer_obj in airport_code[airport].peer_sites_obj:
                    __output = create_row(site_obj, peer_obj, ap_sites)
                    output.extend(__output)
                    update_statistics(__output, airport_code[airport], ap, site_obj)
    #
    # Output Case 1 and 2
    #
    df_out = pd.DataFrame(output)
    df_out = df_out.set_index('ID')
    if s3:
        df_out.to_csv("/tmp/"+args.output_file, encoding='utf-8')
    else:
        df_out.to_csv(args.output_file, encoding='utf-8')

    #
    # Copy files back
    #
    if s3:
        today = datetime.date.today()
        keypath=today.strftime("%Y/%m/%d")
        keypath="cdt/"+keypath+"/"
        print(keypath)
        try:
            #s3.Bucket(args.s3_bucket).download_file(args.s3_file, keypath+"/"+LOCAL_INPUT_FILE)
            s3.Bucket(args.s3_bucket).upload_file("/tmp/"+args.output_file,keypath+args.output_file)
            s3.Bucket(args.s3_bucket).upload_file("/tmp/" + WITHIN_AIRPORT_SUMMARY__OUTPUT_FILE, keypath + WITHIN_AIRPORT_SUMMARY__OUTPUT_FILE)
            #s3.Bucket(args.s3_bucket).upload_file("/tmp/"+WITHIN_AZ_SUMMARY__OUTPUT_FILE,keypath+WITHIN_AZ_SUMMARY__OUTPUT_FILE)
            #s3.Bucket(args.s3_bucket).upload_file("/tmp/"+GLOBAL_OUTPUT_FILE,keypath+GLOBAL_OUTPUT_FILE)

            os.remove("/tmp/"+args.output_file)
            os.remove("/tmp/" + WITHIN_AIRPORT_SUMMARY__OUTPUT_FILE)
            #os.remove("/tmp/"+WITHIN_AZ_SUMMARY__OUTPUT_FILE)
            #os.remove("/tmp/"+GLOBAL_OUTPUT_FILE)

              # Clean up
            os.remove(LOCAL_INPUT_FILE)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
                raise

