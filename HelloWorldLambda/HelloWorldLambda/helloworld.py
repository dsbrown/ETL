
'''
###################################################################################
                        Hello World Python Lambda Test
                            helloworld.py
Author: David S. Brown
All Rights Reserved 17 July 2018
####################################################################################
'''
print "Loading helloworld.py"
#############################################
#          main lambda_handler
#############################################
def lambda_handler(event, context):
    # TODO implement
    print "Inside lambda_handler" 
    return 'Hello World from Deployment Package'