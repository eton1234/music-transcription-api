import json
import boto3
import os
from configparser import ConfigParser
from datetime import datetime
import datatier
# # Load configuration
config = ConfigParser()
config.read('config.ini')

# # Read S3 configurations
s3_region_name = config.get('s3', 'region_name')
print("s3_region_name", s3_region_name)
s3_bucket_name = config.get('s3', 'bucket_name')
print("s3_bucket_name", s3_bucket_name)

# # Initialize S3 client
s3 = boto3.client('s3', region_name=s3_region_name)
# print("success")

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def lambda_handler(event, context):
    try:
        
        config_file = 'config.ini'
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
        
        configur = ConfigParser()
        configur.read(config_file)
        
        #
        # configure for S3 access:
        #
        s3_profile = 's3readwrite'
        boto3.setup_default_session(profile_name=s3_profile)
        
        bucketname = configur.get('s3', 'bucket_name')
        
        # s3 = boto3.resource('s3')
        # bucket = s3.Bucket(bucketname)
        
        #
        # configure for RDS access
        #
        rds_endpoint = config.get('rds', 'endpoint')
        rds_portnum = int(config.get('rds', 'port_number'))
        rds_username = config.get('rds', 'user_name')
        rds_pwd = config.get('rds', 'user_pwd')
        rds_dbname = config.get('rds', 'db_name')
        
        if "userid" in event:
            userid = event["userid"]
        elif "pathParameters" in event:
            if "userid" in event["pathParameters"]:
                userid = event["pathParameters"]["userid"]
            else:
                raise Exception("requires userid parameter in pathParameters")
        else:
            raise Exception("requires userid parameter in event")
        
        print("userid:", userid)
        
        print("**Opening connection**")
    
        dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    
        #
        # first we need to make sure the userid is valid:
        #
        print("**Checking if userid is valid**")
        
        sql = "SELECT * FROM users WHERE userid = %s;"
        
        row = datatier.retrieve_one_row(dbConn, sql, [userid])
        
        if row == ():  # no such user
          print("**No such user, returning...**")
          return {
            'statusCode': 400,
            'body': json.dumps("no such user...")
          }
        
        print(row)
        
        username = row[1]
        prefixstr = "benfordapp/" + username + "/"
        
        params = {
            'Bucket': s3_bucket_name,
            'Prefix': prefixstr,
        }

        response = s3.list_objects_v2(**params)
        print("response", response)
        data = response.get('Contents', [])
        print("data", data)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'success', 'data': data}, default=json_serial)
        }
    except Exception as err:
        print("**Error in call to get /bucket")
        print(err)

        return {
            'statusCode': 400,
            'body': json.dumps({'message': str(err), 'data': []})
        }
