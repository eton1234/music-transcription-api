import json
import boto3
import os
import base64
import datatier
from configparser import ConfigParser

def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: proj03_download**")

        # Setup AWS based on config file:
        config_file = 'benfordapp-config.ini'
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

        configur = ConfigParser()
        configur.read(config_file)

        # Configure for S3 access:
        s3_profile = 's3readonly'
        boto3.setup_default_session(profile_name=s3_profile)

        bucketname = configur.get('s3', 'bucket_name')

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketname)

        # Configure for RDS access
        rds_endpoint = configur.get('rds', 'endpoint')
        rds_portnum = int(configur.get('rds', 'port_number'))
        rds_username = configur.get('rds', 'user_name')
        rds_pwd = configur.get('rds', 'user_pwd')
        rds_dbname = configur.get('rds', 'db_name')

        # jobid from event: could be a parameter or part of URL path ("pathParameters"):
        if "jobid" in event:
            jobid = event["jobid"]
        elif "pathParameters" in event:
            if "jobid" in event["pathParameters"]:
                jobid = event["pathParameters"]["jobid"]
            else:
                raise Exception("requires jobid parameter in pathParameters")
        else:
            raise Exception("requires jobid parameter in event")

        print("jobid:", jobid)

        # Open connection to the database:
        print("**Opening connection**")
        dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

        # Check if jobid is valid:
        print("**Checking if jobid is valid**")
        sql = "SELECT * FROM jobs WHERE jobid = %s;"
        row = datatier.retrieve_one_row(dbConn, sql, [jobid])

        if row == ():  # no such job
            print("**No such job, returning...**")
            return {
                'statusCode': 400,
                'body': json.dumps("no such job...")
            }

        print(row)

        status = row[2]
        original_data_file = row[3]
        results_file_key = row[5]

        print("job status:", status)
        print("original data file:", original_data_file)
        print("results file key:", results_file_key)

        # Handle different statuses
        if status in ["uploaded", "processing"]:
            print(f"**Job status '{status}', returning...**")
            return {
                'statusCode': 400,
                'body': json.dumps(status)
            }

        if status == 'error':
            if results_file_key == "":
                print("**Job status 'unknown error', returning...**")
                return {
                    'statusCode': 400,
                    'body': json.dumps("error: unknown")
                }

            local_filename = "/tmp/results.txt"
            print("**Job status 'error', downloading error results from S3**")
            bucket.download_file(results_file_key, local_filename)

            with open(local_filename, "r") as infile:
                lines = infile.readlines()

            if not lines:
                print("**Job status 'unknown error', given empty results file, returning...**")
                return {
                    'statusCode': 400,
                    'body': json.dumps("ERROR: unknown, results file was empty")
                }

            msg = "error: " + lines[0]
            print("**Job status 'error', results msg:", msg)
            print("**Returning error msg to client...**")
            return {
                'statusCode': 400,
                'body': json.dumps(msg)
            }

        if status != "completed":
            msg = "error: unexpected job status of '" + status + "'"
            return {
                'statusCode': 400,
                'body': json.dumps(msg)
            }

        local_filename = "/tmp/results.txt"
        print("**Downloading results from S3**")
        bucket.download_file(results_file_key, local_filename)

        print("**Results:")
        with open(local_filename, "rb") as infile:
            data = infile.read()
            print(data.decode(errors='ignore'))  # Print data for debug purposes, ignoring errors

        data_encoded = base64.b64encode(data).decode()
        print("**DONE, returning results**")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'results': data_encoded,
                'original_data_file': original_data_file
                })
        }

    except Exception as err:
        print("**ERROR**")
        print(str(err))
        return {
            'statusCode': 400,
            'body': json.dumps(str(err))
        }
