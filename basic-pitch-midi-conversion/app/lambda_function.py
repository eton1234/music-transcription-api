from basic_pitch.inference import predict, Model
from basic_pitch import ICASSP_2022_MODEL_PATH
import sys

import json
import boto3
import os
import uuid
import base64
import pathlib
import datatier
import urllib.parse
import string

from configparser import ConfigParser

basic_pitch_model = Model(ICASSP_2022_MODEL_PATH)

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: proj03_compute**")
    
    # 
    # in case we get an exception, initial this filename
    # so we can write an error message if need be:
    #
    bucketkey_results_file = ""
    
    #
    # setup AWS based on config file:
    #
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
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    
    #
    # configure for RDS access
    #
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')
    
    #
    # this function is event-driven by a PDF being
    # dropped into S3. The bucket key is sent to 
    # us and obtain as follows:
    #
    bucketkey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    print("bucketkey:", bucketkey)
      
    extension = pathlib.Path(bucketkey).suffix
    
    if extension != ".wav" : 
      raise Exception("expecting S3 document to have .wav extension")
    
    bucketkey_results_file = bucketkey[0:-4] + ".mid"
    
    print("bucketkey results file:", bucketkey_results_file)
      
    #
    # download PDF from S3 to LOCAL file system:
    #
    print("**DOWNLOADING '", bucketkey, "'**")

    #
    # TODO #1 of 8: where do we write local files? Replace
    # the ??? with the local directory where we have access.
    #
    local_wav = "/tmp/data.wav"
    
    bucket.download_file(bucketkey, local_wav)

    #
    # open LOCAL pdf file:
    #
    print("**PROCESSING local WAV**")

    #
    # TODO #2 of 8: update status column in DB for this job,
    # change the value to "processing - starting". Use the
    # bucketkey --- stored as datafilekey in the table ---
    # to identify the row to update. Use the datatier.
    #
    # open connection to the database:
    #
    print("**Opening DB connection**")
    #
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    
    print("setting status of job to processing")
    sql = "UPDATE jobs SET status='processing - starting' WHERE datafilekey = %s"
    datatier.perform_action(dbConn,sql, parameters=[bucketkey] )
    
    #start processing the wav with basic-pitch
    local_results_file = "/tmp/results.mid"
    model_output, midi_data, note_events = predict(local_wav, basic_pitch_model)
    midi_data.write(local_results_file)

    
    sql = f'UPDATE jobs SET status="processing - analyzed" WHERE datafilekey = %s'
    datatier.perform_action(dbConn,sql, parameters=[bucketkey] )
    

    print("**UPLOADING to S3 file", bucketkey_results_file, "**")

    bucket.upload_file(local_results_file,
                       bucketkey_results_file,
                       ExtraArgs={
                         'ACL': 'public-read',
                         'ContentType': 'audio/midi'
                       })
    

    sql = f'UPDATE jobs SET status="completed", resultsfilekey=%s WHERE datafilekey = %s'
    datatier.perform_action(dbConn,sql, parameters=[bucketkey_results_file, bucketkey])
    
    
    
    #
    # done!
    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning success**")
    
    return {
      'statusCode': 200,
      'body': json.dumps("success")
    }
    
  #
  # on an error, try to upload error message to S3:
  #
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    

    #
    # update jobs row in database:
    #
    # TODO #8 of 8: open connection, update job in database
    # to reflect that an error has occurred. The job is 
    # identified by the bucketkey --- datafilekey in the 
    # table. Set the status column to 'error' and set the
    # resultsfilekey column to the contents of the variable
    # bucketkey_results_file.
    #
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    sql = "UPDATE jobs SET status ='error', resultsfilekey= %s  WHERE datafilekey = %s"
    datatier.perform_action(dbConn,sql, parameters=[str(bucketkey_results_file),str(bucketkey)])
    #
    # done, return:
    #    
    return {
      'statusCode': 400,
      'body': json.dumps(str(err))
    }
