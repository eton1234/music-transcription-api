from pytube import YouTube
import pytube
import os
import subprocess
import ssl
from io import BytesIO
import sys

#dd
# Uploads an audio file to S3 and then inserts a new job record
# in the BenfordApp database with a status of 'uploaded'.
# Sends the job id back to the client.
#

import json
import boto3
import os
import uuid
import base64
import pathlib
import datatier

from configparser import ConfigParser

#Function Purpose: 
#1. Downloads a song from a youtube url using Pytube as an .mp4 byte stream
#2. Converts .mp4 into a .wav file and downloads locally
#3. Uploads to s3 similarly to project03 upload


#needed to prevent weird ssl issues with pytube
#ref: https://stackoverflow.com/questions/72306952/error-in-pytube-urlopen-error-ssl-certificate-verify-failed-certificate-veri
ssl._create_default_https_context = ssl._create_stdlib_context
#downloads the youtube video into memory
def download_audio(link,):
    yt = YouTube(str(link))
    audio_stream = yt.streams.filter(only_audio=True).first()
    buffer = BytesIO()
    audio_stream.stream_to_buffer(buffer)
    print("audio_stream", audio_stream)
    return buffer
    
#Converts mp4 from byte stream into a wav which is downloaded as "/tmp/data.wav"
def mp4_to_wav_download(buffer):
    # Convert mp4 to wav using ffmpeg
    #consulted chatgpt for the ffmpeg arguments
    #referenced: https://stackoverflow.com/questions/67877611/passing-bytes-to-ffmpeg-in-python-with-io
    process = subprocess.Popen(['ffmpeg', '-i', '-', '-vn', '-ac', '2', '-f', 'wav', '/tmp/data.wav'], 
                               stdin=subprocess.PIPE)
    process.communicate(input=buffer.getvalue())

        
        
def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: proj03_upload**")
    
    #
    # setup AWS based on config file:
    #
    config_file = 'benfordapp-config.ini'
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
    # userid from event: could be a parameter
    # or could be part of URL path ("pathParameters"):
    #
    print("**Accessing event/pathParameters**")
    
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
  
    print("**Accessing request body**")
    
    if "body" not in event:
      raise Exception("event has no body")
      
    body = json.loads(event["body"]) # parse the json
    

    filename = body["filename"]
    url = body["url"]
    
    print("filename:", filename)
    print("url is:", url)

    #
    # open connection to the database:
    #
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

    print("**Downloading file to lambda*")    
    #download the mp3 now 
    mp4_to_wav_download(download_audio(url))
    #
    # generate unique filename in preparation for the S3 upload:
    #
    print("**Uploading local file to S3**")
    
    basename = pathlib.Path(filename).stem
    extension = pathlib.Path(filename).suffix
    
    if extension != ".wav" : 
      raise Exception("expecting filename to have .wav extension")
    
    bucketkey = "benfordapp/" + username + "/" + basename + "-" + str(uuid.uuid4()) + ".wav"
    
    print("S3 bucketkey:", bucketkey)
    

    #
    # Remember that the processing of the PDF is event-triggered,
    # and that lambda function is going to update the database as
    # is processes. So let's insert a job record into the database
    # first, THEN upload the PDF to S3. The status column should 
    # be set to 'uploaded':
    #
    print("**Adding jobs row to database**")
    
    sql = """
      INSERT INTO jobs(userid, status, originaldatafile, datafilekey, resultsfilekey)
                  VALUES(%s, %s, %s, %s, '');
    """
    
    #
    # TODO #2 of 3: what values should we insert into the database?
    #
    datatier.perform_action(dbConn, sql, [int(userid), "uploaded", str(filename), str(bucketkey)])

    #
    # grab the jobid that was auto-generated by mysql:
    #
    sql = "SELECT LAST_INSERT_ID();"
    
    row = datatier.retrieve_one_row(dbConn, sql)
    
    jobid = row[0]
    
    print("jobid:", jobid)

    #
    # now that DB is updated, let's upload PDF to S3:
    #
    print("**Uploading data file to S3**")

    #
    # TODO #3 of 3: what are we uploading to S3? replace the
    # ??? with what we are uploading:
    #
    bucket.upload_file("/tmp/data.wav",
                      bucketkey, 
                      ExtraArgs={
                        'ContentType': 'audio/wav'
                      })

    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning jobid**")
    
    return {
      'statusCode': 200,
      'body': json.dumps(str(jobid))
    }
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    
    return {
      'statusCode': 400,
      'body': json.dumps(str(err))
    }
