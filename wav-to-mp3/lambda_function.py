# import json
# import boto3
# import os
# import pathlib
# import urllib.parse
# import datatier
# from configparser import ConfigParser
# from pydub import AudioSegment

# def lambda_handler(event, context):
#     try:
#         print("**STARTING**")
#         print("**lambda: wav_to_mp3**")
        
#         # 
#         # in case we get an exception, initial this filename
#         # so we can write an error message if need be:
#         #
#         bucketkey_results_file = ""
        
#         # Setup AWS based on config file
#         config_file = 'benfordapp-config.ini'
#         os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
        
#         configur = ConfigParser()
#         configur.read(config_file)
        
#         # Configure for S3 access
#         s3_profile = 's3readwrite'
#         boto3.setup_default_session(profile_name=s3_profile)
        
#         bucketname = configur.get('s3', 'bucket_name')
        
#         s3 = boto3.resource('s3')
#         bucket = s3.Bucket(bucketname)
        
#         #
#         # configure for RDS access
#         #
#         rds_endpoint = configur.get('rds', 'endpoint')
#         rds_portnum = int(configur.get('rds', 'port_number'))
#         rds_username = configur.get('rds', 'user_name')
#         rds_pwd = configur.get('rds', 'user_pwd')
#         rds_dbname = configur.get('rds', 'db_name')
        
#         print("**Accessing event/pathParameters**")
        
#         # if "assetid" not in event:
#         #     raise Exception("Asset ID is missing in the event")
        
#         # assetid = event['assetid']
#         if "assetid" in event:
#             assetid = event["assetid"]
#         elif "pathParameters" in event:
#           if "assetid" in event["pathParameters"]:
#             assetid = event["pathParameters"]["assetid"]
#           else:
#             raise Exception("requires assetid parameter in pathParameters")
#         else:
#             raise Exception("requires assetid parameter in event")
            
#         print("assetid:", assetid)
        
        
#         # print("**Accessing request body**")

#         # if "body" not in event:
#         #     raise Exception("event has no body")
          
#         # body = json.loads(event["body"]) # parse the json
        
#         # Open connection to the database
#         print("**Opening connection to database**")
#         dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

#         print("**Retrieving file information from the database**")
#         sql = "SELECT * FROM assets WHERE assetid = %s;"
#         file_info = datatier.retrieve_one_row(dbConn, sql, [assetid])
        
#         if not file_info:
#             raise Exception("No file found for the provided asset ID")
        
#         filename = file_info[1]
#         wav_file_key = file_info[2]

#         print("**Downloading and processing WAV file:", filename, "**")

#         # Verify that the uploaded file is a WAV file
#         bucketkey = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#         print("bucketkey:", bucketkey)
        
#         extension = pathlib.Path(bucketkey).suffix
#         if extension != ".wav":
#             raise Exception("expecting S3 document to have .wav extension")

#         # Create local file paths
#         local_wav = f"/tmp/{os.path.basename(filename)}"
#         local_mp3 = local_wav.replace('.wav', '.mp3')

#         print("**Downloading the WAV file**")
#         s3.download_file(bucketname, wav_file_key, local_wav)

#         print("**Converting WAV to MP3**")
#         audio = AudioSegment.from_wav(local_wav)
#         audio.export(local_mp3, format="mp3")

#         print("**Uploading the MP3 file**")
#         mp3_file_key = wav_file_key.replace('.wav', '.mp3')
#         s3.upload_file(local_mp3, bucketname, mp3_file_key, ExtraArgs={
#             'ACL': 'public-read',
#             'ContentType': 'audio/mpeg'
#         })

#         print("**DONE, returning success**")
#         return {
#             'statusCode': 200,
#             'body': json.dumps("success")
#         }
    
#     except Exception as err:
#         print("**ERROR**")
#         print(str(err))
        
#         # Write error to local file
#         local_results_file = "/tmp/results.txt"
#         with open(local_results_file, "w") as outfile:
#             outfile.write(str(err))
#             outfile.write("\n")
        
#         if bucketkey_results_file == "":
#             #
#             # we can't upload the error file:
#             #
#             pass
#         else:
#             # 
#               # upload the error file to S3
#               #
#             print("**UPLOADING**")
#             bucket.upload_file(local_results_file,
#                               bucketkey_results_file,
#                               ExtraArgs={
#                                   'ACL': 'public-read',
#                                   'ContentType': 'text/plain'
#                               })
        
#         # Done, return
#         return {
#             'statusCode': 400,
#             'body': json.dumps(str(err))
#         }

import json
import boto3
import os
import pathlib
import urllib.parse
import base64
from configparser import ConfigParser
from pydub import AudioSegment

def lambda_handler(event, context):
    try:
        print("**STARTING**")
        print("**lambda: wav_to_mp3**")
        
        # 
        # in case we get an exception, initial this filename
        # so we can write an error message if need be:
        #
        bucketkey_results_file = ""
        
        # Setup AWS based on config file
        config_file = 'benfordapp-config.ini'
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
        
        configur = ConfigParser()
        configur.read(config_file)
        
        # Configure for S3 access
        s3_profile = 's3readwrite'
        boto3.setup_default_session(profile_name=s3_profile)
        
        bucketname = configur.get('s3', 'bucket_name')
        
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketname)
        
        # Get the list of WAV files in the bucket
        wav_files = [obj.key for obj in bucket.objects.all() if pathlib.Path(obj.key).suffix == ".wav"]
        
        print(wav_files)
        
        # Check if a specific WAV file is specified
        if 'filename' in event:
            bucketkey = event['filename']
        elif "pathParameters" in event:
          if "filename" in event["pathParameters"]:
            bucketkey = event["pathParameters"]["filename"]
          else:
            raise Exception("requires filename parameter in pathParameters")
        else:
            raise Exception("requires filename parameter in event")
            
        print("bucketkey:", bucketkey)
        
        extension = pathlib.Path(bucketkey).suffix
        if bucketkey not in wav_files:
            raise Exception(f"Specified file {bucketkey} not found in the bucket.")
        elif extension != ".wav":
            raise Exception("expecting S3 file to have .wav extension")
        wav_files = [bucketkey]
        
        for wav_file in wav_files:
            print("Processing file:", wav_file)
            local_wav = "/tmp/" + os.path.basename(wav_file)
            local_mp3 = local_wav.replace('.wav', '.mp3')
            
            # Download the WAV file from S3 to the local file system
            print("**DOWNLOADING '", wav_file, "'**")
            bucket.download_file(wav_file, local_wav)
            
            # Convert WAV to MP3
            print("**CONVERTING '", wav_file, "' to MP3**")
            audio = AudioSegment.from_wav(local_wav)
            audio.export(local_mp3, format="mp3")
            
            # Upload the MP3 file back to S3
            mp3_file_key = wav_file.replace('.wav', '.mp3')
            print("**UPLOADING to S3 file", mp3_file_key, "**")
            bucket.upload_file(local_mp3,
                         mp3_file_key,
                         ExtraArgs={
                           'ACL': 'public-read',
                           'ContentType': 'audio/mpeg'
                         })
                    
        local_filename = "/tmp/results.txt"
        print("**Downloading results from S3**")
        results_file_key = mp3_file_key
        bucket.download_file(results_file_key, local_filename)

        print("**Results:")
        with open(local_filename, "rb") as infile:
            data = infile.read()
            print(data.decode(errors='ignore'))  # Print data for debug purposes, ignoring errors

        data_encoded = base64.b64encode(data).decode()

        print("**DONE, returning success**")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'results': data_encoded,
                'results_file_key': results_file_key
                })
        }
    
    except Exception as err:
        print("**ERROR**")
        print(str(err))
        
        # Write error to local file
        local_results_file = "/tmp/results.txt"
        outfile = open(local_results_file, "w")
        outfile.write(str(err))
        outfile.write("\n")
        outfile.close()
        
        if bucketkey_results_file == "":
            #
            # we can't upload the error file:
            #
            pass
        else:
            # 
            # upload the error file to S3
            #
            print("**UPLOADING**")
            bucket.upload_file(local_results_file,
                               bucketkey_results_file,
                               ExtraArgs={
                                   'ACL': 'public-read',
                                   'ContentType': 'text/plain'
                               })
        #
        # done, return:
        #  
        
        return {
            'statusCode': 400,
            'body': json.dumps(str(err))
        }

