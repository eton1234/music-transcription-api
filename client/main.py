import json
import requests
import jsons

import uuid
import pathlib
import logging
import sys
import os
import base64

import time

from configparser import ConfigParser

from getpass import getpass



############################################################
#
# classes
#
class User:

  def __init__(self, row):
    self.userid = row[0]
    self.username = row[1]
    self.pwdhash = row[2]


class Job:

  def __init__(self, row):
    self.jobid = row[0]
    self.userid = row[1]
    self.status = row[2]
    self.originaldatafile = row[3]
    self.datafilekey = row[4]
    self.resultsfilekey = row[5]

class Asset:

  def __init__(self, row):
    self.assetid = row[0]
    self.userid = row[1]
    self.assetname = row[2]
    self.bucketkey = row[3]


############################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number

  Parameters
  ----------
  None

  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => end")
  print("   1 => songs")
  print("   2 => users")
  print("   3 => jobs")
  print("   4 => reset jobs")
  print("   5 => upload song")
  print("   6 => upload song via youtube")
  print("   7 => download song")
  print("   8 => login")
  print("   9 => authenticate")
  print("  10 => new user")
  print("  11 => convert wav to mp3 file")
  print("  12 => logout")

  cmd = input()

  if cmd == "":
    cmd = -1
  elif not cmd.isnumeric():
    cmd = -1
  else:
    cmd = int(cmd)

  return cmd


############################################################
#
# users
#
def songs(baseurl):
  """
  Prints out all the songs in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #

    print("Enter user id>")
    userid = input()

    api = '/songs'
    url = baseurl + api + "/" + userid

    res = requests.get(url)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    body = res.json()
    body = body['data']

    assets = []
    for row in body:
      if row['Key'].endswith('.mid'):
        assets.append(row) 

    if len(assets) == 0:
      print("no songs...")
      return

    for asset in assets:
      print(asset['Key'])

    return

  except Exception as e:
    logging.error("songs() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return

def users(baseurl):
  """
  Prints out all the users in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/users'
    url = baseurl + api

    res = requests.get(url)

    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    body = res.json()

    users = []
    for row in body:
      user = User(row)
      users.append(user)

    if len(users) == 0:
      print("no users...")
      return

    for user in users:
      print(user.userid)
      print(" ", user.username)
      print(" ", user.pwdhash)
    #
    return

  except Exception as e:
    logging.error("users() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return

############################################################
#
# jobs
#
def jobs(baseurl, token):
  """
  Prints out all the jobs in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/jobs'
    url = baseurl + api

    #
    # make request:
    #
    if token is None:
      print("No current token, please login")
      return
    req_header = {"Authentication": token}
    res = requests.get(url, headers=req_header)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      # elif res.status_code == 401:
      #   body = res.json()
      #   print(body)
      #   return
      #
      return

    #
    # deserialize and extract jobs:
    #

    #
    # let's map each row into an Job object:
    #
    body = res.json()
    jobs = []
    for row in body:
      job = Job(row)
      jobs.append(job)
    #
    # Now we can think OOP:
    #
    if len(jobs) == 0:
      print("no jobs...")
      return

    for job in jobs:
      print(job.jobid)
      print(" ", job.userid)
      print(" ", job.status)
      print(" ", job.originaldatafile)
      print(" ", job.datafilekey)
      print(" ", job.resultsfilekey)
    #
    return

  except Exception as e:
    logging.error("jobs() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# reset
#
def reset(baseurl):
  """
  Resets the database back to initial state.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/reset'
    url = baseurl + api

    res = requests.delete(url)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # deserialize and print message
    #
    body = res.json()

    msg = body

    print(msg)
    return

  except Exception as e:
    logging.error("reset() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return
############################################################
#
# reset
#
def reset(baseurl):
  """
  Resets the database back to initial state.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    #
    # call the web service:
    #
    api = '/reset'
    url = baseurl + api

    res = requests.delete(url)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # deserialize and print message
    #
    body = res.json()

    msg = body

    print(msg)
    return

  except Exception as e:
    logging.error("reset() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# upload
#
def uploadyoutube(baseurl):
  print("Enter userid>")
  userid = input()
  print("Enter wav filename>")
  local_filename = input()
  print("Enter url>")
  url = input()
  print("the url was" , url)
  # call the web service:
  #
  data = {"filename": local_filename, "url": url}
  api = '/song/youtube'
  api_url = baseurl + api + "/" + userid

  res = requests.post(api_url, json=data)

  #
  # let's look at what we got back:
  #
  if res.status_code != 200:
    # failed:
    print("Failed with status code:", res.status_code)
    print("url: " + url)
    if res.status_code == 400:
      # we'll have an error message
      body = res.json()
      print("Error message:", body)
    #
    return

  #
  # success, extract jobid:
  #
  body = res.json()

  jobid = body

  print("Wav file uploaded, job id =", jobid)


def upload(baseurl):
  """
  Prompts the user for a local filename and user id, 
  and uploads that asset (PDF) to S3 for processing. 

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """
  print("Enter the userid")
  userid = input()

  print("Enter wav filename>")
  local_filename = input()

  if not pathlib.Path(local_filename).is_file():
    print("Wav file '", local_filename, "' does not exist...")
    return

  try:
    #
    # build the data packet:
    #
    infile = open(local_filename, "rb")
    bytes = infile.read()
    infile.close()

    #
    # now encode the pdf as base64. Note b64encode returns
    # a bytes object, not a string. So then we have to convert
    # (decode) the bytes -> string, and then we can serialize
    # the string as JSON for upload to server:
    #
    data = base64.b64encode(bytes)
    datastr = data.decode()

    data = {"filename": local_filename, "data": datastr}

    #
    # call the web service:
    #
    api = '/song' + "/" + userid
    url = baseurl + api 
    res = requests.post(url, json=data)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # success, extract jobid:
    #
    body = res.json()

    jobid = body

    print("Wav file uploaded, job id =", jobid)
    return

  except Exception as e:
    logging.error("upload() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# download
#
def download(baseurl):
  """
  Prompts the user for the job id, and downloads
  that asset (PDF).

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  print("Enter job id>")
  jobid = input()

  try:

    api = '/results'
    url = baseurl + api + '/' + jobid

    res = requests.get(url)

    #
    # let's look at what we got back:
    #
    if res.status_code != 200:
      #
      # failed: but "failure" with download is how status
      # is returned, so let's look at what we got back
      #
      msg = res.json()

      if msg.startswith("uploaded"):
        print("No results available yet...")
        print("Job status:", msg)
        return

      if msg.startswith("processing"):
        print("No results available yet...")
        print("Job status:", msg)
        return

      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    body = res.json()

    datastr = body.get("results")
    original_data_file = body.get("original_data_file")
    
    newfilename = original_data_file.replace(".wav", ".mid")

    if not datastr:
      print("No results found in the response.")
      return

    base64_bytes = datastr.encode()
    bytes = base64.b64decode(base64_bytes)

    with open(newfilename, 'wb') as file:
      file.write(bytes)

    print("File downloaded and saved as '", newfilename, "'")
    return

  except Exception as e:
    logging.error("download() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return
def login(baseurl):
  """
  Prompts the user for a username and password, then tries
  to log them in. If successful, returns the token returned
  by the authentication service.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  token if successful, None if not
  """

  try:
    username = input("username: ")
    password = getpass()
    duration = input("# of minutes before expiration? ")

    #
    # build message:
    #
    data = {"username": username, "password": password, "duration": duration}

    #
    # call the web service to upload the PDF:
    #
    api = '/auth'
    url = baseurl + api

    res = requests.post(url, json=data)

    #
    # clear password variable:
    #
    password = None

    #
    # let's look at what we got back:
    #
    if res.status_code == 401:
      #
      # authentication failed:
      #
      body = res.json()
      print(body)
      return None

    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # success, extract token:
    #
    body = res.json()

    token = body

    print("logged in, token:", token)
    return token

  except Exception as e:
    logging.error("login() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return None


############################################################
#
# authenticate
#
def authenticate(baseurl, token):
  """
  Since tokens expire, this function authenticates the 
  current token to see if still valid. Outputs the result.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  try:
    if token is None:
      print("No current token, please login")
      return

    print("token:", token)

    #
    # build message:
    #
    data = {"token": token}

    #
    # call the web service to upload the PDF:
    #
    api = '/auth'
    url = baseurl + api

    res = requests.post(url, json=data)

    #
    # let's look at what we got back:
    #
    if res.status_code == 401:
      #
      # authentication failed:
      #
      body = res.json()
      print(body)
      return

    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    #
    # success, token is valid:
    #
    print("token is valid!")
    print("the userid is: ", res.json())
    return

  except Exception as e:
    logging.error("authenticate() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return
  
def wavtomp3(baseurl):
  try:

    print("Enter filename>")
    filename = input()

    api = '/wav-to-mp3'
    url = baseurl + api + "/" + filename

    res = requests.post(url)

    if res.status_code != 200:
      # failed:
      print("Failed with status code:", res.status_code)
      print("url: " + url)
      if res.status_code == 400:
        # we'll have an error message
        body = res.json()
        print("Error message:", body)
      #
      return

    body = res.json()

    datastr = body.get("results")
    results_file_key = body.get("results_file_key")

    if not datastr:
      print("No results found in the response.")
      return

    base64_bytes = datastr.encode()
    bytes = base64.b64decode(base64_bytes)

    with open(results_file_key, 'wb') as file:
      file.write(bytes)

    print("File downloaded and saved as '" + results_file_key + "'")
    return
    


  except Exception as e:
    logging.error("authenticate() failed:")
    logging.error("url: " + url)
    logging.error(e)
    return




def newuser(baseurl):
  print("Enter username>")
  username = input()
  print("Enter password>")
  password = getpass()
  data = {"user": username, "pass": password}
  url = baseurl + "/user"
  res = requests.post(url, json = data)
  if (res.status_code != 200):
    print("Failed with status code:", res.status_code)
  else:
    print("new user created!")
  return
  
############################################################
# main
#
try:
  print('** Welcome to BenfordApp **')
  print()

  # eliminate traceback so we just get error message:
  sys.tracebacklimit = 0

  #
  # what config file should we use for this session?
  #
  config_file = 'benfordapp-client-config.ini'

  print("Config file to use for this session?")
  print("Press ENTER to use default, or")
  print("enter config file name>")
  s = input()

  if s == "":  # use default
    pass  # already set
  else:
    config_file = s

  #
  # does config file exist?
  #
  if not pathlib.Path(config_file).is_file():
    print("**ERROR: config file '", config_file, "' does not exist, exiting")
    sys.exit(0)

  #
  # setup base URL to web service:
  #
  configur = ConfigParser()
  configur.read(config_file)
  baseurl = configur.get('client', 'webservice')

  #
  # make sure baseurl does not end with /, if so remove:
  #
  if len(baseurl) < 16:
    print("**ERROR: baseurl '", baseurl, "' is not nearly long enough...")
    sys.exit(0)

  if baseurl == "https://YOUR_GATEWAY_API.amazonaws.com":
    print("**ERROR: update config file with your gateway endpoint")
    sys.exit(0)

  if baseurl.startswith("http:"):
    print("**ERROR: your URL starts with 'http', it should start with 'https'")
    sys.exit(0)

  lastchar = baseurl[len(baseurl) - 1]
  if lastchar == "/":
    baseurl = baseurl[:-1]
  token = None
  #
  # main processing loop:
  #
  cmd = prompt()

  while cmd != 0:
    #
    if cmd == 1:
      songs(baseurl)
    elif cmd == 2:
      users(baseurl)
    elif cmd == 3:
      jobs(baseurl,token)
    elif cmd == 4:
      reset(baseurl)
    elif cmd == 5:
      upload(baseurl)
    elif cmd == 6:
      uploadyoutube(baseurl)
    elif cmd == 7:
      download(baseurl)
    elif cmd == 8:
      token = login(baseurl)
    elif cmd == 9:
      authenticate(baseurl, token)
    elif cmd == 10:
      newuser(baseurl)
    elif cmd == 11:
      wavtomp3(baseurl)
    elif cmd == 12:
      #
      # logout
      #
      token = None
    else:
      print("** Unknown command, try again...")
    #
    cmd = prompt()

  #
  # done
  #
  print()
  print('** done **')
  sys.exit(0)

except Exception as e:
  logging.error("**ERROR: main() failed:")
  logging.error(e)
  sys.exit(0)
