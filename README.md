# Setup and Purpose
The main purpose of this app is to run basic-pitch which converts a song into a midi file. https://basicpitch.spotify.com/

Each folder represents an AWWS Lambda function. Need to create an API gateway, AWS RDS instance (with the same database schema as in the repo), and S3 bucket. Also need to create custom layers for functions which use specific libraries/packages (e.g. ffmpeg). Finally, need to create a docker image for the basic-pitch function and push it to ECR for deployment
# Music Analysis Service

Asynchronous API to run Basic-pitch by Spotify. 

<img width="460" alt="image" src="https://github.com/eton1234/music-transcription-api/assets/50380126/d5a01586-4e1e-42f5-8dcf-7bde37bb9b69">

User uploads a song either from source or through youtube. Song is downloaded to S3 bucket and an S3 trigger kicks off the basic-pitch async API. RDS is used to keep track of the status of basic-pitch jobs in the queue.


# Auth Service 
Auth service implemented from scratch (for learning) using session-based auth.

<img width="591" alt="image" src="https://github.com/eton1234/music-transcription-api/assets/50380126/5ca6c9f6-9b1d-4536-9681-4587362de356">
The username and password is inputted by the client, the /post new_user function hashes the password, then commits the username,pwd_hash combination into the users table. Any service requiring authentication interacts with the /auth microservice and verifies the session token.


# File Conversion (WAV to MP3)

<img width="294" alt="image" src="https://github.com/eton1234/music-transcription-api/assets/50380126/470647f1-e23a-4d01-a5e0-7f9ccf25dcef">

For converting the WAV to MP3, we would have the client specify which WAV file they want to convert, it calls the wav_to_mp3 lambda function that does the actual conversion (using fffmpeg/pydub), and it posts the MP3 results in the S3 bucket. Now we have a compressed audio file! 

