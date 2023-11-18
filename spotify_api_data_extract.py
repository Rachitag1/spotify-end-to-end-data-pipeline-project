import json
import os
import spotipy 
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    
    client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret= client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_link.split('/')
    playlist_URI=playlist_link.split('/')[4]
    data = sp.playlist_tracks(playlist_URI)
    
    #print(data)
    client = boto3.client('s3')
    
    filename = "spotify_raw_"+str(datetime.now()) + ".json"  #added a file , now we will use this name to store my data in the bucket. warna bina kisi name ke store hoga data sirf '/' mein.
    
    client.put_object(
        Bucket= "spotify-etl-project-rachita" ,   #bucket is the name of the bucket where we need to store the data
        Key= "raw_data/to_processed/" + filename,            # key is the path of the folder where i need to store the data. humne slash wala delete krdiya object.
        Body= json.dumps(data)                    #data which we need to store. Now the data pulled from spotify is in json format. dumps will convert the entire thing to json string and will upload in s3.  
        )   