import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd



def album(data):
    album_collated_data = []
    for i in data['items']:
        album_id = i['track']['album']['id']
        album_name = i['track']['album']['name']
        album_release_date = i['track']['album']['release_date']
        album_total_tracks = i['track']['album']['total_tracks']
        album_info = {'album_id': album_id, 'name': album_name, 'album_release_date': album_release_date, 'album_total_tracks':album_total_tracks}
        album_collated_data.append(album_info)
    return album_collated_data
    
def artist(data):
    artist_list=[]
    for i in data['items']:
        for key,value in i.items():
            if key == 'track':
                for j in value['artists']:
                    artist_info = {'artist_id': j['id'],'artist_name':j['name']} #and other fields can be pulled the same way
                    artist_list.append(artist_info)
    return artist_list
    
def songs(data):
    songs_list =[]
    for i in data['items']:
        song_id = i['track']['id']
        song_name = i['track']['name']
        song_duration = i['track']['duration_ms']
        song_popularity = i['track']['popularity']
        album_id = i['track']['album']['id']
        artist_id = i['track']['album']['artists'][0]['id']
        songs_dict={'song_id': song_id,'song_name':song_name,'song_duration':song_duration,'song_popularity':song_popularity,
                'album_id':album_id, 'artist_id':artist_id}
        songs_list.append(songs_dict)
    return songs_list
    
def lambda_handler(event, context):
    s3=boto3.client('s3')                             #s3 object created, to access s3 services.
    
    Bucket="spotify-etl-project-rachita"
    Key="raw_data/to_processed/"
    
  #  print(s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents'])   # prefix is the key. 
    spotify_data=[]
    spotify_keys=[]

    
    for i in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        
        file_key=i['Key']
   #     print(file_key)
        if(file_key.split('.')[-1]=='json'):
            data = s3.get_object(Bucket=Bucket, Key=file_key)
            content=data['Body']
            jsonObject=json.loads(content.read())
            print(jsonObject)
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:      #called the functions to get the lists and transformed them into df and applied other functions.Now we will put it in the targetted location in s3
        album_list = album(data)
        artist_list = album(data)
        songs_list = songs(data)
        
        album_df = pd.DataFrame(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'],keep='first')
        
        artist_df = pd.DataFrame(artist_list)
        #artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        songs_df=pd.DataFrame(songs_list)
        
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        
        #now we will create the file name in the transformed folder for proper storage of the data/logical saving of the data.
        
        song_key = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) + ".csv"
        
        #steps to convert song df to csv format.
        #key is nothing but the path of the location where i want my data to be stored.
        song_buffer=StringIO()
        songs_df.to_csv(song_buffer,index=False)  # index = false added tp disable the index from the csv file as glue crawler cannot read through entire schema if we have index in place.
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body = song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        #steps to convert song df to csv format.
        #key is nothing but the path of the location where i want my data to be stored.
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body = album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        #steps to convert song df to csv format.
        #key is nothing but the path of the location where i want my data to be stored.
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body = artist_content)
        
    
    #now, since, each time the function runs, the data will be processed and the same info will be stored in the folders with different date and timestamp.
    #to avoid this , we will process the json files, copy them in the processed  folders and delete it from to be processed folder.
    #for that the below code will be used. we will utilise, resource service. 
    #earlier to communicate with s3 (read and write data), we were using boto3.client('s3'), now we will use. boto3.resource('s3')
    
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {               #dictionary created for the source features i.e. source bucket and source key.
            'Bucket' : Bucket,        #source bucket
            'Key' : key
        }
        
        #syntax to copy the data file from one location and paste to another.
        #we pass the source information (i.e. source details dict) and the tareg information (i.e. target bucket and target key)
        #bucket in the code below is the target bucket. humne bucket hi pass kiya hai kyunki in this case, target and source bucket is same.
        
        s3_resource.meta.client.copy(copy_source, Bucket,'raw_data/processed/'+ key.split('/')[-1])
        
        #delete krne ka syntax
        s3_resource.Object(Bucket,key).delete()
            


   