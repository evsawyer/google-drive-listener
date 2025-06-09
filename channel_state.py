from google.cloud import storage
import json
from dotenv import load_dotenv
load_dotenv()
import os
from config import settings

def update_channel_state(new_token):
    client = storage.Client()
    bucket = client.bucket(settings.channel_state_bucket_name)
    blob = bucket.blob(settings.channel_state_bucket_folder + '/channel_state.json')
    
    # First get existing state
    try:
        existing_state = json.loads(blob.download_as_string())
    except:
        existing_state = {}
    
    # Update only the specific fields
    existing_state.update({
        'startPageToken': new_token,
    })
    
    # Upload the merged state back
    blob.upload_from_string(json.dumps(existing_state))

def get_channel_state():
    client = storage.Client()
    bucket = client.bucket(settings.channel_state_bucket_name)
    blob = bucket.blob(settings.channel_state_bucket_folder + '/channel_state.json')
    return json.loads(blob.download_as_string())
