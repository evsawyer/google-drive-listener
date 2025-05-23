from google.cloud import storage
import json
from dotenv import load_dotenv
load_dotenv()
import os
from config import settings
def update_drive_state(new_token, files):
    client = storage.Client()
    bucket = client.bucket(settings.drive_state_bucket_name)
    blob = bucket.blob(settings.drive_state_bucket_folder + '/drive_state.json')
    
    # First get existing state
    try:
        existing_state = json.loads(blob.download_as_string())
    except:
        existing_state = {}
    
    # Update only the specific fields
    existing_state.update({
        'startPageToken': new_token,
        'lastKnownFiles': files
    })
    
    # Upload the merged state back
    blob.upload_from_string(json.dumps(existing_state))

def get_drive_state():
    client = storage.Client()
    bucket = client.bucket(settings.drive_state_bucket_name)
    blob = bucket.blob(settings.drive_state_bucket_folder + '/drive_state.json')
    return json.loads(blob.download_as_string())
