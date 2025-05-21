from google.cloud import storage
import json
from dotenv import load_dotenv
load_dotenv()
import os

def update_drive_state(new_token, files):
    client = storage.Client()
    bucket = client.bucket(os.getenv('BUCKET_NAME'))
    blob = bucket.blob('drive_state.json')
    
    state = {
        'startPageToken': new_token,
        'lastKnownFiles': files
    }
    blob.upload_from_string(json.dumps(state))

def get_drive_state():
    client = storage.Client()
    bucket = client.bucket(os.getenv('BUCKET_NAME'))
    blob = bucket.blob('drive_state.json')
    return json.loads(blob.download_as_string())
