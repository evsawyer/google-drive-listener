from google.cloud import storage
import json

def update_drive_state(new_token, files):
    client = storage.Client()
    bucket = client.bucket('your-bucket')
    blob = bucket.blob('drive_state.json')
    
    state = {
        'startPageToken': new_token,
        'lastKnownFiles': files
    }
    blob.upload_from_string(json.dumps(state))

def get_drive_state():
    client = storage.Client()
    bucket = client.bucket('your-bucket')
    blob = bucket.blob('drive_state.json')
    return json.loads(blob.download_as_string())
