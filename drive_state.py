from google.cloud import storage
import json
from dotenv import load_dotenv
load_dotenv()
import os
from config import settings
import logging
logger = logging.getLogger(__name__)

def ensure_bucket_exists(client, bucket_name):
    """Create bucket if it doesn't exist"""
    try:
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            bucket.create()
        return bucket
    except Exception as e:
        # Create the bucket
        bucket = client.create_bucket(bucket_name)
        return bucket


def update_drive_state(new_token):
    client = storage.Client()
    bucket = ensure_bucket_exists(client, settings.bucket_name)
    blob = bucket.blob(settings.drive_state_folder + '/drive_state.json')
    
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

def get_drive_state():
    client = storage.Client()
    bucket = client.bucket(settings.bucket_name)
    blob = bucket.blob(settings.drive_state_folder + '/drive_state.json')
    try:
        return json.loads(blob.download_as_string())
    except Exception as e:
        logger.warning(f"Drive state not found in bucket '{settings.bucket_name}' and folder '{settings.drive_state_folder}': {e}")
        return "No drive state found"
