# the purpose of this script is to refresh the channel for a given google drive.
# this channel id should be stored in cloud storage in a folder channel/<drive id>/<channel id>.json

import os
import uuid
import time
import json
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage
from google.auth.transport import requests as google_auth_requests
from config import settings
from cloud_storage_functions import get_service_account_info
from drive_functions import get_watched_files
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
# FOLDER_ID = settings.folder_id
DRIVE_ID = settings.drive_id
# WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_URL = 'https://scout-listener-104817932138.europe-west1.run.app/drive-notifications'
DRIVE_STATE_BUCKET_NAME = settings.drive_state_bucket_name
BUCKET_FOLDER = settings.drive_state_bucket_folder
SERVICE_ACCOUNT_BUCKET_NAME = settings.service_account_bucket_name

def setup_drive_notifications():
    """Set up Google Drive API notifications for changes using the changes API."""
    logger.info(f"Setting up Drive notifications for Drive: {DRIVE_ID}")
    logger.info(f"Using webhook URL: {WEBHOOK_URL}")
    
    # Set up credentials and drive service
    SCOPES = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.readonly'  # Added for channels.list
    ]
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # Get initial list of files in the folder
    watched_files = get_watched_files(drive_service=drive_service, drive_id=DRIVE_ID)
    
    logger.info(f"Found {len(watched_files)} files in folder")
    
    # Rest of your existing setup code...
    start_page_token_response = drive_service.changes().getStartPageToken().execute()
    start_page_token = start_page_token_response.get('startPageToken')
    
    channel_id = f"drive-webhook-{DRIVE_ID}-{uuid.uuid4()}"
    channel = {
        'id': channel_id,
        'type': 'web_hook',
        'address': WEBHOOK_URL,
    }
    
    response = drive_service.changes().watch(
        pageToken=start_page_token,
        body=channel,
    ).execute()
    
    # Return channel info with the lastKnownFiles included
    return {
        'channelId': response['id'],
        'webhookUrl': WEBHOOK_URL,
        'resourceId': response['resourceId'],
        'expiration': response.get('expiration'),
        'startPageToken': start_page_token,
        'driveId': DRIVE_ID,
    }

def store_channel_info(channel_info):
    """Store channel information in Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(DRIVE_STATE_BUCKET_NAME)  # Use DRIVE_STATE_BUCKET_NAME for drive state
        blob = bucket.blob(f'{BUCKET_FOLDER}/drive_state.json')
        blob.upload_from_string(json.dumps(channel_info, indent=2))
        # Log the last 7 characters of the channel ID
        channel_id = channel_info.get('channelId', '')
        last_seven = channel_id[-7:]
        logger.info(f"Channel information saved to Cloud Storage (Channel ID: ...{last_seven})")
    except Exception as e:
        logger.error(f"Error saving to Cloud Storage: {e}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Initializing Google Drive notification channel...")
        
        # Verify required environment variables are set
        if not WEBHOOK_URL:
            raise ValueError("WEBHOOK_URL environment variable not set in .env file")
        if not DRIVE_STATE_BUCKET_NAME:
            raise ValueError("DRIVE_STATE_BUCKET_NAME environment variable not set in .env file")
        if not SERVICE_ACCOUNT_BUCKET_NAME:
            raise ValueError("SERVICE_ACCOUNT_BUCKET_NAME environment variable not set in .env file")
            
        channel_info = setup_drive_notifications()

        
        logger.info(f"Successfully set up notifications!")
        
        # Store channel info in Cloud Storage
        store_channel_info(channel_info)
        
        logger.info("\nIMPORTANT: Channel information has been saved to Cloud Storage!")
        
    except Exception as e:
        logger.error(f"Error setting up notifications: {e}")