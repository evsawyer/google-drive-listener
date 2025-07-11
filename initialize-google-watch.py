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
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
FOLDER_ID = settings.folder_id
DRIVE_ID = settings.drive_id
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
channel_state_BUCKET_NAME = settings.channel_state_bucket_name
BUCKET_FOLDER = settings.channel_state_bucket_folder
SERVICE_ACCOUNT_BUCKET_NAME = settings.service_account_bucket_name
SERVICE_ACCOUNT_KEY = settings.service_account_key

def store_channel_info(channel_info):
    """Store channel information in Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(CHANNEL_STATE_BUCKET_NAME)  # Use CHANNEL_STATE_BUCKET_NAME for drive state
        blob = bucket.blob(f'{BUCKET_FOLDER}/channel_state.json')
        blob.upload_from_string(json.dumps(channel_info, indent=2))
        # Log the last 7 characters of the channel ID
        channel_id = channel_info.get('channelId', '')
        last_seven = channel_id[-7:]
        logger.info(f"Channel information saved to Cloud Storage (Channel ID: ...{last_seven})")
    except Exception as e:
        logger.error(f"Error saving to Cloud Storage: {e}")
        raise

def get_service_account_info():
    """Get service account info from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(SERVICE_ACCOUNT_BUCKET_NAME)  # Use SERVICE_ACCOUNT_BUCKET_NAME for service account
    blob = bucket.blob(SERVICE_ACCOUNT_KEY)
    return json.loads(blob.download_as_string())

def setup_drive_notifications():
    """Set up Google Drive API notifications for changes using the changes API."""
    logger.info(f"Setting up Drive notifications for folder: {FOLDER_ID}")
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
    logger.info(f"Getting initial list of files in folder {FOLDER_ID}")
    folder_files_response = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name, mimeType, modifiedTime)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="drive",
        driveId=DRIVE_ID
    ).execute()
    
    watched_files = folder_files_response.get('files', [])
    logger.info(f"Found {len(watched_files)} files in folder")
    
    # Rest of your existing setup code...
    start_page_token_response = drive_service.changes().getStartPageToken().execute()
    start_page_token = start_page_token_response.get('startPageToken')
    
    channel_id = f"drive-webhook-{uuid.uuid4()}"
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
        'folderID': FOLDER_ID,
        'driveId': DRIVE_ID,
        'lastKnownFiles': watched_files  # Add the initial file list
    }

if __name__ == "__main__":
    try:
        logger.info("Initializing Google Drive notification channel...")
        
        # Verify required environment variables are set
        if not FOLDER_ID:
            raise ValueError("FOLDER_ID environment variable not set in .env file")
        if not WEBHOOK_URL:
            raise ValueError("WEBHOOK_URL environment variable not set in .env file")
        if not CHANNEL_STATE_BUCKET_NAME:
            raise ValueError("CHANNEL_STATE_BUCKET_NAME environment variable not set in .env file")
        if not SERVICE_ACCOUNT_BUCKET_NAME:
            raise ValueError("SERVICE_ACCOUNT_BUCKET_NAME environment variable not set in .env file")
            
        channel_info = setup_drive_notifications()

        
        logger.info(f"Successfully set up notifications!")
        
        # Store channel info in Cloud Storage
        store_channel_info(channel_info)
        
        logger.info("\nIMPORTANT: Channel information has been saved to Cloud Storage!")
        
    except Exception as e:
        logger.error(f"Error setting up notifications: {e}")