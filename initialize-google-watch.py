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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
FOLDER_ID = os.getenv("FOLDER_ID")
SERVICE_ACCOUNT_INFO = os.getenv("SERVICE_ACCOUNT_INFO")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
BUCKET_NAME = os.getenv("BUCKET_NAME")  # Add this to your .env file

def store_channel_info(channel_info):
    """Store channel information in Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob('drive_state.json')
        blob.upload_from_string(json.dumps(channel_info, indent=2))
        logger.info("Channel information saved to Cloud Storage")
    except Exception as e:
        logger.error(f"Error saving to Cloud Storage: {e}")
        raise

def setup_drive_notifications():
    """
    Set up Google Drive API notifications for changes using the changes API.
    
    Returns:
        dict: Channel information including channelId, resourceId, and startPageToken
    """
    logger.info(f"Setting up Drive notifications for folder: {FOLDER_ID}")
    logger.info(f"Using webhook URL: {WEBHOOK_URL}")
    
    # Set up credentials
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_INFO), scopes=SCOPES)
    
    # Build the Drive API service
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # First, get the current startPageToken - this marks the current state of the user's Drive
    start_page_token_response = drive_service.changes().getStartPageToken().execute()
    start_page_token = start_page_token_response.get('startPageToken')
    
    logger.info(f"Starting with page token: {start_page_token}")
    
    # Create a unique channel ID
    channel_id = f"drive-webhook-{uuid.uuid4()}"
    
    # Set up notification channel parameters
    channel = {
        'id': channel_id,
        'type': 'web_hook',
        'address': WEBHOOK_URL,
        # Optional: Set expiration time (max 7 days)
        # 'expiration': int((time.time() + 604800) * 1000)  # 7 days in milliseconds
    }

        # Get an authentication token for the webhook
    auth_req = google_auth_requests.Request()
    credentials.refresh(auth_req)
    token = credentials.token
        # Add authorization header to the request
    headers = {
        'Authorization': f'Bearer {token}'
    }
    # Make the watch request on the CHANGES resource, not the FILES resource
    response = drive_service.changes().watch(
        pageToken=start_page_token,
        body=channel,
        # headers=headers
    ).execute()
    
    logger.info(f"Notification channel created: {channel_id}")
    logger.info(f"Channel ID: {response['id']}, Resource ID: {response['resourceId']}")
    
    if 'expiration' in response:
        expiration_time = time.strftime('%Y-%m-%d %H:%M:%S', 
                                       time.localtime(int(response['expiration'])/1000))
        logger.info(f"Channel will expire on: {expiration_time}")
    
    # Store these values - you'll need them to stop notifications later
    # Also store the startPageToken - you'll need it to process changes
    return {
        'channelId': response['id'],
        'resourceId': response['resourceId'],
        'expiration': response.get('expiration'),
        'startPageToken': start_page_token,
        'folderID': FOLDER_ID  # Store this for reference
    }

if __name__ == "__main__":
    try:
        logger.info("Initializing Google Drive notification channel...")
        
        # Verify required environment variables are set
        if not FOLDER_ID:
            raise ValueError("FOLDER_ID environment variable not set in .env file")
        if not SERVICE_ACCOUNT_INFO:
            raise ValueError("SERVICE_ACCOUNT_INFO environment variable not set in .env file")
        if not WEBHOOK_URL:
            raise ValueError("WEBHOOK_URL environment variable not set in .env file")
        if not BUCKET_NAME:
            raise ValueError("BUCKET_NAME environment variable not set in .env file")
            
        channel_info = setup_drive_notifications()
        
        logger.info(f"Successfully set up notifications!")
        
        # Store channel info in Cloud Storage
        store_channel_info(channel_info)
        
        logger.info("\nIMPORTANT: Channel information has been saved to Cloud Storage!")
        
    except Exception as e:
        logger.error(f"Error setting up notifications: {e}")