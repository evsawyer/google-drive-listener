import os
import json
import logging
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from llama_parse_google_drive_reader import LlamaParseGoogleDriveReader
from run_pipeline import run_pipeline_for_documents
from drive_state import get_drive_state, update_drive_state
import sys
from google.cloud import storage

# Load environment variables
load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
FOLDER_ID = os.getenv("FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_service_account_info():
    """Get service account info from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(os.getenv('SERVICE_ACCOUNT_BUCKET_NAME'))  # Use SERVICE_ACCOUNT_BUCKET_NAME for service account
    blob = bucket.blob(os.getenv('SERVICE_ACCOUNT_KEY'))
    return json.loads(blob.download_as_string())

def get_drive_service():
    """Create and return an authorized Drive API service instance."""
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials)

@app.route('/drive-notifications', methods=['POST'])
def handle_drive_notification():
    """Handle Google Drive change notifications."""
    # Get headers sent by Google
    channel_id = request.headers.get('X-Goog-Channel-ID')
    resource_id = request.headers.get('X-Goog-Resource-ID')
    resource_state = request.headers.get('X-Goog-Resource-State')
    message_number = request.headers.get('X-Goog-Message-Number')
    
    logger.info(f"Received notification #{message_number} for channel {channel_id}")
    logger.info(f"Resource {resource_id} - State: {resource_state}")
    
    # Get the stored state from Cloud Storage to validate the channel ID
    try:
        stored_info = get_drive_state()
        stored_channel_id = stored_info.get('channelId')
        stored_resource_id = stored_info.get('resourceId')
        
        # Validate the channel ID and resource ID
        if not channel_id or channel_id != stored_channel_id:
            logger.error(f"Invalid channel ID: {channel_id}")
            return 'Invalid channel ID', 403
            
        if not resource_id or resource_id != stored_resource_id:
            logger.error(f"Invalid resource ID: {resource_id}")
            return 'Invalid resource ID', 403
            
        logger.info("Channel ID and resource ID validated successfully")
    except Exception as e:
        logger.error(f"Error validating notification: {e}")
        return 'Error validating notification', 500
    # 'sync' is sent when the notification channel is first created
    if resource_state != 'sync':
        try:
            # Get the Drive API service
            drive_service = get_drive_service()
            
            # Get the stored state from Cloud Storage
            stored_info = get_drive_state()
            start_page_token = stored_info.get('startPageToken')
            
            logger.info(f"Using page token: {start_page_token}")
            
            # Get changes since last notification
            response = drive_service.changes().list(
                pageToken=start_page_token,
                spaces='drive',
                fields='changes(fileId, removed, time), newStartPageToken',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()
            
            changes = response.get('changes', [])
            logger.info(f"Received {len(changes)} changes since last notification")
            
            # Extract all changed file IDs
            changed_file_ids = [change.get('fileId') for change in changes]
            logger.info(f"Changed file IDs: {changed_file_ids}")
            
            # Update the page token for next time
            new_start_page_token = response.get('newStartPageToken')
            stored_info['startPageToken'] = new_start_page_token
            
            # If we have any changes, check what files are currently in the folder
            if changes:
                logger.info(f"Checking files in folder {FOLDER_ID}...")
                
                # Query for current files in the folder
                folder_files_response = drive_service.files().list(
                    q=f"'{FOLDER_ID}' in parents and trashed = false",
                    fields="files(id, name, mimeType, modifiedTime)",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    corpora="drive", 
                    driveId=os.getenv('DRIVE_ID')
                ).execute()
                
                current_files = folder_files_response.get('files', [])
                logger.info(f"Found {len(current_files)} files currently in the folder")
                
                # Log all files in the folder
                for file in current_files:
                    logger.info(f"File in folder: {file.get('name')} (ID: {file.get('id')})")
                
                # Process all changed files together with LlamaParseGoogleDriveReader
                docs = process_changed_files(changed_file_ids, current_files)
                
                if docs:
                    # Do something with the processed documents
                    logger.info(f"Sending {len(docs)} documents to LlamaIndex pipeline...")
                    pipeline_success = run_pipeline_for_documents(docs)

                    if pipeline_success:
                        logger.info("Documents successfully processed through the pipeline")
                    else:
                        logger.error("Failed to process documents through the pipeline")
                
                # Check if there is a "lastKnownFiles" list in the stored info
                if 'lastKnownFiles' in stored_info:
                    last_known_files = stored_info.get('lastKnownFiles', [])
                    last_known_ids = [f.get('id') for f in last_known_files]
                    
                    # Check for files that were in the folder before but no longer are
                    current_ids = [f.get('id') for f in current_files]
                    for file in last_known_files:
                        file_id = file.get('id')
                        if file_id not in current_ids and file_id in changed_file_ids:
                            logger.info(f"File was removed from folder: {file.get('name')} (ID: {file_id})")
                            process_removed_file(file)
                
                # Update the last known files in Cloud Storage
                stored_info['lastKnownFiles'] = current_files
                update_drive_state(new_start_page_token, current_files)
            
        except Exception as e:
            logger.error(f"Error processing changes: {e}")
            logger.exception("Full traceback:")
            return 'Internal Server Error', 500
    else:
        # Initial sync - store the current files in the folder
        try:
            drive_service = get_drive_service()
            
            folder_files_response = drive_service.files().list(
                q=f"'{FOLDER_ID}' in parents and trashed = false",
                fields="files(id, name, mimeType, modifiedTime)",
                includeItemsFromAllDrives=True,  # Required for Shared Drives
                supportsAllDrives=True,          # Required for Shared Drives
                corpora="drive",                 # Required for Shared Drives
                driveId=os.getenv('DRIVE_ID')    # Specify the Shared Drive ID
            ).execute()
            
            current_files = folder_files_response.get('files', [])
            logger.info(f"Initial sync: Found {len(current_files)} files in the folder")
            
            for file in current_files:
                logger.info(f"Initial file: {file.get('name')} (ID: {file.get('id')})")
            
            # Store the current files in Cloud Storage
            stored_info = get_drive_state()
            stored_info['lastKnownFiles'] = current_files
            update_drive_state(stored_info['startPageToken'], current_files)
            
        except Exception as e:
            logger.error(f"Error in initial sync: {e}")
            logger.exception("Full traceback:")
    
    # Google expects a 2xx response within 30 seconds
    return 'OK', 200

def process_changed_files(changed_file_ids, current_files):
    """Process all changed files in the folder using LlamaParseGoogleDriveReader."""
    try:
        # Filter to only get file IDs that exist in the folder
        folder_file_ids = [file.get('id') for file in current_files]
        file_ids_to_process = [file_id for file_id in changed_file_ids if file_id in folder_file_ids]
        
        if not file_ids_to_process:
            logger.info("No files to process with LlamaParseGoogleDriveReader")
            return None
        
        # Log what we're about to process
        logger.info(f"Processing {len(file_ids_to_process)} files with LlamaParseGoogleDriveReader")
        for file_id in file_ids_to_process:
            for file in current_files:
                if file.get('id') == file_id:
                    logger.info(f"- {file.get('name')} (ID: {file_id})")
                    break
        
        # Check if LLAMA_CLOUD_API_KEY is set
        if not os.getenv("LLAMA_CLOUD_API_KEY"):
            logger.error("LLAMA_CLOUD_API_KEY is not set. Cannot process files with LlamaParseGoogleDriveReader.")
            return None
        
        # Initialize the loader
        loader = LlamaParseGoogleDriveReader()
        
        # Load the documents with the list of file IDs
        logger.info(f"Calling loader.load_data with file_ids={file_ids_to_process}")
        docs = loader.load_data(file_ids=file_ids_to_process)
        
        if not docs:
            logger.warning("No documents returned from LlamaParseGoogleDriveReader")
            return None
        
        # Log successful loading
        logger.info(f"Successfully loaded {len(docs)} documents")
        
        return docs
        
    except Exception as e:
        logger.error(f"Error processing files with LlamaParseGoogleDriveReader: {e}")
        logger.exception("Full traceback:")
        return None

def process_removed_file(file):
    """Process a file that was removed from the folder."""
    file_id = file.get('id')
    file_name = file.get('name')
    
    logger.info(f"Processing removed file: {file_name} (ID: {file_id})")
    
    # Implement your custom removal logic here
    # For example, you might want to:
    # - Remove the file from your database
    # - Send a notification that a file was removed
    # - Update any related records

@app.route('/stop-notifications', methods=['POST'])
def stop_notifications():
    """Stop receiving notifications for a channel."""
    try:
        # Get the stored channel info
        channel_info_path = 'channel_info.json'
        if not os.path.exists(channel_info_path):
            return jsonify({
                'error': 'Channel info file not found'
            }), 404
            
        with open(channel_info_path, 'r') as f:
            stored_info = json.load(f)
            
        channel_id = stored_info.get('channelId')
        resource_id = stored_info.get('resourceId')
        
        if not channel_id or not resource_id:
            return jsonify({
                'error': 'Missing channelId or resourceId in stored info'
            }), 400
        
        # Get the Drive API service
        drive_service = get_drive_service()
        
        # Stop the notification channel
        drive_service.channels().stop(body={
            'id': channel_id,
            'resourceId': resource_id
        }).execute()
        
        logger.info(f"Stopped notifications for channel {channel_id}")
        
        # Rename the channel info file to indicate it's stopped
        os.rename(channel_info_path, f"{channel_info_path}.stopped")
        
        return jsonify({
            'success': True,
            'message': f"Stopped notifications for channel {channel_id}"
        })
    except Exception as e:
        logger.error(f"Error stopping notifications: {e}")
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # In production, use a proper WSGI server like gunicorn
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)