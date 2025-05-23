import os
import json
import logging
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# unfortunately this is deprecated for now.
# from llama_parse_google_drive_reader import LlamaParseGoogleDriveReader
from llama_index.readers.google import GoogleDriveReader

from run_pipeline import run_pipeline_for_documents
from drive_state import get_drive_state, update_drive_state
import sys
from google.cloud import storage
import aiohttp
import asyncio
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn
from pydantic_settings import BaseSettings
from contextlib import asynccontextmanager

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    # Google Drive related
    folder_id: str = os.getenv("FOLDER_ID")
    drive_id: str = os.getenv("DRIVE_ID")
    
    # Service Account and Storage
    service_account_bucket_name: str = os.getenv("SERVICE_ACCOUNT_BUCKET_NAME")
    service_account_key: str = os.getenv("SERVICE_ACCOUNT_KEY")
    bucket_name: str = os.getenv("BUCKET_NAME")
    credentials_bucket_name: str = os.getenv("CREDENTIALS_BUCKET_NAME")
    
    # API Keys
    pinecone_api_key: str = os.getenv("PINECONE_API_KEY")
    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    llama_cloud_api_key: str = os.getenv("LLAMA_CLOUD_API_KEY")
    
    # Database Configuration
    postgres_password: str = os.getenv("POSTGRES_PASSWORD")
    project_id: str = os.getenv("PROJECT_ID")
    db_region: str = os.getenv("DB_REGION")
    db_instance: str = os.getenv("DB_INSTANCE")
    db_name: str = os.getenv("DB_NAME")
    db_user: str = os.getenv("DB_USER")
    
    # Pinecone Configuration
    pinecone_index_name: str = os.getenv("PINECONE_INDEX_NAME")
    pinecone_namespace: str = os.getenv("PINECONE_NAMESPACE")

    class Config:
        case_sensitive = False

# Initialize settings
settings = Settings()

client = storage.Client()
bucket = client.bucket(settings.service_account_bucket_name)
blob = bucket.blob(settings.service_account_key)
service_account_key = json.loads(blob.download_as_string())

# Add service_account_key to kwargs

        

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI app"""
    logger.info("Starting up FastAPI application...")
    try:
        await process_all_existing_files()
        yield
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        yield
    finally:
        logger.info("Shutting down FastAPI application...")

app = FastAPI(lifespan=lifespan)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_service_account_info():
    """Get service account info from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(settings.service_account_bucket_name)
    blob = bucket.blob(settings.service_account_key)
    return json.loads(blob.download_as_string())

def get_drive_service():
    """Create and return an authorized Drive API service instance."""
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials)

@app.post("/drive-notifications")
async def handle_drive_notification(
    request: Request,
    x_goog_channel_id: str = Header(..., alias="X-Goog-Channel-ID"),
    x_goog_resource_id: str = Header(..., alias="X-Goog-Resource-ID"),
    x_goog_resource_state: str = Header(..., alias="X-Goog-Resource-State"),
    x_goog_message_number: str = Header(..., alias="X-Goog-Message-Number")
):
    """Handle Google Drive change notifications."""
    logger.info(f"Received notification #{x_goog_message_number} for channel {x_goog_channel_id}")
    
    try:
        stored_info = get_drive_state()
        start_page_token = stored_info.get('startPageToken')
        stored_channel_id = stored_info.get('channelId')
        
        if not x_goog_channel_id or x_goog_channel_id != stored_channel_id:
            logger.error(f"Received notification from unauthorized channel: {x_goog_channel_id}")
            logger.error(f"Expected channel: {stored_channel_id}")
            raise HTTPException(status_code=403, detail="Unauthorized channel")
        
        # Get the Drive API service
        drive_service = get_drive_service()
        
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
            logger.info(f"Checking files in folder {settings.folder_id}...")
            
            # Query for current files in the folder
            folder_files_response = drive_service.files().list(
                q=f"'{settings.folder_id}' in parents and trashed = false",
                fields="files(id, name, mimeType, modifiedTime)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="drive", 
                driveId=settings.drive_id
            ).execute()
            
            current_files = folder_files_response.get('files', [])
            logger.info(f"Found {len(current_files)} files currently in the folder")
            
            # Log all files in the folder
            for file in current_files:
                logger.info(f"File in folder: {file.get('name')} (ID: {file.get('id')})")
            
            # Process all changed files together with GoogleDriveReader
            docs = process_files_in_folder(changed_file_ids, current_files)
        
            if docs:
                logger.info(f"Processing {len(docs)} documents through pipeline...")
                pipeline_success = await run_pipeline_for_documents(docs)
                if pipeline_success:
                    logger.info("Successfully processed documents")
                else:
                    logger.error("Failed to process documents through the pipeline")
        
            # Update the stored state with current files
            stored_info['lastKnownFiles'] = current_files
            update_drive_state(stored_info['startPageToken'], current_files)
        
        return {"status": "OK"}
        
    except Exception as e:
        logger.error(f"Error handling notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def process_files_in_folder(changed_file_ids, current_files):
    """Process all changed files in the folder using GoogleDriveReader."""
    try:
        # Filter to only get file IDs that exist in the folder
        folder_file_ids = [file.get('id') for file in current_files]
        file_ids_to_process = [file_id for file_id in changed_file_ids if file_id in folder_file_ids]
        
        if not file_ids_to_process:
            logger.info("No files to process with GoogleDriveReader")
            return None
        
        # Log what we're about to process
        logger.info(f"Processing {len(file_ids_to_process)} files with GoogleDriveReader")
        for file_id in file_ids_to_process:
            for file in current_files:
                if file.get('id') == file_id:
                    logger.info(f"- {file.get('name')} (ID: {file_id})")
                    break
        
        # Check if LLAMA_CLOUD_API_KEY is set
        if not settings.llama_cloud_api_key:
            logger.error("LLAMA_CLOUD_API_KEY is not set...")
            return None
        
        # Initialize the loader
        loader = GoogleDriveReader(service_account_key=service_account_key, is_cloud=True)
        
        # Load the documents with the list of file IDs
        logger.info(f"Calling loader.load_data with file_ids={file_ids_to_process}")

        docs = loader.load_data(file_ids=file_ids_to_process)
        
        if not docs:
            logger.warning("No documents returned from GoogleDriveReader")
            return None
        
        # Log successful loading
        logger.info(f"Successfully loaded {len(docs)} documents")
        
        return docs
        
    except Exception as e:
        logger.error(f"Error processing files with GoogleDriveReader: {e}")
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

async def process_all_existing_files():
    """Process all files stored in the drive state at startup."""
    try:
        logger.info("Starting initial processing of all existing files...")
        
        # Get the stored state from Cloud Storage
        stored_info = get_drive_state()
        current_files = stored_info.get('lastKnownFiles', [])
        
        if not current_files:
            logger.info("No existing files found in drive state")
            return
            
        logger.info(f"Found {len(current_files)} files to process")
        
        # Extract all file IDs
        file_ids = [file.get('id') for file in current_files]
        
        docs = process_files_in_folder(file_ids, current_files)
        
        if docs:
            logger.info(f"Processing {len(docs)} documents through pipeline...")
            pipeline_success = await run_pipeline_for_documents(docs)
            if pipeline_success:
                logger.info("Successfully processed all existing documents")
            else:
                logger.error("Failed to process documents through the pipeline")
        else:
            logger.warning("No documents were processed")
            
    except Exception as e:
        logger.error(f"Error processing existing files at startup: {e}")
        logger.exception("Full traceback:")

if __name__ == "__main__":
    port = settings.port
    uvicorn.run(app, host="0.0.0.0", port=port)