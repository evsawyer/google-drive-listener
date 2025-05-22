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
import aiohttp
import asyncio
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn

# Load environment variables
load_dotenv()

app = FastAPI()
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

@app.on_event("startup")
async def startup():
    """Initialize resources on startup"""
    # Your startup code here
    await process_all_existing_files()

@app.on_event("shutdown")
async def shutdown():
    """Cleanup resources on shutdown"""
    # Your cleanup code here
    pass

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
        stored_channel_id = stored_info.get('channelId')
        
        if not x_goog_channel_id or x_goog_channel_id != stored_channel_id:
            logger.error(f"Received notification from unauthorized channel: {x_goog_channel_id}")
            logger.error(f"Expected channel: {stored_channel_id}")
            raise HTTPException(status_code=403, detail="Unauthorized channel")
        
        # Rest of your notification handling code...
        
        return {"status": "OK"}
        
    except Exception as e:
        logger.error(f"Error handling notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        
        async with aiohttp.ClientSession() as session:
            # Use existing process_changed_files function
            docs = process_changed_files(file_ids, current_files)
            
            if docs:
                logger.info(f"Processing {len(docs)} documents through pipeline...")
                pipeline_success = run_pipeline_for_documents(docs)
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
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)