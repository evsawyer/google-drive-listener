import os
import logging
from dotenv import load_dotenv
from config import settings
from run_pipeline import run_pipeline_for_documents
from channel_state import get_channel_state, update_channel_state
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn
from contextlib import asynccontextmanager
from drive_functions import get_watched_files, process_files, get_shared_files
from cloud_storage_functions import get_drive_service
import json
from refresh_drive_channel import setup_drive_notifications, store_channel_info
import time
# Load environment variables
load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI app"""
    logger.info("Starting up FastAPI application...")
    try:
        yield
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        yield
    finally:
        logger.info("Shutting down FastAPI application...")

app = FastAPI(lifespan=lifespan)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# hoepfully prevents app shutdown when no requersts are coming through
@app.get("/health")
async def health_check():
    return {"status": "ok"}


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
        stored_info = get_channel_state()
        start_page_token = stored_info.get('startPageToken')
        stored_channel_id = stored_info.get('channelId')
        drive_id = stored_info.get('driveId')
        
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
        
        # If we have any changes, check what files are currently in the watched folder
        if changes:

            #  this is MOOT because any notficiation will be of a file that needs to be processed
            # i.e. we only get notifications for filew sand folders our S.A. is allowed to see


            # watched_files = get_watched_files(drive_id=settings.drive_id)
            # file_ids_to_process = [file.get('id') for file in watched_files if file.get('id') in changed_file_ids]
            # file_names_to_process = [file.get('name') for file in watched_files if file.get('id') in changed_file_ids]
            # for file_id, file_name in zip(file_ids_to_process, file_names_to_process):
            #     logger.info(f"Preparing to process file: {file_name} (ID: {file_id})")
            # Process all changed files together with GoogleDriveReader
            docs = process_files(changed_file_ids)
        
            if docs:
                logger.info(f"Processing {len(docs)} documents through pipeline...")
                pipeline_success = await run_pipeline_for_documents(docs)
                if pipeline_success:
                    logger.info("Successfully processed documents")
                else:
                    logger.error("Failed to process documents through the pipeline")
        
            # Update the stored state with current files
            update_channel_state(stored_info['startPageToken'])
        
        return {"status": "OK"}
        
    except Exception as e:
        logger.error(f"Error handling notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.route('/stop-notifications', methods=['POST'])
def stop_notifications(channel_id: str, resource_id: str):
    """Stop receiving notifications for a channel."""
    try:
        if not channel_id or not resource_id:
            return JSONResponse({
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
        
        return JSONResponse({
            'success': True,
            'message': f"Stopped notifications for channel {channel_id}"
        })
    except Exception as e:
        logger.error(f"Error stopping notifications: {e}")
        return JSONResponse({
            'error': str(e)
        }), 500
    
from typing import Annotated
@app.post("/process-all-shared-files")
async def process_all_shared_files(
    x_goog_channel_id: Annotated[str, Header(alias="X-Goog-Channel-ID")]
):
    """Process all files stored in the drive state at startup."""
    stored_info = get_channel_state()
    stored_channel_id = stored_info.get('channelId')
    logger.info(f"Stored channel ID: {stored_channel_id}")
    logger.info(f"X-Goog-Channel-ID: {x_goog_channel_id}")
    if x_goog_channel_id != stored_channel_id:
        logger.error(f"Unauthorized access attempt with channel ID: {x_goog_channel_id}")
        raise HTTPException(status_code=403, detail="Unauthorized channel ID")

    try:
        logger.info("Starting initial processing of all existing files...")
        
        # Get the stored state from Cloud Storage
        # watched_files = get_watched_files(drive_id=settings.drive_id)
        shared_files = get_shared_files()
        if not shared_files:
            logger.info("No existing files found in drive state")
            return
        # Extract all file IDs
        file_ids = [file.get('id') for file in shared_files]
        docs = process_files(file_ids)
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

@app.post("/refresh-drive-channel")
async def refresh_drive_channel(
    x_refresh_key: Annotated[str, Header(alias="X-Refresh-Key")]):

    """Refresh the drive channel."""
    if x_refresh_key != settings.refresh_key:
        logger.error(f"Unauthorized access attempt with refresh key: {x_refresh_key}")
        raise HTTPException(status_code=403, detail="Unauthorized refresh key")
    
    logging.info("stopping notifications on previous channel")
    channel_info = get_channel_state()
    channel_id = channel_info.get('channelId')
    resource_id = channel_info.get('resourceId')
    expiration = float(channel_info.get('expiration'))
    if expiration > time.time():
        logger.info("Channel is still valid, stopping notifications")
        try:
            stop_notifications(channel_id, resource_id)
        except Exception as e:
            logger.error(f"Failed to stop notifications: {e}")
            logger.exception("Full traceback:")
            raise HTTPException(status_code=500, detail="Failed to stop previous notifications")
    else:
        logger.info("Channel has expired, no need to stop notifications")
    try:
        logger.info("Initializing Google Drive notification channel...")
        # Verify required environment variables are set
        if not settings.webhook_url:
            raise ValueError("WEBHOOK_URL environment variable not set in .env file")
        if not settings.channel_state_bucket_name:
            raise ValueError("CHANNEL_STATE_BUCKET_NAME environment variable not set in .env file")
        if not settings.service_account_bucket_name:
            raise ValueError("SERVICE_ACCOUNT_BUCKET_NAME environment variable not set in .env file")
            
        channel_info = setup_drive_notifications()
        logger.info(f"Successfully set up notifications!")
        # Store channel info in Cloud Storage
        store_channel_info(channel_info)
        logger.info("\nIMPORTANT: Channel information has been saved to Cloud Storage!")
        
    except Exception as e:
        logger.error(f"Error setting up notifications: {e}")