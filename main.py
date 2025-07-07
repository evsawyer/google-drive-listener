import os
import logging
from dotenv import load_dotenv
from config import settings
from run_pipeline import run_pipeline_for_documents
from drive_state import get_drive_state, update_drive_state
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
from contextlib import asynccontextmanager
from drive_functions import get_watched_files, process_files, get_shared_files
from service_functions import get_drive_service
import json
# from refresh_drive_channel import setup_drive_notifications, store_channel_info
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

# Pydantic model for file ID request
class FileIdRequest(BaseModel):
    file_id: str

# Process a single file
@app.post("/process-file")
async def process_file(request: FileIdRequest):
    file_id = request.file_id
    doc = process_files([file_id])  # Pass as a list
    if doc:
        logger.info(f"Processing {len(doc)} documents through pipeline...")
        pipeline_success = await run_pipeline_for_documents(doc)
        if pipeline_success:
            logger.info("Successfully processed document")
        else:
            logger.error("Failed to process document through the pipeline")
    else:
        logger.warning("No document was processed")
    return {"status": "OK"}

@app.post("/drive-notifications")
async def handle_drive_notification(
    request: Request,
    # x_goog_channel_id: str = Header(..., alias="X-Goog-Channel-ID"),
    # x_goog_resource_id: str = Header(..., alias="X-Goog-Resource-ID"),
    # x_goog_resource_state: str = Header(..., alias="X-Goog-Resource-State"),
    # x_goog_message_number: str = Header(..., alias="X-Goog-Message-Number")
):
    # """Handle Google Drive change notifications."""
    # logger.info(f"Received notification #{x_goog_message_number} for channel {x_goog_channel_id}")
    
    try:
        stored_info = get_drive_state()
        start_page_token = stored_info.get('startPageToken')
        # stored_channel_id = stored_info.get('channelId')
        
        # if not x_goog_channel_id or x_goog_channel_id != stored_channel_id:
        #     logger.error(f"Received notification from unauthorized channel: {x_goog_channel_id}")
        #     logger.error(f"Expected channel: {stored_channel_id}")
        #     raise HTTPException(status_code=403, detail="Unauthorized channel")
        
        # Get the Drive API service
        drive_service = get_drive_service()
        
        logger.info(f"Using page token: {start_page_token}")
        
        try:
            # Get changes using the token
            response = drive_service.changes().list(
                pageToken=start_page_token,
                spaces='drive',
                fields='changes(fileId, removed, time, file(mimeType))',
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"Received changes response: {response}")

            # Always get a fresh token after processing changes
            token_response = drive_service.changes().getStartPageToken(
                supportsAllDrives=True
            ).execute()
            new_start_page_token = token_response.get('startPageToken')
            logger.info(f"Got fresh token: {new_start_page_token}")
            
            if new_start_page_token:
                stored_info['startPageToken'] = new_start_page_token
                update_drive_state(new_start_page_token)
                logger.info(f"Updated stored token to: {new_start_page_token}")
            else:
                logger.error("Failed to get new token")
                raise HTTPException(status_code=500, detail="Could not get new token")

            # Filter out folder changes
            file_changes = []
            for change in response.get('changes', []):
                file_id = change.get('fileId')
                # Skip removed files
                if change.get('removed', False):
                    logger.info(f"Skipping removed item: {file_id}")
                    continue
                # Skip folders
                if 'file' in change and change['file'].get('mimeType') == 'application/vnd.google-apps.folder':
                    logger.info(f"Skipping folder change: {file_id}")
                    continue
                file_changes.append(change)

            logger.info(f"Found {len(file_changes)} file changes to process")

            # Extract all changed file IDs
            changed_file_ids = [file.get('fileId') for file in file_changes]
            changed_file_paths = [file.get('file').get('name') for file in file_changes]
            logger.info(f"Changed file IDs: {changed_file_ids} with paths: {changed_file_paths}")
            
            # Process changes if any were found
            if changed_file_ids:
                docs = process_files(changed_file_ids)
                if docs:
                    logger.info(f"Processing {len(docs)} documents through pipeline...")
                    pipeline_success = await run_pipeline_for_documents(docs)
                    if pipeline_success:
                        logger.info("Successfully processed documents")
                    else:
                        logger.error("Failed to process documents through the pipeline")
            
            return {"status": "OK"}

        except Exception as e:
            logger.error(f"Error processing changes: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    except Exception as e:
        logger.error(f"Error handling notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# @app.route('/stop-notifications', methods=['POST'])
# def stop_notifications(channel_id: str, resource_id: str):
#     """Stop receiving notifications for a channel."""
#     try:
#         if not channel_id or not resource_id:
#             return JSONResponse({
#                 'error': 'Missing channelId or resourceId in stored info'
#             }), 400
#         # Get the Drive API service
#         drive_service = get_drive_service()
        
#         # Stop the notification channel
#         drive_service.channels().stop(body={
#             'id': channel_id,
#             'resourceId': resource_id
#         }).execute()
        
#         logger.info(f"Stopped notifications for channel {channel_id}")
        
#         return JSONResponse({
#             'success': True,
#             'message': f"Stopped notifications for channel {channel_id}"
#         })
#     except Exception as e:
#         logger.error(f"Error stopping notifications: {e}")
#         return JSONResponse({
#             'error': str(e)
#         }), 500
    
from typing import Annotated
@app.post("/process-all-shared-files")
async def process_all_shared_files(
    x_goog_channel_id: Annotated[str, Header(alias="X-Goog-Channel-ID")]
):
    """Process all files stored in the drive state at startup."""
    # stored_info = get_drive_state()
    # stored_channel_id = stored_info.get('channelId')
    # logger.info(f"Stored channel ID: {stored_channel_id}")
    # logger.info(f"X-Goog-Channel-ID: {x_goog_channel_id}")
    # if x_goog_channel_id != stored_channel_id:
    #     logger.error(f"Unauthorized access attempt with channel ID: {x_goog_channel_id}")
    #     raise HTTPException(status_code=403, detail="Unauthorized channel ID")

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

# @app.post("/refresh-drive-channel")
# async def refresh_drive_channel(
#     x_refresh_key: Annotated[str, Header(alias="X-Refresh-Key")]):

#     """Refresh the drive channel."""
#     if x_refresh_key != settings.refresh_key:
#         logger.error(f"Unauthorized access attempt with refresh key: {x_refresh_key}")
#         raise HTTPException(status_code=403, detail="Unauthorized refresh key")
    
#     logging.info("stopping notifications on previous channel")
#     channel_info = get_channel_state()
#     if channel_info == "No channel found":
#         logger.info("No channel found, setting up new channel")
#         pass
#     else:
#         logger.info("Channel found, stopping notifications")
#         channel_id = channel_info.get('channelId')
#         resource_id = channel_info.get('resourceId')
#         expiration = float(channel_info.get('expiration'))
#         if expiration > time.time():
#             logger.info("Channel is still valid, stopping notifications")
#             try:
#                 stop_notifications(channel_id, resource_id)
#             except Exception as e:
#                 logger.error(f"Failed to stop notifications: {e}")
#                 logger.exception("Full traceback:")
#                 raise HTTPException(status_code=500, detail="Failed to stop previous notifications")
#         else:
#             logger.info("Channel has expired, no need to stop notifications")

            
#     try:
#         logger.info("Initializing Google Drive notification channel...")
#         # Verify required environment variables are set
#         if not settings.webhook_url:
#             raise ValueError("WEBHOOK_URL environment variable not set in .env file")
#         if not settings.channel_state_bucket_name:
#             raise ValueError("CHANNEL_STATE_BUCKET_NAME environment variable not set in .env file")
#         if not settings.service_account_bucket_name:
#             raise ValueError("SERVICE_ACCOUNT_BUCKET_NAME environment variable not set in .env file")
            
#         channel_info = setup_drive_notifications()
#         logger.info(f"Successfully set up notifications!")
#         # Store channel info in Cloud Storage
#         store_channel_info(channel_info)
#         logger.info("\nIMPORTANT: Channel information has been saved to Cloud Storage!")
        
#     except Exception as e:
#         logger.error(f"Error setting up notifications: {e}")