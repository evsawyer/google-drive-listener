import logging
from typing import List, Dict
from googleapiclient.errors import HttpError
from config import settings
from batch_llama_parse_google_drive_reader import BatchLlamaParseGoogleDriveReader
from cloud_storage_functions import get_drive_service, get_service_account_info

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_watched_files(
    drive_id: str = None
) -> List[Dict]:
    """
    Get all files that are in folders containing '--watched' in their name.
    
    Args:
        drive_id: The ID of the shared drive to search in
        
    Returns:
        List of dictionaries containing file information (id, name, mimeType, modifiedTime)
        
    Raises:
        ValueError: If neither drive_service nor credentials are provided
        HttpError: If there's an error accessing the Google Drive API
    """
    try:
        drive_service = get_drive_service()
        # Get all folders with '--watched' in their name
        logger.info(f"Getting list of watched folders in drive {drive_id}")
        folder_files_response = drive_service.files().list(
            q="name contains '--watched' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            fields="files(id)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="drive",
            driveId=drive_id
        ).execute()

        # Get the IDs of all watched folders
        watched_folder_ids = [folder['id'] for folder in folder_files_response.get('files', [])]
        
        # Then get all files in these folders
        if watched_folder_ids:
            files_query = " or ".join([f"'{folder_id}' in parents" for folder_id in watched_folder_ids])
            files_response = drive_service.files().list(
                q=f"({files_query}) and trashed = false",
                fields="files(id, name, mimeType, modifiedTime)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="drive",
                driveId=drive_id
            ).execute()
            watched_files = files_response.get('files', [])
        else:
            watched_files = []
        
        logger.info(f"Found {len(watched_files)} watched files in drive")
        for file in watched_files:
            logger.info(f"Watched file in drive {drive_id}: {file.get('name')} (ID: {file.get('id')})")
        return watched_files

    except HttpError as e:
        logger.error(f"Error accessing Google Drive API: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def process_files(file_ids_to_process):
    """Process all changed files in the folder using BatchLlamaParseGoogleDriveReader."""
    try:
        if not file_ids_to_process:
            logger.info("No files to process with BatchLlamaParseGoogleDriveReader")
            return None
        # Check if LLAMA_CLOUD_API_KEY is set
        if not settings.llama_cloud_api_key:
            logger.error("LLAMA_CLOUD_API_KEY is not set...")
            return None

        # Initialize the loader
        loader = BatchLlamaParseGoogleDriveReader(
                                                    service_account_key=get_service_account_info(), 
                                                    is_cloud=True,
                                                    llama_parse_result_type="markdown",
                                                    llama_parse_verbose=True,
                                                    split_by_page=False,
        )
        
        # Load the documents with the list of file IDs
        logger.info(f"Calling loader.load_data with file_ids={file_ids_to_process}")

        docs = loader.load_data(file_ids=file_ids_to_process)
        
        if not docs:
            logger.warning("No documents returned from BatchLlamaParseGoogleDriveReader")
            return None
        
        # Log successful loading
        logger.info(f"Successfully loaded {len(docs)} documents")
        
        return docs
        
    except Exception as e:
        logger.error(f"Error processing files with BatchLlamaParseGoogleDriveReader: {e}")
        logger.exception("Full traceback:")
        return None