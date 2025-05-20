#!/usr/bin/env python3
"""
LlamaParse Google Drive Reader

A version of llama_index's GoogleDriveReader that uses LlamaParse for document parsing
and includes the description field from Google Drive files in the document metadata.
"""

from typing import Dict, List, Optional, Any, Union, Tuple
import os
import json
from pathlib import Path
import tempfile
import logging

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from llama_parse import LlamaParse

# Import the base GoogleDriveReader
from llama_index.readers.google import GoogleDriveReader
from llama_index.core.schema import Document
from llama_index.core.readers.base import BaseReader

logger = logging.getLogger(__name__)

class LlamaParseGoogleDriveReader(GoogleDriveReader):
    """Google Drive Reader that uses LlamaParse and includes description field in metadata.
    
    This class extends the standard GoogleDriveReader to:
    1. Use LlamaParse instead of SimpleDirectoryReader for better document parsing
    2. Include the file's description field from Google Drive in the document metadata
    
    All parameters are the same as the base GoogleDriveReader, with additional
    LlamaParse-specific parameters.
    """
    
    def __init__(
        self,
        llama_cloud_api_key: Optional[str] = None,
        result_type: str = "markdown",
        verbose: bool = True,
        split_by_page: bool = False,
        **kwargs
    ):
        """Initialize the reader with LlamaParse configuration.
        
        Args:
            llama_cloud_api_key (Optional[str]): API key for LlamaParse. If not provided,
                will try to get from environment variable LLAMA_CLOUD_API_KEY
            result_type (str): Output format - "markdown" or "text"
            verbose (bool): Whether to show verbose output
            split_by_page (bool): Whether to split documents by page
            **kwargs: Additional arguments passed to GoogleDriveReader
        """
        super().__init__(**kwargs)
        
        # Initialize LlamaParse as a private attribute
        self._parser = LlamaParse(
            api_key=llama_cloud_api_key or os.environ.get("LLAMA_CLOUD_API_KEY"),
            result_type=result_type,
            verbose=verbose,
            split_by_page=split_by_page,
        )
    
    def _get_fileids_meta(
        self,
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_id: Optional[str] = None,
        mime_types: Optional[List[str]] = None,
        query_string: Optional[str] = None,
        current_path: Optional[str] = None,
    ) -> List[List[Any]]:
        """Enhanced version that also gets the description field from Google Drive.
        
        Extends the tuple returned by the original method with the description field.
        """
        from googleapiclient.discovery import build

        try:
            service = build("drive", "v3", credentials=self._creds)
            fileids_meta = []

            if folder_id and not file_id:
                try:
                    folder = (
                        service.files()
                        .get(fileId=folder_id, supportsAllDrives=True, fields="name")
                        .execute()
                    )
                    current_path = (
                        f"{current_path}/{folder['name']}"
                        if current_path
                        else folder["name"]
                    )
                except Exception as e:
                    logger.warning(f"Could not get folder name: {e}")

                folder_mime_type = "application/vnd.google-apps.folder"
                query = "('" + folder_id + "' in parents)"

                # Add mimeType filter to query
                if mime_types:
                    if folder_mime_type not in mime_types:
                        mime_types.append(folder_mime_type)  # keep the recursiveness
                    mime_query = " or ".join(
                        [f"mimeType='{mime_type}'" for mime_type in mime_types]
                    )
                    query += f" and ({mime_query})"

                # Add query string filter
                if query_string:
                    query += (
                        f" and ((mimeType='{folder_mime_type}') or ({query_string}))"
                    )

                items = []
                page_token = ""
                # get files taking into account that the results are paginated
                while True:
                    if drive_id:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                driveId=drive_id,
                                corpora="drive",
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields="*",
                                pageToken=page_token,
                            )
                            .execute()
                        )
                    else:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields="*",
                                pageToken=page_token,
                            )
                            .execute()
                        )
                    items.extend(results.get("files", []))
                    page_token = results.get("nextPageToken", None)
                    if page_token is None:
                        break

                for item in items:
                    item_path = (
                        f"{current_path}/{item['name']}"
                        if current_path
                        else item["name"]
                    )

                    if item["mimeType"] == folder_mime_type:
                        if drive_id:
                            fileids_meta.extend(
                                self._get_fileids_meta(
                                    drive_id=drive_id,
                                    folder_id=item["id"],
                                    mime_types=mime_types,
                                    query_string=query_string,
                                    current_path=current_path,
                                )
                            )
                        else:
                            fileids_meta.extend(
                                self._get_fileids_meta(
                                    folder_id=item["id"],
                                    mime_types=mime_types,
                                    query_string=query_string,
                                    current_path=current_path,
                                )
                            )
                    else:
                        # Check if file doesn't belong to a Shared Drive
                        is_shared_drive = "driveId" in item
                        author = (
                            item["owners"][0]["displayName"]
                            if not is_shared_drive
                            else "Shared Drive"
                        )
                        # Include description field (may be None)
                        description = item.get("description", None)
                        
                        fileids_meta.append(
                            (
                                item["id"],
                                author,
                                item_path,
                                item["mimeType"],
                                item["createdTime"],
                                item["modifiedTime"],
                                self._get_drive_link(item["id"]),
                                description,  # Add description as 8th element
                            )
                        )
            else:
                # Get the file details
                file = (
                    service.files()
                    .get(fileId=file_id, supportsAllDrives=True, fields="*")
                    .execute()
                )
                # Get metadata of the file
                is_shared_drive = "driveId" in file
                author = (
                    file["owners"][0]["displayName"]
                    if not is_shared_drive
                    else "Shared Drive"
                )

                # Get the full file path
                file_path = self._get_relative_path(
                    service, file_id, folder_id or self.folder_id
                )
                
                # Get the description field (may be None)
                description = file.get("description", None)

                fileids_meta.append(
                    (
                        file["id"],
                        author,
                        file_path,
                        file["mimeType"],
                        file["createdTime"],
                        file["modifiedTime"],
                        self._get_drive_link(file["id"]),
                        description,  # Add description as 8th element
                    )
                )
            return fileids_meta

        except Exception as e:
            logger.error(
                f"An error occurred while getting fileids metadata: {e}", exc_info=True
            )

    def _load_data_fileids_meta(self, fileids_meta: List[List[Any]]) -> List[Document]:
        """Enhanced version that uses LlamaParse instead of SimpleDirectoryReader."""
        try:
            documents = []
            
            for fileid_meta in fileids_meta:
                with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                    # Download file to temporary location
                    fileid = fileid_meta[0]
                    final_filepath = self._download_file(fileid, temp_file.name)
                    
                    # Create metadata dictionary
                    metadata = {
                        "file_id": fileid_meta[0],
                        "author": fileid_meta[1],
                        "file_path": fileid_meta[2],
                        "mime_type": fileid_meta[3],
                        "created_at": fileid_meta[4],
                        "modified_at": fileid_meta[5],
                        "drive_link": fileid_meta[6],
                    }
                    
                    # Add description if it exists (index 7)
                    if len(fileid_meta) > 7 and fileid_meta[7] is not None:
                        metadata["description"] = fileid_meta[7]
                    
                    try:
                        # Use LlamaParse to parse the document
                        parsed_docs = self._parser.load_data(final_filepath)
                        
                        # Add our metadata to each parsed document
                        for doc in parsed_docs:
                            doc.metadata.update(metadata)
                            doc.id_ = metadata["file_id"]
                            documents.append(doc)
                            
                    except Exception as parse_error:
                        logger.error(
                            f"Error parsing file {fileid}: {str(parse_error)}", 
                            exc_info=True
                        )
                        continue

            return documents
            
        except Exception as e:
            logger.error(
                f"An error occurred while loading data from fileids meta: {e}",
                exc_info=True,
            )
            return []  # Return empty list on error to avoid None issues


# Example usage
if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Load documents from Google Drive using LlamaParse")
    parser.add_argument("--folder_id", help="ID of the Google Drive folder")
    parser.add_argument("--file_id", help="ID of a specific Google Drive file")
    args = parser.parse_args()
    
    # Create the LlamaParse reader
    loader = LlamaParseGoogleDriveReader()
    
    # Load documents based on provided arguments
    if args.folder_id:
        docs = loader.load_data(folder_id=args.folder_id)
    elif args.file_id:
        docs = loader.load_data(file_ids=[args.file_id])
    else:
        print("Please provide either --folder_id or --file_id")
        exit(1)
    
    # Display the documents and their metadata
    print(f"Loaded {len(docs)} documents")
    for i, doc in enumerate(docs):
        print(f"\nDocument {i+1}: {doc.id_}")
        print(f"Metadata: {doc.metadata}")
        print(f"Content preview: {doc.text[:100]}...") 