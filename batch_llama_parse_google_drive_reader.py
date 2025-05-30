import os
import tempfile
import json # For potential LlamaParse specific JSON handling if needed
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader # The base class
from llama_parse import LlamaParse # The parser we want to use in batch

import logging
logger = logging.getLogger(__name__)

class BatchLlamaParseGoogleDriveReader(GoogleDriveReader):
    """
    Google Drive Reader that downloads all specified files and then uses a single
    LlamaParse call to process them in a batch. Includes Google Drive file description
    in the metadata.
    """

    def __init__(
        self,
        # LlamaParse specific arguments
        llama_cloud_api_key: Optional[str] = None,
        llama_parse_result_type: str = "markdown",
        llama_parse_verbose: bool = True,
        service_account_key: dict = None, # Now required for GDrive authentication
        # You can add other LlamaParse specific constructor arguments here
        # e.g., split_by_page: bool = False, use_vendor_multimodal_models: bool = False etc.
        # Pass them as kwargs if not explicitly listed.
        # GoogleDriveReader arguments (copied from base for clarity and to ensure they are passed)
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        query_string: Optional[str] = None,
        is_cloud: Optional[bool] = False,
        # credentials_path: str = "credentials.json",
        # token_path: str = "token.json",
        # service_account_key_path: str = "service_account_key.json",
        # client_config: Optional[dict] = None,
        # authorized_user_info: Optional[dict] = None,
  
        **llama_parse_kwargs: Any, # Catch-all for other LlamaParse constructor args
    ) -> None:
        """
        Initialize the reader.

        Args:
            llama_cloud_api_key (Optional[str]): API key for LlamaParse.
                Defaults to LLAMA_CLOUD_API_KEY environment variable.
            llama_parse_result_type (str): Output format for LlamaParse ("markdown" or "text").
            llama_parse_verbose (bool): Whether LlamaParse should show verbose output.
            drive_id (Optional[str]): Drive id of the shared drive.
            folder_id (Optional[str]): Folder id to read from.
            file_ids (Optional[List[str]]): Specific file ids to read.
            query_string (Optional[str]): Google Drive query string.
            is_cloud (Optional[bool]): Cloud environment flag.
            credentials_path (str): Path to client config JSON for OAuth.
            token_path (str): Path to token JSON for OAuth.
            service_account_key_path (str): Path to service account key JSON.
            client_config (Optional[dict]): Client config dictionary for OAuth.
            authorized_user_info (Optional[dict]): Authorized user info dict for OAuth.
            service_account_key (Optional[dict]): Service account key dictionary.
            **llama_parse_kwargs: Additional keyword arguments to pass to the LlamaParse constructor.
        """
        # Initialize the GoogleDriveReader part
        # We explicitly set file_extractor to None as we are bypassing SimpleDirectoryReader
        super().__init__(
            drive_id=drive_id,
            folder_id=folder_id,
            file_ids=file_ids,
            query_string=query_string,
            is_cloud=is_cloud,
            credentials_path=credentials_path,
            token_path=token_path,
            service_account_key_path=service_account_key_path,
            client_config=client_config,
            authorized_user_info=authorized_user_info,
            service_account_key=service_account_key,
            file_extractor=None, # This reader does not use the file_extractor mechanism
        )

        # Store LlamaParse configuration
        self._llama_cloud_api_key = llama_cloud_api_key or os.getenv("LLAMA_CLOUD_API_KEY")
        self._llama_parse_result_type = llama_parse_result_type
        self._llama_parse_verbose = llama_parse_verbose
        self._llama_parse_kwargs = llama_parse_kwargs # Store other LlamaParse specific kwargs

        if not self._llama_cloud_api_key:
            raise ValueError(
                "LlamaParse API key must be provided via 'llama_cloud_api_key' argument "
                "or LLAMA_CLOUD_API_KEY environment variable."
            )

    def _get_fileids_meta(
        self,
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_id: Optional[str] = None,
        mime_types: Optional[List[str]] = None,
        query_string: Optional[str] = None,
        current_path: Optional[str] = None,
    ) -> List[List[Any]]: # Return type changed to List[List[Any]] to match original
        """Enhanced version that also gets the description field from Google Drive.
        
        Extends the tuple returned by the original method with the description field.
        This method is adapted from the user's LlamaParseGoogleDriveReader.
        """
        from googleapiclient.discovery import build

        try:
            # self._creds should be initialized by the load_data method before this is called.
            # If self._creds is not available, it might indicate an issue with the calling sequence.
            if not hasattr(self, '_creds') or self._creds is None:
                 # Attempt to get credentials if not already set (e.g., if called directly for testing)
                 # In normal operation, load_data() calls _get_credentials() first.
                logger.warning("_creds not found, attempting to get credentials in _get_fileids_meta. This might not work if called outside load_data context.")
                self._creds = self._get_credentials()


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
                # Base query for items within the folder_id
                query = f"'{folder_id}' in parents and trashed=false"

                # Add mimeType filter to query
                if mime_types:
                    mime_type_conditions = [f"mimeType='{mt}'" for mt in mime_types]
                    # Ensure recursive search for folders if specific mime_types are given
                    if folder_mime_type not in mime_types:
                         mime_type_conditions.append(f"mimeType='{folder_mime_type}'")
                    mime_query_part = " or ".join(mime_type_conditions)
                    query += f" and ({mime_query_part})"
                
                # Add query string filter
                if query_string:
                     # If query_string is provided, it should apply to files.
                     # Folders should still be traversed.
                    query += f" and (mimeType='{folder_mime_type}' or ({query_string}))"

                items = []
                page_token = None # Initialize page_token to None for the first call
                while True:
                    request_fields = "nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, owners, description, driveId, parents)"
                    if drive_id:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                driveId=drive_id,
                                corpora="drive",
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields=request_fields,
                                pageToken=page_token,
                            )
                            .execute()
                        )
                    else:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                corpora="allDrives" if self.drive_id else "user", # Adjust corpora based on context
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields=request_fields,
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
                        # Recursive call for subfolders
                        fileids_meta.extend(
                            self._get_fileids_meta( # Pass all relevant params for recursion
                                drive_id=drive_id or item.get("driveId"), # Use item's driveId if available
                                folder_id=item["id"],
                                mime_types=mime_types,
                                query_string=query_string,
                                current_path=item_path, # Pass the updated path
                            )
                        )
                    else:
                        # File processing
                        is_shared_drive_file = "driveId" in item
                        author = "Shared Drive" # Default for shared drive files
                        if not is_shared_drive_file and item.get("owners"):
                            author = item["owners"][0].get("displayName", "Unknown Owner")
                        
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
            elif file_id: # Handling single file_id
                file = (
                    service.files()
                    .get(fileId=file_id, supportsAllDrives=True, fields="id, name, mimeType, createdTime, modifiedTime, owners, description, driveId, parents")
                    .execute()
                )
                is_shared_drive_file = "driveId" in file
                author = "Shared Drive"
                if not is_shared_drive_file and file.get("owners"):
                     author = file["owners"][0].get("displayName", "Unknown Owner")

                # For _get_relative_path, the root_folder_id is typically self.folder_id
                # If self.folder_id is None, _get_relative_path defaults to just file name
                file_actual_path = self._get_relative_path(service, file_id, self.folder_id)
                
                description = file.get("description", None)

                fileids_meta.append(
                    (
                        file["id"],
                        author,
                        file_actual_path, # Use the path relative to the initial folder_id if provided
                        file["mimeType"],
                        file["createdTime"],
                        file["modifiedTime"],
                        self._get_drive_link(file["id"]),
                        description,  # Add description as 8th element
                    )
                )
            # If neither folder_id nor file_id is provided, and self.query_string is,
            # we might need a top-level query. This part depends on GoogleDriveReader's intent.
            # For now, assuming folder_id or file_ids are the primary drivers as per GoogleDriveReader.load_data
            elif self.query_string and not folder_id and not file_ids : # if only query string is provided to the reader instance
                 logger.info(f"Performing a global query with: {self.query_string}")
                 query = self.query_string
                 # This section would be similar to folder listing but without 'parents' in query
                 # And would need to handle mime_types if provided for the global query.
                 # For simplicity, this specific path (global query) might need more fleshing out
                 # or rely on how GoogleDriveReader itself uses query_string at the top level.
                 # The current structure of GoogleDriveReader.load_data() prioritizes folder_id or file_ids.
                 # If query_string is meant to be global, the base _get_fileids_meta would need to handle it or
                 # this overridden version would need a new section for it.
                 # For now, this specific case (only query_string) is complex to add here without
                 # fully replicating and potentially conflicting with base class logic for query_string.
                 # The most robust way is to use query_string in conjunction with folder_id or file_ids.
                 pass # Placeholder for potential global query logic if needed


            return fileids_meta

        except Exception as e:
            logger.error(
                f"An error occurred while getting fileids metadata: {e}", exc_info=True
            )
            return [] # Return empty list on error

    def _load_data_fileids_meta(self, fileids_meta: List[List[Any]]) -> List[Document]: # Changed List[List[str]] to List[List[Any]]
        """
        Downloads files specified by fileids_meta and then processes all of them
        in a single batch call to LlamaParse.

        Args:
            fileids_meta: List of metadata for each file, as returned by _get_fileids_meta.
                          Each item is a tuple: (id, author, gdrive_path, mimeType, createdTime, modifiedTime, drive_link, description)

        Returns:
            List[Document]: A list of Document objects parsed by LlamaParse.
        """
        if not fileids_meta:
            return []

        downloaded_file_paths = []
        temp_path_to_metadata_map = {}

        try:
            with tempfile.TemporaryDirectory() as temp_dir_path_str:
                temp_dir = Path(temp_dir_path_str)

                for item_meta in fileids_meta:
                    file_id = item_meta[0]
                    # Base name for the temporary file, _download_file will add the extension
                    temp_file_base = temp_dir / file_id

                    try:
                        # Download the file
                        # self._creds should be set by the load_data method before this is called
                        final_temp_filepath_str = self._download_file(file_id, str(temp_file_base))
                        if final_temp_filepath_str:
                            downloaded_file_paths.append(final_temp_filepath_str)
                            # Store the rich metadata against the actual path LlamaParse will see
                            description_value = ""
                            if len(item_meta) > 7 and item_meta[7] is not None:
                                description_value = item_meta[7]
                            
                            temp_path_to_metadata_map[final_temp_filepath_str] = {
                                "file_id": item_meta[0],
                                "author": item_meta[1],
                                "file_path": item_meta[2], # Original Google Drive path
                                "mime_type": item_meta[3],
                                "created_at": item_meta[4],
                                "modified_at": item_meta[5],
                                "drive_link": item_meta[6],
                                "description": description_value, # Access description (8th element, index 7)
                            }
                            # Removed verbose logging of temp_path_to_metadata_map for brevity
                        else:
                            logger.warning(f"Failed to download file with ID: {file_id}")
                    except Exception as e:
                        logger.error(f"Error downloading file {file_id}: {e}", exc_info=True)
                        continue # Skip this file

                if not downloaded_file_paths:
                    logger.info("No files were successfully downloaded to parse.")
                    return []

                # Initialize LlamaParse
                parser = LlamaParse(
                    api_key=self._llama_cloud_api_key,
                    result_type=self._llama_parse_result_type,
                    verbose=self._llama_parse_verbose,
                    **self._llama_parse_kwargs
                )

                logger.info(f"Parsing {len(downloaded_file_paths)} files in a batch with LlamaParse: {downloaded_file_paths}")
                parsed_docs_from_llamaparse = parser.load_data(downloaded_file_paths)
                
                # --- DIAGNOSTIC LOGGING ---
                logger.info("--- Metadata from LlamaParse Documents (for verification) ---")
                if parsed_docs_from_llamaparse:
                    for i, parsed_doc in enumerate(parsed_docs_from_llamaparse):
                        logger.info(f"Doc {i} (LlamaParse ID: {parsed_doc.id_}) metadata from LlamaParse: {parsed_doc.metadata}")
                else:
                    logger.info("LlamaParse returned no documents.")
                logger.info("-----------------------------------------------------------")
                # --- END DIAGNOSTIC LOGGING ---

                final_documents = []
                
                num_downloaded = len(downloaded_file_paths)
                num_parsed = len(parsed_docs_from_llamaparse)

                if num_parsed == num_downloaded:
                    logger.info(f"Number of parsed documents ({num_parsed}) matches number of downloaded files ({num_downloaded}). Proceeding with index-based metadata mapping.")
                    for i, doc in enumerate(parsed_docs_from_llamaparse):
                        original_temp_path = downloaded_file_paths[i]
                        if original_temp_path in temp_path_to_metadata_map:
                            rich_metadata = temp_path_to_metadata_map[original_temp_path]
                            
                            # Ensure metadata is initialized if it's None from LlamaParse
                            if doc.metadata is None:
                                doc.metadata = {}
                                
                            doc.metadata.update(rich_metadata)
                            doc.id_ = rich_metadata.get("file_id", doc.id_) # Set doc.id_ to Google Drive file ID
                            final_documents.append(doc)
                        else:
                            # This should ideally not happen if lists are corresponding
                            logger.error(
                                f"Critical mapping error: Downloaded file path '{original_temp_path}' "
                                f"(at index {i}) not found in temp_path_to_metadata_map. "
                                f"Skipping metadata enrichment for this document."
                            )
                            # Optionally, append the doc with only LlamaParse's minimal metadata, or skip it
                            # For now, let's append it to not lose the parsed content, but log severity
                            final_documents.append(doc) 
                else:
                    logger.error(
                        f"Mismatch between number of downloaded files ({num_downloaded}) "
                        f"and parsed documents ({num_parsed}) from LlamaParse. "
                        f"Cannot reliably map all metadata using index-based approach. "
                        f"Documents will be returned with minimal metadata from LlamaParse."
                    )
                    # If counts don't match, we can't trust index-based mapping for all.
                    # We'll just append the documents as LlamaParse returned them.
                    # The user needs to be aware that metadata might be missing or incorrect.
                    final_documents.extend(parsed_docs_from_llamaparse)
                
                return final_documents

        except Exception as e:
            logger.error(f"An error occurred during batch LlamaParse processing: {e}", exc_info=True)
            return []


# --- Example Usage (Conceptual) ---
# if __name__ == "__main__":
#     # Ensure LLAMA_CLOUD_API_KEY is set in your environment
#     # os.environ["LLAMA_CLOUD_API_KEY"] = "your_llama_cloud_api_key"

#     # Your Google service account key (dictionary loaded from JSON)
#     # with open("path/to/your/service_account_key.json", "r") as f:
#     #     sa_key = json.load(f)
#     sa_key = None # Replace with your actual key dict or ensure service_account_key_path is valid

#     drive_folder_id_to_read = "YOUR_GOOGLE_DRIVE_FOLDER_ID"

#     batch_reader = BatchLlamaParseGoogleDriveReader(
#         folder_id=drive_folder_id_to_read,
#         service_account_key=sa_key, # Provide the key dict
#         # llama_cloud_api_key="...", # Or provide here if not in env
#         llama_parse_result_type="markdown",
#         llama_parse_verbose=True,
#         # Add other LlamaParse options here if needed, e.g.:
#         # split_by_page=True 
#     )

#     try:
#         documents = batch_reader.load_data()
#         print(f"\nSuccessfully loaded and parsed {len(documents)} documents using BatchLlamaParseGoogleDriveReader.")
#         for i, doc in enumerate(documents):
#             print(f"\n--- Document {i+1} ---")
#             print(f"ID (Google Drive File ID): {doc.id_}")
#             print(f"Metadata: {doc.metadata}")
#             # print(f"Content Preview: {doc.text[:200]}...") # If result_type is "text"
#     except Exception as e:
#         print(f"An error occurred: {e}")