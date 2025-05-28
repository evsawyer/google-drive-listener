import os
from pathlib import Path
from typing import List, Dict, Any, Optional

from llama_index.core.readers.base import BaseReader
from llama_index.core.schema import Document
from llama_parse import LlamaParse # Ensure LlamaParse is installed

# Make sure LLAMA_CLOUD_API_KEY environment variable is set,
# or pass the api_key directly to LlamaParseReader.

class LlamaParseReader(BaseReader):
    """
    A LlamaIndex reader that uses LlamaParse to load data from a file.
    A new LlamaParse instance is created for each call to load_data
    to avoid issues with async event loop reuse.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        result_type: str = "text",  # Or "markdown"
        verbose: bool = True,
        **llama_parse_kwargs: Any
    ):
        """
        Initialize the LlamaParseReader with configuration for LlamaParse.

        Args:
            api_key (Optional[str]): LlamaParse API key.
                                     Defaults to LLAMA_CLOUD_API_KEY env var.
            result_type (str): The result type for LlamaParse ("text" or "markdown").
            verbose (bool): Whether LlamaParse should be verbose.
            **llama_parse_kwargs: Additional keyword arguments to pass to LlamaParse
                                 when creating a new instance in load_data.
        """
        super().__init__()
        self._api_key = api_key # Store for use in load_data
        self._result_type = result_type
        self._verbose = verbose
        self._llama_parse_kwargs = llama_parse_kwargs

    def load_data(self, file: Path, extra_info: Optional[Dict] = None) -> List[Document]:
        """
        Load data from the given file path using a new LlamaParse instance.

        Args:
            file (Path): The path to the file to parse.
            extra_info (Optional[Dict]): Extra info to be associated with the Document metadata.

        Returns:
            List[Document]: A list of Document objects from LlamaParse.
        """
        resolved_api_key = self._api_key or os.getenv("LLAMA_CLOUD_API_KEY")
        if not resolved_api_key:
            raise ValueError(
                "LlamaParse API key must be provided either via api_key argument "
                "during LlamaParseReader initialization or as LLAMA_CLOUD_API_KEY "
                "environment variable."
            )

        # Create a new LlamaParse instance for each file
        parser = LlamaParse(
            api_key=resolved_api_key,
            result_type=self._result_type,
            verbose=self._verbose,
            **self._llama_parse_kwargs
        )

        # LlamaParse's load_data expects a string file path
        parsed_documents = parser.load_data(str(file))

        if extra_info:
            for doc in parsed_documents:
                if doc.metadata is None:
                    doc.metadata = {}
                doc.metadata.update(extra_info)
        
        return parsed_documents