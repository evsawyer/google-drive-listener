import os
import json
import logging
import asyncio
from dotenv import load_dotenv
from typing import Dict, List, Sequence, Type, Any, Optional
from pydantic import BaseModel, Field
# import marvin

# LlamaIndex imports
from llama_index.core.schema import Document
from llama_index.core.extractors.interface import BaseExtractor
from llama_index.core.schema import BaseNode
from llama_index.core.utils import get_tqdm_iterable
from llama_index.core.node_parser import TokenTextSplitter
from llama_index.core.ingestion import IngestionPipeline, DocstoreStrategy
from llama_index.core.indices import VectorStoreIndex
from llama_index_cloud_sql_pg import PostgresEngine, PostgresDocumentStore
from llama_index.vector_stores.pinecone import PineconeVectorStore
# import marvin
from llama_index.embeddings.openai import OpenAIEmbedding

from pinecone import Pinecone
from config import settings
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configure Marvin
# marvin.settings.openai.api_key = os.environ.get("OPENAI_API_KEY")
# marvin.settings.openai.chat.completions.model = "gpt-4o"

# Define a model for what you want to extract from descriptions
# class DescriptionMetadata(BaseModel):
#     source: str = Field(..., description="Origin of the document (e.g., website, internal upload)")
#     user_id: str = Field(..., description="IVC email address")
#     client: str = Field(..., description="Client this document is for")
#     title: str = Field(..., description="Descriptive title of the document")
#     tags: list[str] = Field(..., description="Tags associated with the document")

# class DescriptionMetadataExtractor(BaseExtractor):
#     # Define as a class field with Field annotation
#     marvin_model: Type[BaseModel] = Field(
#         description="The target pydantic model to extract from descriptions"
#     )
    
#     """Metadata extractor for Google Drive description fields using Marvin.
    
#     This extractor processes the 'description' field that was added by the 
#     EnhancedGoogleDriveReader and extracts structured metadata using Marvin.
    
#     Args:
#         marvin_model: The target pydantic model to extract from descriptions.
#     """
    
#     def __init__(
#         self,
#         marvin_model: Type[BaseModel],
#         **kwargs: Any,
#     ) -> None:
#         """Initialize with the marvin model."""
#         # Pass marvin_model to parent constructor
#         super().__init__(marvin_model=marvin_model, **kwargs)
    
#     @classmethod
#     def class_name(cls) -> str:
#         return "DescriptionMetadataExtractor"
    
#     async def aextract(self, nodes: Sequence[BaseNode]) -> List[Dict]:
#         from marvin import cast_async
        
#         metadata_list: List[Dict] = []
        
#         nodes_queue = get_tqdm_iterable(
#             nodes, self.show_progress, "Extracting description metadata"
#         )
        
#         for node in nodes_queue:
#             # Always initialize with an empty dictionary
#             node_metadata = {}
            
#             try:
#                 # Check if description exists in node metadata
#                 if hasattr(node, "metadata") and "description" in node.metadata and node.metadata["description"]:
#                     # Get the description text
#                     description_text = node.metadata["description"]
                    
#                     # Extract structured data using Marvin
#                     extracted = await cast_async(description_text, target=self.marvin_model)
#                     extracted_dict = extracted.model_dump()
                    
#                     # Add each field directly to node_metadata
#                     for key, value in extracted_dict.items():
#                         # Handle the 'tags' field which is a list
#                         if key == 'tags' and isinstance(value, list):
#                             # Convert list to comma-separated string for Pinecone
#                             if value:  # Only join if list is not empty
#                                 node_metadata[key] = ", ".join(value)
#                             else:
#                                 node_metadata[key] = ""  # Empty string for empty list
#                         else:
#                             # Add other fields directly to metadata
#                             node_metadata[key] = value
#             except Exception as e:
#                 logger.error(f"Error extracting metadata from description: {e}")
#                 # Important: Keep node_metadata as empty dict, don't set to None
            
#             # Always append a dictionary to metadata_list
#             metadata_list.append(node_metadata)
        
#         return metadata_list

async def setup_pipeline():
    """Set up the LlamaIndex ingestion pipeline with PostgreSQL and Pinecone."""
    logger.info("Setting up LlamaIndex ingestion pipeline...")
    
    # Set up PostgreSQL connection
    logger.info("Connecting to PostgreSQL...")
    engine = await PostgresEngine.afrom_instance(
        project_id=settings.project_id,
        region=settings.db_region,
        instance=settings.db_instance,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.postgres_password,
        ip_type="public",
    )
    
    # Create document store
    logger.info("Creating PostgreSQL document store...")
    doc_store = await PostgresDocumentStore.create(
        engine=engine,
        table_name="document_store",
        # schema_name=SCHEMA_NAME
    )
    
    # Set up Pinecone
    logger.info("Connecting to Pinecone...")
    pc = Pinecone(api_key=settings.pinecone_api_key)
    index_name = settings.pinecone_index_name
    
    # Create vector store
    namespace = settings.pinecone_namespace
    logger.info(f"Creating Pinecone vector store with index: {index_name}, namespace: {namespace}")
    vector_store = PineconeVectorStore(pc.Index(index_name), namespace=namespace)
    
    # Set up embedding model
    logger.info("Setting up embedding model...")
    embed_model = OpenAIEmbedding(
        model="text-embedding-3-small",
    )

    # Create node parser and extractor
    logger.info("Creating node parser...")
    node_parser = TokenTextSplitter(
        separator=" ", 
        chunk_size=8191, 
        chunk_overlap=0
    )

# works but we dont need marcin on the description field
    # description_extractor = DescriptionMetadataExtractor(
    #     marvin_model=DescriptionMetadata
    # )
    
    # Create ingestion pipeline
    logger.info("Creating ingestion pipeline...")
    pipeline = IngestionPipeline(
        transformations=[
            node_parser,
            # description_extractor,
            embed_model
        ],
        docstore=doc_store,
        vector_store=vector_store,
        docstore_strategy=DocstoreStrategy.UPSERTS
    )
    
    return pipeline

async def process_documents(docs: List[Document]):
    """Process documents through the LlamaIndex pipeline."""
    if not docs:
        logger.warning("No documents to process")
        return None
    
    logger.info(f"Processing {len(docs)} documents through LlamaIndex pipeline...")
    
    try:
        # Set up the pipeline
        pipeline= await setup_pipeline()
        
        # Run the pipeline
        logger.info("Running ingestion pipeline...")
        nodes = pipeline.run(documents=docs, show_progress=True)
        
        # Insert nodes into the index
        logger.info(f"Inserted {len(nodes)} nodes into the index...")
        
        logger.info("Document processing completed successfully")
        return nodes
    
    except Exception as e:
        logger.error(f"Error processing documents: {e}")
        logger.exception("Full traceback:")
        return None

# Function to run from the webhook
async def run_pipeline_for_documents(docs: List[Document]) -> bool:
    """Run the pipeline for a list of documents. Returns True if successful."""
    if not docs:
        logger.warning("No documents to process")
        return False
    
    logger.info(f"Running pipeline for {len(docs)} documents")
    
    try:
        # Run the async pipeline using asyncio
        nodes = await process_documents(docs)
        
        if nodes:
            logger.info(f"Successfully processed {len(nodes)} nodes")
            return True
        else:
            logger.warning("No nodes were created from the documents")
            return False
            
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
        logger.exception("Full traceback:")
        return False

# For testing the module directly
if __name__ == "__main__":
    pass  # or just remove this block entirely