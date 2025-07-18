import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load environment variables from .env file
# It's good practice to call this before settings are loaded,
# especially if any default values in Settings rely on os.getenv
load_dotenv()

class Settings(BaseSettings):

    bucket_name: str = 'drive-listener'
    service_account_folder: str = 'service-account'
    drive_state_folder: str = 'drive-state'

    # Service Account and Storage
    # service_account_bucket_name: str = 'service-account-0'
    service_account_key: str = 'knowledge-base-458316-93655f40ccc1.json'

    # drive_state_bucket_name: str = 'drive-state'
    # the bucket folder should be the drive name - drive id?
    # drive_state_bucket_folder: str = os.getenv("DRIVE_STATE_BUCKET_FOLDER")
    # credentials_bucket_name: str = 'drive-reader-credentials'
    
    # API Keys
    pinecone_api_key: str = os.getenv("PINECONE_API_KEY")
    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    llama_cloud_api_key: str = os.getenv("LLAMA_CLOUD_API_KEY")
    
    # Database Configuration
    postgres_password: str = os.getenv("POSTGRES_PASSWORD")
    project_id: str = 'knowledge-base-458316'
    db_region: str = 'us-central1'
    db_instance: str = 'llamaindex-docstore'
    db_name: str = 'docstore'
    db_user: str = 'docstore_rw'
    
    # Pinecone Configuration
    pinecone_index_name: str = 'google-drive-knowledge-base'
    pinecone_namespace: str = os.getenv("PINECONE_NAMESPACE")

    # API Keys
    pinecone_api_key: str = os.getenv("PINECONE_API_KEY")
    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    llama_cloud_api_key: str = os.getenv("LLAMA_CLOUD_API_KEY")

    # secret key to access refresh drive channel route
    refresh_key: str = os.getenv("REFRESH_KEY")

    # label id
    label_id: str = 'ON9CAVs48dKc7CnxNxcs4mmk9D9JMQ74AOORNNEbbFcb'

    class Config:
        case_sensitive = False

# Initialize settings
settings = Settings() 