from google.cloud import storage
import json
from config import settings
from google.oauth2 import service_account
from googleapiclient.discovery import build
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