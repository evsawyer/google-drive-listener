from google.cloud import storage
import json
from config import settings
from google.oauth2 import service_account
from googleapiclient.discovery import build
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']
LABEL_SCOPES = ['https://www.googleapis.com/auth/drive.labels.readonly']

def get_service_account_info():
    """Get service account info from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(settings.bucket_name)
    blob = bucket.blob(settings.service_account_folder + '/' + settings.service_account_key)
    return json.loads(blob.download_as_string())

def get_drive_service():
    """Create and return an authorized Drive API service instance."""
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=DRIVE_SCOPES)
    return build('drive', 'v3', credentials=credentials)

def get_label_service():
    """Create and return an authorized Label API service instance."""
    service_account_info = get_service_account_info()
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=LABEL_SCOPES)
    credentials = credentials.with_subject('anthony@ivc.media')
    return build('drivelabels', 'v2', credentials=credentials)