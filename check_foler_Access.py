from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

load_dotenv()

FOLDER_ID = os.getenv("FOLDER_ID")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

def check_folder_access():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    drive_service = build('drive', 'v3', credentials=credentials)
    
    print(f"Service account email: {credentials.service_account_email}")
    print(f"Testing access to folder {FOLDER_ID}...")
    
    # Try to get the folder metadata with Shared Drive support
    try:
        folder = drive_service.files().get(
            fileId=FOLDER_ID, 
            fields="name,id,driveId",
            supportsAllDrives=True
        ).execute()
        print(f"Success! Found folder: {folder.get('name')} (ID: {folder.get('id')})")
        if 'driveId' in folder:
            print(f"This folder is in a Shared Drive with ID: {folder.get('driveId')}")
    except Exception as e:
        print(f"Error accessing folder: {e}")
        print("Trying alternative approaches...")
        
        # Try listing all accessible drives
        try:
            drives = drive_service.drives().list().execute()
            print(f"Found {len(drives.get('drives', []))} Shared Drives:")
            for drive in drives.get('drives', []):
                print(f"- {drive.get('name')} (ID: {drive.get('id')})")
        except Exception as e:
            print(f"Error listing drives: {e}")
        
        # Try listing root files
        try:
            results = drive_service.files().list(
                q="'root' in parents", 
                fields="files(id, name)"
            ).execute()
            print(f"Found {len(results.get('files', []))} files in root:")
            for file in results.get('files', [])[:5]:  # Show first 5 only
                print(f"- {file.get('name')} (ID: {file.get('id')})")
        except Exception as e:
            print(f"Error listing root files: {e}")

if __name__ == "__main__":
    check_folder_access()