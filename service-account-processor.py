import json

# Read the service account file
with open('/Users/ivcmedia_evansawyer/dev/google-drive-listener/knowledge-base-458316-966fdfc500f9.json', 'r') as f:
    service_account_json = json.load(f)

# Create the exact string you should put in your .env file
env_var_value = json.dumps(service_account_json, separators=(',', ':'))
print("Put this in your .env file:")
print(f"SERVICE_ACCOUNT_INFO={env_var_value}")