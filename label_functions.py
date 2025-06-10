import logging
from service_functions import get_drive_service, get_label_service

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

label_service = get_label_service()
drive_service = get_drive_service()

def get_file_labels(file_id, label_id):
    full_label = label_service.labels().get(
        name=f'labels/{label_id}@latest',
        view='LABEL_VIEW_FULL'
    ).execute()

    # Build schema map
    schema = {}
    for field in full_label.get('fields', []):
        field_id = field['id']
        field_display = field['properties']['displayName']
        field_choices = {}

        if 'selectionOptions' in field:
            for choice in field['selectionOptions'].get('choices', []):
                cid = choice['id']
                cname = choice['properties']['displayName']
                field_choices[cid] = cname

        schema[field_id] = {
            'display': field_display,
            'choices': field_choices
        }

    file = drive_service.files().get(
        fileId=file_id,
        supportsAllDrives=True,
        includeLabels=label_id,
        fields="labelInfo"
    ).execute()

    output = {}
    labels = file.get('labelInfo', {}).get('labels', [])
    for label in labels:
        fields = label.get('fields', {})
        for fid, val in fields.items():
            if fid not in schema:
                continue

            field_name = schema[fid]['display']
            choices = schema[fid]['choices']
            value_type = val['valueType']

            if value_type == 'text':
                output[field_name] = val['text'][0]

            elif value_type == 'selection':
                selected_ids = val['selection']
                display_vals = [choices.get(cid, cid) for cid in selected_ids]
                output[field_name] = display_vals if len(display_vals) > 1 else display_vals[0]

            elif value_type == 'integer':
                output[field_name] = int(val['integer'][0])

            elif value_type == 'dateString':
                output[field_name] = val['dateString'][0]

            elif value_type == 'user':
                users = val['user']
                emails = [u.get('emailAddress', u.get('displayName')) for u in users]
                output[field_name] = emails  # Always a list

            else:
                output[field_name] = f"<Unhandled type: {value_type}>"

    return output