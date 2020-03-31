from gapi.gapi_connection import get_drive_service


def get_files(cred_path, folder_id):
    dservice = get_drive_service(cred_path)
    all_files = []
    page_token = None
    while True:
        response = dservice.files().list(q="parents in '{}'".format(folder_id),
                                         spaces='drive',
                                         fields='nextPageToken, files(id, name)',
                                         pageToken=page_token).execute()
        for file in response.get('files'):
            # Process change
            all_files.append(file)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return all_files
