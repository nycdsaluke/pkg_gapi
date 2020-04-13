import io
import pandas as pd
from datetime import datetime
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from gapi.gapi_connection import get_drive_service, get_sheet_service


def list_files(cred_path, folder_id):
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


def file2lcl_folder(cred_path, file_id, local_folder):
    """
    
    :param cred_path: str. The path to the folder of credentials (either credentials.json or token.pickle)
    :param file_id: str. The id of a file on Google Drive.
    :param local_folder: str. The local folder to save the file
    :return: 
    """
    dservice = get_drive_service(cred_path)
    request = dservice.files().get_media(fileId=file_id["id"])

    destination = "{}/{}".format(local_folder, file_id["name"])
    fh = io.FileIO(destination, mode='w')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    return done


def update_sheet(cred_path, spredsheet_id, sheet_name, values):
    sservice = get_sheet_service(cred_path)
    value_input_option="USER_ENTERED"

    body = {
        'values': values
    }
    sservice.spreadsheets().values().update(
        spreadsheetId=spredsheet_id, range=sheet_name,
        valueInputOption=value_input_option, body=body).execute()


def rm_sheet_from_workbook(cred_path, spreadsheet_id, sheet_id):
    sservice = get_sheet_service(cred_path)

    requests = []
    requests.append({'deleteSheet': {"sheetId": sheet_id}})

    body = {
        'requests': requests
    }
    sservice.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body).execute()


def get_google_sheet(cred_path, spreadsheet_id, range_name):
    sservice = get_sheet_service(cred_path)

    # Call the Sheets API
    gsheet = sservice.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    return gsheet


def gsheet2df(gsheet):
    """ Converts Google sheet data to a Pandas DataFrame.
    Note: This script assumes that your data contains a header file on the first row!
    Also note that the Google API returns 'none' from empty cells - in order for the code
    below to work, you'll need to make sure your sheet doesn't contain empty cells,
    or update the code to account for such instances.
    """
    header = gsheet.get('values', [])[0]   # Assumes first line is header!
    values = gsheet.get('values', [])[1:]  # Everything else is data.
    if not values:
        print('No data found.')
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                column_data.append(row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
        return df


def create_new_folder(cred_path, foldername, parent_id=None):
    dservice = get_drive_service(cred_path)
    if parent_id:
        file_metadata = {
            'name': foldername,
            'mimeType': 'application/vnd.google-apps.folder',
            "parents": [parent_id]
        }
    else:
        file_metadata = {
            'name': foldername,
            'mimeType': 'application/vnd.google-apps.folder'
        }
    file = dservice.files().create(body=file_metadata,
                                   fields='id').execute()

    return file


def share_folder(cred_path, file_id, share_lst, msg=""):
    dservice = get_drive_service(cred_path)

    def callback(request_id, response, exception):
        if exception:
            # Handle error
            print(exception)
        else:
            print("Permission Id: %s" % response.get('id'))

    def create_permission4writer(share_with):
        permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': share_with
        }

        return permission

    if not msg:
        msg = "Please upload the attendance of {} into this folder".format(str(datetime.now().date()))

    batch = dservice.new_batch_http_request(callback=callback)
    for writer in share_lst:
        user_permission = create_permission4writer(writer)
        batch.add(dservice.permissions().create(
            fileId=file_id,
            body=user_permission,
            fields='id',
            emailMessage=msg
        ))

    batch.execute()


def uploadFile(cred_path, local_file_name, parent_id, remote_file_name, mimetype="*/*", mimeType="*/*"):
    dservice = get_drive_service(cred_path)

    file_metadata = {
    'name': remote_file_name,
    'mimeType': mimeType,
    "parents": [parent_id]
    }
    media = MediaFileUpload(local_file_name,
                            mimetype=mimetype,
                            resumable=True)
    file = dservice.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')


def uploadCSV(cred_path, local_file_name, parent_id, remote_file_name):
    fid = uploadFile(cred_path, local_file_name, parent_id, remote_file_name,
               mimetype="text/csv", mimeType="application/vnd.google-apps.spreadsheet")

    return fid


def list_objects_by_name(cred_path, parent_id, name):
    collected = [
        d for d in list_files(cred_path, parent_id) if d["name"] == name]
    return collected


def fetch_or_create(cred_path, parent_id, name):
    collected = list_objects_by_name(cred_path, parent_id, name)
    if len(collected) == 0:
        collected = create_new_folder(cred_path, name, parent_id)
    else:
        collected = collected[0]

    return collected["id"]
