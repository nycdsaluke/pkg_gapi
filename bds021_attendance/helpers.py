from gapi.utils.drive_io import create_new_folder, share_folder, list_files


def create_daily_attendance_folder(cred_path, parent_id, share_lst, str_date_agg):
    foldername = str_date_agg
    file = create_new_folder(cred_path, foldername, parent_id)
    file_id = file["id"]
    share_folder(cred_path, file_id, share_lst)


def get_existing_remote_attn_shts(cred_path, input_folder, date_to_aggregate, share_lst):
    activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate]

    if len(activities_folder) == 0:
        create_daily_attendance_folder(cred_path, input_folder, share_lst, date_to_aggregate)

    activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate][0]
    folder_id = activities_folder["id"]

    filenames = [d["name"] for d in list_files(cred_path, folder_id)]

    return filenames, folder_id
