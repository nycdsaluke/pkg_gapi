import os, shutil
import numpy as np
import pandas as pd
from gapi.utils.drive_io import create_new_folder, list_files, file2lcl_folder, uploadFile, uploadCSV, \
    get_google_sheet, gsheet2df
from gapi.utils.calendar import get_daily_events

from .helpers import parse_time, noon_split, is_new_csv, add_start_time2path


# Some utilities
def df2sheet(cred_path, to_upload, remote_folder_id, remote_file_name):
    """

    :param cred_path:
    :param to_upload:
    :param remote_folder_id:
    :param remote_file_name:
    :return:
    """
    tmp_fn = "./tobe_uploaded.csv"
    to_upload.to_csv(tmp_fn, index=False)

    """For a data frame that we want to upload, columns whose string values might have multiple lines (like
    summary in Google calendar events) need to be quoted."""
    if "summary" in to_upload:
        to_upload["summary"] = to_upload["summary"].apply(lambda s: '"{}"'.format(s))
    file_id = uploadCSV(cred_path, tmp_fn, remote_folder_id, remote_file_name)
    os.remove(tmp_fn)

    return file_id


# Activities
def create_daily_input_folder(cred_path, parent_id, str_date_agg):
    foldername = str_date_agg
    file = create_new_folder(cred_path, foldername, parent_id)
    file_id = file["id"]

    return file_id


def get_existing_remote_attn_shts(cred_path, input_folder, date_to_aggregate):
    activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate]

    if len(activities_folder) == 0:
        create_daily_input_folder(cred_path, input_folder, date_to_aggregate)

    activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate][0]
    folder_id = activities_folder["id"]

    filenames = [d["name"] for d in list_files(cred_path, folder_id)]

    return filenames, folder_id


def get_raw_daily_activities(cred_path, daily_attendance_folder_id, room_id2name):
    attendance_sheets = list_files(cred_path, daily_attendance_folder_id)
    lst_attendance_df = []

    tmp_folder = "./tmp_folder"
    os.mkdir(tmp_folder)
    for sheet in attendance_sheets:
        file2lcl_folder(cred_path, sheet, tmp_folder)

        df = pd.read_csv(
            "{}/{}".format(tmp_folder, sheet["name"]), usecols=[0, 2, 3, 4],
        )
        df.rename(columns={"Name (Original Name)": "Name"}, inplace=True)

        df["Name"] = df["Name"].str.lower()

        room_id = sheet["name"].split("_")[-1].replace(".csv", "")
        df["Zoom"] = room_id2name[room_id]

        for col in ["Join Time", "Leave Time"]:
            df[col] = df[col].apply(parse_time)

        df.sort_values(["Name", "Join Time"], inplace=True)
        df.reset_index(inplace=True, drop=True)
        lst_attendance_df.append(df)

    shutil.rmtree(tmp_folder)
    return pd.concat(lst_attendance_df)


def upload_non_duplicated_file(
        cred_path, local_files_folder, uploaded, date_to_aggregate, tmp_folder_for_attendance, parent_id):
    all_files = []
    for root, dirs, files in os.walk(local_files_folder):
        all_files.extend(files)

    for fn in all_files:
        if is_new_csv(fn, uploaded):
            old_path = "{}/{}".format(tmp_folder_for_attendance, fn)
            new_filename = add_start_time2path(fn, old_path)

            new_path = "{}/{}".format(tmp_folder_for_attendance, new_filename)
            df = pd.read_csv(old_path, skiprows=2)
            df.to_csv(new_path, index=False)
            os.remove(old_path)
            uploadFile(cred_path, new_path, parent_id, new_filename)


def upload_details4students(cred_path, activities_df, df_students, details_folder_id):
    name_id_pairs = []
    for name in df_students["Name"]:
        print(name)
        remote_file_name = "{}.csv".format(name.title())

        to_upload = activities_df.loc[activities_df["Name"] == name.lower()]
        to_upload = to_upload[
            ["Name", "Join Time", "Leave Time", "Duration (Minutes)", "Zoom"]
        ].assign(Name=lambda x: x["Name"].str.title())

        student_file_id = df2sheet(cred_path, to_upload, details_folder_id, remote_file_name)
        name_id_pairs.append((name, student_file_id))

    name_id_pair = pd.DataFrame(name_id_pairs, columns=["Name", "id"])
    return name_id_pair

# Events
def get_events4attendance(cred_path, calendar_name2id, str_date_agg, dual_track=None):
    daily_events = get_daily_events(cred_path, calendar_name2id, str_date_agg)

    def not_dual_classes(event):
        return event.find("DE:") == -1 and event.find("DL:") == -1

    if dual_track:
        keep = [
            not_dual_classes(event) or event.find(dual_track[room]) >= 0 for event, room \
            in zip(daily_events["summary"], daily_events["Zoom"])]
        daily_events = daily_events.loc[keep]

    noon = noon_split(str_date_agg)
    morning = [int(t >= noon) for t in daily_events["start"].to_list()]
    session = np.array(["morning", "afternoon"])[morning]
    daily_events["session"] = session

    return daily_events


# Students: TODO:: Bad, better rewrite
def get_students_raw(cred_path, SPREADSHEET_ID):
    gsheet1 = get_google_sheet(cred_path, SPREADSHEET_ID, "Room 1")
    gsheet2 = get_google_sheet(cred_path, SPREADSHEET_ID, "Room 2")
    gdf1 = gsheet2df(gsheet1)
    gdf2 = gsheet2df(gsheet2)
    gdf1["Room"] = "Room 1"
    gdf2["Room"] = "Room 2"
    df_students = pd.concat([gdf1, gdf2])

    return df_students
