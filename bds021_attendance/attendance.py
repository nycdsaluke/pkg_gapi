import os
import shutil
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from gapi.utils.drive_io import list_files, file2lcl_folder, get_google_sheet, gsheet2df, uploadFile
from gapi.utils.calendar import get_daily_events


def get_raw_daily_activities(cred_path, daily_attendance_folder_id, room_id2name):
    def parse_time(s):
        parsed = None

        if s.find("AM")>=0 or s.find("PM")>=0:
            try:
                parsed = datetime.strptime(s, "%m/%d/%Y %I:%M %p")
            except:
                pass

            if not parsed:
                try:
                    parsed = datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")
                except:
                    pass

            if parsed:
                return parsed
            else:
                print(s)
                raise ValueError("Failed to parse the time passed.")
        else:
            return datetime.strptime(s, "%m/%d/%Y %I:%M")


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
        df["Room"] = room_id2name[room_id]

        for col in ["Join Time", "Leave Time"]:
            df[col] = df[col].apply(parse_time)

        df.sort_values(["Name", "Join Time"], inplace=True)
        df.reset_index(inplace=True, drop=True)
        lst_attendance_df.append(df)

    shutil.rmtree(tmp_folder)
    return pd.concat(lst_attendance_df)


def get_events4attendance(cred_path, BDS020_calendar_name2id, str_date_agg, dual_track=None):
    daily_events = get_daily_events(cred_path, BDS020_calendar_name2id, str_date_agg)

    def not_dual_classes(event):
        return event.find("DE:")==-1 and event.find("DL:")==-1

    if dual_track:
        keep = [not_dual_classes(event) or event.find(dual_track[room])>=0 for event, room in zip(daily_events["summary"], daily_events["Room"])]
        daily_events = daily_events.loc[keep]

    noon = datetime.strptime(str_date_agg, "%Y-%m-%d") + timedelta(hours=13)
    morning = [int(t >= noon) for t in daily_events["start"].to_list()]
    session = np.array(["morning", "afternoon"])[morning]
    daily_events["session"] = session

    return daily_events


def get_overlap(s1, e1, s2, e2):
    lowb = pd.concat([s1, s2], axis=1).max(axis=1)
    lowb.loc[np.logical_or(
        np.logical_and(s1 > s2, s1 > e2), np.logical_and(s2 > s1, s2 > e1)
    )] = None

    upb = pd.concat([e1, e2], axis=1).min(axis=1)
    upb.loc[np.logical_or(
        np.logical_and(e1 < e2, e1 < s2), np.logical_and(e2 < e1, e2 < s1)
    )] = None

    new = pd.concat([lowb, upb], axis=1).rename(columns={0: "joined", 1: "left"})

    return new


def get_full_overlap(mg):
    s1 = mg["start"]
    e1 = mg["end"]

    s2 = mg["Join Time"]
    e2 = mg["Leave Time"]

    new = get_overlap(s1, e1, s2, e2)
    return pd.concat([mg, new], axis=1)


def get_first_15(mg):
    s1 = mg["start"]
    e1 = s1 + timedelta(minutes=60)

    s2 = mg["Join Time"]
    e2 = mg["Leave Time"]

    new = get_overlap(s1, e1, s2, e2)
    return pd.concat([mg, new], axis=1)


def get_students(cred_path, SPREADSHEET_ID):
    gsheet1 = get_google_sheet(cred_path, SPREADSHEET_ID, "Room 1")
    gsheet2 = get_google_sheet(cred_path, SPREADSHEET_ID, "Room 2")
    gdf1 = gsheet2df(gsheet1)
    gdf2 = gsheet2df(gsheet2)
    gdf1["Room"] = "Room 1"
    gdf2["Room"] = "Room 2"

    df_students = pd.concat([gdf1, gdf2])
    df_students["Name"] = (df_students["First Name"] + " " + df_students["Last Name"]).str.lower()
    df_students["ph"] = 1
    day = pd.DataFrame({"ph": [1] * 2, "session": ["morning", "afternoon"]})
    df_students = df_students[["Room", "Name", "ph"]].merge(day, on="ph")

    return df_students


# def get_existing_remote_attn_shts(cred_path, input_folder, date_to_aggregate, share_lst):
#     activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate]
#
#     if len(activities_folder) == 0:
#         create_daily_attendance_folder(cred_path, input_folder, share_lst, date_to_aggregate)
#
#     activities_folder = [d for d in list_files(cred_path, input_folder) if d["name"] == date_to_aggregate][0]
#     folder_id = activities_folder["id"]
#
#     filenames = [d["name"] for d in list_files(cred_path, folder_id)]
#
#     return filenames, folder_id


def is_new_csv(fn, uploaded, date_to_aggregate):
    boo = fn.find(".csv") >= 0 and \
          fn[:8] != date_to_aggregate.replace("-", "") and \
          fn not in uploaded

    return boo

def get_headstr(old_path):
    meta = pd.read_csv(old_path, nrows= 1)
    headstr = meta["Start Time"].iloc[0]
    headstr = re.sub("[-T:]", "", datetime.strptime(headstr, "%m/%d/%Y %I:%M:%S %p").isoformat())
    return headstr


def add_start_time2path(fn, old_path):
    headstr = get_headstr(old_path)
    new_fn = '{}_{}'.format(headstr, fn)
    new_fn = re.sub(" \(\d+\)", "", new_fn)
    return new_fn


def upload_non_duplicated_file(cred_path, local_files_folder, uploaded, date_to_aggregate, tmp_folder_for_attendance, parent_id):
    all_files = []
    for root, dirs, files in os.walk(local_files_folder):
        all_files.extend(files)

    for fn in all_files:
        if is_new_csv(fn, uploaded, date_to_aggregate):
            old_path = "{}/{}".format(tmp_folder_for_attendance, fn)
            new_filename = add_start_time2path(fn, old_path)

            new_path = "{}/{}".format(tmp_folder_for_attendance, new_filename)
            df = pd.read_csv(old_path, skiprows=2)
            df.to_csv(new_path, index=False)
            os.remove(old_path)
            uploadFile(cred_path, new_path, parent_id, new_filename)