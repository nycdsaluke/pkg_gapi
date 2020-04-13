import numpy as np
from .helpers import get_first_15, add_sessions


def aggregate_attendance(df_activities, df_students, df_events):
    df_students["Name"] = df_students["Name"].str.lower()
    df_students = add_sessions(df_students)
    df_students = df_students[["Room", "Name", "session"]]

    mg = df_activities.merge(df_events, on="Zoom")
    mg = get_first_15(mg) # For each activity, check if it overlaps first 15 minutes of a class

    # For each student in each class (summary), if there is one event logged within the first 15 minutes
    attendance_by_classes = mg.groupby(
        ["Name", "summary"]
    ).agg(
        {"joined":lambda x: np.logical_not(x.isnull()).sum()>0, "session":"first",
         "start":"first"}
    ).reset_index().sort_values(
        ["Name", "start"]
    )
    attendance_by_classes = df_students.merge(
        attendance_by_classes, on=["Name", "session"], how="left"
    )

    return attendance_by_classes


def add_url2attendance(attendance_df, name_id_pair, detail_sheet_link):
    get_full_url = lambda s: detail_sheet_link.format(s)

    attendance_df = attendance_df.merge(
        name_id_pair.assign(
        Name=lambda x: x["Name"].str.lower()), on="Name"
    ).assign(
        URL = lambda x: x["id"].apply(get_full_url)
    )[["Room", "Name", "session", "summary", "joined", "URL"]]

    attendance_df = attendance_df.fillna(False)

    return attendance_df


def attendance4morning_and_afternoon(attendance_by_classes):
    if "URL" in attendance_by_classes:
        agg_methods = {"joined": lambda x: np.sum(x) > 0, "Room": "first", "URL": "first"}
        cols = ["Room", "Name", "session", "joined", "URL"]
    else:
        agg_methods = {"joined": lambda x: np.sum(x) > 0, "Room": "first"}
        cols = ["Room", "Name", "session", "joined"]

    attendance = attendance_by_classes.groupby(
        ["Name", "session"]
    ).agg(agg_methods).reset_index(
    ).sort_values(
        ["Room", "Name", "session"], ascending=[True, True, False]
    )[cols]

    return attendance
