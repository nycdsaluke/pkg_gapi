import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def noon_split(str_date_agg):
    return datetime.strptime(str_date_agg, "%Y-%m-%d") + timedelta(hours=13)


def is_not_date(s):
    try:
        datetime.strptime(s, "%Y%m%d")
        return False
    except ValueError:
        return True


def is_new_csv(fn, uploaded):
    # The second condition should be "is not today and is not older"
    boo = fn.find(".csv") >= 0 and \
          fn not in uploaded and \
          is_not_date(fn[:8])

    return boo


def get_headstr(old_path):
    meta = pd.read_csv(old_path, nrows=1)
    headstr = meta["Start Time"].iloc[0]
    headstr = re.sub("[-T:]", "", datetime.strptime(headstr, "%m/%d/%Y %I:%M:%S %p").isoformat())
    return headstr


def add_start_time2path(fn, old_path):
    headstr = get_headstr(old_path)
    new_fn = '{}_{}'.format(headstr, fn)
    new_fn = re.sub(" \(\d+\)", "", new_fn)
    return new_fn


def parse_time(s):
    """

    :param s: str: Timestamp, might have different formats
    :return:
    """
    parsed = None

    if s.find("AM") >= 0 or s.find("PM") >= 0:
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


def get_activity_class_overlap(mg, minutes=None):
    """

    :param mg:
    :param minutes: int. How long into a class is to be accounted.
                    If None, find any activity happens dring the class time
    :return:
    """
    s1 = mg["start"]
    if minutes:
        e1 = s1 + timedelta(minutes=minutes)
    else:
        e1 = mg["end"]

    s2 = mg["Join Time"]
    e2 = mg["Leave Time"]

    new = get_overlap(s1, e1, s2, e2)
    return pd.concat([mg, new], axis=1)


def get_first_15(mg):
    return get_activity_class_overlap(mg, minutes=15)


def add_sessions(df_students):
    df_students["Name"] = df_students["Name"].str.lower()
    df_students["ph"] = 1
    day = pd.DataFrame({"ph": [1] * 2, "session": ["morning", "afternoon"]})
    df_students = df_students[["Room", "Name", "ph"]].merge(day, on="ph")

    return df_students[["Room", "Name", "session"]]
