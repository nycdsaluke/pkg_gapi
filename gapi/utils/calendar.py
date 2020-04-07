import pandas as pd
from gapi.gapi_connection import get_calendar_service
from datetime import datetime, timedelta
from pytz import timezone


def get_smry_time(item):
    included = {"start", "end", "summary"}
    result = [
        get_time(item[k]) if k in ["start", "end"] else item[k] for k in item if k in included
    ]

    return result


def get_events(cred_path, cid, year, month, day):
    """

    :param cred_path: str. indicate the location of credentials
           (the folder that includes token.pickle or credentials.json)
    :param cid: str. Google Calendar Id
    :param year: int
    :param month: int
    :param day: int
    :return:
    """
    cservice = get_calendar_service(cred_path)
    time_min = datetime(year, month, day, 0, 0, tzinfo=timezone("America/New_York"))
    time_max = time_min + timedelta(seconds=86399)
    time_min = time_min.isoformat()
    time_max = time_max.isoformat()

    events_result = cservice.events().list(calendarId=cid, timeMin=time_min, timeMax=time_max,
                                           maxResults=10, singleEvents=True,
                                           orderBy='startTime').execute()

    return [get_smry_time(item) for item in events_result["items"]]


def get_time(d):
    if "dateTime" in d:
        return d["dateTime"]
    elif "date" in d:
        return d["date"]
    else:
        raise ValueError("Can't see a time representation in this dictionary.")


def exclude_no_tzinfo(df):
    return df.loc[df["start"].apply(lambda x: x.tzinfo is not None)]


def get_daily_events(cred_path, rooms, date_today, exclude_events_for_day=True):
    """

    :param cred_path: str. Indicate the location of credentials
           (the folder that includes token.pickle or credentials.json)
    :param rooms: dict. The corresponding between room number and the Google Calendar Id.
    :param date_today: str. Date today
    :param exclude_events_for_day: bool. Some events are for the day. True if those should be excluded.
    :return:
    """
    lst_event_dfs = []
    year, month, day = [int(s) for s in date_today.split("-")]
    for room in rooms:
        cid = rooms[room]
        events_df = pd.DataFrame(
            get_events(cred_path, cid, year, month, day), columns=["summary", "start", "end"]
        )

        for col in ["start", "end"]:
            events_df[col] = events_df[col].apply(datetime.fromisoformat)

        if exclude_events_for_day:
            events_df = exclude_no_tzinfo(events_df)

        for col in ["start", "end"]:
            events_df[col] = events_df[col].apply(lambda x: x.replace(tzinfo=None))


        events_df["Room"] = room
        lst_event_dfs.append(events_df)

    return pd.concat(lst_event_dfs).reset_index(drop=True)
