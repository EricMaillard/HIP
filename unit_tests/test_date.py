import time
from datetime import datetime as dt

def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

def datetime_from_local_to_utc(local_datetime):
    now_timestamp = time.time()
    offset = dt.fromtimestamp(now_timestamp) - dt.utcfromtimestamp(now_timestamp)
    return local_datetime - offset

date = "2022-11-16T11:32:33"
dt_date = dt.strptime(date, "%Y-%m-%dT%H:%M:%S")


date_local = datetime_from_utc_to_local(dt_date)
if "2022-11-16 12:32:33" == str(date_local):
    print("test1 ok, date = "+str(date_local))

date_utc = datetime_from_local_to_utc(dt_date)
if "2022-11-16 10:32:33" == str(date_utc):
    print("test2 ok, date = "+str(date_utc))
