from datetime import datetime, timedelta, timezone
import time
import math
import json
import hashlib

def time_to_next(sec: float) -> float:
    now = time.time()
    delta = sec - (math.fmod(now, sec))
    return delta

def get_datetime_format(fraction=True):
    isofmt = "%Y-%m-%dT%H:%M:%SZ"
    if fraction:
        isofmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    return isofmt

def get_datetime():
    return datetime.now(timezone.utc)

def get_datetime_string(fraction: bool=True):
    return datetime_to_string(get_datetime(), fraction=fraction)

def datetime_to_string(dt: datetime, fraction: bool=True):
    if dt:
        dt_string = dt.strftime(get_datetime_format(fraction=fraction))
        return dt_string
def string_to_datetime(dt_string: str, fraction: bool=True):
    if dt_string:
        try:
            isofmt = get_datetime_format(fraction=fraction)
            return datetime.strptime(dt_string, get_datetime_format())
        except ValueError:
            return None
        
def timestamp_to_string(ts: float, fraction: bool=True) -> str:
    try:
        dt = datetime.fromtimestamp(ts)
        return datetime_to_string(dt=dt, fraction=fraction)
    except TypeError:
        return None
    
def string_to_timestamp(dt_string: str, fraction: bool=True) -> float:
    if dt := string_to_datetime(dt_string=dt_string, fraction=fraction):
        return dt.timestamp()
    return None

def get_datetime_with_delta(delta: timedelta, dt: datetime=datetime.now(timezone.utc)):
    if timedelta < 0:
        return dt - timedelta(seconds=abs(delta))
    else:
        return dt + timedelta(seconds=abs(delta))

def datetime_mod_sec(sec: int) -> int:
    return get_datetime().second % sec

def seconds_elapsed(initial_dt: datetime) -> float:
    delta = get_datetime() - inital_dt
    return delta.total_seconds()

def get_checksum(data: dict) -> str:
    # return hash(json.dumps(data, sort_keys=True))

    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(data, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()

def round_to_nearest_N_seconds(dt: datetime, Nsec: int) -> datetime:
    try:
        total_seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
        # print(f"total_seconds = {total_seconds}")

        # Calculate the number of N-second intervals
        intervals = total_seconds / Nsec
        # print(f"intervals = {intervals}")

        # Round to the nearest interval
        rounded_intervals = round(intervals)
        # print(f"rounded_intervals = {rounded_intervals}")

        # Calculate the rounded total seconds
        rounded_total_seconds = rounded_intervals * Nsec
        # print(f"rounded_total_seconds = {rounded_total_seconds}")

        # Create a new datetime object with the rounded seconds
        rounded_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=rounded_total_seconds)
        # print(f"rounded_dt = {rounded_dt}")
        # result = timestamp_to_string(rounded_dt)
        result = rounded_dt
        
    except Exception as e:
        print(f"error: round_to_nearest_N_seconds: {e}")
        result = None

    return result