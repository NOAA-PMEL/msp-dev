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

def seconds_elapsed(inital_dt: datetime) -> float:
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