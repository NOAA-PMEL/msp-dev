import asyncio
import importlib
import json
import logging
import math
import sys
from time import sleep
from typing import List

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings, BaseModel, Field
from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

# # from cloudevents.http.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timedelta, timezone
from envds.util.util import (
    get_datetime_string,
    get_datetime,
    datetime_to_string,
    string_to_datetime,
    get_datetime_with_delta,
    string_to_timestamp,
    timestamp_to_string,
    time_to_next,
    round_to_nearest_N_seconds,
    seconds_elapsed,
)

# from envds.daq.event import DAQEvent
# from envds.daq.types import DAQEventType as det
from envds.sampling.event import SamplingEvent
from envds.sampling.types import SamplingEventType as sampet

import logging
from logfmter import Logfmter

# Configure structured logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

## this is the model for the deterministic data approach.
def calculate_heater_setpoint(temp, humidity):
    # DEAD-MAN SWITCH: If either sensor is offline, turn off the heater!
    if temp is None or humidity is None:
        return {"heater_setpoint": 0.0}
        
    # Normal operation
    setpoint = temp * 1.5 + humidity
    return {"heater_setpoint": setpoint}

async def set_isokinetic_inlet_flow(**kwargs):
    """
    Stubbed to return 50% flow for isokinetic sampling.
    Will map the return dict key ("fan_speed_sp") directly to the targets config.
    """
    # Later, calculate real flow using kwargs.get("relative_wind_speed")
    L.info("action_triggered", extra={"action": "set_isokinetic_inlet_flow", "target_val": 50.0})
    
    return {"fan_speed_sp": 50.0}


async def set_reverse_inlet_flow(**kwargs):
    """
    Stubbed to return -80% flow to clear the inlet.
    """
    L.info("action_triggered", extra={"action": "set_reverse_inlet_flow", "target_val": -80.0})
    
    return {"fan_speed_sp": -80.0}