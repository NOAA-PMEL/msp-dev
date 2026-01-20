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

async def set_nominal_inlet_flow(self, nominal_flow_rate):
    # since this is in beacons_msp
    nozzle_diam = .019 # m
    flow_sensor_diam = .1016 # m

    result = {"inlet_flow_sp": 10}
    return result

async def set_isokinetic_inlet_flow(self, relative_wind_speed):
    # since this is in beacons_msp
    nozzle_diam = .019 # m
    flow_sensor_diam = .1016 # m

    # ws (m/s) * xsect sfc area (m2) -> m3/s -> lpm
    flow = relative_wind_speed * (nozzle_diam/2)^2 * math.pi * (60. / 1000.) # lpm
    
    # as a check, velocity at flow_sensor
    inlet_velocity = flow *(1000./60.) / math.pi / (flow_sensor_diam/2)^2 # m/s
    
    result = {"inlet_flow_sp": flow}
    return result
