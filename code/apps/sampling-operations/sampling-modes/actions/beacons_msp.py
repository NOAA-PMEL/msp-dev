import asyncio
import importlib
import json
import logging
import math
import sys
from time import sleep
from typing import List

from ulid import ULID
from pathlib import Path
import os
import httpx
from logfmter import Logfmter

from pydantic import BaseSettings, BaseModel, Field
from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

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
from envds.sampling.event import SamplingEvent
from envds.sampling.types import SamplingEventType as sampet
from envds.daq.event import DAQEvent

# Configure structured logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("beacons_msp")
L.setLevel(logging.INFO)

async def _broadcast_setting_request(sampling_system, variable_name, value):
    """
    Looks up the physical hardware routing ID for a variable via the local 
    VariableMap cache, builds a strict CloudEvent, and broadcasts it to the edge.
    """
    try:
        target_device = "unknown"
        target_type = "sensor"
        dest_path = "envds/sensor/settings/request"
        
        for platform_name, vm_dict in sampling_system.variablemaps.get("platform", {}).items():
            for vm_name, time_dict in vm_dict.items():
                for valid_time, vm_obj in time_dict.items():
                    vm_data = vm_obj["variablemap"].get("data", {})
                    for vs_name, vs_def in vm_data.get("variablesets", {}).items():
                        for v_name, v_def in vm_data.get("variables", {}).items():
                            if v_name == variable_name:
                                active_var = vm_obj["variablesets"].get(vs_name, {}).get("variables", {}).get(v_name, {})
                                attrs = active_var.get("attributes", v_def.get("attributes", {}))
                                
                                target_device = attrs.get("source_id", {}).get("data", "unknown")
                                target_type = attrs.get("source_type", {}).get("data", "sensor").lower()
                                src_var = attrs.get("source_variable", {}).get("data", variable_name)
                                break
        
        if target_device == "unknown":
            L.error(f"Cannot broadcast setting for {variable_name}: hardware source_id not found in local variablemaps.")
            return

        if target_type == "controller":
            dest_path = "envds/controller/settings/request"
            ce_obj = DAQEvent.create_controller_settings_request(
                source=f"envds.{sampling_system.config.daq_id}.sampling-operations",
                data={"settings": {src_var: {"requested": value}}},
                extra_header={"controllerid": target_device, "destpath": dest_path}
            )
        else:
            dest_path = "envds/sensor/settings/request"
            ce_obj = DAQEvent.create_sensor_settings_request(
                source=f"envds.{sampling_system.config.daq_id}.sampling-operations",
                data={"settings": {src_var: {"requested": value}}},
                extra_header={"deviceid": target_device, "destpath": dest_path}
            )

        await sampling_system.send_to_mqtt(dest_path, ce_obj)
        L.info(f"Broadcasted network setting request: {src_var}={value} -> {target_device}")

    except Exception as e:
        L.error(f"Failed to broadcast network setting request for {variable_name}", extra={"reason": str(e)})


## this is the model for the deterministic data approach.
def calculate_heater_setpoint(temp, humidity):
    # DEAD-MAN SWITCH: If either sensor is offline, turn off the heater!
    if temp is None or humidity is None:
        return {"heater_setpoint": 0.0}
        
    # Normal operation
    setpoint = temp * 1.5 + humidity
    return {"heater_setpoint": setpoint}

async def set_isokinetic_inlet_flow(self, **kwargs):
    """
    Stubbed to return 50% flow for isokinetic sampling.
    """
    L.info("action_triggered", extra={"action": "set_isokinetic_inlet_flow", "target_val": 50.0})
    await _broadcast_setting_request(self, "inlet_fan_speed_sp", 50.0)
    return {"inlet_fan_speed_sp": 50.0}

async def set_reverse_inlet_flow(self, **kwargs):
    """
    Stubbed to return -80% flow to clear the inlet.
    """
    L.info("action_triggered", extra={"action": "set_reverse_inlet_flow", "target_val": -80.0})
    await _broadcast_setting_request(self, "inlet_fan_speed_sp", -80.0)
    return {"inlet_fan_speed_sp": -80.0}

async def turn_on_main_power(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_main_power", "target_val": 1})
    await _broadcast_setting_request(self, "main_power_state", 1)
    return {"main_power_state": 1}

async def turn_on_physics_power(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_physics_power", "target_val": 1})
    await _broadcast_setting_request(self, "physics_power_state", 1)
    return {"physics_power_state": 1}

async def turn_on_optics_power(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_optics_power", "target_val": 1})
    await _broadcast_setting_request(self, "optics_power_state", 1)
    return {"optics_power_state": 1}

async def turn_on_spare_power(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_spare_power", "target_val": 1})
    await _broadcast_setting_request(self, "spare_power_state", 1)
    return {"spare_power_state": 1}