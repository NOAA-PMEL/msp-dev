import asyncio
import math
import signal
import sys
import os
import logging
import traceback
import logging.config
import yaml
import random
import json
import numpy as np

from envds.core import envdsLogger
from envds.util.util import time_to_next, get_datetime, get_datetime_string
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

from inversion import StandardInversion, AerosolDynamicsInversion

task_list = []

class SpiderMagic810(Sensor):
    def __init__(self, config=None, **kwargs):
        super(SpiderMagic810, self).__init__(config=config, **kwargs)
        self.data_rate = 1
        self.first_record = 'HDT'
        self.last_record = 'CHANGE'
        self.array_buffer = []
        self.sequence_start = False
        self.sequence_end = False
        self.seq_counter = 0

        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "AerosolDynamics_SPIDERMAGIC810_sensor_definition.json"
        
        # State Tracking
        self.cal_active = False
        self.current_valve_state = None
        self.collecting = False

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        """Restores explicit version tracking from JSON metadata."""
        super(SpiderMagic810, self).configure()

        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {"baudrate": 115200, "bytesize": 8, "parity": "N", "stopbit": 1},
                    "read-properties": {"read-method": "readline", "decode-errors": "strict", "send-method": "ascii"},
                }
            }
        }

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        iface[propname] = prop

        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def["variables"].items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            settings=settings_def["variables"]
        )

        self.config = DeviceConfig(
            make=self.metadata["attributes"]["make"]["data"],
            model=self.metadata["attributes"]["model"]["data"],
            serial_number=conf.get("serial_number", "UNKNOWN"),
            metadata=meta,
            interfaces=conf.get("interfaces", {}),
            daq_id=conf.get("daq_id", "default"),
        )

        # Restore format_version override
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            self.logger.error(f"Interface config error: {e}")

        # Inversion strategy
        inversion_method = "standard" 
        if inversion_method == "aerosol_dynamics":
            self.inversion_routine = AerosolDynamicsInversion(config=conf)
        else:
            self.inversion_routine = StandardInversion(config=conf)

    async def set_ext_valve(self, valve_state):
        """Processes the external 3-way valve state change request."""
        self.logger.info(f"External 3-way valve commanded to: {valve_state}")
        self.current_valve_state = valve_state

    async def settings_check(self):
        """Intercepts MQTT settings requests using set_actual to prevent infinite recursion."""
        await super().settings_check()

        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    
                    setting_obj = self.settings.get_setting(name)
                    # Safely extract requested value from framework dictionary
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    self.logger.info(f"MQTT Settings Request: changing {name} to {target_val}")
                    
                    if name == "ext_valve_state":
                        await self.set_ext_valve(target_val)
                        self.settings.set_actual(name, target_val)
                    elif name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """State machine controller managing the Spider DMA hardware ramps."""
        start_commands = ["v1,5\r", "v2,5000\r", "hvgo\r"]
        stop_command = "stop\r"

        need_start = True
        start_requested = False
        await asyncio.sleep(2)

        while True:
            try:
                # Safely extract sampling state string to prevent dictionary attribute errors
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()
                
                if self.sampling() and state_str == "sampling":
                    if need_start:
                        if self.collecting:
                            await self.interface_send_data(data={"data": stop_command})
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            for cmd in start_commands:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            need_start, start_requested = False, True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            for cmd in start_commands:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            await asyncio.sleep(2)
                            continue
                            
                elif state_str in ["idle", "maintenance", "error", "calibration"]:
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        self.logger.info(f"State transitioned to {state_str}. Halting Spider DMA scans.")
                        await asyncio.sleep(2)
                        self.collecting, need_start, start_requested = False, True, False

                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error(f"sampling monitor error: {e}")
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        """Optimized parser using variable_types to reduce payload size."""
        if not data: return None
        try:
            V1_NAMES = ["tau", "HV_polarity", "scan_dir", "data_freq", "HV_status"]
            STARTING_NAMES = ["vp_rd", "spidermagic_timestamp", "dew_point", "input_T", "input_rh", 
                              "cond_T", "init_T", "mod_T", "opt_T", "heatsink_T", "case_T", 
                              "wick_sensor", "mod_T_sp", "humid_exit_dew_point", "wadc", "DMA_V", 
                              "Qsh", "abs_pressure", "flow", "pHt2.%", "status_hex", "status_ascii", 
                              "spidermagic_serial_number"]
            VI_NAMES = ["Vi", "Vf", "Vmax", "Tc", "elap_time"]
            SCAN_NAMES = ["scan_status", "set_V", "read_V", "concentration", 
                          "raw_counts", "dead_counts", "sh_flow", "aer_flow"]

            raw_data = data.data.get("data", "")
            parts = raw_data.strip().split(",")

            if 'v1' in raw_data:
                parts = [item.replace('Hz', '').replace('tau=', '').strip() for item in (parts[2:3] + parts[4:8])]
                self.extra_var_names, self.extra_vars = V1_NAMES, parts
                return None
            
            elif 'STARTING' in raw_data:
                parts = [item.replace('V', '').strip() for item in (parts[1:3] + parts[4:25])]
                self.extra_var_names += STARTING_NAMES
                self.extra_vars += parts
                return None

            elif 'Vi' in raw_data:
                p = [item.replace('Vi=', '').replace('Vf=', '').replace('Vmax=', '').replace('Tc=', '').strip() for item in parts[0:4]]
                elapsed_time = abs(round((math.log(float(p[1])/float(p[0])))*float(p[3]), 2))
                p.append(elapsed_time)
                self.extra_var_names += VI_NAMES
                self.extra_vars += p
                return None
            
            elif 'START SEQ' in raw_data:
                # Restricted variable types for high-frequency updates
                v_types = ["main", "raw_scan", "setting", "calibration"] if self.include_metadata else ["main", "raw_scan"]
                self.current_record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
                self.include_metadata = False
                self.current_record["timestamp"] = data.data["timestamp"]
                self.current_record["variables"]["time"]["data"] = data.data["timestamp"]
                self.add_to_record(self.current_record, self.extra_var_names, self.extra_vars)
                self.sequence_start, self.array_buffer = True, [] 
                return None
            
            elif 'END SEQ' in raw_data:
                if len(self.array_buffer) > 0:
                    transposed = np.transpose(np.array(list(self.array_buffer), dtype=object)).tolist()
                    self.add_to_record(self.current_record, SCAN_NAMES, transposed)
                self.array_buffer, self.sequence_end, self.sequence_start = [], True, False
                return self.current_record
            
            elif self.sequence_start:
                if len(parts) == 8: self.array_buffer.append(parts)

        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
        return None

    def add_to_record(self, record, names, data):
        for name, v in zip(names, data):
            if name in record["variables"]:
                instvar = self.config.metadata.variables[name]
                try:
                    if instvar.type == "int":
                        record["variables"][name]["data"] = [int(item) for item in v] if isinstance(v, list) else int(v)
                    elif instvar.type == "float":
                        record["variables"][name]["data"] = [float(item) for item in v] if isinstance(v, list) else float(v)
                    else:
                        record["variables"][name]["data"] = v
                except ValueError:
                    record["variables"][name]["data"] = "" if instvar.type in ("str", "char") else None

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                if record and self.sampling():
                    if len(record["variables"].get("read_V", {}).get("data", [])) < 10: continue
                    vp_rd = record["variables"].get("vp_rd", {}).get("data", "").strip()
                    if vp_rd in ["pd", "nd"]:
                        for name, var in self.metadata["variables"].items():
                            if "shape" in var and var["shape"] == ["time", "scan_bins"]:
                                if name in record["variables"] and record["variables"][name]["data"]:
                                    record["variables"][name]["data"].reverse()
                    record = self.inversion_routine.invert(record)
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
            await asyncio.sleep(0.001)

if __name__ == "__main__":
    envdsLogger(level=logging.DEBUG).init_logger()
    inst = SpiderMagic810()
    inst.run()
    asyncio.run(asyncio.sleep(2))
    inst.start()