import asyncio
import math
import signal
import sys
import os
import logging
import traceback
import logging.config
import yaml
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
        self.array_buffer = []
        self.sequence_start = False
        self.sequence_end = False
        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "AerosolDynamics_SPIDERMAGIC810_sensor_definition.json"
        
        self.cal_active = False
        self.current_valve_state = None
        self.collecting = False

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        super(SpiderMagic810, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

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

        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

        inversion_method = "standard" 
        self.inversion_routine = StandardInversion(config=conf)

    async def settings_check(self):
        """Fix: Uses set_actual to prevent recursion depth error."""
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        start_commands = ["v1,5\r", "v2,5000\r", "hvgo\r"]
        stop_command = "stop\r"
        need_start, start_requested = True, False
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()
                if self.sampling() and state_str == "sampling":
                    if need_start:
                        if self.collecting:
                            await self.interface_send_data(data={"data": stop_command})
                            await asyncio.sleep(2)
                            self.collecting = False
                        else:
                            for cmd in start_commands:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            need_start, start_requested = False, True
                            await asyncio.sleep(2)
                    elif start_requested:
                        if self.collecting: start_requested = False
                        else:
                            for cmd in start_commands:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            await asyncio.sleep(2)
                elif state_str in ["idle", "maintenance"]:
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        await asyncio.sleep(2)
                        self.collecting, need_start, start_requested = False, True, False
                await asyncio.sleep(0.1)
            except Exception: await asyncio.sleep(0.1)

    def default_parse(self, data):
        """Fix: Uses variable_types to optimize payload size."""
        if not data: return None
        try:
            v_types = ["main", "raw_scan", "setting", "calibration"] if self.include_metadata else ["main", "raw_scan"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            raw_data = data.data.get("data", "")
            if 'START SEQ' in raw_data:
                record["timestamp"] = data.data["timestamp"]
                record["variables"]["time"]["data"] = data.data["timestamp"]
                # (Add record assembly logic here)
                return None
            return None
        except Exception: return None

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                if record and self.sampling():
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception: pass
            await asyncio.sleep(0.001)

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor: await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    inst = SpiderMagic810()
    inst.run()
    await asyncio.sleep(2)
    inst.start()

    event_loop = asyncio.get_event_loop()
    global do_run
    do_run = True

    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        await asyncio.sleep(1)
    await shutdown(inst)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    asyncio.run(main(ServerConfig()))