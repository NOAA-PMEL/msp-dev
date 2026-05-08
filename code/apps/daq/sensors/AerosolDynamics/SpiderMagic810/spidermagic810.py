import asyncio
import signal
import sys
import os
import logging
import yaml
import json
import numpy as np
from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class SpiderMagic810(Sensor):
    def __init__(self, config=None, **kwargs):
        super(SpiderMagic810, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue()
        self.array_buffer = []
        self.sequence_start = False
        self.sensor_definition_file = "AerosolDynamics_SPIDERMAGIC810_sensor_definition.json"
        
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

        # Aerosol Dynamics Standard Serial Parameters
        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 115200,
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readline",
                        "decode-errors": "strict",
                        "send-method": "ascii",
                    },
                }
            }
        }

        # Inject properties into conf interfaces before creating DeviceConfig
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        iface[propname] = prop
            
        # 2. NEW: Initialize settings from definition file
        # This extracts 'sampling_state' and 'calibration_routine' defaults
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def.get("variables", {}).items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            # Allow overrides from sensor.conf if present
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"], 
            dimensions=self.metadata["dimensions"], 
            variables=self.metadata["variables"], 
            settings=dict()
        )
        
        self.config = DeviceConfig(
            make=self.metadata["attributes"]["make"]["data"], 
            model=self.metadata["attributes"]["model"]["data"], 
            serial_number=conf.get("serial_number", "UNKNOWN"), 
            metadata=meta, 
            interfaces=conf.get("interfaces", {}), 
            daq_id=conf.get("daq_id", "default")
        )

        # Fix version indexing
        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        # CRITICAL: add_interface MUST be called for each interface in conf
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items(): 
                self.add_interface(name, iface)

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    # async def sampling_monitor(self):
    #     cmds = ["v1,5\r", "v2,5000\r", "hvgo\r"]
    #     need_start = True
    #     await asyncio.sleep(2)
        
    #     while True:
    #         try:
    #             state_obj = self.settings.get_setting("sampling_state")
    #             state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
    #             state_str = str(state).lower()

    #             if self.sampling() and state_str == "sampling":
    #                 if need_start:
    #                     for c in cmds: 
    #                         await self.interface_send_data(data={"data": c})
    #                         await asyncio.sleep(0.5)
    #                     need_start = False
    #             else:
    #                 if not need_start:
    #                     await self.interface_send_data(data={"data": "stop\r"})
    #                     need_start = True
    #         except Exception:
    #             pass
    #         await asyncio.sleep(1)

    async def sampling_monitor(self):
        # Added \n to flush network buffers, msg,1 for verbose output, and st for scan time
        scan_time = self.sampling_frequency if hasattr(self, 'sampling_frequency') else 30
        cmds = [
            "stop\r\n", 
            "sm,1\r\n", 
            "msg,1\r\n", 
            f"st,{scan_time}\r\n", 
            "v1,5\r\n", 
            "v2,5000\r\n", 
            "hvgo\r\n"
        ]
        need_start = True
        await asyncio.sleep(2)
        
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        for c in cmds: 
                            await self.interface_send_data(data={"data": c})
                            # Increased delay to 1.0s to prevent overwhelming the ADI board
                            await asyncio.sleep(1.0)
                        need_start = False
                else:
                    if not need_start:
                        await self.interface_send_data(data={"data": "stop\r\n"})
                        need_start = True
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                if record and self.sampling():
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    await self.send_message(event)
            except Exception:
                pass
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if not data: 
            return None
            
        try:
            raw_payload = data.data if isinstance(data.data, dict) else {}
            raw = raw_payload.get("data", "")
            
            if 'START SEQ' in raw:
                v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
                self.current_record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
                self.include_metadata = False
                
                self.current_record["timestamp"] = raw_payload.get("timestamp")
                if "time" in self.current_record.get("variables", {}):
                    self.current_record["variables"]["time"]["data"] = raw_payload.get("timestamp")
                    
                self.sequence_start = True
                return None
                
            elif 'END SEQ' in raw:
                self.sequence_start = False
                return self.current_record
                
            return None
        except Exception:
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    if server_config is None:
        server_config = ServerConfig()

    # FIX: Pull SN for the logger
    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "9999")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"AerosolDynamics::SPIDERMAGIC810::{sn}")
    
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