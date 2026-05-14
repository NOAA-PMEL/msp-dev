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

import math
import traceback
from inversion import StandardInversion

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

                # Initialize inversion routine with the static grid
                target_diameters = self.metadata["variables"]["diameter"]["data"]
                self.inversion_routine = StandardInversion(target_grid=target_diameters)

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

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

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

    async def handle_interface_data(self, message: CloudEvent):
        await super(SpiderMagic810, self).handle_interface_data(message)

        if message.get("type") == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces.get("default", {}).get("path")
                
                if path_id == iface_path or path_id == "default":
                    self.logger.debug(
                        "interface_recv_hit", 
                        extra={"route_path": path_id, "payload": message.data}
                    )
                    await self.default_data_buffer.put(message)
            except KeyError as e:
                self.logger.error("interface_route_error", extra={"missing_key": str(e)})

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
        # cmds = [
        #     # "stop\r", 
        #     # "sm,1\r", 
        #     # "msg,1\r\n", 
        #     # f"st,{scan_time}\r\n", 
        #     "v1,5\r", 
        #     "v2,5000\r", 
        #     "hvgo\r"
        # ]
        # Added \n to flush network buffers...
        cmds = [
            "stop\r\n", 
            "scan, 5, 5000, 4, 0\r\n", 
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

    # async def default_data_loop(self):
    #     while True:
    #         try:
    #             data = await self.default_data_buffer.get()
    #             record = self.default_parse(data)
    #             if record and self.sampling():
    #                 event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
    #                 await self.send_message(event)
    #         except Exception:
    #             pass
    #         await asyncio.sleep(0.1)

    async def default_data_loop(self):
        while True:
            try:
                data_msg = await self.default_data_buffer.get()
                
                if data_msg:
                    self.collecting = True
                    self.logger.debug("buffer_pulled", extra={"payload_type": str(type(data_msg))})

                record = self.default_parse(data_msg)
                
                if not record:
                    continue

                if record and self.sampling():
                    self.logger.debug("scan_complete_starting_inversion", extra={"rec_ts": record.get("timestamp")})
                    
                    try:
                        record = self.inversion_routine.invert(record)
                        self.logger.debug("inversion_complete", extra={"status": "success"})
                    except Exception as e:
                        self.logger.error("inversion_runtime_error", extra={"err_detail": str(e), "trace": traceback.format_exc()})

                    # Handle reverse scans natively
                    vp_rd = record["variables"]["vp_rd"]["data"].strip()
                    self.logger.debug("direction_check", extra={"vp_rd_val": vp_rd})
                    if vp_rd == "pd" or vp_rd == "nd":
                        for name, variable in self.metadata["variables"].items():
                            if name in ["time", "diameter", "dN", "dlogDp", "dNdlogDp", "intN"]:
                                continue
                            if variable["shape"] == ["time", "scan_bins"]: 
                                if isinstance(record["variables"][name]["data"], list):
                                    record["variables"][name]["data"].reverse()

                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    
                    self.logger.debug("publishing_record", extra={"dest_topic": destpath})
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("data_loop_error", extra={"err_detail": str(e), "trace": traceback.format_exc()})
            await asyncio.sleep(0.001)

    # def default_parse(self, data):
    #     if not data: 
    #         return None

    #     try:
    #         raw_payload = data.data if isinstance(data.data, dict) else {}
    #         raw = raw_payload.get("data", "")
            
    #         if 'START SEQ' in raw:
    #             v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
    #             self.current_record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
    #             self.include_metadata = False
                
    #             self.current_record["timestamp"] = raw_payload.get("timestamp")
    #             if "time" in self.current_record.get("variables", {}):
    #                 self.current_record["variables"]["time"]["data"] = raw_payload.get("timestamp")
                    
    #             self.sequence_start = True
    #             return None
                
    #         elif 'END SEQ' in raw:
    #             self.sequence_start = False
    #             return self.current_record
                
    #         return None
    #     except Exception:
    #         return None

    def check_array_buffer(self, parts, array_cond=False):
        try:
            if not array_cond:
                parts = [float(item) for item in parts]
                self.array_buffer.append(parts)
            else:
                self.array_buffer.append(parts)
        except ValueError:
            pass

    def default_parse(self, data):
        if data:
            try:
                variables = list(self.config.metadata.variables.keys())
                if "time" in variables:
                    variables.remove("time")

                self.include_metadata = False
                try:
                    raw_str = data.data["data"]
                    self.logger.debug("parsing_incoming", extra={"raw_string": raw_str.strip()})
                    
                    parts = raw_str.strip().split(",")

                    if 'v1' in raw_str:
                        parts = parts[2:3] + parts[4:8]
                        parts = [item.replace('Hz', '').replace('tau=', '').strip() for item in parts]
                        self.var_name = variables[0:5]
                        self.extra_var_names = variables[0:5]
                        self.extra_vars = parts
                        self.logger.debug("parse_state_v1", extra={"parsed_parts": parts})
                        return None
                    
                    elif 'STARTING' in raw_str:
                        parts = parts[1:3] + parts[4:25]
                        parts = [item.replace('V', '').strip() for item in parts]
                        self.var_name = variables[5:28]
                        self.extra_var_names += variables[5:28]
                        self.extra_vars += parts
                        self.logger.debug("parse_state_STARTING", extra={"parsed_parts": parts})
                        return None

                    elif 'Vi' in raw_str:
                        parts = parts[0:4]
                        parts = [item.replace('Vi=', '').replace('Vf=', '').replace('Vmax=', '').replace('Tc=', '').strip() for item in parts]
                        elapsed_time = abs(round((math.log(float(parts[1])/float(parts[0])))*float(parts[3]), 2))
                        parts.append(elapsed_time)
                        self.var_name = variables[28:33]
                        self.extra_var_names += variables[28:33]
                        self.extra_vars += parts
                        self.logger.debug("parse_state_Vi", extra={"parsed_parts": parts})
                        return None
                    
                    elif 'START SEQ' in raw_str:
                        self.current_record = self.build_data_record(meta=self.include_metadata)
                        self.current_record["timestamp"] = data.data["timestamp"]
                        self.current_record["variables"]["time"]["data"] = data.data["timestamp"]

                        # map extra scalar vars to the record
                        for index, name in enumerate(self.extra_var_names):
                            if name in self.current_record["variables"]:
                                instvar = self.metadata["variables"][name]
                                vartype = instvar["type"]
                                if vartype == "string": vartype = "str"
                                try:
                                    self.current_record["variables"][name]["data"] = eval(vartype)(self.extra_vars[index])
                                except (ValueError, TypeError, IndexError):
                                    self.current_record["variables"][name]["data"] = None

                        self.sequence_start = True
                        self.seq_counter = 0
                        self.logger.debug("parse_state_START_SEQ", extra={"status": "Building new record"})
                        return None
                    
                    elif 'END SEQ' in raw_str:
                        self.scan_var_names = variables[33:41]
                        
                        # Transpose the accumulated array buffer into columns
                        if len(self.array_buffer) > 0:
                            parts = np.array(list(self.array_buffer), dtype=object)
                            transposed = np.transpose(parts).tolist()
                            
                            for index, name in enumerate(self.scan_var_names):
                                if name in self.current_record["variables"]:
                                    self.current_record["variables"][name]["data"] = transposed[index]
                                    
                        self.array_buffer = []
                        self.sequence_end = True
                        self.sequence_start = False
                        self.logger.debug("parse_state_END_SEQ", extra={"status": "Sequence complete"})
                        return self.current_record
                    
                    elif self.sequence_start:
                        self.var_name = variables[33:41]
                        if len(parts) != 8:
                            return None
                        # Middle of the sequence: append row to buffer
                        self.check_array_buffer(parts, array_cond=False)

                    else:
                        return None

                except KeyError:
                    pass
            except Exception as e:
                self.logger.error("default_parse_error", extra={"err_detail": str(e), "trace": traceback.format_exc()})
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