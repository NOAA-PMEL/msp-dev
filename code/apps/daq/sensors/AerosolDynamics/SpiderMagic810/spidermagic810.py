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

        try:            
            if os.path.exists(self.sensor_definition_file):
                self.logger.debug(f"'{self.sensor_definition_file}' exists.")
            if os.path.isfile(self.sensor_definition_file):
                self.logger.debug(f"'{self.sensor_definition_file}' is a file.")
            if os.path.isdir(self.sensor_definition_file):
                self.logger.debug(f"'{self.sensor_definition_file}' is a directory.")

            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
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

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        iface[propname] = prop

            self.logger.debug("SpiderMagic810.configure", extra={"interfaces": conf["interfaces"]})

        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def["variables"].items():
            requested = setting["attributes"]["default_value"]["data"]
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

        self.logger.debug("configure", extra={"conf": conf, "self.config": self.config})

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            print(e)

        self.logger.debug("iface_map", extra={"map": self.iface_map})

        # Set up the inversion strategy
        inversion_method = "standard" 
        if inversion_method == "aerosol_dynamics":
            self.inversion_routine = AerosolDynamicsInversion(config=conf)
            self.logger.info("Initialized Aerosol Dynamics Inversion")
        else:
            self.inversion_routine = StandardInversion(config=conf)
            self.logger.info("Initialized Standard Inversion")

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(SpiderMagic810, self).handle_interface_data(message)

        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    self.logger.debug("interface_recv_data", extra={"data": message.data})
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def settings_check(self):
        """Intercepts MQTT settings requests and acknowledges them."""
        await super().settings_check()

        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    target_val = self.settings.get_setting(name)
                    self.logger.info(f"MQTT Settings Request: changing {name} to {target_val}")
                    
                    if name == "sampling_state":
                        self.settings.set_setting(name, target_val)
                        
                    elif name == "calibration_routine":
                        self.settings.set_setting(name, target_val)

    async def sampling_monitor(self):
        """State machine controller managing the Spider DMA hardware ramps."""
        start_command = "hvgo\r"
        v1_setting = "v1,5\r" 
        v2_setting = "v2,5000\r"
        start_commands = [v1_setting, v2_setting, start_command]
        stop_command = "stop\r"

        need_start = True
        start_requested = False
        
        await asyncio.sleep(2)

        while True:
            try:
                state = self.settings.get_setting("sampling_state") or "idle"
                
                # Check both framework sampling state AND user setting
                if self.sampling() and state.lower() == "sampling":

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

                            need_start = False
                            start_requested = True
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
                            
                elif state.lower() in ["idle", "maintenance", "error", "calibration"]:
                    # If framework stops OR user sets state to non-sampling (e.g., maintenance)
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        self.logger.info(f"State transitioned to {state}. Halting Spider DMA scans.")
                        await asyncio.sleep(2)
                        
                        self.collecting = False
                        need_start = True  # Reset so it can start again if toggled back to sampling
                        start_requested = False

                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error(f"sampling monitor error: {e}")

            await asyncio.sleep(0.1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                if data:
                    self.collecting = True

                record = self.default_parse(data)
                if not record:
                    continue

                if record:
                    self.collecting = True

                if record and self.sampling():
                    try:
                        scan_length = len(record["variables"]["read_V"]["data"])
                        if scan_length < 10:
                            self.logger.info("Scan data too short, skipping", extra={"actual_length": scan_length})
                            continue
                    except (KeyError, TypeError):
                        self.logger.warning("Missing or invalid 'read_V' in record, skipping scan.")
                        continue

                    vp_rd = record["variables"]["vp_rd"]["data"].strip()
                    self.logger.debug("default_data_loop:reverse", extra={"vp_rd": vp_rd})
                    
                    if vp_rd == "pd" or vp_rd == "nd":
                        for name, variable in self.metadata["variables"].items():
                            if "shape" in variable and variable["shape"] == ["time", "scan_bins"]:
                                if name in record["variables"] and record["variables"][name]["data"]:
                                    self.logger.debug("default_data_loop:reverse", extra={"vname": name})
                                    record["variables"][name]["data"].reverse()

                    try:
                        record = self.inversion_routine.invert(record)
                    except Exception as inv_e:
                        self.logger.error("Inversion routine failed", extra={"error": str(inv_e)})
                        print(f"Inversion error details: {inv_e}")

                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    
                    self.logger.debug("default_data_loop", extra={"destpath": destpath})
                    
                    await self.send_message(event)

            except Exception as e:
                print(f"default_data_loop error: {e}")
                print(traceback.format_exc())
            
            await asyncio.sleep(0.001)

    def check_array_buffer(self, data, array_cond = False):
        self.array_buffer.append(data)
        if array_cond:
            return self.array_buffer
        else:
            return

    def add_to_record(self, record, names, data):
        for name, v in zip(names, data):
            if name in record["variables"]:
                instvar = self.config.metadata.variables[name]
                try:
                    if instvar.type == "int":
                        if isinstance(v, list):
                            record["variables"][name]["data"] = [int(item) for item in v]
                        else:
                            record["variables"][name]["data"] = int(v)

                    elif instvar.type == "float":
                        if isinstance(v, list):
                            record["variables"][name]["data"] = [float(item) for item in v]
                        else:
                            record["variables"][name]["data"] = float(v)
                            
                    else:
                        record["variables"][name]["data"] = v

                except ValueError:
                    if instvar.type == "str" or instvar.type == "char":
                        record["variables"][name]["data"] = ""
                    else:
                        record["variables"][name]["data"] = None
        self.logger.debug("default_parse:add_to_record", extra={"record": record})


    def default_parse(self, data):
        if data:
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

                self.include_metadata = False
                try:
                    self.logger.debug("default_parse", extra={"data.data": data.data["data"]})
                    parts = data.data["data"].strip().split(",")

                    if (datavar := 'v1') in data.data["data"]:
                        parts = parts[2:3] + parts[4:8]
                        parts = [item.replace('Hz', '').replace('tau=', '').strip() for item in parts]
                        self.extra_var_names = V1_NAMES
                        self.extra_vars = parts
                        self.logger.debug("default_parse:v1", extra={"var_name": V1_NAMES, "parts": parts})
                        return None
                    
                    elif (datavar := 'STARTING') in data.data["data"]:
                        parts = parts[1:3] + parts[4:25]
                        parts = [item.replace('V', '').strip() for item in parts]
                        self.extra_var_names += STARTING_NAMES
                        self.extra_vars += parts
                        self.logger.debug("default_parse:STARTING", extra={"var_name": STARTING_NAMES, "parts": parts})
                        return None

                    elif (datavar := 'Vi') in data.data["data"]:
                        parts = parts[0:4]
                        parts = [item.replace('Vi=', '').replace('Vf=', '').replace('Vmax=', '').replace('Tc=', '').strip() for item in parts]
                        elapsed_time = abs(round((math.log(float(parts[1])/float(parts[0])))*float(parts[3]), 2))
                        parts.append(elapsed_time)
                        self.extra_var_names += VI_NAMES
                        self.extra_vars += parts
                        self.logger.debug("default_parse:vi", extra={"var_name": VI_NAMES, "parts": parts})
                        return None
                    
                    elif (datavar := 'START SEQ') in data.data["data"]:
                        if self.include_metadata:
                            variable_types = ["main", "raw_scan", "setting", "calibration"]
                        else:
                            variable_types = ["main", "raw_scan"]
                        self.include_metadata = False
                        self.current_record = self.build_data_record(meta=self.include_metadata, variable_types=variable_types)
                        self.current_record["timestamp"] = data.data["timestamp"]
                        self.current_record["variables"]["time"]["data"] = data.data["timestamp"]

                        self.add_to_record(self.current_record, self.extra_var_names, self.extra_vars)
                        
                        self.sequence_start = True
                        self.array_buffer = [] 
                        self.logger.debug("default_parse:START SEQ", extra={"vdata": data.data["data"]})
                        return None
                    
                    elif (datavar := 'END SEQ') in data.data["data"]:
                        if len(self.array_buffer) > 0:
                            parts = np.array(list(self.array_buffer), dtype=object)
                            transposed = np.transpose(parts).tolist()
                            self.add_to_record(self.current_record, SCAN_NAMES, transposed)
                        
                        self.array_buffer = []
                        self.sequence_end = True
                        self.sequence_start = False
                        self.logger.debug("default_parse:END SEQ", extra={"vdata": data.data["data"]})
                        
                        return self.current_record
                    
                    elif self.sequence_start:
                        if len(parts) != 8:
                            return None
                        
                        self.array_buffer.append(parts)

                    else:
                        return None

                except KeyError:
                    pass
            except Exception as e:
                print(f"default_parse error: {e}")
                
        return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    print("shutting down")
    if sensor:
        await sensor.shutdown()

    for task in task_list:
        if task:
            task.cancel()

async def main(server_config: ServerConfig = None):
    if server_config is None:
        server_config = ServerConfig()
    print(server_config)

    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            try:
                sn = conf["serial_number"]
            except KeyError:
                pass
    except FileNotFoundError:
        pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"Aerosol Dynamics::SpiderMagic810::{sn}")

    logger.debug("Starting SpiderMagic810")
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
        logger.debug("spidermagic.run", extra={"do_run": do_run})
        await asyncio.sleep(1)

    logger.info("starting shutdown...")
    await shutdown(inst)
    logger.info("done.")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, BASE_DIR)
    
    config = ServerConfig()
    try:
        index = sys.argv.index("--host")
        config.host = sys.argv[index + 1]
    except (ValueError, IndexError): pass

    try:
        index = sys.argv.index("--port")
        config.port = int(sys.argv[index + 1])
    except (ValueError, IndexError): pass

    try:
        index = sys.argv.index("--log_level")
        config.log_level = sys.argv[index + 1]
    except (ValueError, IndexError): pass

    asyncio.run(main(config))