import asyncio
import signal
import traceback
import sys
import os
import logging
import json
import logging.config
import yaml
import random

from envds.core import envdsLogger
from envds.util.util import time_to_next, get_datetime, get_datetime_string
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel
from datetime import datetime

task_list = []

class APS3321(Sensor):
    def __init__(self, config=None, **kwargs):
        super(APS3321, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        
        self.default_data_buffer = asyncio.Queue(maxsize=1000)
        self.record_counter = 0
        self.var_name = None
        self.first_record = 'C,0,C'
        self.last_record = ',Y,'
        self.array_buffer = []
        self.C_counter = 0
        self.S_counter = 0
        self.dlogDp =  0.0337
        self.diams = [
            0.50468, 0.54215, 0.58166, 0.62506, 0.67305, 0.72353, 0.7775,
            0.83546, 0.89791, 0.96488, 1.0368, 1.1143, 1.1972, 1.2867, 1.3826, 
            1.4855, 1.5965, 1.7154, 1.8433, 1.9812, 2.1291, 2.2875, 2.4579, 
            2.6413, 2.8387, 3.0505, 3.2779, 3.5227, 3.7856, 4.0679, 4.3717, 
            4.698, 5.0482, 5.4245, 5.8292, 6.2644, 6.7317, 7.2338, 7.7735, 
            8.3536, 8.9772, 9.6468, 10.366, 11.14, 11.971, 12.864, 13.824,
            14.855, 15.963, 17.154, 18.435, 19.81
        ]

        self.sensor_definition_file = "TSI_APS3321_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False
        self.sampling_frequency = 30 # default sampling freq in seconds

    def configure(self):
        super(APS3321, self).configure()

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
                        "baudrate": 38400,
                        "bytesize": 7,
                        "parity": "E",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readuntil",
                        "read-terminator": "\r",
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

            self.logger.debug("APS3321.configure", extra={"interfaces": conf["interfaces"]})

        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def.get("variables", {}).items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]

            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            settings=settings_def.get("variables", {})
        )

        self.config = DeviceConfig(
            make=self.metadata["attributes"]["make"]["data"],
            model=self.metadata["attributes"]["model"]["data"],
            serial_number=conf.get("serial_number", "UNKNOWN"),
            metadata=meta,
            interfaces=conf.get("interfaces", {}),
            daq_id=conf.get("daq_id", "default"),
        )

        self.sampling_frequency = 30
        if "sampling_frequency_sec" in conf:
            self.sampling_frequency = conf["sampling_frequency_sec"]

        self.logger.debug("configure", extra={"conf": conf, "self.config": self.config})

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            print(e)

        self.logger.debug("iface_map", extra={"map": self.iface_map})

    async def handle_interface_data(self, message: CloudEvent):
        await super(APS3321, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    self.logger.debug("interface_recv_data", extra={"data": message.data})
                    await self.default_data_buffer.put(message)
            except KeyError: pass

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

    async def sampling_monitor(self):
        stop_command = 'S0\r'
        need_start = True
        start_requested = False

        await asyncio.sleep(2)

        while True:
            try:
                state = self.settings.get_setting("sampling_state") or "idle"

                if self.sampling() and state.lower() == "sampling":
                    if need_start:
                        if self.collecting:
                            await self.interface_send_data(data={"data": stop_command})
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            # Construct start commands to initiate records
                            start_commands = [
                                'S0\r', 
                                f'SMT2,{self.sampling_frequency}\r', 
                                'UC1\r', 'UD1\r', 'US1\r', 'UY1\r', 'U1\r', 'S1\r'
                            ]
                            for start_command in start_commands:
                                self.logger.debug("sampling_monitor", extra={"start_command": start_command})
                                await self.interface_send_data(data={"data": start_command})
                                await asyncio.sleep(.5)

                            need_start = False
                            start_requested = True
                            await asyncio.sleep(2)
                            continue
                            
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            # Resend start commands just in case
                            start_commands = [
                                'S0\r', 
                                f'SMT2,{self.sampling_frequency}\r', 
                                'UC1\r', 'UD1\r', 'US1\r', 'UY1\r', 'U1\r', 'S1\r'
                            ]
                            for start_command in start_commands:
                                await self.interface_send_data(data={"data": start_command})
                                await asyncio.sleep(.5)
                            await asyncio.sleep(2)
                            continue
                            
                elif state.lower() in ["idle", "maintenance", "error", "calibration"]:
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        self.logger.info(f"State transitioned to {state}. Halting APS sampling.")
                        await asyncio.sleep(2)
                        self.collecting = False
                        need_start = True
                        start_requested = False

                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"sampling monitor error: {e}")
                await asyncio.sleep(1)

    async def default_data_loop(self):
        record_buffer = None  

        while True:
            try:
                data = await self.default_data_buffer.get()
                if data:
                    self.collecting = True
                    
                raw_data = data.data if isinstance(data.data, dict) else {}
                raw_str = raw_data.get('data', '')

                # Start of a new aggregate record
                if self.first_record in raw_str:
                    record_buffer = self.default_parse(data)
                    self.record_counter += 1
                    continue

                # Discard fragments if we don't have a starting record
                if record_buffer is None:
                    continue

                # Last record block
                if self.last_record in raw_str:
                    record2 = self.default_parse(data)
                    if record2:
                        for var in record2["variables"]:
                            if var != 'time' and record2["variables"][var]["data"]:
                                record_buffer["variables"][var]["data"] = record2["variables"][var]["data"]
                    
                    record_buffer["variables"]["diameter"]["data"] = self.diams
                    record_buffer["variables"]["channel"]["data"] = list(range(1, 65))
                    record_buffer["variables"]["dN"]["data"] = [None]*52
                    record_buffer["variables"]["dlogDp"]["data"] = [self.dlogDp]*52
                    record_buffer["variables"]["dNdlogDp"]["data"] = [None]*52
                    record_buffer["variables"]["intN"]["data"] = None

                    # Do the math and trigger the event
                    if self.sampling():
                        dN, dNdlogDp, intN = [], [], 0
                        sample_flow_lpm = record_buffer["variables"]["sflow"]["data"] or 1.0 # fallback
                        sample_flow_ccs = sample_flow_lpm * (1000./60.)
                        
                        counts = record_buffer["variables"].get("particle_counts", {}).get("data")
                        if counts:
                            for cnt in counts:
                                conc = cnt / (sample_flow_ccs * self.sampling_frequency)
                                intN += conc
                                dN.append(round(conc, 3))
                                dNdlogDp.append(round(conc / self.dlogDp, 3))

                        record_buffer["variables"]["intN"]["data"] = intN
                        record_buffer["variables"]["dN"]["data"] = dN
                        record_buffer["variables"]["dNdlogDp"]["data"] = dNdlogDp

                        event = DAQEvent.create_data_update(
                            source=self.get_id_as_source(),
                            data=record_buffer,
                        )
                        event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                        await self.send_message(event)

                    record_buffer = None # Reset for next cycle
                    continue

                # Intermediate fragments
                record2 = self.default_parse(data)
                if record2:
                    for var in record2["variables"]:
                        if var != 'time' and record2["variables"][var]["data"]:
                            record_buffer["variables"][var]["data"] = record2["variables"][var]["data"]

            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
                record_buffer = None # Prevent corruption on error

    def check_array_buffer(self, data, array_cond = False):
        self.array_buffer.append(data)
        if array_cond:
            return self.array_buffer
        else:
            return

    def default_parse(self, data):
        if not data: return None
        try:
            record = self.build_data_record(meta=self.include_metadata)
            self.include_metadata = False
            
            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            raw_str = raw_payload.get("data", "")
            
            self.var_name = None 
            compiled_record = None

            if ',C,' in raw_str:
                if ',C,0' in raw_str:
                    parts = raw_str.strip().split(",")[5:]
                    self.var_name = ['ffff', 'stime', 'dtime', 'evt1', 'evt3', 'evt4', 'total']
                    compiled_record = parts
                    self.C_counter = 0
                    self.array_buffer = [] 
                else:
                    parts = raw_str.strip().split(",")[3:]
                    parts = [int(x) if x else 0 for x in parts]
                    parts.extend([0] * (64 - len(parts)))
                    self.var_name = ['particle_counts_accum']

                    if self.C_counter < 51:
                        self.check_array_buffer(parts, array_cond=False)
                        self.C_counter += 1
                        return None 
                    else:
                        compiled_record = self.check_array_buffer(parts, array_cond=True)
                        self.array_buffer = []
                        self.C_counter = 0

            elif ',D,' in raw_str:
                parts = raw_str.strip().split(",")[11:]
                parts = [int(x) if x else 0 for x in parts]
                parts.extend([0] * (52 - len(parts)))
                self.var_name = ['particle_counts']
                compiled_record = parts

            elif ',S,' in raw_str:
                if ',S,C' in raw_str:
                    self.S_counter = 0
                    self.array_buffer = [] 
                    return None
                else:
                    parts = raw_str.strip().split(",")[3:]
                    parts = [int(x) if x else 0 for x in parts]
                    parts.extend([0] * (52 - len(parts)))
                    self.var_name = ['particle_counts_ss']

                    if self.S_counter < 63:
                        self.check_array_buffer(parts, array_cond=False)
                        self.S_counter += 1
                        return None
                    else:
                        compiled_record = self.check_array_buffer(parts, array_cond=True)
                        self.array_buffer = []
                        self.S_counter = 0
            
            elif ',Y,' in raw_str:
                parts = raw_str.strip().split(",")[2:]
                del parts[3:8] # Remove voltages
                self.var_name = ['bpress', 'tflow', 'sflow', 'lpower', 'lcur', 'spumpv', 'tpumpv', 'itemp', 'btemp', 'dtemp', 'Vop']
                compiled_record = parts

            # If no NMEA matches, bail out before causing a TypeError
            if not self.var_name or compiled_record is None:
                return None

            # Assignment logic
            for index, name in enumerate(self.var_name):
                if name in record["variables"]:
                    instvar = self.config.metadata.variables[name]
                    try:
                        if len(self.var_name) == 1:
                            record["variables"][name]["data"] = compiled_record
                        else:
                            val = compiled_record[index]
                            if instvar.type == "int":
                                record["variables"][name]["data"] = int(val)
                            elif instvar.type == "float":
                                record["variables"][name]["data"] = float(val)
                            else:
                                record["variables"][name]["data"] = val
                    except ValueError:
                        record["variables"][name]["data"] = "" if instvar.type in ("str", "char") else None

            return record
            
        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
            return None

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
    logger = logging.getLogger(f"TSI::APS3321::{sn}")

    logger.debug("Starting APS3321")
    inst = APS3321()
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

    logger.info("starting shutdown...")
    await shutdown(inst)
    logger.info("done.")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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