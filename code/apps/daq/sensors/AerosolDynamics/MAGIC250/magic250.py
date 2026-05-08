import asyncio
import signal
import sys
import os
import logging
import logging.config
import yaml
import json

from envds.core import envdsLogger
from envds.util.util import time_to_next, get_datetime, get_datetime_string
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class MAGIC250(Sensor):
    def __init__(self, config=None, **kwargs):
        super(MAGIC250, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_data_buffer = asyncio.Queue(maxsize=100)

        # Explicit hardware-to-variable map to prevent alignment errors
        self.csv_map = [
            "magic_timestamp", "concentration", "dew_point", "input_T",
            "input_rh", "cond_T", "init_T", "mod_T", "opt_T", "heatsink_T",
            "case_T", "wick_sensor", "mod_T_sp", "humid_exit_dew_point",
            "abs_pressure", "flow", "log_interval", "corr_live_time",
            "meas_dead_time", "raw_counts", "dthr2_pctl", "status_hex",
            "status_ascii", "magic_serial_number"
        ]

        self.sensor_definition_file = "AerosolDynamics_MAGIC250_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(MAGIC250, self).configure()

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

            self.logger.debug("magic250.configure", extra={"interfaces": conf["interfaces"]})

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

        # Restored block: Extract format_version to ensure build_data_record uses the JSON schema version
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

        self.logger.debug("configure", extra={"conf": conf, "self.config": self.config})

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            print(e)

        self.logger.debug("iface_map", extra={"map": self.iface_map})

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(MAGIC250, self).handle_interface_data(message)

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
                    
                    # 1. Fetch the full setting dictionary
                    setting_obj = self.settings.get_setting(name)
                    
                    # 2. Extract ONLY the requested string/value
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    self.logger.info(f"MQTT Settings Request: changing {name} to {target_val}")
                    
                    if name in ["sampling_state", "calibration_routine", "pump_state", "q_target"]:
                        # 3. Acknowledge health by setting the ACTUAL state to match the REQUESTED state
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        start_command = "Log,1\n"
        stop_command = "Log,0\n"

        need_start = True
        start_requested = False
        
        await asyncio.sleep(2)

        while True:
            try:
                # Fetch the dictionary and extract the requested target state safely
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
                            await self.interface_send_data(data={"data": start_command})
                            need_start = False
                            start_requested = True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            await self.interface_send_data(data={"data": start_command})
                            await asyncio.sleep(2)
                            continue
                elif state_str in ["idle", "maintenance", "error", "calibration"]:
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        self.logger.info(f"State transitioned to {state_str}. Halting MAGIC250 sampling.")
                        await asyncio.sleep(2)
                        self.collecting = False
                        need_start = True
                        start_requested = False

                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"sampling monitor error: {e}")

            await asyncio.sleep(1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.logger.debug("default_data_loop", extra={"data": data})

                record = self.default_parse(data)
                if record:
                    self.collecting = True

                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    self.logger.debug("default_data_loop", extra={"data": event, "destpath": destpath})
                    
                    await self.send_message(event)

            except Exception as e:
                print(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if not data:
            return None

        try:
            # Filter variables to reduce payload size on standard data updates
            if self.include_metadata:
                variable_types = ["main", "setting", "calibration"]
            else:
                variable_types = ["main"]
                
            record = self.build_data_record(meta=self.include_metadata, variable_types=variable_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            
            raw_str = raw_payload.get("data", "")
            parts = raw_str.split(",")
            
            if len(parts) < len(self.csv_map):
                self.logger.warning(f"Incomplete data frame received. Expected {len(self.csv_map)}, got {len(parts)}")
                return None

            for index, name in enumerate(self.csv_map):
                if name in record["variables"]:
                    instvar = self.config.metadata.variables[name]
                    raw_val = parts[index].strip()
                    
                    try:
                        if instvar.type == "int":
                            record["variables"][name]["data"] = int(raw_val)
                        elif instvar.type == "float":
                            record["variables"][name]["data"] = float(raw_val)
                        else:
                            record["variables"][name]["data"] = raw_val
                            
                    except ValueError:
                        if instvar.type in ("str", "string", "char"):
                            record["variables"][name]["data"] = ""
                        else:
                            record["variables"][name]["data"] = None

            return record

        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
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
    logger = logging.getLogger(f"AerosolDynamics::MAGIC250::{sn}")

    logger.debug("Starting MAGIC250")
    inst = MAGIC250()
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
        logger.debug("magic250.run", extra={"do_run": do_run})
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