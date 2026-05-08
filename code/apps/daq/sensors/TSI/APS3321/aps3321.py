import asyncio
import signal
import sys
import os
import logging
import yaml
import json
from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class APS3321(Sensor):
    def __init__(self, config=None, **kwargs):
        super(APS3321, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        self.default_data_buffer = asyncio.Queue(maxsize=100)

        self.sensor_definition_file = "TSI_APS3321_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(APS3321, self).configure()

        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        # APS 3321 Required Serial Parameters per Manual Appendix C
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
            daq_id=conf.get("daq_id", "default"),
        )

        # Set device format version properly
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_data(self, message: CloudEvent):
        await super(APS3321, self).handle_interface_data(message)

        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def settings_check(self):
        """FIX 1: Uses set_actual to prevent recursion depth error"""
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """FIX 2: Safely extract state dictionary payload"""
        start_commands = ["S0\r", "U1\r", "UD1\r", "UY1\r", "S1\r"]
        stop_command = "S0\r"

        need_start = True
        await asyncio.sleep(2)

        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        for cmd in start_commands:
                            await self.interface_send_data(data={"data": cmd})
                            await asyncio.sleep(0.5)
                        need_start = False
                        self.collecting = True
                else:
                    if not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        self.collecting = False
                        need_start = True
            except Exception:
                pass
            await asyncio.sleep(1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.logger.debug("default_data_loop - Data received from buffer")
                
                record = self.default_parse(data)

                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    self.logger.debug("Publishing DAQ data event", extra={"destpath": str(event["destpath"])})
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        """FIX 3: variable_types filtering to reduce payload"""
        if not data:
            return None

        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            
            raw_str = raw_payload.get("data", "")
            parts = raw_str.strip().split(",")
            
            # Map Aerodynamic (D) and Auxiliary (Y) records
            if ',D,' in raw_str:
                if "particle_counts" in record["variables"]:
                    record["variables"]["particle_counts"]["data"] = [int(x) if x else 0 for x in parts[11:]]
            
            elif ',Y,' in raw_str:
                if len(parts) >= 18:
                    if "bpress" in record["variables"]: record["variables"]["bpress"]["data"] = float(parts[2])
                    if "tflow" in record["variables"]: record["variables"]["tflow"]["data"] = float(parts[3])
                    if "sflow" in record["variables"]: record["variables"]["sflow"]["data"] = float(parts[4])
                    if "itemp" in record["variables"]: record["variables"]["itemp"]["data"] = float(parts[14])

            return record

        except Exception as e:
            self.logger.error("default_parse error", extra={"error": str(e)})
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

    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "9999")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"TSI::APS3321::{sn}")

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

    await shutdown(inst)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    asyncio.run(main(ServerConfig()))