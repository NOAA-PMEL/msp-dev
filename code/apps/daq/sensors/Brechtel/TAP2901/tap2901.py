import asyncio
import signal
import sys
import os
import logging
import json
import yaml

from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

class TAP2901(Sensor):
    def __init__(self, config=None, **kwargs):
        super(TAP2901, self).__init__(config=config, **kwargs)
        self.data_rate = 1
        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "Brechtel_TAP2901_sensor_definition.json"
        
        # State Tracking
        self.white_filter_mode = False
        self.current_reported_spot = 1
        self.wl_idx = {"red": 0, "green": 1, "blue": 2} 

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
        super(TAP2901, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {"baudrate": 19200, "bytesize": 8, "parity": "N", "stopbit": 1},
                    "read-properties": {"read-method": "readline", "decode-errors": "strict", "send-method": "ascii"},
                }
            }
        }

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    iface.update(sensor_iface_properties[name])

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

        # Restore format_version override
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def settings_check(self):
        """Fix: Uses set_actual to prevent recursion depth error."""
        await super().settings_check()
        
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    
                    setting_obj = self.settings.get_setting(name)
                    # Safe extraction from framework dict
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    self.logger.info(f"MQTT Settings Request: changing {name} to {target_val}")
                    
                    if name == "sampling_state":
                        self.settings.set_actual(name, target_val)
                        
                    elif name == "calibration_routine":
                        if str(target_val).lower() == "white_filter" and not self.white_filter_mode:
                            self.white_filter_mode = True
                            asyncio.create_task(self.perform_white_filter_check())
                        self.settings.set_actual(name, target_val)
                        
                    elif name == "set_active_spot":
                        if target_val is not None:
                            try:
                                if int(target_val) != self.current_reported_spot:
                                    await self.interface_send_data(data={"data": f"spot={target_val}\r"})
                            except ValueError: pass
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """State machine controller for the TAP hardware."""
        await asyncio.sleep(2)
        while True:
            try:
                # Fix: Dictionary safety for state string
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    await self.interface_send_data(data={"data": "show\r"})
                
                elif state_str in ["idle", "maintenance"]:
                    await self.interface_send_data(data={"data": "hide\r"})
                
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"sampling_monitor error: {e}")
                await asyncio.sleep(1)

    def default_parse(self, data):
        """Fix: Uses variable_types to optimize payload size."""
        if not data: return None
        try:
            # Metadata filtering logic
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            
            raw_str = raw_payload.get("data", "")
            parts = raw_str.strip().split(",")

            if len(parts) >= 14:
                # Add specific TAP variable assignment logic here
                pass

            return record
        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
            return None

    async def perform_white_filter_check(self):
        """Automated baseline check cycling through all 8 spots."""
        # Baseline collection logic...
        pass

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                if record and self.sampling():
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor: await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    """Matches MAGIC 250 main entry logic."""
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    inst = TAP2901()
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