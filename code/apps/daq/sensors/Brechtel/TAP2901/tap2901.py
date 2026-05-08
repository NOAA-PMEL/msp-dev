import asyncio
import signal
import sys
import os
import logging
import yaml
import json
import struct
from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.event import DAQEvent
from envds.daq.types import DAQEventType as det
from cloudevents.http import CloudEvent
from pydantic import BaseModel

class TAP(Sensor):
    def __init__(self, config=None, **kwargs):
        super(TAP, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.sensor_definition_file = "Brechtel_TAP2901_sensor_definition.json"
        
        # Internal tracking for state persistence
        self.last_active_spot = 1 
        self.last_cal_routine = "none"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        super(TAP, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        # Extract defaults for settings
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def.get("variables", {}).items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        # Extract defaults for persistent calibrations (Option 2 Architecture)
        cal_def = self.get_definition_by_variable_type(self.metadata, variable_type="calibration")
        for name, cal in cal_def.get("variables", {}).items():
            if name == "last_active_spot":
                self.last_active_spot = cal["attributes"].get("default_value", {}).get("data", 1)
            # Support framework-injected saved states
            if "calibrations" in conf and name in conf["calibrations"]:
                if name == "last_active_spot":
                    self.last_active_spot = conf["calibrations"][name]

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
            daq_id=conf.get("daq_id", "default")
        )

        # Safely extract format version
        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        # Subscribe to interface MQTT streams
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_data(self, message: CloudEvent):
        await super(TAP, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message.get("path_id") or message.data.get("path_id")
                if path_id == self.config.interfaces["default"]["path"]:
                    await self.default_data_buffer.put(message)
            except (KeyError, AttributeError):
                pass

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine", "set_active_spot"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        need_start = True
        await asyncio.sleep(2)
        while True:
            try:
                # White Filter Calibration Tracker
                cal_obj = self.settings.get_setting("calibration_routine")
                current_cal = str(cal_obj.get("requested", "none")).lower() if isinstance(cal_obj, dict) else "none"

                # Detect when a white filter calibration finishes
                if self.last_cal_routine == "white_filter" and current_cal != "white_filter":
                    self.logger.info("White filter calibration completed. Resetting active spot to 1.")
                    self.last_active_spot = 1
                    # Clear any manual spot override
                    self.settings.set_setting("set_active_spot", requested=0)
                    self.settings.set_actual("set_active_spot", 0)

                self.last_cal_routine = current_cal

                # Core Sampling Logic
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        target_spot_obj = self.settings.get_setting("set_active_spot")
                        target_spot = target_spot_obj.get("requested", 0) if isinstance(target_spot_obj, dict) else 0
                        
                        # Fallback to persistent spot if override is 0
                        spot_to_request = int(target_spot) if int(target_spot) > 0 else self.last_active_spot
                        
                        await self.interface_send_data(data={"data": f"spot={spot_to_request}\r"})
                        need_start = False
                else:
                    if not need_start:
                        # Stop sampling and flow without triggering filter increment
                        await self.interface_send_data(data={"data": "spot=0\r"})
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
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.01)

    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting", "coordinate", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            
            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            if "time" in record["variables"]:
                record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            
            raw_str = raw_payload.get("data", "").strip()
            parts = [x.strip() for x in raw_str.split(",")]
            
            # Require the full 49-element record (Manual Sec 9.1)
            if len(parts) < 49:
                return None
                
            # 1. Map standard scalar fields
            standard_map = [
                "record_type", "status_flags", "elapsed_time", "filter_id", 
                "active_spot", "flow_rate", "sample_vol_active_spot", 
                "case_T", "sample_T"
            ]
            
            for i, var_name in enumerate(standard_map):
                if var_name in record["variables"]:
                    val = parts[i]
                    instvar = self.config.metadata.variables[var_name]
                    try:
                        record["variables"][var_name]["data"] = int(val) if instvar.type == "int" else (float(val) if instvar.type == "float" else val)
                    except ValueError:
                        record["variables"][var_name]["data"] = "" if instvar.type == "str" else None

            # 2. Map 40 Intensity fields & decode IEEE754 Hex (Manual Sec 9.3)
            intensity_map = []
            for ch in range(10):
                intensity_map.extend([
                    f"ch{ch}_dark_intensity", f"ch{ch}_red_intensity",
                    f"ch{ch}_green_intensity", f"ch{ch}_blue_intensity"
                ])
            
            for i, var_name in enumerate(intensity_map):
                if var_name in record["variables"]:
                    hex_val = parts[i + 9]
                    try:
                        clean_hex = hex_val.replace("0x", "").strip().zfill(8)
                        decoded_float = struct.unpack('!f', bytes.fromhex(clean_hex))[0]
                        record["variables"][var_name]["data"] = round(decoded_float, 4)
                    except (ValueError, struct.error):
                        self.logger.warning(f"Failed to decode hex to float for {var_name}: {hex_val}")
                        record["variables"][var_name]["data"] = None

            # 3. Continuous Tracking for persistent spot recovery
            try:
                current_spot = record["variables"]["active_spot"]["data"]
                if current_spot is not None and int(current_spot) > 0:
                    self.last_active_spot = int(current_spot)
            except KeyError:
                pass

            return record
            
        except Exception as e:
            self.logger.error("default_parse error", extra={"error": str(e)})
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    
    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "9999")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"Brechtel::TAP2901::{sn}")
    
    inst = TAP()
    inst.run()
    await asyncio.sleep(2)
    inst.start()

    global do_run
    do_run = True
    def shutdown_handler(*args):
        global do_run
        do_run = False

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run: await asyncio.sleep(1)
    await inst.shutdown()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    asyncio.run(main(ServerConfig()))