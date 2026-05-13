import asyncio
import signal
import sys
import os
import logging
import yaml
import json

from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.event import DAQEvent
from envds.daq.types import DAQEventType as det
from cloudevents.http import CloudEvent
from pydantic import BaseModel
class Aurora3000(Sensor):
    def __init__(self, config=None, **kwargs):
        super(Aurora3000, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.sensor_definition_file = "ACOEM_Aurora3000_sensor_definition.json"
        
        # Hardware state tracking to prevent command spamming
        self.current_valve_state = "position_0"
        self.current_cal_routine = "none"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.enable_task_list.append(self.polling_loop())

    def configure(self):
        super(Aurora3000, self).configure()
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
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readline",  # readline, read-until, readbytes, readbinary
                        # "read-terminator": "\r",  # only used for read_until
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

            self.logger.debug(
                "aurora3000.configure", extra={"interfaces": conf["interfaces"]}
            )


        # Extract defaults for settings
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
            daq_id=conf.get("daq_id", "default")
        )

        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_data(self, message: CloudEvent):
        await super(Aurora3000, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message.get("path_id") or message.data.get("path_id")
                if path_id == self.config.interfaces["default"]["path"]:
                    if message.data: 
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
                    if name in ["sampling_state", "calibration_routine", "ext_valve_state"]:
                        self.settings.set_actual(name, target_val)

    async def manage_valve(self):
        """Handle 3-way valve state transitions using Digital Aux Out."""
        try:
            valve_obj = self.settings.get_setting("ext_valve_state")
            valve_req = str(valve_obj.get("requested", "position_0")).lower() if isinstance(valve_obj, dict) else "position_0"

            if valve_req != self.current_valve_state:
                self.logger.info(f"Switching 3-way valve to {valve_req}")
                
                if valve_req == "position_1":
                    await self.interface_send_data(data={"data": "DO0041\r"})
                else:
                    await self.interface_send_data(data={"data": "DO0040\r"})
                
                self.current_valve_state = valve_req

        except Exception as e:
            self.logger.error("manage_valve error", extra={"error": str(e)})

    async def manage_calibration(self):
        """Handle Aurora 3000 calibration triggers using **J commands."""
        try:
            cal_obj = self.settings.get_setting("calibration_routine")
            cal_req = str(cal_obj.get("requested", "none")).lower() if isinstance(cal_obj, dict) else "none"

            if cal_req != self.current_cal_routine:
                self.logger.info(f"Initiating Aurora 3000 calibration routine: {cal_req}")
                
                if cal_req == "span_cal_co2":
                    await self.interface_send_data(data={"data": "**0J1\r"})  # Span calibration
                elif cal_req == "zero_cal":
                    await self.interface_send_data(data={"data": "**0J2\r"})  # Zero calibration
                elif cal_req == "span_check":
                    await self.interface_send_data(data={"data": "**0J3\r"})  # Span check
                elif cal_req == "zero_check":
                    await self.interface_send_data(data={"data": "**0J4\r"})  # Zero check
                elif cal_req == "none":
                    await self.interface_send_data(data={"data": "**0J0\r"})  # Normal Monitoring
                
                self.current_cal_routine = cal_req

        except Exception as e:
            self.logger.error("manage_calibration error", extra={"error": str(e)})

    async def sampling_monitor(self):
        need_start = True
        await asyncio.sleep(2)
        while True:
            try:
                # 1. Update peripheral hardware and calibration states
                await self.manage_valve()
                await self.manage_calibration()

                # 2. Check main instrument sampling state
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        self.logger.info("Entering active sampling state.")
                        need_start = False
                else:
                    if not need_start:
                        self.logger.info("Entering idle state.")
                        need_start = True

            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def polling_loop(self):
        """Strictly timed loop to request data without being delayed by control logic."""
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                # If we are actively sampling, fire the data request
                if self.sampling() and state_str == "sampling":
                    await self.interface_send_data(data={"data": "VI099\r"})
                    
            except Exception as e:
                self.logger.error("polling_loop error", extra={"error": str(e)})
                
            # Maintain a strict 1-second polling cadence
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
            
            if not raw_str or len(raw_str) < 5:
                return None
                
            # parts = [x.strip() for x in raw_str.split(",")]
            raw_parts = [x.strip() for x in raw_str.split(",")]
            # --- ALIGNMENT FIX ---
            first_chunk = raw_parts[0].split()
            if len(first_chunk) >= 2:
                # Clean up firmware ASCII bug in the time string
                clean_time = first_chunk[1].replace(";", "0")
                parts = [first_chunk[0], clean_time] + raw_parts[1:]
            else:
                parts = raw_parts
            # ---------------------

            # --- VI099 MAPPING LIST ---
            variable_map = [
                "aurora_date",
                "aurora_time",
                "scat_coef_ch1_red",
                "scat_coef_ch2_green",
                "scat_coef_ch3_blue",
                "backscatter_ch1_red",
                "backscatter_ch2_green",
                "backscatter_ch3_blue",
                "sample_T",
                "enclosure_T",
                "rh",
                "pressure",
                "major_state",
                "DIO_state"
            ]

            for i, var_name in enumerate(variable_map):
                if i < len(parts) and var_name in record["variables"]:
                    val = parts[i]
                    instvar = self.config.metadata.variables[var_name]
                    try:
                        record["variables"][var_name]["data"] = int(val) if instvar.type == "int" else (float(val) if instvar.type == "float" else val)
                    except ValueError:
                        record["variables"][var_name]["data"] = "" if instvar.type in ("str", "char") else None

            # --- CONTINUOUS TRACKING: Auto-reset calibration state ---
            try:
                major_state = record["variables"].get("major_state", {}).get("data")
                # If Aurora returns to Normal Monitoring (0) but framework thinks we are calibrating
                if major_state == 0 and self.current_cal_routine != "none":
                    self.logger.info("Calibration sequence finished. Resetting UI to 'none'.")
                    self.settings.set_setting("calibration_routine", requested="none")
                    self.settings.set_actual("calibration_routine", "none")
                    self.current_cal_routine = "none"
            except (KeyError, TypeError):
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
    logger = logging.getLogger(f"ACOEM::Aurora3000::{sn}")
    
    inst = Aurora3000()
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