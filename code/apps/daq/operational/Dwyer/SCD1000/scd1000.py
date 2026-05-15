import asyncio
import signal
import sys
import os
import logging
import json
import yaml
from envds.core import envdsLogger
from envds.util.util import time_to_next, string_to_datetime
from envds.daq.operational import Operational
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class DwyerSCD(Operational):
    def __init__(self, config=None, **kwargs):
        super(DwyerSCD, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.polling_task = None
        self.sampling_interval = 1
        self.polling_mode = "polled" 
        
        # Modbus Configuration
        self.modbus_address = 1

        self.operational_definition_file = "Dwyer_SCD_operational_definition.json"

        try:
            with open(self.operational_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("operational_definition not found. Exiting")
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(DwyerSCD, self).configure()

        try:
            with open("/app/config/operational.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        env_polling_mode = os.environ.get("POLLING_MODE")
        if env_polling_mode:
            self.polling_mode = env_polling_mode.lower()
        elif "polling_mode" in conf:
            self.polling_mode = str(conf["polling_mode"]).lower()
            
        if "modbus_address" in conf:
            self.modbus_address = int(conf["modbus_address"])

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 9600,
                        "bytesize": 7,
                        "parity": "E",
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
            settings=settings_def.get("variables", {}),
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
            self.device_format_version = self.config.metadata.attributes.get("format_version").data
        except (KeyError, AttributeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    def build_modbus_ascii(self, address, function, register, data):
        """Constructs a raw Modbus ASCII string with LRC checksum."""
        payload = [
            address,
            function,
            (register >> 8) & 0xFF,
            register & 0xFF,
            (data >> 8) & 0xFF,
            data & 0xFF
        ]
        self.logger.debug(f"build_modbus_ascii", extra={"modbus_paylaod": payload})
        # Calculate Longitudinal Redundancy Check (LRC)
        lrc = (~sum(payload) + 1) & 0xFF
        self.logger.debug(f"build_modbus_ascii", extra={"modbus_lrc": lrc})
        
        # Format as : + payload + LRC + CR + LF
        cmd = ":" + "".join([f"{x:02X}" for x in payload]) + f"{lrc:02X}\r\n"
        self.logger.debug(f"build_modbus_ascii", extra={"modbus_cmd": cmd})
        return cmd

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    if name == "sampling_state":
                        self.settings.set_actual(name, target_val)
                        
                    elif name == "set_value":
                        try:
                            # Fetch the actual remote path (e.g., 'port-1') instead of hardcoding 'default'
                            target_path = self.config.interfaces.get("default", {}).get("path", "default")

                            # SCD1000/2000 Modbus write: Set Point is at 1001H, resolution 0.1 degree
                            sv_scaled = int(float(target_val) * 10.0)
                            
                            # Handle negative values for 16-bit registers (if target_val < 0)
                            if sv_scaled < 0:
                                sv_scaled = (abs(sv_scaled) ^ 0xFFFF) + 1 
                            
                            # Function 6 = Write Single Register
                            cmd = self.build_modbus_ascii(self.modbus_address, 6, 0x1001, sv_scaled)
                            await self.interface_send_data(data={"data": cmd}, path_id="default")
                        except Exception as e:
                            self.logger.error("settings_check SV error", extra={"error": str(e)})

    async def handle_interface_data(self, message: CloudEvent):
        await super(DwyerSCD, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                default_path = self.config.interfaces.get("default", {}).get("path")
                
                # Both read and write responses come through the default serial path
                if path_id == default_path:
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def sampling_monitor(self):
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                self.logger.debug(f"sampling_monitor", extra={"sampling_state": state_str})
                if self.sampling() and state_str == "sampling":
                    if self.polling_mode == "polled":
                        if self.polling_task is None or self.polling_task.done():
                            self.logger.info("Starting DwyerSCD polling loop.")
                            self.polling_task = asyncio.create_task(self.polling_loop())
                    elif self.polling_mode == "unpolled":
                        if self.polling_task and not self.polling_task.done():
                            self.logger.info("Stopping DwyerSCD polling loop (Unpolled mode active).")
                            self.polling_task.cancel()
                            self.polling_task = None
                else:
                    if self.polling_task and not self.polling_task.done():
                        self.logger.info("Stopping DwyerSCD polling loop.")
                        self.polling_task.cancel()
                        self.polling_task = None
                        
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def polling_loop(self):
        # SCD1000/2000 Modbus Read: 1000H (PV), 1001H (SV)
        # Function 3 = Read Holding Registers, Count = 2
        self.logger.debug(f"polling_loop")
        cmd = self.build_modbus_ascii(self.modbus_address, 3, 0x1000, 2)
        self.logger.debug(f"polling_loop", extra={"modbus_ascii": cmd.strip()})
        target_path = self.config.interfaces.get("default", {}).get("path", "default")
        self.logger.debug(f"polling_loop", extra={"target_path": target_path})

        while True:
            try:
                self.logger.debug(f"Sending Modbus poll to path: {target_path}", extra={"polling_cmd": cmd.strip()})
                await self.interface_send_data(data={"data": cmd}, path_id="default")
            except Exception as e:
                self.logger.error("polling_loop error", extra={"reason": str(e)})
            await asyncio.sleep(self.sampling_interval)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.logger.debug("default_data_loop - incoming data", extra={"data": data})
                
                record = self.default_parse(data)
                
                if record:
                    self.collecting = True

                # Only emit the event if the parse returned a valid record (i.e., a Read response)
                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    self.logger.debug("default_data_loop - publishing event", extra={"destpath": event["destpath"]})
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if not data: return None
        try:
            raw_payload = data.data if isinstance(data.data, dict) else {}
            timestamp = raw_payload.get("timestamp")
            iface_data = raw_payload.get("data", "")
            
            raw_str = iface_data.strip()
            self.logger.debug("default_parse - raw string received", extra={"raw_str": raw_str})
            
            # Modbus ASCII responses must start with a colon
            if not raw_str or not raw_str.startswith(":"):
                return None
                
            addr = int(raw_str[1:3], 16)
            if addr != self.modbus_address:
                self.logger.debug("default_parse - ignoring message for different Modbus address", extra={"addr": addr})
                return None
                
            func = int(raw_str[3:5], 16)
            
            # Function 03: Read Response
            if func == 3:
                if len(raw_str) < 15: # Needs to be long enough to contain PV and SV
                    return None
                    
                v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
                record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
                self.include_metadata = False
                
                record["timestamp"] = timestamp
                if "time" in record["variables"]:
                    record["variables"]["time"]["data"] = timestamp

                # Extract the 4 hex characters representing the PV
                pv_raw = int(raw_str[7:11], 16)
                
                # Filter out SCD1000 documented hardware error codes (8002H, 8003H, etc.)
                if pv_raw >= 0x8000:
                    self.logger.warning("default_parse - SCD hardware error code detected", extra={"error_code": hex(pv_raw)})
                    record["variables"]["process_value"]["data"] = None
                else:
                    # Handle signed 16-bit integer conversion for negative temperatures
                    if pv_raw > 32767:
                        pv_raw -= 65536
                    record["variables"]["process_value"]["data"] = round(pv_raw / 10.0, 1)

                return record
                
            # Function 06: Write Response (Acknowledge)
            elif func == 6:
                if len(raw_str) < 13:
                    return None
                    
                reg = int(raw_str[5:9], 16)
                val_raw = int(raw_str[9:13], 16)
                
                # If the controller echoes back our write to the SV register (1001H)
                if reg == 0x1001:
                    if val_raw > 32767:
                        val_raw -= 65536
                        
                    sv_actual = round(val_raw / 10.0, 1)
                    self.settings.set_actual("set_value", actual=sv_actual)
                    self.logger.info("default_parse - SV Write Confirmed", extra={"sv_actual": sv_actual})
                
                # We do not return a data record for write acknowledgements
                return None

        except Exception as e:
            self.logger.error("default_parse - critical error", extra={"error": str(e), "data": data})
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()
    for task in task_list:
        if task:
            task.cancel()

async def main(server_config: ServerConfig = None):
    if server_config is None:
        server_config = ServerConfig()

    sn = "9999"
    try:
        with open("/app/config/operational.conf", "r") as f:
            conf = yaml.safe_load(f)
            try:
                sn = conf["serial_number"]
            except KeyError:
                pass
    except FileNotFoundError:
        pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"Dwyer::SCD1000_2000::{sn}")

    logger.debug("Starting Dwyer SCD Controller")
    inst = DwyerSCD()
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
        logger.debug("DwyerSCD.run", extra={"do_run": do_run})
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