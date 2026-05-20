import asyncio
import signal
import sys
import os
import logging
import traceback
import json
import yaml

from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class TimeserverNTP(Sensor):
    """Driver for Phoenix Contact FL TIMESERVER NTP."""

    def __init__(self, config=None, **kwargs):
        super(TimeserverNTP, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        
        self.first_record = 'RMC'
        self.last_record = 'GGA'
        
        # Mapped strictly to the variables defined in your JSON
        self.nmea_map = {
            'RMC': ['ntp_timestamp', 'status', 'lat', 'lat_dir', 'lon', 'lon_dir'],
            'VTG': ['speed'],
            'GGA': ['sv_num']
        }
        
        self.collecting = False
        
        # Initialize timestamp for our stream watchdog
        self.last_data_time = 0.0

        self.sensor_definition_file = "PhoenixContact_NTP_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        # Add the watchdog task to the enable list
        self.enable_task_list.append(self.stream_watchdog())

    def configure(self):
        super(TimeserverNTP, self).configure()
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
                        "baudrate": 9600, 
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readline",
                        "decode-errors": "strict",
                        "send-method": "ascii"
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

        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(TimeserverNTP, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    # Update our watchdog timestamp the moment data arrives
                    self.last_data_time = asyncio.get_event_loop().time()
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def stream_watchdog(self):
        """Monitors the data stream and sends the 'R' trigger if no data is flowing."""
        # Wait 5 seconds on startup to let the TCP connection settle
        await asyncio.sleep(5.0)
        
        while True:
            try:
                current_time = asyncio.get_event_loop().time()
                
                # If we haven't seen data in 5 seconds, assume stream is stopped/toggled off
                if current_time - self.last_data_time > 5.0:
                    self.logger.info("No data received recently. Sending 'R' trigger to hardware.")
                    
                    iface_path = self.config.interfaces["default"]["path"]
                    iface_id = self.config.interfaces["default"]["interface_id"]
                    
                    # Create the event to send "R" down to the interface
                    # Note: Adjust the event creation if your envds core uses a different pattern for sending
                    cmd_event = DAQEvent(
                        source=self.get_id_as_source(),
                        type="envds.interface.data.send", 
                        data={"data": "R\n", "path_id": iface_path}
                    )
                    
                    cmd_event["destpath"] = f"{iface_id}/data/send"
                    await self.send_message(cmd_event)
                    
                    # Wait 5 seconds before checking again to give the hardware time to respond
                    await asyncio.sleep(5.0)
                    
            except Exception as e:
                self.logger.error("stream_watchdog exception", extra={"error": str(e)})
                
            await asyncio.sleep(1.0)

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """Passive monitor for UI state"""
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                # State handled logically in default_data_loop
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
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

                if self.first_record in raw_str:
                    record_buffer = self.default_parse(data)
                    continue

                if record_buffer is None:
                    continue

                parsed_fragment = self.default_parse(data)
                if parsed_fragment:
                    for var, val_dict in parsed_fragment["variables"].items():
                        if var != 'time' and val_dict.get("data") is not None:
                            record_buffer["variables"][var]["data"] = val_dict["data"]

                if self.last_record in raw_str and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record_buffer,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
                    record_buffer = None 

            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
                record_buffer = None

    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            if "time" in record.get("variables", {}):
                record["variables"]["time"]["data"] = raw_payload.get("timestamp")
                
            raw_str = raw_payload.get("data", "")
            parts = raw_str.split(",")

            datavar = next((key for key in self.nmea_map.keys() if key in raw_str), None)
            if not datavar: return None

            if datavar == 'RMC':
                parts = parts[1:7]
            elif datavar == 'VTG':
                parts = parts[7:8]
            elif datavar == 'GGA':
                parts = parts[7:8]

            var_names = self.nmea_map[datavar]
            for index, name in enumerate(var_names):
                if name in record["variables"] and index < len(parts):
                    instvar = self.config.metadata.variables[name]
                    val = parts[index]
                    try:
                        if instvar.type == "int":
                            record["variables"][name]["data"] = int(val)
                        elif instvar.type == "float":
                            record["variables"][name]["data"] = float(val)
                        else:
                            record["variables"][name]["data"] = val
                    except ValueError:
                        record["variables"][name]["data"] = "" if instvar.type in ("str", "char") else None

            if "lat" in record["variables"]:
                lat_data = record["variables"]["lat"]["data"]
                if lat_data is not None and lat_data != "":
                    try:
                        deg = int(lat_data / 100)
                        mm_mm = ((lat_data / 100) - deg) * 100.0
                        dec_deg = deg + (mm_mm / 60.0)
                        if record["variables"].get("lat_dir", {}).get("data") == "S":
                            dec_deg *= -1.0
                        record["variables"]["lat"]["data"] = round(dec_deg, 5)
                    except Exception as e:
                        self.logger.debug(f"Lat conversion warning: {e}")

            if "lon" in record["variables"]:
                lon_data = record["variables"]["lon"]["data"]
                if lon_data is not None and lon_data != "":
                    try:
                        deg = int(lon_data / 100)
                        mm_mm = ((lon_data / 100) - deg) * 100.0
                        dec_deg = deg + (mm_mm / 60.0)
                        if record["variables"].get("lon_dir", {}).get("data") == "W":
                            dec_deg *= -1.0
                        record["variables"]["lon"]["data"] = round(dec_deg, 5)
                    except Exception as e:
                        self.logger.debug(f"Lon conversion warning: {e}")

            return record
            
        except Exception as e:
            self.logger.error("default_parse error", extra={"error": str(e)})
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
    logger = logging.getLogger(f"PhoenixContact::TimeserverNTP::{sn}")

    logger.debug("Starting TimeserverNTP")
    inst = TimeserverNTP()
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
        logger.debug("TimeserverNTP.run", extra={"do_run": do_run})
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
        host = sys.argv[index + 1]
        config.host = host
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        port = sys.argv[index + 1]
        config.port = int(port)
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--log_level")
        ll = sys.argv[index + 1]
        config.log_level = ll
    except (ValueError, IndexError):
        pass

    asyncio.run(main(config))