import asyncio
import math
import signal
import sys
import os
import logging
import json
import yaml
from envds.core import envdsLogger
from envds.util.util import time_to_next
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []
class POPS1100(Sensor):
    def __init__(self, config=None, **kwargs):
        super(POPS1100, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        
        self.sensor_definition_file = "Handix_POPS1100_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        # Default 16-bin configuration per POPS manual
        # self.lower_dp_bound = [
        #     115., 125., 135., 150., 165., 185., 210., 250., 
        #     350., 475., 575., 855., 1220., 1530., 1990., 2585.
        # ]

        # self.upper_dp_bound = [
        #     125., 135., 150., 165., 185., 210., 250., 350., 
        #     475., 575., 855., 1220., 1530., 1990., 2585., 3370.
        # ]

        lower_bnd_var = self.metadata.get("variables", {}).get("diameter_bnd_lower", {})
        self.lower_dp_bound = lower_bnd_var.get("data", [])
        
        upper_bnd_var = self.metadata.get("variables", {}).get("diameter_bnd_upper", {})
        self.upper_dp_bound = upper_bnd_var.get("data", [])

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(POPS1100, self).configure()
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

    async def handle_interface_data(self, message: CloudEvent):
        await super(POPS1100, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "pump_power", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """Passive monitor for UI state (POPS streams continuously on boot)"""
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                # Just keeping the UI logically synced; the loop gatekeeps the data
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                
                if record:
                    self.collecting = True

                if record and self.sampling():
                    
                    # # Inject distribution bounding variables
                    # record["variables"]["diameter_bnd_lower"]["data"] = self.lower_dp_bound
                    # record["variables"]["diameter_bnd_upper"]["data"] = self.upper_dp_bound
                    
                    diams, dlogDp = [], []
                    for lower, upper in zip(self.lower_dp_bound, self.upper_dp_bound):
                        # diams.append(round(math.sqrt(lower * upper), 1))
                        dlogDp.append(round(math.log10(upper / lower), 3))

                    # record["variables"]["diameter"]["data"] = diams
                    record["variables"]["dlogDp"]["data"] = dlogDp

                    bin_counts = record["variables"].get("bin_count", {}).get("data", [])

                    # --- STRICT VALIDATION: Only process if we have exactly 16 bins ---
                    expected_bins = len(self.upper_dp_bound) # Should be 16
                    if len(bin_counts) != expected_bins:
                        self.logger.warning(
                            "Invalid record: bin_count length mismatch", 
                            extra={"expected": expected_bins, "received": len(bin_counts)}
                        )
                        continue # Drop this record and wait for the next one
                    # -----------------------------------------------------------------

                    flow = record["variables"]["POPS_Flow"]["data"] or 1.0
                    
                    dN, dNdlogDp = [], []
                    intN = 0
                    
                    # Calculate size distribution variables based on raw bin counts
                    for i, cnt in enumerate(bin_counts):
                        conc = cnt / (flow * 1.0)
                        intN += conc
                        dN.append(round(conc, 3))
                        dNdlogDp.append(round(conc / dlogDp[i], 3))
                        
                    record["variables"]["dN"]["data"] = dN
                    record["variables"]["dNdlogDp"]["data"] = dNdlogDp
                    record["variables"]["intN"]["data"] = intN

                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.01)

    # def default_parse(self, data):
    #     if not data: return None
    #     try:
    #         v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
    #         record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
    #         self.include_metadata = False

    #         raw_payload = data.data if isinstance(data.data, dict) else {}
    #         record["timestamp"] = raw_payload.get("timestamp")
    #         if "time" in record.get("variables", {}):
    #             record["variables"]["time"]["data"] = raw_payload.get("timestamp")
                
    #         raw_str = raw_payload.get("data", "")
    #         parts = raw_str.strip().split(",")

    #         if len(parts) < 37:
    #             return None

    #         # Remove unused header blocks
    #         parts.pop(0) # Remove "POPS" string
    #         if len(parts) > 1:
    #             parts.pop(1) # Remove secondary unused header string
                
    #         # Safely locate and remove the SD card path if present
    #         fname_idx = next((i for i, p in enumerate(parts) if "/media/uSD" in p), -1)
    #         if fname_idx >= 0:
    #             parts.pop(fname_idx)

    #         # Get target variables to iterate through, excluding array/computed types
    #         exclude_vars = ["time", "diameter", "diameter_bnd_lower", "diameter_bnd_upper", "dN", "dNdlogDp", "dlogDp", "intN"]
    #         variables = [v for v in self.config.metadata.variables.keys() if v not in exclude_vars]
            
    #         dist_index = None

    #         for index, name in enumerate(variables):
    #             if name == "bin_count":
    #                 dist_index = index
    #                 break
                    
    #             if name in record["variables"] and index < len(parts):
    #                 instvar = self.config.metadata.variables[name]
    #                 vartype = "str" if instvar.type == "string" else instvar.type
    #                 val_str = parts[index].strip()
                    
    #                 try:
    #                     if vartype == "int":
    #                         record["variables"][name]["data"] = int(float(val_str))
    #                     elif vartype == "float":
    #                         record["variables"][name]["data"] = float(val_str)
    #                     else:
    #                         record["variables"][name]["data"] = val_str
    #                 except ValueError:
    #                     record["variables"][name]["data"] = "" if vartype in ["str", "char"] else None
            
    #         # Extract raw histogram distribution
    #         if dist_index is not None and dist_index < len(parts):
    #             bin_counts = []
    #             for val in parts[dist_index:]:
    #                 try:
    #                     bin_counts.append(int(float(val.strip())))
    #                 except ValueError:
    #                     bin_counts.append(0)
                        
    #             record["variables"]["bin_count"]["data"] = bin_counts

    #         return record
            
    #     except Exception as e:
    #         self.logger.error("default_parse error", extra={"error": str(e)})
    #         return None

    def default_parse(self, data):
        if not data:
            return None
            
        try:
            # Get all scalar variables defined in the JSON metadata
            exclude_vars = ["time", "diameter", "diameter_bnd_lower", "diameter_bnd_upper", "dN", "dNdlogDp", "dlogDp", "intN", "bin_count"]
            variables = [v for v in self.metadata["variables"].keys() if v not in exclude_vars]
            
            record = self.build_data_record(meta=self.include_metadata)
            self.include_metadata = False
            
            record["timestamp"] = data.data["timestamp"]
            record["variables"]["time"]["data"] = data.data["timestamp"]
            
            # Split raw string
            parts = data.data["data"].strip().split(",")

            # 1. Clean Headers: Remove "POPS" and "POPS-347" strings
            if parts and parts[0] == "POPS":
                parts.pop(0) # [cite: 17, 216]
            parts = [p for p in parts if "POPS-" not in p]

            # 2. Clean SD Path: Remove the filename string (e.g., /media/uSD/...)
            parts = [p for p in parts if "/media/uSD" not in p]

            # 3. Scalar Mapping: JSON variables now align 1:1 with parts indices
            # Status (6), DataStatus (7), PartCt (8), HistSum (9), PartCon (10)...
            for index, name in enumerate(variables):
                if name in record["variables"]:
                    instvar = self.metadata["variables"][name]
                    vartype = instvar["type"]
                    if vartype == "string": vartype = "str"
                    
                    try:
                        # Map exactly as in your original file 
                        record["variables"][name]["data"] = eval(vartype)(parts[index].strip())
                    except (ValueError, TypeError, IndexError):
                        record["variables"][name]["data"] = None

            # 4. Bin Extraction: Start after the last scalar variable (RawPts)
            raw_bins = parts[len(variables):]
            
            # STRICT VALIDATION: Bad records are truncated (e.g., the 15-bin log seen earlier)
            # NBins is 16 by default [cite: 482, 890]
            if len(raw_bins) != 16:
                self.logger.warning("Malformed record: dropping truncated data", 
                                    extra={"expected": 16, "received": len(raw_bins)})
                return None
                
            record["variables"]["bin_count"]["data"] = [int(val.strip()) for val in raw_bins]

            return record

        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
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
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            try:
                sn = conf["serial_number"]
            except KeyError:
                pass
    except FileNotFoundError:
        pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"Handix::POPS1100::{sn}")

    logger.debug("Starting POPS1100")
    inst = POPS1100()
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
        logger.debug("POPS1100.run", extra={"do_run": do_run})
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