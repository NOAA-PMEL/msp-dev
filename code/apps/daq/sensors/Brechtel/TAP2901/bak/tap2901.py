import asyncio
import signal
import sys
import os
import logging
import yaml
import json
import struct
import math
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
        self.wf_ratios = [[1.0, 1.0, 1.0] for _ in range(8)]
        
        # Internal tracking for absorption physics calculation
        self.last_I = {"red": None, "green": None, "blue": None}
        self.last_sample_vol = None
        self.last_spot_for_abs = None

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


        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # Inject default serial parameters if not specified in conf
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
                        "read-method": "readuntil",
                        "read-terminator": "\r",
                        "decode-errors": "strict",
                        "send-method": "ascii",
                    },
                }
            }
        }

        # Safe update to avoid overwriting user configs
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        if propname not in iface:
                            iface[propname] = prop
        
        # Extract defaults for settings
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def.get("variables", {}).items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        # Extract defaults for persistent calibrations
        cal_def = self.get_definition_by_variable_type(self.metadata, variable_type="calibration")
        for name, cal in cal_def.get("variables", {}).items():
            if name == "last_active_spot":
                self.last_active_spot = cal["attributes"].get("default_value", {}).get("data", 1)
            elif name == "last_calibrated_wf_ratios":
                self.wf_ratios = cal["attributes"].get("default_value", {}).get("data", [[1.0]*3]*8)
                
            # Support framework-injected saved states
            if "calibrations" in conf and name in conf["calibrations"]:
                if name == "last_active_spot":
                    self.last_active_spot = conf["calibrations"][name]
                elif name == "last_calibrated_wf_ratios":
                    self.wf_ratios = conf["calibrations"][name]

        # NEW: Load from disk to override defaults / configmap states if it exists
        self.load_state()

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

    def load_state(self):
        """Loads state from persistent storage, overriding defaults."""
        state_file = "/app/state/tap_state.json"
        try:
            with open(state_file, "r") as f:
                state = json.load(f)
                if "last_active_spot" in state:
                    self.last_active_spot = state["last_active_spot"]
                if "last_calibrated_wf_ratios" in state:
                    self.wf_ratios = state["last_calibrated_wf_ratios"]
            self.logger.info("Loaded persisted TAP state from disk.")
        except FileNotFoundError:
            self.logger.info("No persisted state found. Using definition defaults.")
        except Exception as e:
            self.logger.error("Error loading state", extra={"error": str(e)})

    def save_state(self):
        """Saves current state to persistent storage."""
        state_file = "/app/state/tap_state.json"
        try:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            with open(state_file, "w") as f:
                json.dump({
                    "last_active_spot": self.last_active_spot,
                    "last_calibrated_wf_ratios": self.wf_ratios
                }, f)
        except Exception as e:
            self.logger.error("Error saving state", extra={"error": str(e)})

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
        if not hasattr(self, 'current_requested_spot'):
            self.current_requested_spot = 0
            
        await asyncio.sleep(2)
        while True:
            try:
                # White Filter Calibration Tracker
                cal_obj = self.settings.get_setting("calibration_routine")
                current_cal = str(cal_obj.get("requested", "none")).lower() if isinstance(cal_obj, dict) else "none"

                if self.last_cal_routine == "white_filter" and current_cal != "white_filter":
                    self.logger.info("White filter calibration completed. Resetting active spot to 1.")
                    self.last_active_spot = 1
                    self.save_state()
                    self.settings.set_setting("set_active_spot", requested=0)
                    self.settings.set_actual("set_active_spot", 0)

                self.last_cal_routine = current_cal

                # Core Sampling Logic
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    target_spot_obj = self.settings.get_setting("set_active_spot")
                    target_spot = target_spot_obj.get("requested", 0) if isinstance(target_spot_obj, dict) else 0
                    spot_to_request = int(target_spot) if int(target_spot) > 0 else self.last_active_spot
                    
                    if need_start or spot_to_request != self.current_requested_spot:
                        await self.interface_send_data(data={"data": f"spot={spot_to_request}\r"})
                        self.current_requested_spot = spot_to_request
                        need_start = False
                else:
                    if not need_start:
                        await self.interface_send_data(data={"data": "spot=0\r"})
                        self.current_requested_spot = 0
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

            # 2. Map 40 Intensity fields & decode IEEE754 Hex
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
                        if len(clean_hex) == 8:
                            decoded_float = struct.unpack('!f', bytes.fromhex(clean_hex))[0]
                            record["variables"][var_name]["data"] = round(decoded_float, 4)
                        else:
                            record["variables"][var_name]["data"] = None
                    except (ValueError, struct.error):
                        record["variables"][var_name]["data"] = None

            # 3. Physics Calculations (Transmission & Absorption)
            try:
                active_spot = record["variables"]["active_spot"]["data"]
                if active_spot is not None and int(active_spot) > 0:
                    sample_ch = int(active_spot)
                    # self.last_active_spot = sample_ch
                    # NEW: If the instrument advanced the spot, persist the new state
                    if self.last_active_spot != sample_ch:
                        self.last_active_spot = sample_ch
                        self.save_state()
                    
                    # Ref logic per manual (Ch 9 for odd spots, Ch 0 for even spots)
                    ref_ch = 9 if sample_ch % 2 != 0 else 0
                    
                    def calc_I(color):
                        try:
                            s_dark = record["variables"][f"ch{sample_ch}_dark_intensity"]["data"]
                            s_col = record["variables"][f"ch{sample_ch}_{color}_intensity"]["data"]
                            r_dark = record["variables"][f"ch{ref_ch}_dark_intensity"]["data"]
                            r_col = record["variables"][f"ch{ref_ch}_{color}_intensity"]["data"]
                            if None in (s_dark, s_col, r_dark, r_col): return None
                            if (r_col - r_dark) == 0: return None
                            return (s_col - s_dark) / (r_col - r_dark)
                        except KeyError:
                            return None

                    I_curr = {
                        "red": calc_I("red"),
                        "green": calc_I("green"),
                        "blue": calc_I("blue")
                    }
                    
                    # Safely fetch white filter ratios for the active spot
                    spot_wf = self.wf_ratios[sample_ch - 1] if sample_ch <= len(self.wf_ratios) else [1.0, 1.0, 1.0]
                    iwf = {"red": spot_wf[0], "green": spot_wf[1], "blue": spot_wf[2]}
                    
                    tau = {}
                    for color in ["red", "green", "blue"]:
                        if I_curr[color] is not None and iwf[color]:
                            tau[color] = I_curr[color] / iwf[color]
                            record["variables"][f"{color}_transmission"]["data"] = round(tau[color], 4)
                        else:
                            tau[color] = None
                            record["variables"][f"{color}_transmission"]["data"] = None

                    # Ogren 2010 Absorption Calculation
                    curr_vol = record["variables"]["sample_vol_active_spot"]["data"]
                    
                    if self.last_spot_for_abs != sample_ch:
                        self.last_I = I_curr
                        self.last_sample_vol = curr_vol
                        self.last_spot_for_abs = sample_ch
                    else:
                        delta_V = curr_vol - self.last_sample_vol if curr_vol and self.last_sample_vol else 0
                        if delta_V > 0:
                            A = 2.5281e-5 # m^2 filter area for Azumi
                            
                            for color in ["red", "green", "blue"]:
                                I_c = I_curr[color]
                                I_p = self.last_I[color]
                                t_c = tau[color]
                                
                                if I_c and I_p and I_c > 0 and I_p > 0 and t_c:
                                    f_tau = 1.0 / (1.0796 * t_c + 0.71)
                                    sigma_psap = f_tau * (A / delta_V) * math.log(I_p / I_c)
                                    sigma_ap = 0.85 * sigma_psap / 1.22
                                    record["variables"][f"{color}_absorption"]["data"] = round(sigma_ap * 1e6, 4)
                                else:
                                    record["variables"][f"{color}_absorption"]["data"] = None
                                    
                        self.last_I = I_curr
                        self.last_sample_vol = curr_vol
            except Exception as e:
                self.logger.error("TAP physics calc error", extra={"error": str(e)})

            # 4. Inject Calibration State Variables
            try:
                if "last_active_spot" in record["variables"]:
                    record["variables"]["last_active_spot"]["data"] = self.last_active_spot
                if "last_calibrated_wf_ratios" in record["variables"]:
                    record["variables"]["last_calibrated_wf_ratios"]["data"] = self.wf_ratios
                if "white_filter_ratios" in record["variables"]:
                    record["variables"]["white_filter_ratios"]["data"] = self.wf_ratios
            except Exception as e:
                self.logger.error("Calibration injection error", extra={"error": str(e)})

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