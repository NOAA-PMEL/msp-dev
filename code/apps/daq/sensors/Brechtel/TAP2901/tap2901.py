import asyncio
import signal
import sys
import os
import logging
import yaml
import json
import struct
import math
from collections import deque
import statistics
import time
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

        # --- NEW: Calibration specific tracking ---
        self.cal_buffer_size = 60
        self.cal_start_time = None
        self.cal_buffers = {
            spot: {
                "red": deque(maxlen=self.cal_buffer_size),
                "green": deque(maxlen=self.cal_buffer_size),
                "blue": deque(maxlen=self.cal_buffer_size)
            }
            for spot in range(1, 9)
        }
        # ------------------------------------------

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

    # async def sampling_monitor(self):
    #     need_start = True
    #     if not hasattr(self, 'current_requested_spot'):
    #         self.current_requested_spot = 0
            
    #     await asyncio.sleep(2)
    #     while True:
    #         try:
    #             # White Filter Calibration Tracker
    #             cal_obj = self.settings.get_setting("calibration_routine")
    #             current_cal = str(cal_obj.get("requested", "none")).lower() if isinstance(cal_obj, dict) else "none"

    #             if self.last_cal_routine == "white_filter" and current_cal != "white_filter":
    #                 self.logger.info("White filter calibration completed. Resetting active spot to 1.")
    #                 self.last_active_spot = 1
    #                 self.settings.set_setting("set_active_spot", requested=0)
    #                 self.settings.set_actual("set_active_spot", 0)

    #             self.last_cal_routine = current_cal

    #             # Core Sampling Logic
    #             state_obj = self.settings.get_setting("sampling_state")
    #             state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
    #             state_str = str(state).lower()

    #             if self.sampling() and state_str == "sampling":
    #                 target_spot_obj = self.settings.get_setting("set_active_spot")
    #                 target_spot = target_spot_obj.get("requested", 0) if isinstance(target_spot_obj, dict) else 0
    #                 spot_to_request = int(target_spot) if int(target_spot) > 0 else self.last_active_spot
                    
    #                 if need_start or spot_to_request != self.current_requested_spot:
    #                     await self.interface_send_data(data={"data": f"spot={spot_to_request}\r"})
    #                     self.current_requested_spot = spot_to_request
    #                     need_start = False
    #             else:
    #                 if not need_start:
    #                     await self.interface_send_data(data={"data": "spot=0\r"})
    #                     self.current_requested_spot = 0
    #                     need_start = True
    #         except Exception as e:
    #             self.logger.error("sampling_monitor error", extra={"error": str(e)})
    #         await asyncio.sleep(1)

    async def sampling_monitor(self):
        need_start = True
        if not hasattr(self, 'current_requested_spot'):
            self.current_requested_spot = 0
            
        await asyncio.sleep(2)
        while True:
            try:
                cal_obj = self.settings.get_setting("calibration_routine")
                current_cal = str(cal_obj.get("requested", "none")).lower() if isinstance(cal_obj, dict) else "none"

                # ---------------------------------------------------------
                # NEW: Calibration State Machine
                # ---------------------------------------------------------
                # Phase 1: Detect switch to calibration
                if self.last_cal_routine != "white_filter" and current_cal == "white_filter":
                    self.logger.debug("[CAL DEBUG] Phase 1: Starting white filter calibration. Pausing flow.")
                    self.cal_start_time = time.time()
                    
                    # Clear buffers for fresh tracking
                    for spot in range(1, 9):
                        for color in ["red", "green", "blue"]:
                            self.cal_buffers[spot][color].clear()
                    
                    self.settings.set_setting("calibration_status", requested="in_progress")
                    self.settings.set_actual("calibration_status", "in_progress")
                    
                    # Force instrument to spot=0 (stops flow and active sampling)
                    await self.interface_send_data(data={"data": "spot=0\r"})
                    self.current_requested_spot = 0
                    need_start = True

                # Phase 2: Monitor stability over time
                elif self.last_cal_routine == "white_filter" and current_cal == "white_filter":
                    if self.cal_start_time is not None:
                        elapsed_minutes = (time.time() - self.cal_start_time) / 60.0
                        
                        # Only calculate CV if the buffer is entirely full
                        if len(self.cal_buffers[1]["red"]) == self.cal_buffer_size:
                            is_stable = True
                            new_wf_ratios = [[1.0, 1.0, 1.0] for _ in range(8)]
                            
                            # Dynamic easing threshold
                            current_threshold = 0.001
                            if elapsed_minutes > 30.0:
                                extra_minutes = int(elapsed_minutes - 30.0)
                                current_threshold += (extra_minutes * 0.001)
                            if current_threshold > 0.005:  # Hard Cap
                                current_threshold = 0.005
                                
                            self.logger.debug(f"[CAL DEBUG] Phase 2: Buffer Full. Elapsed: {elapsed_minutes:.1f}m | Dynamic Threshold: {current_threshold:.4f}")
                                
                            for spot in range(1, 9):
                                for i, color in enumerate(["red", "green", "blue"]):
                                    data = self.cal_buffers[spot][color]
                                    mean_val = statistics.mean(data)
                                    
                                    if mean_val > 0:
                                        cv = statistics.stdev(data) / mean_val
                                        
                                        # [CAL DEBUG]: Print Spot 1 stats to watch it stabilize
                                        if spot == 1 and color == "red":
                                            self.logger.debug(f"[CAL DEBUG] Spot 1 Red -> Mean: {mean_val:.4f} | CV: {cv:.5f}")
                                            
                                        if cv > current_threshold:
                                            is_stable = False
                                            break
                                    
                                    new_wf_ratios[spot-1][i] = round(mean_val, 6)
                                if not is_stable:
                                    break
                            
                            # Success Event
                            if is_stable:
                                self.logger.debug("[CAL DEBUG] Phase 2: STABILITY ACHIEVED! Saving ratios.")
                                self.wf_ratios = new_wf_ratios
                                
                                self.settings.set_setting("calibration_status", requested="success")
                                self.settings.set_actual("calibration_status", "success")
                                self.settings.set_setting("calibration_routine", requested="none")
                                self.settings.set_actual("calibration_routine", "none")
                        
                        # Phase 3: Timeout and Fallback
                        if elapsed_minutes > 45.0:
                            self.logger.debug(f"[CAL DEBUG] Phase 3: TIMEOUT at {elapsed_minutes:.1f}m. Falling back.")
                            for spot in range(1, 9):
                                for color in ["red", "green", "blue"]:
                                    self.cal_buffers[spot][color].clear()
                                    
                            self.settings.set_setting("calibration_status", requested="timeout_fallback")
                            self.settings.set_actual("calibration_status", "timeout_fallback")
                            self.settings.set_setting("calibration_routine", requested="none")
                            self.settings.set_actual("calibration_routine", "none")

                # Phase 4: Detect successful end of calibration (External or Internal)
                elif self.last_cal_routine == "white_filter" and current_cal != "white_filter":
                    self.logger.debug("[CAL DEBUG] Phase 4: Calibration closed. Sending spot=1 command.")
                    self.last_active_spot = 1
                    self.settings.set_setting("set_active_spot", requested=0)
                    self.settings.set_actual("set_active_spot", 0)

                self.last_cal_routine = current_cal

                # ---------------------------------------------------------
                # Core Sampling Logic
                # (Only execute if we are NOT in the middle of a calibration)
                # ---------------------------------------------------------
                if current_cal != "white_filter":
                    state_obj = self.settings.get_setting("sampling_state")
                    state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                    state_str = str(state).lower()

                    if self.sampling() and state_str == "sampling":
                        target_spot_obj = self.settings.get_setting("set_active_spot")
                        
                        req = target_spot_obj.get("requested") if isinstance(target_spot_obj, dict) else None
                        target_spot = int(req) if req is not None else 0
                        
                        spot_to_request = target_spot if target_spot > 0 else int(self.last_active_spot)
                        
                        if need_start or spot_to_request != self.current_requested_spot:
                            self.logger.debug(f"[CAL DEBUG] Resuming flow -> Sending spot={spot_to_request} command.")
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
            self.logger.debug("default_parse: STARTING PARSE ROUTINE")
            
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
                self.logger.warning("default_parse: Payload length < 49. Aborting parse.")
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

            # --- NEW: Siphon data into calibration buffers if routine is active ---
            if self.last_cal_routine == "white_filter":
                for spot in range(1, 9):
                    ref_ch = 9 if spot % 2 != 0 else 0
                    for color in ["red", "green", "blue"]:
                        try:
                            s_dark = record["variables"][f"ch{spot}_dark_intensity"]["data"]
                            s_col = record["variables"][f"ch{spot}_{color}_intensity"]["data"]
                            r_dark = record["variables"][f"ch{ref_ch}_dark_intensity"]["data"]
                            r_col = record["variables"][f"ch{ref_ch}_{color}_intensity"]["data"]
                            
                            if None not in (s_dark, s_col, r_dark, r_col):
                                if (r_col - r_dark) > 0:
                                    ratio = (s_col - s_dark) / (r_col - r_dark)
                                    self.cal_buffers[spot][color].append(ratio)
                                    
                                    # [CAL DEBUG]: Print Spot 1 Red
                                    if spot == 1 and color == "red":
                                        buf_len = len(self.cal_buffers[spot][color])
                                        self.logger.debug(f"[CAL DEBUG] Siphoning -> Spot 1 Red Ratio: {ratio:.4f} | Buffer Size: {buf_len}/{self.cal_buffer_size}")
                        except KeyError:
                            pass
            # ----------------------------------------------------------------------

            # 3. Physics Calculations (Transmission & Absorption)
            try:
                active_spot = record["variables"]["active_spot"]["data"]
                
                if active_spot is not None and int(active_spot) > 0:
                    sample_ch = int(active_spot)
                    self.last_active_spot = sample_ch
                    
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

                    # ---------------------------------------------------------
                    # Ogren 2010 Absorption Calculation
                    # ---------------------------------------------------------
                    # BYPASS FIRMWARE BUG: Calculate delta_V using live flow rate
                    flow_lpm = record["variables"]["flow_rate"]["data"]
                    
                    if self.last_spot_for_abs != sample_ch:
                        self.logger.debug(f"default_parse: Spot changed to {sample_ch}. Resetting physics baselines.")
                        self.last_I = I_curr
                        self.last_spot_for_abs = sample_ch
                    else:
                        # Convert LPM to m^3 per second: (L/min) * (1 min / 60 sec) * (0.001 m^3 / L)
                        delta_V = (flow_lpm / 60.0) * 0.001 if flow_lpm is not None else 0
                        
                        if delta_V > 0:
                            # Use Pall Emfab/E70 Filter Area constant
                            A = 3.0721e-5 
                            
                            for color in ["red", "green", "blue"]:
                                I_c = I_curr[color]
                                I_p = self.last_I[color]
                                t_c = tau[color]
                                
                                if I_c and I_p and I_c > 0 and I_p > 0 and t_c:
                                    # Perform Ogren Math
                                    f_tau = 1.0 / (1.0796 * t_c + 0.71)
                                    sigma_psap = f_tau * (A / delta_V) * math.log(I_p / I_c)
                                    sigma_ap = 0.85 * sigma_psap / 1.22
                                    
                                    record["variables"][f"{color}_absorption"]["data"] = round(sigma_ap * 1e6, 4)
                                else:
                                    record["variables"][f"{color}_absorption"]["data"] = None
                                    
                            # CRITICAL FIX: Only update the baseline AFTER a successful calculation tick
                            self.last_I = I_curr
                        else:
                            self.logger.debug("default_parse: Flow rate is 0. SKIPPING absorption calculation.")
                            for color in ["red", "green", "blue"]:
                                record["variables"][f"{color}_absorption"]["data"] = None
                else:
                    self.logger.debug("default_parse: active_spot is invalid or 0. SKIPPING physics block.")
            except Exception as e:
                self.logger.error("TAP physics calc error", extra={"error": str(e)})

            # ---------------------------------------------------------
            # 4. Pack Calibration Data (only included on metadata ticks)
            # ---------------------------------------------------------
            if "last_active_spot" in record["variables"]:
                record["variables"]["last_active_spot"]["data"] = self.last_active_spot
                
            if "last_calibrated_wf_ratios" in record["variables"]:
                record["variables"]["last_calibrated_wf_ratios"]["data"] = self.wf_ratios
                
            if "white_filter_ratios" in record["variables"]:
                record["variables"]["white_filter_ratios"]["data"] = self.wf_ratios
                
            # Add new status variable to UI updates
            if "calibration_status" in record["variables"]:
                cal_stat = self.settings.get_setting("calibration_status")
                record["variables"]["calibration_status"]["data"] = cal_stat.get("actual", "none") if isinstance(cal_stat, dict) else "none"
            # ---------------------------------------------------------
            
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