import asyncio
import signal
import sys
import os
import logging
import logging.config
import json
import yaml
import struct
import traceback
import math

from envds.core import envdsLogger
from envds.util.util import (
    time_to_next,
    get_datetime,
    get_datetime_string,
)
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class TAP2901(Sensor):
    """
    Tricolor Absorption Photometer (TAP) Model 2901 Driver.
    Implements absorption and transmission calculations based on the BMI System Manual.
    """

    def __init__(self, config=None, **kwargs):
        super(TAP2901, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "Brechtel_TAP2901_sensor_definition.json"

        # State dictionaries for log-ratio absorption calculations
        self.last_I_t = {} 
        
        # State variables for the automated White Filter Check (WFC)
        self.white_filter_mode = False
        self.wf_task = None
        self.wf_collection_buffer = {}

        # Wavelength Alias Map (populated from metadata in configure)
        self.wl_idx = {"red": 0, "green": 1, "blue": 2}

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
        super(TAP2901, self).configure()

        # Load configuration from file
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # Hardware connection properties for Model 2901
        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 57600, # cite: 529
                        "bytesize": 8,     # cite: 529
                        "parity": "N",     # cite: 529
                        "stopbit": 1,      # cite: 529
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

        # Initialize wavelength aliases from metadata
        try:
            labels = self.metadata["variables"]["wavelength_label"]["attributes"]["data"]
            self.wl_idx = {label.lower(): i for i, label in enumerate(labels)}
        except (KeyError, TypeError):
            pass

        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def["variables"].items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            settings=settings_def["variables"]
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
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            self.logger.error(f"Error adding interface: {e}")

    async def handle_interface_data(self, message: CloudEvent):
        await super(TAP2901, self).handle_interface_data(message)
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
        # Monitor for White Filter Check trigger
        trigger = self.settings.get_setting("run_white_filter_check")
        if trigger is True and not self.white_filter_mode:
            self.white_filter_mode = True
            self.wf_task = asyncio.create_task(self.perform_white_filter_check())

    async def perform_white_filter_check(self):
        """
        Orchestrates spot switching and collects baseline intensity ratios.
        Calculates a [channel][wavelength] array of ratios.
        """
        self.logger.info("White Filter Check triggered.")
        self.wf_collection_buffer = {str(s): {"r": [], "g": [], "b": []} for s in range(1, 9)}
        
        # Step through all 8 sample spots [cite: 529]
        for spot in range(1, 9):
            self.logger.info(f"WFC: Sampling spot {spot}/8...")
            await self.interface_send_data(data={"data": f"spot={spot}\r"})
            # Sample for stability interval (45s per spot block) [cite: 275]
            await asyncio.sleep(45) 

        # Calculate the 8x3 baseline matrix
        final_matrix = []
        for s in range(1, 9):
            d = self.wf_collection_buffer[str(s)]
            # Ratios for indices matching wl_idx: red, green, blue
            spot_ratios = [
                sum(d['r'])/len(d['r']) if d['r'] else 1.0,
                sum(d['g'])/len(d['g']) if d['g'] else 1.0,
                sum(d['b'])/len(d['b']) if d['b'] else 1.0
            ]
            final_matrix.append(spot_ratios)

        # Update persistent baseline and reset trigger
        self.settings.set_setting("last_calibrated_wf_ratios", final_matrix)
        self.settings.set_setting("run_white_filter_check", False)
        self.white_filter_mode = False
        self.wf_collection_buffer = {}
        self.logger.info("White Filter Check complete. Baselines saved.")
        await self.interface_send_data(data={"data": "spot=1\r"})

    async def sampling_monitor(self):
        """Controls instrument broadcast state."""
        while True:
            try:
                if self.sampling():
                    if not self.collecting:
                        await self.interface_send_data(data={"data": "show\r"}) # cite: 576
                        self.collecting = True
                else:
                    if self.collecting:
                        await self.interface_send_data(data={"data": "hide\r"}) # cite: 574
                        self.collecting = False
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"sampling_monitor error: {e}")
                await asyncio.sleep(1)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

    def calculate_absorption(self, I_t, I_prev, tau, flow_lpm):
        """
        Calculates aerosol absorption coefficient (sigma_ap) in Mm^-1.
        Formula: sigma_ap = 0.85 * (sigma_psap / 1.22) [cite: 111, 115]
        """
        if flow_lpm <= 0 or not I_prev or not I_t:
            return 0.0
            
        # Loading correction f(tau) [cite: 109]
        f_tau = 1.0 / (1.0796 * tau + 0.71)
        
        # Volumetric flow conversion and spot area (Azumi M371) [cite: 119, 165]
        flow_m3s = (flow_lpm * 0.001) / 60
        area = 2.5281E-5
        dt_sec = 1.0 # Assuming 1Hz interval [cite: 497]
        
        try:
            ln_ratio = math.log(I_prev / I_t)
            sigma_psap = f_tau * (area / (flow_m3s * dt_sec)) * ln_ratio # cite: 95
            sigma_ap = 0.85 * (sigma_psap / 1.22) # cite: 111, 115
            return sigma_ap * 1e6 # Convert to Mm^-1
        except (ValueError, ZeroDivisionError):
            return 0.0

    def default_parse(self, data):
        if not data: return None
        try:
            variables = list(self.config.metadata.variables.keys())
            variables.remove("time")

            record = self.build_data_record(meta=self.include_metadata)
            self.include_metadata = False
            
            record["timestamp"] = data.data["timestamp"]
            record["variables"]["time"]["data"] = data.data["timestamp"]
            
            parts = data.data["data"].split(",")
            if len(parts) < 49: return None # cite: 650

            # Parse base instrument fields
            for index, name in enumerate(variables):
                if name in record["variables"]:
                    instvar = self.config.metadata.variables[name]
                    vartype = "str" if instvar.type == "string" else instvar.type
                    
                    if index >= len(parts): continue
                    
                    # Detectors transmit intensities as reinterpreted hex floats [cite: 670, 672]
                    if 'intensity' in name:
                        try:
                            val = struct.unpack('!f', bytes.fromhex(parts[index].strip()))[0]
                        except (ValueError, TypeError): val = 0.0
                    else:
                        val = parts[index].strip()
                        
                    try:
                        if vartype == 'float':
                            record["variables"][name]["data"] = round(float(val), 2)
                        else:
                            record["variables"][name]["data"] = eval(vartype)(val)
                    except (ValueError, TypeError):
                        record["variables"][name]["data"] = "" if vartype in ["str", "char"] else None

            # --- ABSORPTION & TRANSMISSION CALCULATIONS ---
            try:
                active_spot = int(record["variables"]["active_spot"]["data"])
                if 0 < active_spot <= 8:
                    s_idx, ref_idx = active_spot - 1, (8 if active_spot % 2 != 0 else 9) # cite: 664

                    # Net Intensities (Raw - Dark) [cite: 109, 485]
                    def get_net(idx):
                        d = record["variables"].get(f"ch{idx}_dark_intensity", {}).get("data", 0) or 0
                        return [
                            (record["variables"].get(f"ch{idx}_{c}_intensity", {}).get("data", 0) or 0) - d
                            for c in ["red", "green", "blue"]
                        ]

                    s_net = get_net(s_idx)
                    r_net = get_net(ref_idx)

                    # Current intensity ratios I(t) [cite: 109]
                    I_t = [s / r if r > 0 else 1.0 for s, r in zip(s_net, r_net)]

                    # Record WFC baseline samples
                    if self.white_filter_mode:
                        self.wf_collection_buffer[str(active_spot)]['r'].append(I_t[0])
                        self.wf_collection_buffer[str(active_spot)]['g'].append(I_t[1])
                        self.wf_collection_buffer[str(active_spot)]['b'].append(I_t[2])

                    # Fetch persistent baselines for tau and sigma math
                    wf_baselines = self.settings.get_setting("last_calibrated_wf_ratios")
                    record["variables"]["white_filter_ratios"]["data"] = wf_baselines # cite: User Intent
                    
                    flow = record["variables"].get("flow_rate", {}).get("data", 0) or 0
                    prev = self.last_I_t.get(active_spot, {'r': I_t[0], 'g': I_t[1], 'b': I_t[2]})

                    # Color-specific mapping
                    for color, c_idx in self.wl_idx.items():
                        # Transmission (tau) [cite: 109]
                        tau = I_t[c_idx] / wf_baselines[s_idx][c_idx] if wf_baselines[s_idx][c_idx] > 0 else 1.0
                        record["variables"][f"{color}_transmission"]["data"] = round(tau, 4)
                        
                        # Absorption [cite: 95, 115]
                        sigma = self.calculate_absorption(I_t[c_idx], prev[color[0]], tau, flow)
                        record["variables"][f"{color}_absorption"]["data"] = round(sigma, 4)

                    self.last_I_t[active_spot] = {'r': I_t[0], 'g': I_t[1], 'b': I_t[2]}

            except (KeyError, IndexError, ZeroDivisionError) as e:
                self.logger.debug(f"Math Error: {e}")

            return record
        except Exception as e:
            self.logger.error(f"Parse error: {e}")
            self.logger.debug(traceback.format_exc())
        return None

# --- APP BOILERPLATE ---
class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def main(server_config: ServerConfig = None):
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

    await inst.shutdown()

if __name__ == "__main__":
    asyncio.run(main(ServerConfig()))