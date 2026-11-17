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
from cloudevents.http import CloudEvent
from pydantic import BaseModel
from envds.daq.types import DAQEventType as det
import traceback

class TD3105(Sensor):
    def __init__(self, config=None, **kwargs):
        super(TD3105, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "Brechtel_TD3105_sensor_definition.json"
        
        # Expected sequence of variables from the "read" command
        self.read_vars = [
            'stpt_temp', 'stpt_time', 'stpt_byps', 'prog_nmbr', 'ctdwn_tmr', 'err_rpt',
            'tmp_fr_sf', 'tmp_bk_sf', 'tmp_tb_sf', 'tmp_dn_ex', 'tmp_cl_fn', 'tmp_board',
            'ht_pwr_fr', 'ht_pwr_bk', 'ht_pwr_dn', 'fn_pwr_ht', 'fn_pwr_dn'
        ]

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)
            
        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        super(TD3105, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # Typical RS232 configuration
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
                        "decode-errors": "ignore",
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
            settings=dict()
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
        await super(TD3105, self).handle_interface_data(message)
        if message.get("type") == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces.get("default", {}).get("path")
                if path_id == iface_path or path_id == "default":
                    await self.default_data_buffer.put(message)
            except KeyError as e:
                self.logger.error("interface_route_error", extra={"missing_key": str(e)})

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "td_mode", "set_auto_program"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        need_start = True
        scan_time = getattr(self, 'sampling_frequency', 5) # Default 5s polling
        
        await asyncio.sleep(2)
        
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                # Process Settings Updates
                mode_obj = self.settings.get_setting("td_mode")
                if mode_obj and mode_obj.get("requested") != mode_obj.get("actual"):
                    mode_val = int(mode_obj.get("requested", 2))
                    await self.interface_send_data(data={"data": f"td_mode={mode_val}\r\n"})
                    self.settings.set_actual("td_mode", mode_val)

                # Convert Base-10 Auto Program string to Hex command and send
                auto_prog_obj = self.settings.get_setting("set_auto_program")
                if auto_prog_obj and auto_prog_obj.get("requested") != auto_prog_obj.get("actual"):
                    prog_str = auto_prog_obj.get("requested", "none")
                    if prog_str != "none":
                        try:
                            # Parse "prog,temp,time,bypass"
                            p_parts = [int(p.strip()) for p in prog_str.split(",")]
                            if len(p_parts) == 4:
                                hex_prog = hex(p_parts[0])[2:]
                                hex_temp = hex(p_parts[1])[2:]
                                hex_time = hex(p_parts[2])[2:]
                                hex_byps = hex(p_parts[3])[2:]
                                cmd = f"{hex_prog}={hex_temp},{hex_time},{hex_byps}\r\n"
                                await self.interface_send_data(data={"data": cmd})
                                self.logger.debug("Set Auto Program", extra={"cmd": cmd.strip()})
                        except Exception as e:
                            self.logger.error("Auto Program Parse Error", extra={"err": str(e), "input": prog_str})
                    self.settings.set_actual("set_auto_program", prog_str)

                # Send Polling Request
                if self.sampling() and state_str == "sampling":
                    await self.interface_send_data(data={"data": "read\r\n"})
                    need_start = False
                else:
                    if not need_start:
                        # Instrument handles its own Off mode via td_mode=0
                        self.logger.debug("Sampling stopped in settings.")
                        need_start = True
                        
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            
            await asyncio.sleep(scan_time)

    async def default_data_loop(self):
        while True:
            try:
                data_msg = await self.default_data_buffer.get()
                record = self.default_parse(data_msg)
                
                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("data_loop_error", extra={"err_detail": str(e), "trace": traceback.format_exc()})
            await asyncio.sleep(0.001)

    def default_parse(self, data_obj):
        if not data_obj: 
            return None
            
        try:
            raw_payload = data_obj.data if isinstance(data_obj.data, dict) else {}
            raw_str = raw_payload.get("data", "").strip()
            
            if not raw_str or "read" in raw_str:
                return None
                
            parts = [p.strip() for p in raw_str.split(",")]
            
            # Ensure it looks like the expected 17-value response
            if len(parts) >= 17:
                v_types = ["main", "setting"] if self.include_metadata else ["main"]
                record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
                self.include_metadata = False
                
                record["timestamp"] = raw_payload.get("timestamp")
                if "time" in record.get("variables", {}):
                    record["variables"]["time"]["data"] = raw_payload.get("timestamp")

                self._map_scalars(record, self.read_vars, parts[:17])
                
                return record
                
        except Exception as e:
            self.logger.error("parse_failure", extra={"err_detail": str(e), "trace": traceback.format_exc()})
            
        return None

    def _map_scalars(self, record, keys, values):
        for i, name in enumerate(keys):
            if name in record["variables"] and i < len(values):
                instvar = self.metadata["variables"][name]
                vartype = instvar["type"]
                try:
                    val = values[i]
                    record["variables"][name]["data"] = int(val) if vartype == "int" else (float(val) if vartype == "float" else val)
                except ValueError:
                    record["variables"][name]["data"] = None


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
    logger = logging.getLogger(f"Brechtel::TD3105::{sn}")
    
    inst = TD3105()
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