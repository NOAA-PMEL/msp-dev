import asyncio
import logging
import yaml
import json
import sys
import traceback
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from envds.daq.types import DAQEventType as det

class CCNBase(Sensor):
    def __init__(self, config=None, **kwargs):
        # Child classes MUST set self.sensor_definition_file before calling super()
        super(CCNBase, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue()
        
        # Buffer to hold the 'H' line components until the 'C' line arrives
        self.h_buffer = None
        self.h_timestamp = None

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Missing sensor definition file: {self.sensor_definition_file}")
            sys.exit(1)
            
        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        super(CCNBase, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # Expected Normal Mode output (57600, 8-N-1, \r delimited line endings)
        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 57600,
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
        await super(CCNBase, self).handle_interface_data(message)
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
                    if name in ["sampling_state", "Delta_T_A_set", "Delta_T_B_set"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        need_start = True
        await asyncio.sleep(2)
        
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                delta_t_A_obj = self.settings.get_setting("Delta_T_A_set")
                if delta_t_A_obj and delta_t_A_obj.get("requested") != delta_t_A_obj.get("actual"):
                    self.logger.info("Placeholder: Send Delta_T_A command via serial here.", extra={"val": delta_t_A_obj.get("requested")})
                    self.settings.set_actual("Delta_T_A_set", delta_t_A_obj.get("requested"))

                delta_t_B_obj = self.settings.get_setting("Delta_T_B_set")
                if delta_t_B_obj and delta_t_B_obj.get("requested") != delta_t_B_obj.get("actual"):
                    self.logger.info("Placeholder: Send Delta_T_B command via serial here.", extra={"val": delta_t_B_obj.get("requested")})
                    self.settings.set_actual("Delta_T_B_set", delta_t_B_obj.get("requested"))

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        self.logger.debug("Sampling activated in settings.")
                        need_start = False
                else:
                    if not need_start:
                        self.logger.debug("Sampling stopped in settings.")
                        need_start = True
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

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
            
            if not raw_str:
                return None
                
            parts = [p.strip() for p in raw_str.split(",")]
            line_type = parts[0]
            
            if line_type == "H":
                self.h_timestamp = raw_payload.get("timestamp")
                self.h_buffer = parts[2:] # Strip "H" and timestamp
                return None

            elif line_type == "C":
                if not self.h_buffer:
                    self.logger.warning("Received C line without preceding H line. Dropping.")
                    return None
                    
                v_types = ["main", "coordinate"]
                if self.include_metadata:
                    v_types.extend(["setting"])
                    
                record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
                self.include_metadata = False
                
                record["timestamp"] = self.h_timestamp
                if "time" in record.get("variables", {}):
                    record["variables"]["time"]["data"] = self.h_timestamp

                # Map H Line using lengths defined by child class
                if len(self.h_buffer) >= self.expected_h_len:
                    self._map_scalars(record, self.h_vars_A, self.h_buffer[0:16])
                    
                    if hasattr(self, 'h_vars_B') and self.h_vars_B:
                        self._map_scalars(record, self.h_vars_B, self.h_buffer[16:32])
                        self._map_scalars(record, ['Alarm_Code'], [self.h_buffer[32]])
                    else:
                        self._map_scalars(record, ['Alarm_Code'], [self.h_buffer[16]])
                else:
                    self.logger.warning(f"H line array length < {self.expected_h_len}. Dropping.")
                    self.h_buffer = None
                    self.h_timestamp = None
                    return None

                # Map C Line using lengths defined by child class
                c_data = parts[1:] # Strip 'C'
                if len(c_data) >= self.expected_c_len:
                    self._map_scalars(record, self.c_vars_A, c_data[0:3])
                    self._map_array(record, 'Bin_Array_A', c_data[3:23])
                    
                    if hasattr(self, 'c_vars_B') and self.c_vars_B:
                        self._map_scalars(record, self.c_vars_B, c_data[23:26])
                        self._map_array(record, 'Bin_Array_B', c_data[26:46])
                else:
                    self.logger.warning(f"C line array length < {self.expected_c_len}. Dropping.")
                    self.h_buffer = None
                    self.h_timestamp = None
                    return None

                self.h_buffer = None
                self.h_timestamp = None
                return record

        except Exception as e:
            self.logger.error("parse_failure", extra={"err_detail": str(e), "trace": traceback.format_exc()})
            self.h_buffer = None
            
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

    def _map_array(self, record, key, values):
        if key in record["variables"]:
            clean_arr = []
            for v in values:
                try:
                    clean_arr.append(int(v))
                except ValueError:
                    clean_arr.append(None)
            record["variables"][key]["data"] = clean_arr