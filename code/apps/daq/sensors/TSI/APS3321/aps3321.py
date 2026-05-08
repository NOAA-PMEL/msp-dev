import asyncio
import signal
import sys
import os
import logging
import yaml
import json
from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

task_list = []

class APS3321(Sensor):
    def __init__(self, config=None, **kwargs):
        super(APS3321, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.sensor_definition_file = "TSI_APS3321_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting.")
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(APS3321, self).configure()
        self.logger.info("Configuring TSI APS 3321")
        
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.warning("sensor.conf not found, using default configuration")
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {"baudrate": 38400, "bytesize": 7, "parity": "E", "stopbit": 1},
                    "read-properties": {"read-method": "readuntil", "read-terminator": "\r", "decode-errors": "strict", "send-method": "ascii"},
                }
            }
        }
        
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        iface[propname] = prop

        meta = DeviceMetadata(attributes=self.metadata["attributes"], dimensions=self.metadata["dimensions"], variables=self.metadata["variables"], settings=dict())
        self.config = DeviceConfig(make="TSI", model="APS3321", serial_number=conf.get("serial_number", "UNKNOWN"), metadata=meta, interfaces=conf.get("interfaces", {}), daq_id="default")
        
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError): 
            self.logger.debug("No format_version found in metadata attributes")

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)
                self.logger.debug("Interface added", extra={"interface_name": str(name)})

    async def handle_interface_data(self, message: CloudEvent):
        await super(APS3321, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                if message["path_id"] == self.config.interfaces["default"]["path"]:
                    await self.default_data_buffer.put(message)
            except KeyError: 
                self.logger.warning("Received data with missing or mismatched path_id", extra={"path_id": str(message.get("path_id"))})

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    self.logger.info("MQTT Settings Request", extra={"setting": str(name), "target_val": str(target_val)})
                    
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        start_commands = ["S0\r", "U1\r", "UD1\r", "UY1\r", "S1\r"]
        need_start = True
        self.logger.info("Sampling monitor initialized")
        await asyncio.sleep(2)
        
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        self.logger.info("Initiating APS continuous sampling sequence")
                        for cmd in start_commands:
                            await self.interface_send_data(data={"data": cmd})
                            self.logger.debug("Sent hardware command", extra={"command": str(cmd).strip()})
                            await asyncio.sleep(0.5)
                        need_start = False
                        self.collecting = True
                else:
                    if not need_start:
                        self.logger.info("Halting APS sampling", extra={"requested_state": state_str})
                        await self.interface_send_data(data={"data": "S0\r"})
                        self.collecting = False
                        need_start = True
            except Exception as e:
                self.logger.error("Error in sampling_monitor", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def default_data_loop(self):
        self.logger.info("Default data loop initialized")
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                
                if record and self.sampling():
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    
                    self.logger.debug("Publishing DAQ data event", extra={
                        "destpath": str(destpath), 
                        "record_timestamp": str(record.get("timestamp"))
                    })
                    await self.send_message(event)
                    
                elif not record:
                    self.logger.warning("Parsed record returned empty")
                    
            except Exception as e: 
                self.logger.error("Error in default_data_loop", extra={"error": str(e)})
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if not data: 
            self.logger.debug("default_parse received empty data object")
            return None
            
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            
            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            raw_str = raw_payload.get("data", "")
            
            self.logger.debug("Parsing raw serial string", extra={"raw_string_head": repr(raw_str)[:50]})
            parts = raw_str.strip().split(",")
            
            if ',D,' in raw_str:
                if "particle_counts" in record["variables"]:
                    record["variables"]["particle_counts"]["data"] = [int(x) if x else 0 for x in parts[11:]]
            elif ',Y,' in raw_str:
                if len(parts) >= 18:
                    if "bpress" in record["variables"]: record["variables"]["bpress"]["data"] = float(parts[2])
                    if "tflow" in record["variables"]: record["variables"]["tflow"]["data"] = float(parts[3])
                    if "sflow" in record["variables"]: record["variables"]["sflow"]["data"] = float(parts[4])
                    if "itemp" in record["variables"]: record["variables"]["itemp"]["data"] = float(parts[14])
                else:
                    self.logger.warning("Auxiliary (Y) record too short to parse", extra={"length": str(len(parts))})
            else:
                self.logger.debug("Ignoring unmapped or incomplete record type")
                return None
                
            return record
        except Exception as e: 
            self.logger.error("Error in default_parse", extra={"error": str(e)})
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    
    inst = APS3321()
    inst.run()
    await asyncio.sleep(2)
    inst.start()
    
    do_run = True
    while do_run: 
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main(ServerConfig()))