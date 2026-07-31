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

class KLD20S(Operational):
    def __init__(self, config=None, **kwargs):
        super(KLD20S, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.operational_definition_file = "KLD_KLD20S_operational_definition.json"

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
        super(KLD20S, self).configure()

        try:
            with open("/app/config/operational.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

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

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    def check_valve_state(self, data):
        try:
            setting_obj = self.settings.get_setting("valve_state")
            if not setting_obj: return
            
            requested_raw = setting_obj.get("requested", 0)
            requested_state = int(requested_raw) if requested_raw is not None else 0
            
            raw_payload = data if isinstance(data, dict) else getattr(data, "data", {})
            level1_data = raw_payload.get("data", {})
            
            # Read back from LabJack to confirm state
            state_fb = level1_data.get("data")
            if state_fb is not None:
                if int(state_fb) == requested_state:
                    self.settings.set_actual("valve_state", actual=requested_raw)
        except Exception as e:
            self.logger.error("check_valve_state error", extra={"error": str(e)})

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    if name == "sampling_state":
                        self.settings.set_actual(name, target_val)
                    elif name == "valve_state":
                        try:
                            state = int(target_val)
                            
                            # --- THE UNIVERSAL PAYLOAD ---
                            # Packs both analog (DAC) and digital (DIO) commands.
                            # The tx.py client will only read the keys it cares about!
                            payload = {
                                "ouput_volts": 5.0 if state == 1 else 0.0, # Used by DACClient
                                "dio_mode": "output",                      # Used by DIOClient
                                "do_state": state                          # Used by DIOClient
                            }
                            
                            await self.interface_send_data(data=payload, path_id="valve_control")
                            
                            # Optimistically set the actual state so the UI updates instantly
                            self.settings.set_actual("valve_state", actual=state)
                        except Exception as e:
                            self.logger.error("settings_check valve_state error", extra={"error": str(e)})

    async def handle_interface_data(self, message: CloudEvent):
        await super(KLD20S, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                valve_path = self.config.interfaces.get("valve_control", {}).get("path")
                
                if path_id == valve_path:
                    self.check_valve_state(message.data)
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass

    async def sampling_monitor(self):
        await asyncio.sleep(2)
        while True:
            # Polling is handled strictly by settings changes, so we just idle here
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
            await asyncio.sleep(0.1)

    # def default_parse(self, data):
    #     if not data: return None
    #     try:
    #         v_types = ["main", "setting"] if self.include_metadata else ["main"]
    #         record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
    #         self.include_metadata = False

    #         raw_payload = data.data if isinstance(data.data, dict) else {}
    #         timestamp = raw_payload.get("timestamp")
            
    #         if not timestamp: return None

    #         record["timestamp"] = timestamp
    #         if "time" in record["variables"]:
    #             record["variables"]["time"]["data"] = timestamp

    #         return record

    #     except Exception as e:
    #         self.logger.error("default_parse - critical error", extra={"error": str(e)})
    #         return None

    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            timestamp = raw_payload.get("timestamp")
            
            if not timestamp: return None

            record["timestamp"] = timestamp
            if "time" in record["variables"]:
                record["variables"]["time"]["data"] = timestamp
                
            if "valve_state" in record["variables"]:
                sp_setting = self.settings.get_setting("valve_state")
                if sp_setting:
                    sp_val = sp_setting.get("actual") if isinstance(sp_setting, dict) and "actual" in sp_setting else (sp_setting.get("requested") if isinstance(sp_setting, dict) else sp_setting)
                    if sp_val is not None:
                        try:
                            record["variables"]["valve_state"]["data"] = int(float(sp_val))
                        except (ValueError, TypeError):
                            record["variables"]["valve_state"]["data"] = sp_val

            return record

        except Exception as e:
            self.logger.error("default_parse - critical error", extra={"error": str(e)})
            return None
        
class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()
    for task in task_list:
        if task: task.cancel()

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()

    sn = "UNKNOWN"
    try:
        with open("/app/config/operational.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "UNKNOWN")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"KLD::KLD20S::{sn}")

    inst = KLD20S()
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

    while do_run: await asyncio.sleep(1)
    await shutdown(inst)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    
    config = ServerConfig()
    try:
        index = sys.argv.index("--host")
        config.host = sys.argv[index + 1]
    except (ValueError, IndexError): pass
    asyncio.run(main(config))