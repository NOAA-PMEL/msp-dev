import asyncio
import json
import signal
import sys
import os
import logging
import yaml
import traceback
from envds.core import envdsLogger
from envds.daq.controller import Controller, ControllerMetadata, ControllerConfig 
from envds.daq.event import DAQEvent
from pydantic import BaseModel
from envds.util.util import time_to_next

task_list = []

class ShellyPro4PM(Controller):
    def __init__(self, config=None, **kwargs):
        super(ShellyPro4PM, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_client_module = "envds.daq.clients.mqtt_client"
        self.default_client_class = "MQTT_Client"
        self.default_client_host = "mqtt.default"
        self.default_client_port = 1883        

        self.controller_id_prefix = "shellypro4pm"
        self.polling_task = None

        self.controller_definition_file = "Shelly_ShellyPro4PM_controller_definition.json"

        try:            
            with open(self.controller_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("controller_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.recv_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(ShellyPro4PM, self).configure()

        try:
            try:
                with open("/app/config/controller.conf", "r") as f:
                    conf = yaml.safe_load(f)
            except FileNotFoundError:
                conf = {"uid": "UNKNOWN", "attributes": {}}

            host = conf.get("host", "localhost")
            port = conf.get("port", 80)
            
            client_module = conf.get("client_module", self.default_client_module)
            client_class = conf.get("client_class", self.default_client_class)
            client_host = conf.get("client_host", self.default_client_host)
            client_port = conf.get("client_port", self.default_client_port)
            self.controller_id_prefix = conf.get("controller_id_prefix", "shellypro4pm")
            
            client_subscriptions_list = conf.get("client_subscriptions", "")
            client_subscriptions = client_subscriptions_list.split(",") if client_subscriptions_list else []

            status_sub = f"{self.controller_id_prefix}/status"
            if status_sub not in client_subscriptions:
                client_subscriptions.append(status_sub)

            attrs = self.metadata["attributes"]

            if "attributes" in conf:
                for name, val in conf["attributes"].items():
                    if name in attrs:
                        attrs[name]["data"] = val

            settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
            for name, setting in settings_def.get("variables", {}).items():
                requested = setting["attributes"].get("default_value", {}).get("data")
                if "settings" in conf and name in conf["settings"]:
                    requested = conf["settings"][name]
                self.settings.set_setting(name, requested=requested)

            meta = ControllerMetadata(
                attributes=self.metadata["attributes"],
                dimensions=self.metadata["dimensions"],
                variables=self.metadata["variables"],
                settings=settings_def.get("variables", {})
            )

            daq_namespace = self.core_settings.namespace_prefix if hasattr(self, 'core_settings') else conf.get("daq_id", "default")

            self.config = ControllerConfig(
                make=self.metadata["attributes"]["make"]["data"],
                model=self.metadata["attributes"]["model"]["data"],
                serial_number=self.metadata["attributes"]["serial_number"]["data"],
                format_version=self.metadata["attributes"]["format_version"]["data"],
                metadata=meta,
                host=host,
                port=port,
                daq_id=daq_namespace
            )

            self.client_config = {
                "client_module": client_module,
                "client_class": client_class,
                "properties": {
                    "host": client_host,
                    "port": client_port,
                    "subscriptions": client_subscriptions,
                }
            }

            self.logger.debug("configure", extra={"conf": conf, "self.config": self.config})
        except Exception as e:
            self.logger.error("ShellyPro4PM:configure", extra={"error": str(e)})

    def active(self):
        """Bypass for base class self.sampling() to check for 'active' instead of 'sampling'"""
        state_obj = self.settings.get_setting("sampling_state")
        state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
        return str(state).lower() == "active"

    async def get_status_loop(self):
        """Actively requests status updates from the Shelly over MQTT"""
        while True:
            data = {
                "path": f"{self.controller_id_prefix}/command",
                "message": "status_update"
            }
            self.logger.debug("get_status_loop", extra={"payload": data})
            await self.send_data(data)
            await asyncio.sleep(time_to_next(5))
            
    async def sampling_monitor(self):
        """Watchdog to manage active MQTT polling loop based on UI state"""
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.active() and state_str == "active":
                    if self.polling_task is None or self.polling_task.done():
                        self.logger.info("Starting ShellyPro4PM status polling loop.")
                        self.polling_task = asyncio.create_task(self.get_status_loop())
                else:
                    if self.polling_task and not self.polling_task.done():
                        self.logger.info("Stopping ShellyPro4PM status polling loop.")
                        self.polling_task.cancel()
                        self.polling_task = None
                        
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def set_channel_power(self, channel, state):
        if isinstance(state, str):
            state = 1 if state.lower() in ["on", "yes", "1"] else 0 
            
        cmd = "on" if state else "off"
        data = {
            "path": f"{self.controller_id_prefix}/command/switch:{channel}",
            "message": cmd
        }
        self.logger.debug("set_channel_power", extra={"payload": data})
        await self.send_data(data)

    async def recv_data_loop(self):
        while True:
            try:
                data = await self.client_recv_buffer.get()
                self.logger.debug("recv_data_loop - incoming data", extra={"recv_data": data})
                
                record = self.default_parse(data)
                
                if record:
                    self.collecting = True

                if record and self.active():
                    event = DAQEvent.create_controller_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"{self.get_id_as_topic()}/controller/data/update"
                    event["destpath"] = destpath
                    self.logger.debug("recv_data_loop - publishing event", extra={"destpath": destpath})
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("recv_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.01)

    def default_parse(self, data):
        if not data: return None
        try:
            # Safely unpack the MQTT payload mapping
            raw_payload = data if isinstance(data, dict) else getattr(data, "data", {})
            timestamp = raw_payload.get("timestamp")
            shelly_data = raw_payload.get("data", {})
            
            if not timestamp or not shelly_data:
                return None

            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            record["timestamp"] = timestamp
            if "time" in record["variables"]:
                record["variables"]["time"]["data"] = timestamp

            parsed_any = False
            
            # Map through available MQTT channels
            for input_key in ["switch:0", "switch:1", "switch:2", "switch:3"]:
                if input_key in shelly_data:
                    parsed_any = True
                    channel_data = shelly_data[input_key]
                    
                    channel = channel_data.get("id")
                    output = channel_data.get("output")
                    voltage = channel_data.get("voltage")
                    current = channel_data.get("current")
                    power = channel_data.get("apower")
                    
                    temp_dict = channel_data.get("temperature", {})
                    temperature = temp_dict.get("tC") if isinstance(temp_dict, dict) else None

                    # Parse values if present
                    if temperature is not None and "temperature" in record["variables"]:
                        record["variables"]["temperature"]["data"] = round(float(temperature), 2)
                        
                    if voltage is not None and f"channel_{channel}_voltage" in record["variables"]:
                        record["variables"][f"channel_{channel}_voltage"]["data"] = round(float(voltage), 2)
                        
                    if current is not None and f"channel_{channel}_current" in record["variables"]:
                        record["variables"][f"channel_{channel}_current"]["data"] = round(float(current), 3)
                        
                    if power is not None and f"channel_{channel}_power_meas" in record["variables"]:
                        record["variables"][f"channel_{channel}_power_meas"]["data"] = round(float(power), 2)

                    # Update actual state of channel output back to settings manager
                    if output is not None and channel is not None:
                        name = f"channel_{channel}_power"
                        actual = 1 if output else 0
                        self.settings.set_actual(name=name, actual=actual)

            if not parsed_any:
                self.logger.debug("default_parse - no switch data found in payload", extra={"payload": shelly_data})
                return None

            return record

        except Exception as e:
            self.logger.error("default_parse - critical error", extra={"error": str(e), "data": data})
            return None

    async def send_data(self, data):
        try:
            self.logger.debug("send_data", extra={"payload": data})
            if self.client:
                await self.client.send_to_client(data)
        except Exception as e:
            self.logger.error("send_data error", extra={"error": str(e)})

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health(): 
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    try:
                        setting_obj = self.settings.get_setting(name)
                        target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                        
                        if name == "sampling_state":
                            self.settings.set_actual(name, target_val)
                            
                        elif name in ["channel_0_power", "channel_1_power", "channel_2_power", "channel_3_power"]:
                            ch = self.metadata["variables"][name]["attributes"]["channel"]["data"]
                            await self.set_channel_power(ch, target_val)

                    except Exception as e:
                        self.logger.error("settings_check error", extra={"reason": str(e)})

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(interface):
    if interface:
        await interface.shutdown()
    for task in task_list:
        if task:
            task.cancel()

async def main(server_config: ServerConfig = None):
    if server_config is None:
        server_config = ServerConfig()

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger("interface::ShellyPro4PM")

    iface = ShellyPro4PM()
    iface.run()
    iface.enable()
    logger.debug("Starting Shelly Pro 4PM Controller")

    event_loop = asyncio.get_event_loop()
    global do_run 
    do_run = True
    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        logger.debug("ShellyPro4PM.run", extra={"do_run": do_run})
        await asyncio.sleep(1)

    print("starting shutdown...")
    await shutdown(iface)
    print("done.")

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