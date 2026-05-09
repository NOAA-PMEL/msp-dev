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

class SanAce40(Operational):
    def __init__(self, config=None, **kwargs):
        super(SanAce40, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.polling_task = None
        self.sampling_interval = 1
        self.polling_mode = "unpolled" # Default, overridden by config

        self.last_read_timestamp = None
        self.last_read_count = None

        self.operational_definition_file = "SanyoDenki_SanAce40_operational_definition.json"

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
        super(SanAce40, self).configure()

        try:
            # Note: Looking for operational.conf instead of sensor.conf
            with open("/app/config/operational.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        env_polling_mode = os.environ.get("POLLING_MODE")
        if env_polling_mode:
            self.polling_mode = env_polling_mode.lower()
        elif "polling_mode" in conf:
            self.polling_mode = str(conf["polling_mode"]).lower()

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

        try:
            self.device_format_version = self.config.metadata.attributes.get("format_version").data
        except (KeyError, AttributeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    def check_fan_speed_sp(self, data):
        try:
            setting_obj = self.settings.get_setting("fan_speed_sp")
            requested_sp = setting_obj.get("requested", 0.0)
            
            raw_payload = data if isinstance(data, dict) else getattr(data, "data", {})
            duty_cycle = raw_payload.get("data", {}).get("duty_cycle")
            
            if duty_cycle is not None:
                # Unidirectional fan: direct 1:1 mapping (0-100% SP = 0-100% DC)
                sp = float(duty_cycle)
                
                # If within +/- 5% of target, mark as achieved
                if (requested_sp - 5.0) < sp < (requested_sp + 5.0):
                    self.settings.set_actual("fan_speed_sp", actual=requested_sp)
        except Exception as e:
            self.logger.error("check_fan_speed_sp error", extra={"error": str(e)})

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    if name == "sampling_state":
                        self.settings.set_actual(name, target_val)
                    elif name == "fan_speed_sp":
                        try:
                            sp = float(target_val)
                            # Unidirectional fan: direct 1:1 mapping
                            pwm_data = sp
                            await self.interface_send_data(data={"data": {"pwm-data": pwm_data}}, path_id="fan_speed_sp")
                        except Exception as e:
                            self.logger.error("settings_check pwm error", extra={"error": str(e)})

    async def handle_interface_data(self, message: CloudEvent):
        await super(SanAce40, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                default_path = self.config.interfaces.get("default", {}).get("path")
                pwm_path = self.config.interfaces.get("fan_speed_sp", {}).get("path")
                
                if path_id == default_path:
                    await self.default_data_buffer.put(message)
                elif path_id == pwm_path:
                    self.check_fan_speed_sp(message.data)
            except KeyError:
                pass

    async def sampling_monitor(self):
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if self.polling_mode == "polled":
                        if self.polling_task is None or self.polling_task.done():
                            self.logger.info("Starting SanAce40 polling loop.")
                            self.polling_task = asyncio.create_task(self.polling_loop())
                    elif self.polling_mode == "unpolled":
                        if self.polling_task and not self.polling_task.done():
                            self.logger.info("Stopping SanAce40 polling loop (Unpolled mode active).")
                            self.polling_task.cancel()
                            self.polling_task = None
                else:
                    if self.polling_task and not self.polling_task.done():
                        self.logger.info("Stopping SanAce40 polling loop.")
                        self.polling_task.cancel()
                        self.polling_task = None
                        
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)

    async def polling_loop(self):
        while True:
            try:
                await self.interface_send_data(data={}, path_id="default")
            except Exception as e:
                self.logger.error("polling_loop error", extra={"error": str(e)})
            await asyncio.sleep(self.sampling_interval)

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.logger.debug("default_data_loop - incoming data", extra={"data": data})
                
                record = self.default_parse(data)
                self.logger.debug("default_data_loop - parsed record", extra={"record": record})
                
                if record:
                    self.collecting = True

                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    self.logger.debug("default_data_loop - publishing event", extra={"destpath": event["destpath"]})
                    await self.send_message(event)

            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            timestamp = raw_payload.get("timestamp")
            iface_data = raw_payload.get("data", {})
            
            self.logger.debug(
                "default_parse - raw iface_data received", 
                extra={"iface_data": iface_data, "timestamp": timestamp}
            )
            
            if not timestamp or "data" not in iface_data:
                self.logger.warning(
                    "default_parse - missing timestamp or data key", 
                    extra={"raw_payload": raw_payload}
                )
                return None

            current_read_timestamp = string_to_datetime(timestamp)
            
            try:
                dataRead = float(iface_data["data"])  # Raw pulse count
            except (ValueError, TypeError) as e:
                self.logger.warning(
                    "default_parse - failed to cast pulse count to float", 
                    extra={"error": str(e), "data_value": iface_data.get("data")}
                )
                return None

            # Require two points to calculate RPM over time
            if not self.last_read_timestamp or self.last_read_count is None:
                self.logger.debug("default_parse - initializing first data point, skipping RPM calculation")
                self.last_read_timestamp = current_read_timestamp
                self.last_read_count = dataRead
                return None

            elapsed_time = (current_read_timestamp - self.last_read_timestamp).total_seconds()
            
            if elapsed_time > 0:
                # 2 pulses per revolution
                revs = (dataRead - self.last_read_count) / 2.0
                speed = 60.0 * revs / elapsed_time
                
                record["timestamp"] = timestamp
                if "time" in record["variables"]:
                    record["variables"]["time"]["data"] = timestamp
                if "fan_speed" in record["variables"]:
                    record["variables"]["fan_speed"]["data"] = round(speed, 3)
            else:
                self.logger.warning(
                    "default_parse - zero or negative elapsed time", 
                    extra={"elapsed_time": elapsed_time, "current_ts": current_read_timestamp, "last_ts": self.last_read_timestamp}
                )
                # Drop out early to prevent updating last_read_timestamp if time went backwards
                return None

            self.last_read_timestamp = current_read_timestamp
            self.last_read_count = dataRead

            return record

        except Exception as e:
            self.logger.error("default_parse - critical error", extra={"error": str(e), "data": data})
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
        with open("/app/config/operational.conf", "r") as f:
            conf = yaml.safe_load(f)
            try:
                sn = conf["serial_number"]
            except KeyError:
                pass
    except FileNotFoundError:
        pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"SanyoDenki::SanAce40::{sn}")

    logger.debug("Starting SanAce40")
    inst = SanAce40()
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
        logger.debug("SanAce40.run", extra={"do_run": do_run})
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