import asyncio
import binascii
import signal
from struct import unpack

# import uvicorn
# from uvicorn.config import LOGGING_CONFIG
import sys
import os
import logging

# from logfmter import Logfmter
import logging.config

# from pydantic import BaseSettings, Field
# import json
import yaml
import random
from envds.core import envdsLogger  # , envdsBase, envdsStatus
from envds.util.util import (
    # get_datetime_format,
    time_to_next,
    get_datetime,
    get_datetime_string,
)

from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata

# from envds.event.event import create_data_update, create_status_update
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from envds.message.message import Message

# from envds.exceptions import envdsRunTransitionException

# from typing import Union
# from cloudevents.http import CloudEvent, from_dict, from_json
# from cloudevents.conversion import to_json, to_structured
from cloudevents.http import CloudEvent

from pydantic import BaseModel
import json

# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class SDP810(Sensor):
    def __init__(self, config=None, **kwargs):
        super(SDP810, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=1000)
        self.polling_task = None
        self.sampling_interval = 1
        
        self.i2c_address = "25"
        
        self.sensor_definition_file = "Sensirion_SDP810_sensor_definition.json"

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
        super(SDP810, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        sensor_iface_properties = {
            "default": {
                "sensor-interface-properties": {
                    "connection-properties": {},
                    "read-properties": {
                        "read-method": "readline",
                        "decode-errors": "strict",
                        "send-method": "ascii"
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

        # Pull I2C address from config if it exists
        if "i2c_address" in conf:
            self.i2c_address = str(conf["i2c_address"])

        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_message(self, message: Message):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(SDP810, self).handle_interface_data(message)
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
                    if name in ["sampling_state"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if self.polling_task is None or self.polling_task.done():
                        self.logger.info("Starting SDP810 I2C polling loop.")
                        self.polling_task = asyncio.create_task(self.polling_loop())
                else:
                    if self.polling_task and not self.polling_task.done():
                        self.logger.info("Stopping SDP810 I2C polling loop.")
                        self.polling_task.cancel()
                        self.polling_task = None
                        
            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})
            await asyncio.sleep(1)


    async def polling_loop(self):
        i2c_write = {
            "address": self.i2c_address,
            "data": "00"
        }
        i2c_read = {
            "address": self.i2c_address,
            "read-length": 4,
            "delay-ms": 50 
        }
        data = {
            "data": {
                "i2c-write": i2c_write,
                "i2c-read": i2c_read
            }
        }

        while True:
            try:
                await self.interface_send_data(data=data)
            except Exception as e:
                self.logger.error("polling_loop error", extra={"error": str(e)})
            await asyncio.sleep(time_to_next(self.sampling_interval))


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
            await asyncio.sleep(0.01)


    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            if "time" in record.get("variables", {}):
                record["variables"]["time"]["data"] = raw_payload.get("timestamp")

            iface_data = raw_payload.get("data", {})
            self.logger.debug("default_parse - raw iface_data received", extra={"iface_data": iface_data})
            
            address = str(iface_data.get("address", ""))
            
            # Reject data if it's from a different I2C address
            if not address or address != str(self.i2c_address):
                self.logger.debug(
                    "default_parse - I2C address mismatch or missing", 
                    extra={"received_address": address, "expected_address": self.i2c_address}
                )
                return None
                
            dataRead = iface_data.get("data", [])
            
            # Ensure we have the 4 bytes required for decoding
            if not isinstance(dataRead, list) or len(dataRead) < 9:
                self.logger.warning(
                    "default_parse - incomplete I2C data frame", 
                    extra={
                        "dataRead_length": len(dataRead) if isinstance(dataRead, list) else "not_a_list", 
                        "expected": 9
                    }
                )
                return None

            try:
                # IST specific hex-to-float decoding logic
                raw_dp = (dataRead[0] << 8) | dataRead[1]
                if raw_dp & 0x8000:
                    raw_dp -= 65536
                if raw_dp < 1:
                    raw_dp = raw_dp/-1

                raw_temp = ((dataRead[3] << 8) | dataRead[4])
                if raw_temp & 0x8000:
                    raw_temp -= 65536

                dp_scale = 240 # Pa^-1
                temp_scale = 200 # degrees C^-1
            
                dp = raw_dp / dp_scale
                temp = raw_temp / temp_scale
            
                rho = 1.297
                A2 = 3.1415*((0.0508/2.0)**2.0)
                v2 = ((2.0*dp)/(rho*(1.0-(0.6135**4.0))))**0.5
                Re = rho*v2*0.0508/0.0000179
                Cd = 1.0054-(6.88*(Re**-0.5))
                Q = Cd*A2*v2 # flow in m3/s
                Q_cfm = Q*2118.88 # flow in CFM
                Q_lpm = Q_cfm*28.3168 # flow in LPM

                if "temperature" in record["variables"]:
                    record["variables"]["temperature"]["data"] = round(temp, 3)
                if "pressure" in record["variables"]:
                    record["variables"]["pressure"]["data"] = round(dp, 3)
                if "flow" in record["variables"]:
                    record["variables"]["flow"]["data"] = round(Q_lpm, 3)

            except Exception as e:
                self.logger.warning(
                    "default_parse - failed to decode I2C bytes", 
                    extra={"error": str(e), "dataRead": dataRead}
                )
                return None

            return record
            
        except Exception as e:
            self.logger.error("default_parse - critical error", extra={"error": str(e), "data": data})
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"


async def shutdown(sensor):
    print("shutting down")
    if sensor:
        await sensor.shutdown()

    for task in task_list:
        print(f"cancel: {task}")
        if task:
            task.cancel()


async def main(server_config: ServerConfig = None):
    # uiconfig = UIConfig(**config)
    if server_config is None:
        server_config = ServerConfig()
    print(server_config)

    # print("starting mock1 test task")

    # test = envdsBase()
    # task_list.append(asyncio.create_task(test_task()))

    # get config from file
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
    logger = logging.getLogger(f"Sensirion::SDP810::{sn}")

    logger.debug("Starting Sensirion SDP810")
    inst = SDP810()
    # print(inst)
    # await asyncio.sleep(2)
    inst.run()
    # print("running")
    # task_list.append(asyncio.create_task(inst.run()))
    # await asyncio.sleep(2)
    await asyncio.sleep(2)
    inst.start()
    # logger.debug("Starting Mock1")

    # remove fastapi ----
    # root_path = f"/envds/sensor/MockCo/Mock1/{sn}"
    # # print(f"root_path: {root_path}")

    # # TODO: get serial number from config file
    # config = uvicorn.Config(
    #     "main:app",
    #     host=server_config.host,
    #     port=server_config.port,
    #     log_level=server_config.log_level,
    #     root_path=f"/envds/sensor/MockCo/Mock1/{sn}",
    #     # log_config=dict_config,
    # )

    # server = uvicorn.Server(config)
    # # test = logging.getLogger()
    # # test.info("test")
    # await server.serve()
    # ----

    event_loop = asyncio.get_event_loop()
    global do_run
    do_run = True

    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        logger.debug("mock1.run", extra={"do_run": do_run})
        await asyncio.sleep(1)

    logger.info("starting shutdown...")
    await shutdown(inst)
    logger.info("done.")


if __name__ == "__main__":

    BASE_DIR = os.path.dirname(
        # os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        os.path.dirname(os.path.abspath(__file__))
    )
    # insert BASE at beginning of paths
    sys.path.insert(0, BASE_DIR)
    print(sys.path, BASE_DIR)

    print(sys.argv)
    config = ServerConfig()
    try:
        index = sys.argv.index("--host")
        host = sys.argv[index + 1]
        config.host = host
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        port = sys.argv[index + 1]
        config.port = int(port)
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--log_level")
        ll = sys.argv[index + 1]
        config.log_level = ll
    except (ValueError, IndexError):
        pass

    asyncio.run(main(config))

    