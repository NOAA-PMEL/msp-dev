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
import json
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

from pydantic import BaseModel

# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class SPLite2(Sensor):
    """docstring for SPLite2."""

    metadata = {
        "attributes": {
            "make": {"type": "string", "data": "KippZonen"},
            "model": {"type": "string", "data": "SPLite2"},
            "description": {
                "type": "string",
                "data": "Pyranometer",
            },
            "tags": {
                "type": "char",
                "data": "met, temperature, rh, sensor",
            },
            "format_version": {"type": "char", "data": "1.0.0"},
        },
        "dimensions": {"time": 0},
        "variables": {
            "time": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Time"}
                    },
            },
            "volts": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Measured voltage"},
                    "units": {"type": "char", "data": "volts"},
                },
            },
            "sensitivity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sensitivity of sensor (uV / W*m2)"},
                    "units": {"type": "char", "data": "uV W-1 m2"},
                },
            },
            "irradiance": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Irradiance of sensor (uV / W*m2)"},
                    "units": {"type": "char", "data": "W m-2"},
                },
            },
        }
    }

    def __init__(self, config=None, **kwargs):
        super(SPLite2, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        self.sampling_interval = 1

        self.default_data_buffer = asyncio.Queue()

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

        self.collecting = False
        self.sampling_mode = "unpolled" 

        self.sensor_definition_file = "KippZonen_SPLite2_sensor_definition.json"


        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)


    def configure(self):
        super(SPLite2, self).configure()

        # get config from file
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
                    "connection-properties": {
                    },
                    "read-properties": {
                        "read-method": "readline",  # readline, read-until, readbytes, readbinary
                        # "read-terminator": "\r",  # only used for read_until
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

            self.logger.debug(
                "splite2.configure", extra={"interfaces": conf["interfaces"]}
            )

            settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")

            for name, setting in settings_def["variables"].items():
                requested = setting["attributes"]["default_value"]["data"]
                if "settings" in config and name in config["settings"]:
                    requested = config["settings"][name]

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
            serial_number=conf["serial_number"],
            metadata=meta,
            interfaces=conf["interfaces"],
            daq_id=conf["daq_id"],
        )

        print(f"self.config: {self.config}")

        try:
            self.sensor_format_version = self.config.metadata.attributes[
                "format_version"
            ].data
        except KeyError:
            pass

        self.logger.debug(
            "configure",
            extra={"conf": conf, "self.config": self.config},
        )

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    print(f"add: {name}, {iface}")
                    self.add_interface(name, iface)
                    # self.iface_map[name] = iface
        except Exception as e:
            print(e)

        self.logger.debug("iface_map", extra={"map": self.iface_map})


    async def handle_interface_message(self, message: Message):
        pass


    async def handle_interface_data(self, message: Message):
        await super(SPLite2, self).handle_interface_data(message)

        self.logger.debug("interface_recv_data", extra={"data": message})
        if message["type"] == det.interface_data_recv():
            try:
                self.logger.debug(
                    "interface_recv_data", extra={"data": message}
                )
                path_id = message["path_id"]
                default_path = self.config.interfaces["default"]["path"]
                ref_path = self.config.interfaces["default"]["path"]
                self.logger.debug("interface_recv_data", extra={"path_id": path_id, "iface_path": iface_path})
                # if path_id == "default":
                if path_id == default_path:
                    self.logger.debug(
                        "interface_recv_data", extra={"here default data": message.data}
                    )
                    await self.default_data_buffer.put(message)
                if path_id == ref_path:
                    self.logger.debug(
                        "interface_recv_data", extra={"here ref data": message.data}
                    )
                    await self.default_data_buffer.put(message)
            except KeyError:
                pass


    async def settings_check(self):
        await super().settings_check()

        if not self.settings.get_health():  # something has changed
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    self.logger.debug(
                        "settings_check - set setting",
                        extra={
                            "setting-name": name,
                            "setting": self.settings.get_setting(name),
                        },
                    )

    async def sampling_monitor(self):
        need_start = True
        start_requested = False
        # wait to see if data is already streaming
        await asyncio.sleep(2)

        while True:
            try:

                if self.sampling():

                    if need_start:
                        if self.collecting:
                            if self.polling_task:
                                self.polling_task.cancel()
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            self.polling_task = asyncio.create_task(self.polling_loop())
                            need_start = False
                            start_requested = True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            if not self.polling_task:
                                self.polling_task = asyncio.create_task(self.polling_loop())

                            await asyncio.sleep(2)
                            continue
                else:
                    if self.collecting:
                        if self.polling_task:
                            self.polling_task.cancel()
                        await asyncio.sleep(2)
                        self.collecting = False

                await asyncio.sleep(0.1)

            except Exception as e:
                print(f"sampling monitor error: {e}")

            await asyncio.sleep(0.1)


    async def polling_loop(self):
        data = {}
        while True:
            if self.sampling():
                await self.interface_send_data(data=data)
                await asyncio.sleep(time_to_next(self.sampling_interval))


    async def default_data_loop(self):

        while True:
            try:
                data = await self.default_data_buffer.get()
                # self.collecting = True
                self.logger.debug("default_data_loop", extra={"data": data})
                # continue
                record = self.default_parse(data)
                self.logger.debug("default_data_loop", extra={"record": record})
                if record:
                    self.collecting = True

                # print(record)
                # print(self.sampling())
                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        # source="sensor.mockco-mock1-1234", data=record
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    destpath = f"/{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    self.logger.debug(
                        "default_data_loop", extra={"data": event, "destpath": destpath}
                    )
                    message = event
                    # self.logger.debug("default_data_loop", extra={"m": message})
                    await self.send_message(message)

                # self.logger.debug("default_data_loop", extra={"record": record})
            except Exception as e:
                print(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

    def default_parse(self, data):
        if data:
            try:
                timestamp = data.data["timestamp"]
                iface_data = data.data["data"]
                volt_val = iface_data["data"]
                
                variables = list(self.config.metadata.variables.keys())
                variables.remove("time")
                record = self.build_data_record(meta=self.include_metadata)
                print(f"default_parse: data: {data}, record: {record}")
                self.include_metadata = False
                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]
                    # parts = data.data["data"].split(",")

                    result = data.data["data"]
                    record["variables"]["volts"]["data"] = float(result["volts_mean"])
                    # nominal value of 80
                    record["variables"]["sensitivity"]["data"] = 80.0

                    uvolts = record["variables"]["volts"]["data"] * 1000000.0
                    E = uvolts / record["variables"]["sensitivity"]["data"]
                    record["variables"]["irradiance"]["data"] = E
                    return record
                    
                except KeyError:
                    pass
            except Exception as e:
                print(f"default_parse error: {e}")
        # else:
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
    logger = logging.getLogger(f"KippZonen::SPLite2::{sn}")

    logger.debug("Starting SPLite2")
    inst = SPLite2()
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
        logger.debug("splite2.run", extra={"do_run": do_run})
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