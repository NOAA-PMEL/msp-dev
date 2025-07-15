import asyncio
import signal

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
from envds.core import envdsLogger

# from envds.daq.db import get_sensor_registration, register_sensor  # , envdsBase, envdsStatus
from envds.util.util import (
    # get_datetime_format,
    time_to_next,
    get_datetime,
    get_datetime_string,
)
# from envds.daq.sensor import Sensor, SensorConfig, SensorVariable, SensorMetadata
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceVariable, DeviceMetadata

# from envds.event.event import create_data_update, create_status_update
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
# from envds.message.message import Message

# from envds.exceptions import envdsRunTransitionException

# from typing import Union
from cloudevents.http import CloudEvent
# from cloudevents.conversion import to_json, to_structured

from pydantic import BaseModel


# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class WXT536(Sensor):
    """docstring for WXT536."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "Vaisala"},
            "model": {"type": "string", "data": "WXT536"},
            "description": {
                "type": "string",
                "data": "Weather Transmitter manufactured and distributed by Vaisala",
            },
            "tags": {
                "type": "char",
                "data": "weather, met, wind, rain, temperature, pressure, sensor",
            },
            "format_version": {"type": "char", "data": "1.0.0"},
            "variable_types": {"type": "string", "data": "main, setting, calibration"},
            "serial_number": {"type": "string", "data": ""},
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
            "Dn": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Wind Direction Minimum"},
                    "units": {"type": "char", "data": "degrees"}
                },
            },
            "Dm": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Wind Direction Average"},
                    "units": {"type": "char", "data": "degrees"}
                },
            },
            "Dx": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Wind Direction Maximum"},
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "Sn": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Wind Speed Minimum"},
                    "units": {"type": "char", "data": "m/s"},
                },
            },
            "Sm": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Wind Speed Average"},
                    "units": {"type": "char", "data": "m/s"},
                },
            },
            "Sx": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Wind Speed Maximum"},
                    "units": {"type": "char", "data": "m/s"},
                },
            },
            "Ta": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Air Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "Ua": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Relative Humidity"},
                    "units": {"type": "char", "data": "%"},
                },
            },
            "Pa": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Air Pressure"},
                    "units": {"type": "char", "data": "hPa"},
                },
            },
            "Rc": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Rain Accumulation"},
                    "units": {"type": "char", "data": "mm"},
                },
            },
            "Rd": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Rain Duration"},
                    "units": {"type": "char", "data": "seconds"},
                },
            },
            "Ri": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Rain Intensity"},
                    "units": {"type": "char", "data": "mm/h"},
                },
            },
            "Hc": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Hail Accumulation"},
                    "units": {"type": "char", "data": "hits/cm2"},
                },
            },
            "Hd": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Hail Duration"},
                    "units": {"type": "char", "data": "seconds"},
                },
            },
            "Hi": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Hail Intensity"},
                    "units": {"type": "char", "data": "hits/cm2h"},
                },
            },
            "Rp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Rain Peak Intensity"},
                    "units": {"type": "char", "data": "mm/h"},
                },
            },
            "Hp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Hail Peak Intensity"},
                    "units": {"type": "char", "data": "hits/cm2h"},
                },
            },
            "Th": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Heating Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "Vh": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Heating Voltage"},
                    "units": {"type": "char", "data": "V"},
                },
            },
            "Vs": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Supply Voltage"},
                    "units": {"type": "char", "data": "V"},
                },
            },
            "Vr": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "3.5V Reference Voltage"},
                    "units": {"type": "char", "data": "V"},
                },
            },
        }
    }


    def __init__(self, config=None, **kwargs):
        super(WXT536, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        # self.configure()

        self.default_data_buffer = asyncio.Queue()

        # os.environ["REDIS_OM_URL"] = "redis://redis.default"

        # self.data_loop_task = None

        # all handled in run_setup ----
        # self.configure()

        # # self.logger = logging.getLogger(f"{self.config.make}-{self.config.model}-{self.config.serial_number}")
        # self.logger = logging.getLogger(self.build_app_uid())

        # # self.update_id("app_uid", f"{self.config.make}-{self.config.model}-{self.config.serial_number}")
        # self.update_id("app_uid", self.build_app_uid())

        # self.logger.debug("id", extra={"self.id": self.id})
        # print(f"config: {self.config}")
        # ----

        # self.sampling_task_list.append(self.data_loop())
        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        # self.enable_task_list.append(self.register_sensor())
        # asyncio.create_task(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(WXT536, self).configure()

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
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 19200,
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readuntil",  # readline, read-until, readbytes, readbinary
                        "read-terminator": "V\r",  # only used for read_until
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

            self.logger.debug(
                "WXT536.configure", extra={"interfaces": conf["interfaces"]}
            )

        # TODO change settings for new sensor definition
        '''
        The new settings are part [variables] now so this is a bit of a hack to use the existing structure
        with the new format.
        '''
        settings_def = self.get_definition_by_variable_type(WXT536.metadata, variable_type="setting")
        # for name, setting in MAGIC250.metadata["settings"].items():
        for name, setting in settings_def["variables"].items():
        
            requested = setting["attributes"]["default_value"]["data"]
            if "settings" in config and name in config["settings"]:
                requested = config["settings"][name]

            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=WXT536.metadata["attributes"],
            dimensions=WXT536.metadata["dimensions"],
            variables=WXT536.metadata["variables"],
            # settings=WXT536.metadata["settings"],
            settings=settings_def["variables"]
        )

        self.config = DeviceConfig(
            make=WXT536.metadata["attributes"]["make"]["data"],
            model=WXT536.metadata["attributes"]["model"]["data"],
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

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(WXT536, self).handle_interface_data(message)

        # self.logger.debug("interface_recv_data", extra={"data": message})
        # if message.data["type"] == det.interface_data_recv():
        if message["type"] == det.interface_data_recv():
            try:
                # path_id = message.data["path_id"]
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                # if path_id == "default":
                if path_id == iface_path:
                    self.logger.debug(
                        # "interface_recv_data", extra={"data": message.data.data}
                        "interface_recv_data", extra={"data": message.data}
                    )
                    # await self.default_data_buffer.put(message.data)
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
                            # await self.interface_send_data(data={"data": stop_command})
                            if self.polling_task:
                                self.polling_task.cancel()
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            # await self.interface_send_data(data={"data": start_command})
                            # await self.interface_send_data(data={"data": "\n"})
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
                            # await self.interface_send_data(data={"data": start_command})
                            # await self.interface_send_data(data={"data": "\n"})
                            await asyncio.sleep(2)
                            continue
                else:
                    if self.collecting:
                        # await self.interface_send_data(data={"data": stop_command})
                        if self.polling_task:
                            self.polling_task.cancel()
                        await asyncio.sleep(2)
                        self.collecting = False

                await asyncio.sleep(0.1)

                # if self.collecting:
                #     # await self.stop_command()
                #     self.logger.debug("sampling_monitor:5", extra={"self.collecting": self.collecting})
                #     await self.interface_send_data(data={"data": stop_command})
                #     # self.logger.debug("sampling_monitor:6", extra={"self.collecting": self.collecting})
                #     self.collecting = False
                #     # self.logger.debug("sampling_monitor:7", extra={"self.collecting": self.collecting})
            except Exception as e:
                print(f"sampling monitor error: {e}")

            await asyncio.sleep(0.1)


    async def polling_loop(self):

        poll_cmd = '0R\r\n'
        while True:
            try:
                self.logger.debug("polling_loop", extra={"poll_cmd": poll_cmd})
                await self.interface_send_data(data={"data": poll_cmd})
                await asyncio.sleep(time_to_next(self.data_rate))
            except Exception as e:
                self.logger.error("polling_loop", extra={"e": e})
    
    async def default_data_loop(self):

        while True:
            try:
                data = await self.default_data_buffer.get()
                # self.collecting = True
                self.logger.debug("default_data_loop", extra={"data": data})
                # continue
                record = self.default_parse(data)
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
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    self.logger.debug(
                        "default_data_loop",
                        extra={"data": event, "destpath": destpath},
                    )
                    # message = Message(data=event, destpath=destpath)
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
                variables = list(self.config.metadata.variables.keys())
                variables.remove("time")
                print(f"variables: \n{variables}")

                print(f"include metadata: {self.include_metadata}")
                record = self.build_data_record(meta=self.include_metadata)
                print(f"default_parse: data: {data}, record: {record}")
                self.include_metadata = False
                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]
                    parts = data.data["data"].split(",")
                    # print(f"parts: {parts}, {variables}")
                    if len(parts) < 19:
                        return None
                    for index, name in enumerate(variables):
                        if name in record["variables"]:
                            # instvar = self.config.variables[name]
                            instvar = self.config.metadata.variables[name]
                            vartype = instvar.type
                            if instvar.type == "string":
                                vartype = "str"
                            try:
                                # find corresponding variable
                                measurement = [item for item in parts if name in item]
                                # split the string to remove the variable name 
                                measurement = measurement[0].split("=")
                                # remove anything after carriage return chars - must be done because data is spit out on 4 separate lines
                                measurement = measurement[1].split('\r', 1)[0]
                                # remove the last character (the units)
                                measurement = measurement[:-1]
                                record["variables"][name]["data"] = eval(vartype)(measurement)
                            # except ValueError:
                            except Exception as e:
                                if vartype == "str" or vartype == "char":
                                    record["variables"][name]["data"] = ""
                                else:
                                    record["variables"][name]["data"] = None
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
    logger = logging.getLogger(f"Vaisala::WXT536::{sn}")

    logger.debug("Starting WXT536")
    inst = WXT536()
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



