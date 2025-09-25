import asyncio
import signal

# import uvicorn
# from uvicorn.config import LOGGING_CONFIG
import sys
import os
import logging
import traceback

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
import json

# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class SCX21(Sensor):
    """docstring for SCX21."""

    metadata = {
        "attributes": {
            "make": {"type": "string", "data": "Furuno"},
            "model": {"type": "string", "data": "SCX21"},
            "description": {
                "type": "string",
                "data": "Compact Dome Satellite Compass manufactured by Furuno Electric Company and distributed by The GPS Store",
            },
            "tags": {
                "type": "char",
                "data": "satellite, compass, GPS, sensor",
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
            "scx21_timestamp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Internal Timestamp (UTC)"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: ZDA",
                    }
                },
            },
            "lat": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Latitude"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "lat_dir": {
                "type": "char",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Direction of Latitude"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                },
            },
            "lon": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Longitude"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "lon_dir": {
                "type": "char",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Direction of Longitude"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                    "units": {"type": "char", "data": "unitless"},
                },
            },
            "mode": {
                "type": "char",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Mode Indicator"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                    "units": {"type": "char", "data": "unitless"},
                },
            },
            "sv_num": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Number of SVs in Use"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GNS",
                    },
                    "units": {"type": "char", "data": "count"},
                },
            },
            "heading": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Heading from True north"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: HDT",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "speed": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Speed Over Ground"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: VTG",
                    },
                    "units": {"type": "char", "data": "kilometers/hour"},
                },
            },
            "yaw": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Yaw"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GPatt",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "pitch": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Pitch"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GPatt",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "roll": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Roll"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GPatt",
                    },
                    "units": {"type": "char", "data": "degrees"},
                },
            },
            "heave": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Heave"},
                    "description": {
                        "type": "char",
                        "data": "NMEA 0183 ID: GPhve",
                    },
                    "units": {"type": "char", "data": "meters"},
                },
            },
        },
    }

    def __init__(self, config=None, **kwargs):
        super(SCX21, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        # self.configure()
        self.first_record = 'ZDA'
        self.last_record = 'GPhve'
        self.array_buffer = []

        self.default_data_buffer = asyncio.Queue()

        # self.sensor_definition_file = "Furuno_SCX21_sensor_definition.json"

        # try:            
        #     with open(self.sensor_definition_file, "r") as f:
        #         self.metadata = json.load(f)
        # except FileNotFoundError:
        #     self.logger.error("sensor_definition not found. Exiting")            
        #     sys.exit(1)

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
        # self.enable_task_list.append(self.sampling_monitor())
        # self.enable_task_list.append(self.register_sensor())
        # asyncio.create_task(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(SCX21, self).configure()

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
                        "baudrate": 38400,
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readline",  # readline, read-until, readbytes, readbinary
                        # "read-terminator": "\r",  # only used for read_until
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
                "scx21.configure", extra={"interfaces": conf["interfaces"]}
            )

        # TODO change settings for new sensor definition
        '''
        The new settings are part [variables] now so this is a bit of a hack to use the existing structure
        with the new format.
        '''
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        # for name, setting in self.metadata["settings"].items():
        for name, setting in settings_def["variables"].items():
        
            requested = setting["attributes"]["default_value"]["data"]
            if "settings" in config and name in config["settings"]:
                requested = config["settings"][name]

            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            # settings=self.metadata["settings"],
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

    # async def handle_interface_message(self, message: Message):
    async def handle_interface_message(self, message: CloudEvent):
        pass

    # async def handle_interface_data(self, message: Message):
    async def handle_interface_data(self, message: CloudEvent):
        await super(SCX21, self).handle_interface_data(message)

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

    # async def register_sensor(self):
    #     try:

    #         make = self.config.make
    #         model = self.config.model
    #         serial_number = self.config.serial_number
    #         if not await get_sensor_registration(make=make, model=model, serial_number=serial_number):

    #             await register_sensor(
    #                 make=make,
    #                 model=model,
    #                 serial_number=serial_number,
    #                 source_id=self.get_id_as_source(),
    #             )

    #     except Exception as e:
    #         self.logger.error("sensor_reg error", extra={"e": e})

    async def sampling_monitor(self):

        # start_command = f"Log,{self.sampling_interval}\n"
        start_command = "Log,1\n"
        stop_command = "Log,0\n"

        need_start = True
        start_requested = False
        # wait to see if data is already streaming
        await asyncio.sleep(2)
        # # if self.collecting:
        # await self.interface_send_data(data={"data": stop_command})
        # await asyncio.sleep(2)
        # self.collecting = False
        # init to stopped
        # await self.stop_command()

        while True:
            try:

                if self.sampling():

                    if need_start:
                        if self.collecting:
                            await self.interface_send_data(data={"data": stop_command})
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            await self.interface_send_data(data={"data": start_command})
                            # await self.interface_send_data(data={"data": "\n"})
                            need_start = False
                            start_requested = True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            await self.interface_send_data(data={"data": start_command})
                            # await self.interface_send_data(data={"data": "\n"})
                            await asyncio.sleep(2)
                            continue
                else:
                    if self.collecting:
                        await self.interface_send_data(data={"data": stop_command})
                        await asyncio.sleep(2)
                        self.collecting = False

                await asyncio.sleep(0.1)

            except Exception as e:
                print(f"sampling monitor error: {e}")

            await asyncio.sleep(0.1)


    async def default_data_loop(self):

        while True:
            try:
                data = await self.default_data_buffer.get()
                if data:
                    self.collecting = True

                if self.first_record in data.data['data']:
                    record1 = self.default_parse(data)
                    # self.record_counter += 1
                    continue

                elif self.last_record in data.data['data']:
                    record2 = self.default_parse(data)
                    for var in record2["variables"]:
                        if var != 'time':
                            if record2["variables"][var]["data"] is not None:
                                record1["variables"][var]["data"] = record2["variables"][var]["data"]

                else:
                    record2 = self.default_parse(data)
                    if not record2:
                        continue
                    else:
                        for var in record2["variables"]:
                            if var != 'time':
                                if record2["variables"][var]["data"] is not None:
                                    record1["variables"][var]["data"] = record2["variables"][var]["data"]
                        continue
                record = record1
                # record = self.default_parse(data)
                if record:
                    self.collecting = True


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

                self.logger.debug("default_data_loop", extra={"record": record})
            except Exception as e:
                print(f"default_data_loop error: {e}")
                print(traceback.format_exc())
            await asyncio.sleep(0.001)

    def default_parse(self, data):
        if data:
            try:
                variables = list(self.config.metadata.variables.keys())
                variables.remove("time")
                print(f"variables: \n{variables}")

                record = self.build_data_record(meta=self.include_metadata)
                self.include_metadata = False

                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]
                    parts = data.data["data"].split(",")

                    if (datavar := 'HDT') in data.data["data"]:
                        parts = parts[1:2]
                    elif (datavar := 'GPatt') in data.data["data"]:
                        parts = parts[2:5]
                        parts = [x.split("*")[0] for x in parts]
                    elif (datavar := 'GPhve') in data.data["data"]:
                        parts = parts[2:3]
                    elif (datavar := 'ZDA') in data.data["data"]:
                        parts = parts[1:2]
                    elif (datavar := 'VTG') in data.data["data"]:
                        parts = parts[7:8]
                    elif (datavar := 'GNS') in data.data["data"]:
                        parts = parts[2:8]
                    else:
                        return None
                                        
                    self.var_name = []
                    for key, value in self.config.metadata.variables.items():
                        try:
                            if value.attributes["description"].data:
                                if datavar in value.attributes["description"].data:
                                    self.var_name.append(key)
                        except Exception as e:
                            continue

                    for index, name in enumerate(self.var_name):
                        if name in record["variables"]:
                            instvar = self.config.metadata.variables[name]
                            try:
                                if instvar.type == "int":
                                    if isinstance(parts[index], list):
                                        record["variables"][name]["data"] = [int(item) for item in parts[index]]
                                    else:
                                        record["variables"][name]["data"] = int(parts[index])

                                elif instvar.type == "float":
                                    if isinstance(parts[index], list):
                                        record["variables"][name]["data"] = [float(item) for item in parts[index]]
                                    else:
                                        record["variables"][name]["data"] = float(parts[index])
                                        
                                else:
                                    record["variables"][name]["data"] = parts[index]

                            except ValueError:
                                if instvar.type == "str" or instvar.type == "char":
                                    record["variables"][name]["data"] = ""
                                else:
                                    record["variables"][name]["data"] = None
                    return record
                except KeyError:
                    pass
            except Exception as e:
                print(f"default_parse error: {e}")
                print(traceback.format_exc())
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
    logger = logging.getLogger(f"Furuno::SCX21::{sn}")

    logger.debug("Starting SCX21")
    inst = SCX21()
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
