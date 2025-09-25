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
import json
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
from datetime import datetime


# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class Aurora3000(Sensor):
    """docstring for Aurora3000."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "ACOEM"},
            "model": {"type": "string", "data": "Aurora3000"},
            "description": {
                "type": "string",
                "data": "Three wavelength interating nephelometer manufactured and distributed by ACOEM / Ecotech",
            },
            "tags": {
                "type": "char",
                "data": "aerosol, nephelometer, scattering, backscatter, sensor",
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
            "aurora_date_time": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Internal Date and Timestamp"}
                },
            },
            "scat_coef_ch1_red": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Scattering Coefficient (Red)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 635},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "scat_coef_ch2_green": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Scattering Coefficient (Green)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 525},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "scat_coef_ch3_blue": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Scattering Coefficient (Blue)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 450},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "backscatter_ch1_red": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Backscatter (Red)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 635},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "backscatter_ch2_green": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Backscatter (Green)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 525},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "backscatter_ch3_blue": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Backscatter (Blue)"},
                    "units": {"type": "char", "data": "Mm-1"},
                    "wavelength": {"type": "int", "data": 450},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "sample_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sample Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "enclosure_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Enclosure Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "rh": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Relative Humidity"},
                    "units": {"type": "char", "data": "%"},
                },
            },
            "pressure": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Barometric Pressure"},
                    "units": {"type": "char", "data": "%"},
                },
            },
            "major_state": {
                "type": "string",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Major State"},
                },
            },
            "DIO_state": {
                "type": "string",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "DIO State"},
                },
            },
            "Dark_count_avg": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Dark Count (moving average)"},
                },
            },
            "Dark_count_read": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Last Dark Count Reading"},
                },
            },
        },
    }


    def __init__(self, config=None, **kwargs):
        super(Aurora3000, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        # self.configure()

        self.default_data_buffer = asyncio.Queue()

        self.sensor_definition_file = "ACOEM_Aurora3000_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        # self.command_list = ['VI099\r', 'VI004\r', 'VI005\r']
        self.command_list = ['VI099\r']
        self.command_counter = 0

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
        self.collecting = False

    def configure(self):
        super(Aurora3000, self).configure()

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
                "aurora3000.configure", extra={"interfaces": conf["interfaces"]}
            )

        # TODO change settings for new sensor definition
        '''
        The new settings are part [variables] now so this is a bit of a hack to use the existing structure
        with the new format.
        '''
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        # for name, setting in MAGIC250.metadata["settings"].items():
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

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(Aurora3000, self).handle_interface_data(message)

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

        while True:
            try:
                # for poll_cmd in self.command_list:
                poll_cmd = 'VI099\r'
                self.logger.debug("polling_loop", extra={"poll_cmd": poll_cmd})
                await self.interface_send_data(data={"data": poll_cmd})
                    # await asyncio.sleep(0.001)
                # await asyncio.sleep(time_to_next(self.data_rate/2.))
                await asyncio.sleep(time_to_next(self.data_rate))
            except Exception as e:
                self.logger.error("polling_loop", extra={"e": e})
    

    async def default_data_loop(self):

        while True:
            try:
                print("command number", self.command_counter)
                data = await self.default_data_buffer.get()
                self.logger.debug("default_data_loop", extra={"data": data})
                record = self.default_parse(data)
                cmd_list_len = len(self.command_list)

                # check to see if the current data stream is supposed to be output from the first command
                if self.command_counter == 0:
                    # if the first variable is the internal timestamp, this is the first record
                    try:
                        if bool(datetime.strptime(record["variables"]['aurora_date_time']["data"], "%d/%m/%Y %H:%M:%S")):
                            record1 = record
                            self.command_counter += 1
                            continue
                    except Exception as e:
                        print(f"default_data_loop error: {e}")

                # consecutive records after the first one are moved into the first one
                else:
                    record2 = record
                    var_index = 13 + self.command_counter
                    record1["variables"][list(self.config.metadata.variables.keys())[var_index]]["data"] = record2["variables"]['aurora_date_time']["data"]
                    self.command_counter += 1

                    # once the number of commands sent has surpassed the expected number, this function will finish and return the completed record
                    if self.command_counter < (cmd_list_len - 1):
                        continue
                    else:
                        self.command_counter = 0
                        pass

                record = record1
                print("COMPLETE RECORD", record)

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
                    print(f"parts: {parts}, {variables}")
                    # if len(parts) < 13:
                    #     return None
                    # if len(parts) > 12:

                    for index, name in enumerate(variables):
                        if name in record["variables"]:
                            # instvar = self.config.variables[name]
                            instvar = self.config.metadata.variables[name]
                            vartype = instvar.type
                            if instvar.type == "string":
                                vartype = "str"
                            try:
                                # print(f"default_parse: {record['variables'][name]} - {parts[index].strip()}")
                                record["variables"][name]["data"] = eval(vartype)(
                                    parts[index].strip()
                                )
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
    logger = logging.getLogger(f"ACOEM::Aurora3000::{sn}")

    logger.debug("Starting Aurora3000")
    inst = Aurora3000()
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



