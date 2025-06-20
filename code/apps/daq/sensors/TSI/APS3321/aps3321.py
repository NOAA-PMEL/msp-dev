import asyncio
import signal
import traceback

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
from datetime import datetime


# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class APS3321(Sensor):
    """docstring for APS3321."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "TSI"},
            "model": {"type": "string", "data": "APS3321"},
            "description": {
                "type": "string",
                "data": "Aerodynamic particle sizer spectrometer manufactured and distributed by TSI",
            },
            "tags": {
                "type": "char",
                "data": "aerosol, spectrometer, particles, sizing, aerodynamic, diameter, sensor",
            },
            "format_version": {"type": "char", "data": "1.0.0"},
            "variable_types": {"type": "string", "data": "main, setting, calibration"},
            "serial_number": {"type": "string", "data": ""},
        },
        "dimensions": {"time": 0, "aerodynamic diameter": 52, "side scatter channel": 64},
        "variables": {
            "time": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Time"}
                },
            },
            # "mode": {
            #     "type": "str",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "main"},
            #         "long_name": {"type": "string", "data": "Sampling Mode"}
            #     },
            # },
            # "cal_mode": {
            #     "type": "str",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "main"},
            #         "long_name": {"type": "string", "data": "Calibration Mode"}
            #     },
            # },
            "ffff": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Status Flags (4 digit hex)"},
                },
            },
            "stime": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sample Time (not corrected for dead time)"},
                    "units": {"type": "char", "data": "seconds"},
                },
            },
            "dtime": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Dead Time"},
                    "units": {"type": "char", "data": "milliseconds"},
                },
            },
            "evt1": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Number of Single Hump Events"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "evt3": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Number of 3+ Hump Events"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "evt4": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Number of Timer Overflow Events"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "total": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Total (2-hump) Particles Measured "},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "particle_counts_accum": {
                "type": "int",
                "shape": ["time", "diameter", "channel"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Particle Counts in Each Pulse Height Accumulator Bin (Correlated / Paired Record)"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "particle_counts": {
                "type": "int",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Particle Counts in Calibrated Diameter Channels"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "particle_counts_ss": {
                "type": "int",
                "shape": ["time", "channel", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Particle Counts in Side Scatter Channels"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "bpress": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Barometric Inlet Pressure (average over sample time)"},
                    "units": {"type": "char", "data": "mbar"},
                },
            },
            "tflow": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Total Flow (average over sample time)"},
                    "units": {"type": "char", "data": "lpm"},
                },
            },
            "sflow": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sheath Flow (average over sample time)"},
                    "units": {"type": "char", "data": "lpm"},
                },
            },
            "lpower": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Laser Power (% of maximum power)"},
                    "units": {"type": "char", "data": "%"},
                },
            },
            "lcur": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Laser Current"},
                    "units": {"type": "char", "data": "mA"},
                },
            },
            "spumpv": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sheath Pump Voltage"},
                    "units": {"type": "char", "data": "V"},
                },
            },
            "tpumpv": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Total Pump Voltage)"},
                    "units": {"type": "char", "data": "V"},
                },
            },
            "itemp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Inlet Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "btemp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Internal Box Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "dtemp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Detector Temperature for APD and Optics"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "Vop": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "APD Operating Voltage"},
                    "units": {"type": "char", "data": "V"},
                },
            },
        },
    }


    def __init__(self, config=None, **kwargs):
        super(APS3321, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        # self.configure()

        self.default_data_buffer = asyncio.Queue()
        self.record_counter = 0
        self.var_name = None
        self.first_record = 'C,0,C'
        self.last_record = ',Y,'
        self.array_buffer = []
        self.C_counter = 0
        self.S_counter = 0

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
        super(APS3321, self).configure()

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
                        "bytesize": 7,
                        "parity": "E",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        # "read-method": "readline",
                        "read-method": "readuntil",  # readline, read-until, readbytes, readbinary
                        "read-terminator": "\r",  # only used for read_until
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
                "APS3321.configure", extra={"interfaces": conf["interfaces"]}
            )

        # TODO change settings for new sensor definition
        '''
        The new settings are part [variables] now so this is a bit of a hack to use the existing structure
        with the new format.
        '''
        settings_def = self.get_definition_by_variable_type(APS3321.metadata, variable_type="setting")
        # for name, setting in MAGIC250.metadata["settings"].items():
        for name, setting in settings_def["variables"].items():
        
            requested = setting["attributes"]["default_value"]["data"]
            if "settings" in config and name in config["settings"]:
                requested = config["settings"][name]

            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=APS3321.metadata["attributes"],
            dimensions=APS3321.metadata["dimensions"],
            variables=APS3321.metadata["variables"],
            # settings=MAGIC250.metadata["settings"],
            settings=settings_def["variables"]
        )

        self.config = DeviceConfig(
            make=APS3321.metadata["attributes"]["make"]["data"],
            model=APS3321.metadata["attributes"]["model"]["data"],
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
        await super(APS3321, self).handle_interface_data(message)

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

        # start_command = f"Log,{self.sampling_interval}\n"
        # start_command = "Log,1\n"
        # stop_command = "Log,0\n"
        # start_commands = ['S0\r', 'SMT2,5\r', 'UC\r', 'UD\r', 'US\r', 'UY\r', 'U1\r', 'S1\r']
        start_commands = ['S1\r']
        stop_command = 'S0\r'

        need_start = True
        start_requested = False
        # wait to see if data is already streaming
        await asyncio.sleep(2)

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
                            # for start_command in start_commands:
                            #     await self.interface_send_data(data={"data": start_command})
                            await self.interface_send_data(data={"data": start_commands[0]})
                            need_start = False
                            start_requested = True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            # for start_command in start_commands:
                            #     await self.interface_send_data(data={"data": start_command})
                            await self.interface_send_data(data={"data": start_commands[0]})
                            await asyncio.sleep(2)
                            continue
                else:
                    if self.collecting:
                        await self.interface_send_data(data={"data": stop_command})
                        await asyncio.sleep(2)
                        self.collecting = False

                await asyncio.sleep(0.0001)

            except Exception as e:
                print(f"sampling monitor error: {e}")

            await asyncio.sleep(0.0001)


    async def default_data_loop(self):

        while True:
            try:
                data = await self.default_data_buffer.get()
                if data:
                    self.collecting = True

                if self.first_record in data.data['data']:
                    record1 = self.default_parse(data)
                    self.record_counter += 1
                    continue

                elif self.last_record in data.data['data']:
                    record2 = self.default_parse(data)
                    for var in record2["variables"]:
                        if var != 'time':
                            if record2["variables"][var]["data"]:
                                record1["variables"][var]["data"] = record2["variables"][var]["data"]

                else:
                    record2 = self.default_parse(data)
                    if not record2:
                        continue
                    else:
                        for var in record2["variables"]:
                            if var != 'time':
                                if record2["variables"][var]["data"]:
                                    record1["variables"][var]["data"] = record2["variables"][var]["data"]
                        continue
                record = record1
                record = self.default_parse(data)
                if record:
                    self.collecting = True


                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        # source="sensor.mockco-mock1-1234", data=record
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    dest_path = f"/{self.get_id_as_topic()}/data/update"
                    event["dest_path"] = dest_path
                    self.logger.debug(
                        "default_data_loop",
                        extra={"data": event, "dest_path": dest_path},
                    )
                    # message = Message(data=event, dest_path=dest_path)
                    message = event
                    # self.logger.debug("default_data_loop", extra={"m": message})
                    await self.send_message(message)

                self.logger.debug("default_data_loop", extra={"record": record})
            except Exception as e:
                print(f"default_data_loop error: {e}")
            await asyncio.sleep(0.0001)


    def check_array_buffer(self, data, array_cond = False):
        self.array_buffer.append(data)
        if array_cond:
            return self.array_buffer
        else:
            return

    
    def default_parse(self, data):
        if data:
            try:
                variables = list(self.config.metadata.variables.keys())
                variables.remove("time")
                # print(f"variables: \n{variables}")

                # print(f"include metadata: {self.include_metadata}")
                record = self.build_data_record(meta=self.include_metadata)
                # print(f"default_parse: data: {data}, record: {record}")
                self.include_metadata = False
                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]

                    if ',C,' in data.data["data"]:
                        if ',C,0' in data.data["data"]:
                            parts = data.data["data"].split(",")
                            parts = parts[5:]
                            parts = [x.replace("\r", "") for x in parts]
                            self.var_name = ['ffff', 'stime', 'dtime', 'evt1', 'evt3', 'evt4', 'total']
                            compiled_record = parts
                            self.C_counter = 0
                        else:
                            parts = data.data["data"].split(",")
                            parts = parts[3:]
                            parts = [x.replace("\r", "") for x in parts]
                            # Replace all empty strings with values of 0
                            parts = [int(x) if x else 0 for x in parts]
                            # Add zeros to the end of the list until the list length reaches 64
                            zeros_to_add = 64 - len(parts)
                            parts.extend([0] * zeros_to_add)
                            self.var_name = ['particle_counts_accum']

                            if self.C_counter < 51:
                                self.check_array_buffer(parts, array_cond=False)
                                self.C_counter += 1
                                return

                            else:
                                compiled_record = self.check_array_buffer(parts, array_cond=True)
                                self.array_buffer = []
                                self.C_counter = 0
                    
                    if ',D,' in data.data["data"]:
                        parts = data.data["data"].split(",")
                        parts = parts[11:]
                        parts = [x.replace("\r", "") for x in parts]
                        # Replace all empty list items with values of 0
                        parts = [int(x) if x else 0 for x in parts]
                        # Add zeros to the end of the list until the list length reaches 52
                        zeros_to_add = 52 - len(parts)
                        parts.extend([0] * zeros_to_add)
                        self.var_name =['particle_counts']
                        compiled_record = parts
                    
                    if ',S,' in data.data["data"]:
                        if ',S,C' in data.data["data"]:
                            self.S_counter = 0
                            return
                        else:
                            parts = data.data["data"].split(",")
                            parts = parts[3:]
                            parts = [x.replace("\r", "") for x in parts]
                            # Replace all empty strings with values of 0
                            parts = [int(x) if x else 0 for x in parts]
                            # Add zeros to the end of the list until the list length reaches 52
                            zeros_to_add = 52 - len(parts)
                            parts.extend([0] * zeros_to_add)
                            self.var_name = ['particle_counts_ss']

                            if self.S_counter < 63:
                                self.check_array_buffer(parts, array_cond=False)
                                self.S_counter += 1
                                return

                            else:
                                compiled_record = self.check_array_buffer(parts, array_cond=True)
                                self.array_buffer = []
                                self.S_counter = 0
                    
                    if ',Y,' in data.data["data"]:
                        parts = data.data["data"].split(",")
                        parts = parts[2:]
                        parts = [x.replace("\r", "") for x in parts]
                        # Remove the analog and digital input voltage levels from the middle of the list
                        del parts[3:8]
                        compiled_record = parts
                        self.var_name = ['bpress', 'tflow', 'sflow', 'lpower', 'lcur', 'spumpv', 'tpumpv', 'itemp', 'btemp', 'dtemp', 'Vop']


                    for index, name in enumerate(self.var_name):
                    # for index, name in enumerate(variables):
                        if name in record["variables"]:
                            instvar = self.config.metadata.variables[name]
                            try:
                                if len(self.var_name) == 1:
                                    record["variables"][name]["data"] = compiled_record
                                else:
                                    if instvar.type == "int":
                                        record["variables"][name]["data"] = int(compiled_record[index])
                                    elif instvar.type == "float":
                                        record["variables"][name]["data"] = float(compiled_record[index])
                                    else:
                                        record["variables"][name]["data"] = compiled_record[index]
                            except ValueError:
                                if instvar.type == "str" or instvar.type == "char":
                                    record["variables"][name]["data"] = ""
                                else:
                                    record["variables"][name]["data"] = None
                    return record
                except KeyError:
                    pass
            except Exception as e:
                # print(traceback.extract_tb(e.__traceback__))
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
    logger = logging.getLogger(f"TSI::APS3321::{sn}")

    logger.debug("Starting APS3321")
    inst = APS3321()
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



