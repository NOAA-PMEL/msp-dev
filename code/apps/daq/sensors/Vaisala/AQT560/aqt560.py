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


# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class AQT560(Sensor):
    """docstring for AQT560."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "Vaisala"},
            "model": {"type": "string", "data": "AQT560"},
            "description": {
                "type": "string",
                "data": "Air Quality Transmitter manufactured and distributed by Vaisala",
            },
            "tags": {
                "type": "char",
                "data": "aerosol, air quality, particles, gas, sensor",
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
            "NO2": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Nitrogen Dioxide"},
                    "units": {"type": "char", "data": "ppm"},
                },
            },
            "CO": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Carbon Monoxide"},
                    "units": {"type": "char", "data": "ppm"},
                },
            },
            "O3": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Ozone"},
                    "units": {"type": "char", "data": "ppm"},
                },
            },
            "NO": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Nitric Oxide"},
                    "units": {"type": "char", "data": "ppm"},
                },
            },
            "PM1": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "PM1"},
                    "units": {"type": "char", "data": "ug/m3"},
                },
            },
            "PM2.5": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "PM2.5"},
                    "units": {"type": "char", "data": "ug/m3"},
                },
            },
            "PM10": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "PM10"},
                    "units": {"type": "char", "data": "ug/m3"},
                },
            },
            "TEMP": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "HUM": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Relative Humidity"},
                    "units": {"type": "char", "data": "%"},
                },
            },
            "PRES": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Pressure"},
                    "units": {"type": "char", "data": "hPa"},
                },
            },
            "Uptime": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Time Running"},
                    "units": {"type": "char", "data": "seconds"}
                },
            },
        }
    }


    def __init__(self, config=None, **kwargs):
        super(AQT560, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=100)
        self.data_rate = 1
        self.polling_task = None

        self.sensor_definition_file = "Vaisala_AQT560_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False

    # def configure(self):
    #     super(AQT560, self).configure()

    #     # get config from file
    #     try:
    #         with open("/app/config/sensor.conf", "r") as f:
    #             conf = yaml.safe_load(f)
    #     except FileNotFoundError:
    #         conf = {"serial_number": "UNKNOWN", "interfaces": {}}

    #     if "metadata_interval" in conf:
    #         self.include_metadata_interval = conf["metadata_interval"]

    #     sensor_iface_properties = {
    #         "default": {
    #             "device-interface-properties": {
    #                 "connection-properties": {
    #                     "baudrate": 115200,
    #                     "bytesize": 8,
    #                     "parity": "N",
    #                     "stopbit": 1,
    #                 },
    #                 "read-properties": {
    #                     # "read-method": "readline",  # readline, read-until, readbytes, readbinary
    #                     "read-method": "readuntil",
    #                     "read-terminator": "$",  # only used for read_until
    #                     "decode-errors": "strict",
    #                     "send-method": "ascii",
    #                 },
    #             }
    #         }
    #     }

    #     if "interfaces" in conf:
    #         for name, iface in conf["interfaces"].items():
    #             if name in sensor_iface_properties:
    #                 for propname, prop in sensor_iface_properties[name].items():
    #                     iface[propname] = prop

    #         self.logger.debug(
    #             "AQT560.configure", extra={"interfaces": conf["interfaces"]}
    #         )

    #     # TODO change settings for new sensor definition
    #     '''
    #     The new settings are part [variables] now so this is a bit of a hack to use the existing structure
    #     with the new format.
    #     '''
    #     settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
    #     # for name, setting in AQT560.metadata["settings"].items():
    #     for name, setting in settings_def["variables"].items():
        
    #         requested = setting["attributes"]["default_value"]["data"]
    #         if "settings" in config and name in config["settings"]:
    #             requested = config["settings"][name]

    #         self.settings.set_setting(name, requested=requested)

    #     meta = DeviceMetadata(
    #         attributes=self.metadata["attributes"],
    #         dimensions=self.metadata["dimensions"],
    #         variables=self.metadata["variables"],
    #         # settings=MAGIC250.metadata["settings"],
    #         settings=settings_def["variables"]
    #     )

    #     self.config = DeviceConfig(
    #         make=self.metadata["attributes"]["make"]["data"],
    #         model=self.metadata["attributes"]["model"]["data"],
    #         serial_number=conf["serial_number"],
    #         metadata=meta,
    #         interfaces=conf["interfaces"],
    #         daq_id=conf["daq_id"],
    #     )

    #     print(f"self.config: {self.config}")

    #     try:
    #         self.device_format_version = self.config.metadata.attributes[
    #             "format_version"
    #         ].data
    #     except KeyError:
    #         pass

    #     self.logger.debug(
    #         "configure",
    #         extra={"conf": conf, "self.config": self.config},
    #     )

    #     try:
    #         if "interfaces" in conf:
    #             for name, iface in conf["interfaces"].items():
    #                 print(f"add: {name}, {iface}")
    #                 self.add_interface(name, iface)
    #                 # self.iface_map[name] = iface
    #     except Exception as e:
    #         print(e)

    #     self.logger.debug("iface_map", extra={"map": self.iface_map})

    def configure(self):
        super(AQT560, self).configure()
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
                        "baudrate": 115200,
                        "bytesize": 8,
                        "parity": "N",
                        "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readuntil",
                        "read-terminator": "$",
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

        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, TypeError):
            pass

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_message(self, message: CloudEvent):
        pass

    async def handle_interface_data(self, message: CloudEvent):
        await super(AQT560, self).handle_interface_data(message)
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

    # async def sampling_monitor(self):

    #     # start_command = f"Log,{self.sampling_interval}\n"
    #     # start_command = "Log,1\n"
    #     # stop_command = "Log,0\n"

    #     need_start = True
    #     start_requested = False
    #     # wait to see if data is already streaming
    #     await asyncio.sleep(2)

    #     while True:
    #         try:

    #             if self.sampling():

    #                 if need_start:
    #                     if self.collecting:
    #                         # await self.interface_send_data(data={"data": stop_command})
    #                         if self.polling_task:
    #                             self.polling_task.cancel()
    #                         await asyncio.sleep(2)
    #                         self.collecting = False
    #                         continue
    #                     else:
    #                         # await self.interface_send_data(data={"data": start_command})
    #                         # await self.interface_send_data(data={"data": "\n"})
    #                         self.polling_task = asyncio.create_task(self.polling_loop())
    #                         need_start = False
    #                         start_requested = True
    #                         await asyncio.sleep(2)
    #                         continue
    #                 elif start_requested:
    #                     if self.collecting:
    #                         start_requested = False
    #                     else:
    #                         if not self.polling_task:
    #                             self.polling_task = asyncio.create_task(self.polling_loop())
    #                         # await self.interface_send_data(data={"data": start_command})
    #                         # await self.interface_send_data(data={"data": "\n"})
    #                         await asyncio.sleep(2)
    #                         continue
    #             else:
    #                 if self.collecting:
    #                     # await self.interface_send_data(data={"data": stop_command})
    #                     if self.polling_task:
    #                         self.polling_task.cancel()
    #                     await asyncio.sleep(2)
    #                     self.collecting = False

    #             await asyncio.sleep(0.1)

    #             # if self.collecting:
    #             #     # await self.stop_command()
    #             #     self.logger.debug("sampling_monitor:5", extra={"self.collecting": self.collecting})
    #             #     await self.interface_send_data(data={"data": stop_command})
    #             #     # self.logger.debug("sampling_monitor:6", extra={"self.collecting": self.collecting})
    #             #     self.collecting = False
    #             #     # self.logger.debug("sampling_monitor:7", extra={"self.collecting": self.collecting})
    #         except Exception as e:
    #             print(f"sampling monitor error: {e}")

    #         await asyncio.sleep(0.1)

    async def sampling_monitor(self):
        await asyncio.sleep(2)
        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if self.polling_task is None or self.polling_task.done():
                        self.logger.info("Starting AQT560 polling loop.")
                        self.polling_task = asyncio.create_task(self.polling_loop())
                else:
                    if self.polling_task and not self.polling_task.done():
                        self.logger.info("Stopping AQT560 polling loop.")
                        self.polling_task.cancel()
                        self.polling_task = None

            except Exception as e:
                self.logger.error("sampling_monitor error", extra={"error": str(e)})

            await asyncio.sleep(1)

    # async def start_command(self):
    #     pass # Log,{sampling interval}

    # async def stop_command(self):
    #     pass # Log,0

    # def stop(self):
    #     asyncio.create_task(self.stop_sampling())
    #     super().start()

    async def polling_loop(self):
        poll_cmd = 'meas\r'
        while True:
            try:
                await self.interface_send_data(data={"data": poll_cmd})
            except Exception as e:
                self.logger.error("polling_loop error", extra={"error": str(e)})
            await asyncio.sleep(time_to_next(self.data_rate))
    
    # async def default_data_loop(self):

    #     while True:
    #         try:
    #             data = await self.default_data_buffer.get()
    #             # self.collecting = True
    #             self.logger.debug("default_data_loop", extra={"data": data})
    #             # continue
    #             record = self.default_parse(data)
    #             if record:
    #                 self.collecting = True

    #             # print(record)
    #             # print(self.sampling())
    #             if record and self.sampling():
    #                 event = DAQEvent.create_data_update(
    #                     # source="sensor.mockco-mock1-1234", data=record
    #                     source=self.get_id_as_source(),
    #                     data=record,
    #                 )
    #                 destpath = f"{self.get_id_as_topic()}/data/update"
    #                 event["destpath"] = destpath
    #                 self.logger.debug(
    #                     "default_data_loop",
    #                     extra={"data": event, "destpath": destpath},
    #                 )
    #                 # message = Message(data=event, destpath=destpath)
    #                 message = event
    #                 # self.logger.debug("default_data_loop", extra={"m": message})
    #                 await self.send_message(message)

    #             # self.logger.debug("default_data_loop", extra={"record": record})
    #         except Exception as e:
    #             print(f"default_data_loop error: {e}")
    #         await asyncio.sleep(0.1)
    
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

    # def default_parse(self, data):
    #     if data:
    #         try:
    #             variables = list(self.config.metadata.variables.keys())
    #             variables.remove("time")
    #             print(f"variables: \n{variables}")

    #             print(f"include metadata: {self.include_metadata}")
    #             record = self.build_data_record(meta=self.include_metadata)
    #             print(f"default_parse: data: {data}, record: {record}")
    #             self.include_metadata = False

    #             try:
    #                 record["timestamp"] = data.data["timestamp"]
    #                 record["variables"]["time"]["data"] = data.data["timestamp"]
    #                 parts = data.data["data"].splitlines()[1:12]    
    #                 print('parts', parts)                
    #                 if len(parts) < 11:
    #                     return None
    #                 for index, name in enumerate(variables):
    #                     if name in record["variables"]:
    #                         instvar = self.config.metadata.variables[name]
    #                         vartype = instvar.type
    #                         if instvar.type == "string":
    #                             vartype = "str"
    #                         try:
    #                             if name == 'PM2_5':
    #                                 new_name = 'PM2.5 '
    #                             else:
    #                                 new_name = name + " "
    #                             print('vartype', vartype)
    #                             print('name', name)
    #                             print('name1', new_name)
    #                             # find the corresponding variable
    #                             measurement = [item for item in parts if new_name in item]
    #                             print('measurement1', measurement)
    #                             # get rid of the variable label (i.e. just get the value)
    #                             measurement = measurement[0].split(":")                     
    #                             measurement = measurement[1].strip()
    #                             print('measurement2', measurement)
    #                             record["variables"][name]["data"] = eval(vartype)(measurement)
    #                         except ValueError:
    #                             if vartype == "str" or vartype == "char":
    #                                 record["variables"][name]["data"] = ""
    #                             else:
    #                                 print('value error float reached') 
    #                                 record["variables"][name]["data"] = None 
    #                 return record
    #             except KeyError:
    #                 pass
    #         except Exception as e:
    #             print(f"default_parse error: {e}")
    #     # else:
    #     return None

    def default_parse(self, data):
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            
            # Safely handle the CloudEvent dictionary
            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            if "time" in record.get("variables", {}):
                record["variables"]["time"]["data"] = raw_payload.get("timestamp")
                
            raw_str = raw_payload.get("data", "")
            self.logger.debug("default_parse - raw string received", extra={"raw_str": raw_str})
            
            # AQT560 returns multiline data blocks
            parts = raw_str.splitlines()
            
            for name in record["variables"]:
                if name == "time": continue
                instvar = self.config.metadata.variables[name]
                vartype = "str" if instvar.type == "string" else instvar.type
                val_str = None
                
                try:
                    # PM2.5 in JSON is labeled PM2_5, but hardware outputs "PM2.5 "
                    search_name = "PM2.5 " if name == "PM2_5" else f"{name} "
                    
                    # Find corresponding variable string (e.g. "TEMP : 23.4")
                    measurement = next((item for item in parts if search_name in item), None)
                    if measurement:
                        # Split by ':' and strip surrounding whitespace
                        val_str = measurement.split(":")[1].strip()
                        
                        # Apply safe explicit type casting
                        if vartype == "int":
                            record["variables"][name]["data"] = int(float(val_str))
                        elif vartype == "float":
                            record["variables"][name]["data"] = float(val_str)
                        else:
                            record["variables"][name]["data"] = val_str
                            
                except (ValueError, IndexError, TypeError) as e:
                    self.logger.debug(
                        f"default_parse - type conversion failed for {name}", 
                        extra={"error": str(e), "attempted_value": val_str, "target_type": vartype}
                    )
                    if vartype in ["str", "char"]:
                        record["variables"][name]["data"] = ""
                    else:
                        record["variables"][name]["data"] = None
                        
            # If we didn't populate anything meaningful, drop it
            if all(v["data"] is None or v["data"] == "" for k, v in record["variables"].items() if k != "time"):
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
    logger = logging.getLogger(f"Vaisala::AQT560::{sn}")

    logger.debug("Starting AQT560")
    inst = AQT560()
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



