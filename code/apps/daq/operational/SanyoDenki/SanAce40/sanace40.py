import asyncio
import binascii
import signal
from struct import unpack

import sys
import os
import logging

import logging.config

import yaml
import random
from envds.core import envdsLogger  # , envdsBase, envdsStatus
from envds.util.util import (
    # get_datetime_format,
    time_to_next,
    get_datetime,
    get_datetime_string,
    string_to_datetime,
)
from envds.daq.operational import Operational
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
import json

# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class SanAce40(Operational):
    """docstring for SanAce40."""

    metadata = {
        "attributes": {
            "make": {"type": "string", "data": "SanyoDenki"},
            "model": {"type": "string", "data": "SanAce40"},
            "description": {"type": "string", "data": "9GA0424P3J0011"},
            "tags": {"type": "char", "data": "flow"},
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
                    "long_name": {"type": "string", "data": "Time"},
                },
            },
            "rpm": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Fan RPM"},
                    "units": {"type": "char", "data": "count"},
                },
            },
            "fan_speed": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "setting"},
                    "long_name": {"type": "char", "data": "Fan Speed"},
                    "units": {"type": "string", "data": "%"},
                    "valid_min": {"type": "float", "data": -100.0},
                    "valid_max": {"type": "int", "data": 100.0},
                    "step_increment": {"type": "int", "data": 10},
                    "default_value": {"type": "int", "data": 0},
                },
            },
        },
    }

    def __init__(self, config=None, **kwargs):
        super(SanAce40, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        self.sampling_interval = 1
        # self.configure()

        self.default_data_buffer = asyncio.Queue()

        self.operational_definition_file = (
            "SanyoDenki_SanAce40_operational_definition.json"
        )

        self.last_read_timestamp = None
        self.last_read_count = None

        try:
            with open(self.operational_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")
            sys.exit(1)

        # self.sampling_task_list.append(self.data_loop())
        self.enable_task_list.append(self.default_data_loop())
        # self.enable_task_list.append(self.sampling_monitor())
        self.enable_task_list.append(self.polling_loop())
        # asyncio.create_task(self.sampling_monitor())
        self.collecting = False

        # TODO make this configurable
        self.sampling_mode = "unpolled" 

        # self.i2c_address = "28"

    def configure(self):
        super(SanAce40, self).configure()

        # get config from file
        try:
            with open("/app/config/operational.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # This device does not have a serial/tcp option

        # sensor_iface_properties = {
        #     "default": {
        #         "sensor-interface-properties": {
        #             "connection-properties": {
        #             },
        #             "read-properties": {
        #                 "read-method": "readline",  # readline, read-until, readbytes, readbinary
        #                 # "read-terminator": "\r",  # only used for read_until
        #                 "decode-errors": "strict",
        #                 "send-method": "ascii"
        #             },
        #         }
        #     }
        # }

        # if "interfaces" in conf:
        #     for name, iface in conf["interfaces"].items():
        #         if name in sensor_iface_properties:
        #             for propname, prop in sensor_iface_properties[name].items():
        #                 iface[propname] = prop

        #     self.logger.debug(
        #         "sanace92rf.configure", extra={"interfaces": conf["interfaces"]}
        #     )

        settings_def = self.get_definition_by_variable_type(
            self.metadata, variable_type="setting"
        )
        # for name, setting in self.metadata["settings"].items():
        for name, setting in settings_def["variables"].items():

            requested = setting["attributes"]["default_value"]["data"]

            # override default setting if in config
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]

            self.settings.set_setting(name, requested=requested)
            self.logger.debug("configure", extra={"setting_name": name, "requested": requested, "settings": self.settings})

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            settings=settings_def["variables"],
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

    def check_fan_speed_sp(self, data):
        self.logger.debug("check_fan_speed_sp", extra={"fs-data": data})
        # TODO set setting actual if ok
        
        try:
            requested_sp = self.settings.get_setting("fan_speed_sp")["requested"]
            duty_cycle = data["data"]["data"].get("duty_cycle", None)
            if duty_cycle:
                sp = (duty_cycle-50.0) * 2
                self.logger.debug("check_fan_speed_sp", extra={"sp": sp, "minus": (sp-5), "plus": (sp+5)})
                if (sp > (requested_sp-5)) and (sp < (requested_sp+5)):
                    self.logger.debug("check_fan_speed_sp: here1")
                    self.settings.set_actual("fan_speed_sp", actual=requested_sp)
                    self.logger.debug("check_fan_speed_sp: here2")
                
                # self.logger.debug(
                #     "check_fan_speed_sp",
                #     extra={
                #         "setting-name": "fan_speed_sp",
                #         "setting-value": self.settings.get_setting("fan_speed_sp")
                #     },
                # )
        except Exception as e:
            self.logger.error("check_fan_speed_sp", extra={"reason": e})

    async def handle_interface_data(self, message: Message):
        await super(SanAce40, self).handle_interface_data(message)

        self.logger.debug("interface_recv_data", extra={"data": message})
        if message["type"] == det.interface_data_recv():
            try:
                self.logger.debug("interface_recv_data", extra={"data": message})
                path_id = message["path_id"]
                default_path = self.config.interfaces["default"]["path"]
                pwm_path = self.config.interfaces["fan_speed_sp"]["path"]
                self.logger.debug(
                    "interface_recv_data",
                    extra={"path_id": path_id, "iface_path": self.config.interfaces["default"]["path"]},
                )
                # if path_id == "default":
                # if path_id == iface_path:
                if path_id == default_path:
                    self.logger.debug(
                        "interface_recv_data", extra={"data": message.data}
                    )
                    await self.default_data_buffer.put(message)
                elif path_id == pwm_path:
                    self.check_fan_speed_sp(message.data)
            except KeyError:
                pass

    async def polling_loop(self):

        # TODO add sensor address to config

        # redo format to be more generic (based on new labjack code)
        # i2c_write = {
        #     "address": self.i2c_address,
        #     "data": "00"
        # }
        # i2c_read = {
        #     "address": self.i2c_address,
        #     "read-length": 4,
        #     "delay-ms": 50 # timeout in ms to wait for ACK
        # }
        # data = {
        #     "data": {
        #         "i2c-write": i2c_write,
        #         "i2c-read": i2c_read
        #     }
        # }

        data = (
            {}
        )  # no data required for counter but that may change for other interfaces

        # write_command = {
        #     "i2c-command": "write-byte",
        #     "address": "28",
        #     "data": "00",
        #     # "delay-ms": 50 # 50ms in seconds
        # }
        # read_command = {
        #     "i2c-command": "read-buffer",
        #     "address": "28",
        #     "read-length": 4,
        #     "delay-ms": 50 # timeout in ms to wait for ACK
        # }
        # i2c_commands = [write_command, read_command]
        # data = {
        #     "data": {"i2c-commands": i2c_commands}
        # }

        while True:
            if self.sampling_mode == "polled":
                if self.sampling():
                    await self.interface_send_data(data=data, path_id="default")
                    await asyncio.sleep(time_to_next(self.sampling_interval))
            else:
                await asyncio.sleep(5)
    # async def sampling_monitor(self):

    #     # start_command = f"Log,{self.sampling_interval}\n"
    #     start_command = "Log,1\n"
    #     stop_command = "Log,0\n"
    #     need_start = True
    #     start_requested = False
    #     # wait to see if data is already streaming
    #     await asyncio.sleep(2)
    #     # # if self.collecting:
    #     # await self.interface_send_data(data={"data": stop_command})
    #     # await asyncio.sleep(2)
    #     # self.collecting = False
    #     # init to stopped
    #     # await self.stop_command()

    #     while True:
    #         try:
    #             # self.logger.debug("sampling_monitor", extra={"self.collecting": self.collecting})
    #             # while self.sampling():
    #             #     # self.logger.debug("sampling_monitor:1", extra={"self.collecting": self.collecting})
    #             #     if not self.collecting:
    #             #         # await self.start_command()
    #             #         self.logger.debug("sampling_monitor:2", extra={"self.collecting": self.collecting})
    #             #         await self.interface_send_data(data={"data": start_command})
    #             #         await asyncio.sleep(1)
    #             #         # self.logger.debug("sampling_monitor:3", extra={"self.collecting": self.collecting})
    #             #         self.collecting = True
    #             #         # self.logger.debug("sampling_monitor:4", extra={"self.collecting": self.collecting})

    #             if self.sampling():

    #                 if need_start:
    #                     if self.collecting:
    #                         await self.interface_send_data(data={"data": stop_command})
    #                         await asyncio.sleep(2)
    #                         self.collecting = False
    #                         continue
    #                     else:
    #                         await self.interface_send_data(data={"data": start_command})
    #                         # await self.interface_send_data(data={"data": "\n"})
    #                         need_start = False
    #                         start_requested = True
    #                         await asyncio.sleep(1)
    #                         continue
    #                 elif start_requested:
    #                     if self.collecting:
    #                         start_requested = False
    #                     else:
    #                         await self.interface_send_data(data={"data": start_command})
    #                         # await self.interface_send_data(data={"data": "\n"})
    #                         await asyncio.sleep(1)
    #                         continue
    #             else:
    #                 if self.collecting:
    #                     await self.interface_send_data(data={"data": stop_command})
    #                     await asyncio.sleep(2)
    #                     self.collecting = False

    #             await asyncio.sleep(.1)

    #             # if self.collecting:
    #             #     # await self.stop_command()
    #             #     self.logger.debug("sampling_monitor:5", extra={"self.collecting": self.collecting})
    #             #     await self.interface_send_data(data={"data": stop_command})
    #             #     # self.logger.debug("sampling_monitor:6", extra={"self.collecting": self.collecting})
    #             #     self.collecting = False
    #             #     # self.logger.debug("sampling_monitor:7", extra={"self.collecting": self.collecting})
    #         except Exception as e:
    #             print(f"sampling monitor error: {e}")

    #         await asyncio.sleep(.1)

    # async def start_command(self):
    #     pass # Log,{sampling interval}

    # async def stop_command(self):
    #     pass # Log,0

    # def stop(self):
    #     asyncio.create_task(self.stop_sampling())
    #     super().start()

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
                    destpath = f"{self.get_id_as_topic()}/data/update"
                    event["destpath"] = destpath
                    self.logger.debug(
                        "default_data_loop", extra={"data": event, "destpath": destpath}
                    )
                    message = event
                    # message = Message(data=event, destpath=destpath)
                    self.logger.debug("default_data_loop", extra={"m": message})
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
                # address = iface_data["address"]
                self.logger.debug("default_parse", extra={"iface_data": iface_data})
                # if address == "" or address != self.i2c_address:
                #     return None
                current_read_timestamp = string_to_datetime(timestamp)
                if not self.last_read_timestamp:
                    self.last_read_timestamp = string_to_datetime(timestamp)
                    return None

                variables = list(self.config.metadata.variables.keys())
                # print(f"variables: \n{variables}\n{variables2}")
                variables.remove("time")
                record = self.build_data_record(meta=self.include_metadata)
                # print(f"default_parse: data: {data}, record: {record}")
                self.include_metadata = False
                try:
                    # record["timestamp"] = data.data["timestamp"]
                    record["timestamp"] = timestamp
                    record["variables"]["time"]["data"] = timestamp
                    # parts = data.data["data"].split(",")

                    dataRead = iface_data["data"]  # should return as bytearray

                    self.logger.debug(
                        "default_parse",
                        extra={
                            "last_ts": self.last_read_timestamp,
                            "ts": timestamp,
                            "last_cnt": self.last_read_count,
                            "cnt": dataRead,
                        },
                    )
                    if not self.last_read_timestamp or not self.last_read_count:
                        self.last_read_timestamp = string_to_datetime(timestamp)
                        self.last_read_count = dataRead
                        return None

                    elapsed_time = (
                        current_read_timestamp - self.last_read_timestamp
                    ).total_seconds()
                    revs = (dataRead - self.last_read_count) / 2
                    speed = 60.0 * revs / elapsed_time
                    record["variables"]["fan_speed"]["data"] = round(speed, 3)

                    self.last_read_timestamp = string_to_datetime(timestamp)
                    self.last_read_count = dataRead

                    return record

                except KeyError:
                    pass
            except Exception as e:
                print(f"default_parse error: {e}")
        # else:
        return None

    async def settings_check(self):
        await super().settings_check()

        if not self.settings.get_health():  # something has changed
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):

                    try:
                        # set channel power
                        setting = self.settings.get_setting(name)
                        # TODO: debug here
                        # self.logger.debug("settings_check", extra={"setting": setting, "setting_name": name})
                        if name in ["fan_speed_sp"]:
                            sp = setting["requested"]
                            # convert +/- pct to duty_cycle
                            pwm_data = float((sp / 2.0) + 50.0)
                            data = {"data": {"pwm-data": pwm_data}}
                            # self.logger.debug("settings_check:set_channel_power", extra={"ch": ch, "requested": setting["requested"]})
                            # await self.set_channel_power(ch, setting["requested"])
                            await self.interface_send_data(
                                data=data, path_id="fan_speed_sp"
                            )

                        # self.logger.debug(
                        #     "settings_check - set setting",
                        #     extra={
                        #         "setting-name": name,
                        #         "setting": self.settings.get_setting(name),
                        #     },
                        # )
                    except Exception as e:
                        self.logger.error("settings_check", extra={"reason": e})


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
    logger = logging.getLogger(f"SanyoDenki::SanAce40::{sn}")

    logger.debug("Starting SanAce40")
    inst = SanAce40()
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
