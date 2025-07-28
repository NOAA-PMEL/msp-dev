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
import numpy as np

# from envds.daq.db import init_sensor_type_registration, register_sensor_type

task_list = []


class SpiderMagic810(Sensor):
    """docstring for SpiderMagic810."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "AerosolDynamics"},
            "model": {"type": "string", "data": "Spider-MAGIC810"},
            "description": {
                "type": "string",
                "data": "Aerosol particle mobility spectrometer (combines Spider DMA and MAGIC CPC) manufactured by Aerosol Dyanamics and distributed by Aerosol Devices/Handix",
            },
            "tags": {
                "type": "char",
                "data": "aerosol, cpc, dma, spectrometer, size distribution, particles, concentration, sensor",
            },
            "format_version": {"type": "char", "data": "1.0.0"},
            "variable_types": {"type": "string", "data": "main, setting, calibration"},
            "serial_number": {"type": "string", "data": ""},
        },
        "dimensions": {"time": 0, "aerodynamic diameter": 53},
        "variables": {
            "time": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Time"}
                },
            },
            "tau": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "CHANGE"}
                },
            },
            "HV_polarity": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "High Voltage Polarity"}
                },
            },
            "scan_dir": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Scan Sequence Direction"}
                },
            },
            "data_freq": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Data Frequency for Display"},
                    "units": {"type": "char", "data": "Hz"},
                },
            },
            "HV_status": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "High Voltage Status"}
                },
            },
            "vp_rd": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Voltage Polarity and Ramp Direction"}
                },
            },
            "spidermagic_timestamp": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Internal Timestamp"}
                },
            },
            # "concentration": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "main"},
            #         "long_name": {"type": "char", "data": "Concentration"},
            #         "units": {"type": "char", "data": "cm-3"},
            #     },
            # },
            "dew_point": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Dew Point"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "input_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Input T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "input_rh": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Input RH"},
                    "units": {"type": "char", "data": "percent"},
                },
            },
            "cond_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Conditioner T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "init_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Initiator T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "mod_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Moderator T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "opt_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Optics Head T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "heatsink_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Heat Sink T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "case_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Case T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "wick_sensor": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Wick Sensor"},
                    "units": {"type": "char", "data": "percent"},
                },
            },
            "mod_T_sp": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Moderator Set Point T"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "humid_exit_dew_point": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Humidifier Exit Dew Point (estimated)",
                    },
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "wadc": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Wick Sensor Raw ADC Signal",
                    }
                },
            },
            "DMA_V": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Voltage (read) supplied to the Spider DMA electrode",
                    },
                    "units": {"type": "char", "data": "Volts"},
                },
            },
            "Qsh": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Volumetric flowrate of the DMA sheath flow",
                    },
                    "units": {"type": "char", "data": "cm3 min-1"},
                },
            },
            "abs_pressure": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Absolute Pressure"},
                    "units": {"type": "char", "data": "mbar"},
                },
            },
            "flow": {
                "type": "int",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Volumetric Flow Rate"},
                    "units": {"type": "char", "data": "cm3 min-1"},
                    # "valid_min": {"type": "float", "data": 0.0},
                    # "valid_max": {"type": "float", "data": 5.0},
                },
            },
            "pHt2.%": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Upper Threshold.Percentile"},
                    "description": {
                        "type": "char",
                        "data": "Two pieces of information. The number before the decimal is the upper threshold in mV. The number after the decimal represents the percentage of light-scattering pulses that were large enough to exceed the upper threshold. Under default settings dhtr2 represents the median pulse height.",
                    },
                },
            },
            "status_hex": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Compact display of status codes",
                    },
                },
            },
            "status_ascii": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Alphabetic list of all abnormal status codes",
                    },
                },
            },
            "spidermagic_serial_number": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {
                        "type": "char",
                        "data": "Serial Number",
                    },
                },
            },
            "Vi": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Starting Voltage"},
                    "units": {"type": "char", "data": "volts"}
                },
            },
            "Vf": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Ending Voltage"},
                    "units": {"type": "char", "data": "volts"}
                },
            },
            "Vmax": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Maximum Voltage"},
                    "units": {"type": "char", "data": "volts"}
                },
            },
            "Tc": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Time Constant"},
                },
            },
            "elap_time": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Elapsed Time during Scan"},
                    "units": {"type": "char", "data": "seconds"},
                },
            },
            "scan_status": {
                "type": "char",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Scan Status"},
                },
            },
            "set_V": {
                "type": "float",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Set Voltage"},
                    "units": {"type": "char", "data": "volts"}
                },
            },
            "read_V": {
                "type": "float",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Read Voltage"},
                    "units": {"type": "char", "data": "volts"}
                },
            },
            "concentration": {
                "type": "float",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Concentration"},
                    "units": {"type": "char", "data": "cm-3"},
                },
            },
            "raw_counts": {
                "type": "int",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Raw Counts"},
                    "units": {"type": "char", "data": "counts"},
                },
            },
            "dead_counts": {
                "type": "int",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Dead Counts"},
                    "units": {"type": "char", "data": "counts"},
                },
            },
            "sh_flow": {
                "type": "int",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sheath Flow"},
                    "units": {"type": "char", "data": "cm3 min-1"},
                },
            },
            "aer_flow": {
                "type": "int",
                "shape": ["time", "diameter"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Aerosol Flow"},
                    "units": {"type": "char", "data": "cm3 min-1"},
                },
            },
        },
    }


    def __init__(self, config=None, **kwargs):
        super(SpiderMagic810, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1
        # self.configure()
        self.first_record = 'HDT'
        self.last_record = 'CHANGE'
        self.array_buffer = []
        self.sequence_start = False
        self.sequence_end = False
        self.seq_counter = 0

        self.default_data_buffer = asyncio.Queue()

        # self.sensor_definition_file = "AerosolDynamics_SpiderMagic810_sensor_definition.json"

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
        self.enable_task_list.append(self.sampling_monitor())
        # self.enable_task_list.append(self.register_sensor())
        # asyncio.create_task(self.sampling_monitor())
        self.collecting = False

    def configure(self):
        super(SpiderMagic810, self).configure()

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
                        "baudrate": 115200,
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
                "SpiderMagic810.configure", extra={"interfaces": conf["interfaces"]}
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
        await super(SpiderMagic810, self).handle_interface_data(message)

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
        start_command = "hvgo\r"
        stop_command = "stop\r"

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

                if "v1" in data.data['data']:
                    record_heading = self.default_parse(data)
                    continue

                elif self.sequence_end:
                    self.sequence_end = False
                    for var in record_heading["variables"]:
                        if record_heading["variables"][var]["data"]:
                            ongoing_record["variables"][var]["data"] = record_heading["variables"][var]["data"]
                
                elif "STARTING" in data.data['data']:
                    ongoing_record = self.default_parse(data)
                    continue

                else:
                    record2 = self.default_parse(data)
                    if not record2:
                        continue
                    else:
                        for var in record2["variables"]:
                            if var != 'time':
                                try:
                                    if record2["variables"][var]["data"].any():
                                        ongoing_record["variables"][var]["data"] = record2["variables"][var]["data"]
                                except Exception:
                                    if record2["variables"][var]["data"]:
                                        ongoing_record["variables"][var]["data"] = record2["variables"][var]["data"]
                        continue
                record = ongoing_record
                print('FINAL RECORD', record)
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

                record = self.build_data_record(meta=self.include_metadata)
                self.include_metadata = False
                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]
                    parts = data.data["data"].split(",")
                    parts = [item.replace('\r\n', '').strip() for item in parts]

                    if (datavar := 'v1') in data.data["data"]:
                        parts = parts[2:3] + parts[4:8]
                        parts = [item.replace('Hz', '').replace('tau=', '') for item in parts]
                        self.var_name = variables[0:5]

                    elif (datavar := 'STARTING') in data.data["data"]:
                        parts = parts[1:3] + parts[4:25]
                        parts = [item.replace('V', '') for item in parts]
                        self.var_name = variables[5:28]

                    elif (datavar := 'Vi') in data.data["data"]:
                        parts = parts[0:4]
                        parts = [item.replace('Vi=', '').replace('Vf=', '').replace('Vmax=', '').replace('Tc=', '') for item in parts]
                        elapsed_time = abs(round((np.log(float(parts[1])/float(parts[0])))*float(parts[3]), 2))
                        parts.append(elapsed_time)
                        self.var_name = variables[28:33]

                    elif (datavar := 'START SEQ') in data.data["data"]:
                        self.sequence_start = True
                        self.seq_counter = 0
                        return None
                    
                    elif (datavar := 'END SEQ') in data.data["data"]:
                        self.sequence_end = True
                        self.sequence_start = False
                        return None
                    
                    elif self.sequence_start:
                        self.var_name = variables[33:41]

                        if self.seq_counter < 52:
                            self.check_array_buffer(parts, array_cond=False)
                            self.seq_counter += 1
                            return
                        
                        else:
                            parts = self.check_array_buffer(parts, array_cond=True)
                            self.seq_counter = 0
                            parts = np.array(list(parts), dtype=object)
                            transposed = np.transpose(parts)
                            parts = transposed.tolist()
                            self.array_buffer = []
                        pass

                    else:
                        return None

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
    logger = logging.getLogger(f"Aerosol Dynamics::SpiderMagic810::{sn}")

    logger.debug("Starting SpiderMagic810")
    inst = SpiderMagic810()
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
