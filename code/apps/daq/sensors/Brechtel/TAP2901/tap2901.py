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
import struct
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


class TAP2901(Sensor):
    """docstring for TAP2901."""

    metadata = {
        "attributes": {
            # "name": {"type"mock1",
            "make": {"type": "string", "data": "Brechtel"},
            "model": {"type": "string", "data": "TAP2901"},
            "description": {
                "type": "string",
                "data": "Tricolor Absorption Photometer (TAP) manufactured and distributed by Brechtel",
            },
            "tags": {
                "type": "char",
                "data": "aerosol, photometer, absorption, sensor",
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
            "record_type": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Record Type (03)"}
                },
            },
            "status_flags": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Status Flags"}
                },
            },
            "elapsed_time": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Elapsed Time"}
                },
            },
            "filter_id": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Filter ID"}
                },
            },
            "active_spot": {
                "type": "str",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "string", "data": "Active Sample Channel"}
                },
            },
            "flow_rate": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Flow Rate"},
                    "units": {"type": "char", "data": "SLPM"},
                },
            },
            "sample_vol_active_spot": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sample Volume for Active Spot"},
                    "units": {"type": "char", "data": "m3"},
                },
            },
            "case_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Case Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "sample_T": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Sample Air Temperature"},
                    "units": {"type": "char", "data": "degrees_C"},
                },
            },
            "ch0_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 0 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch0_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 0 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch0_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 0 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch0_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 0 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch1_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch1_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch1_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch1_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 1 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch2_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch2_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch2_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch2_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 2 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch3_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch3_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch3_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch3_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 3 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch4_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 4 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch4_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 4 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch4_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 4 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch4_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 4 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch5_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 5 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch5_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 5 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch5_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 5 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch5_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 5 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch6_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 6 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch6_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 6 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch6_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 6 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch6_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 6 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch7_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 7 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch7_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 7 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch7_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 7 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch7_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 7 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch8_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 8 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch8_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 8 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch8_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 8 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch8_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 8 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch9_dark_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 9 Dark Intensity"},
                    "units": {"type": "char", "data": "EDIT"}
                },
            },
            "ch9_red_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 9 Red Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 652},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch9_green_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 9 Green Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 528},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            "ch9_blue_intensity": {
                "type": "float",
                "shape": ["time"],
                "attributes": {
                    "variable_type": {"type": "string", "data": "main"},
                    "long_name": {"type": "char", "data": "Channel 9 Blue Intensity"},
                    "units": {"type": "char", "data": "EDIT"},
                    "wavelength": {"type": "int", "data": 467},
                    "wavelength_units": {"type": "char", "data": "nm"}
                },
            },
            # "oint": {
            #     "type": "int",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "configuration"},
            #         "long_name": {
            #             "type": "char",
            #             "data": "Output Interval",
            #         },
            #         "units": {"type": "char", "data": "seconds"},
            #         # "valid_min": {"type": "int", "data": 240},
            #         # "valid_max": {"type": "int", "data": 360},
            #         # "step_increment": {"type": "int", "data": 10},
            #         "default_value": {"type": "int", "data": 300},
            #     },
            # },
            # "lpf": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "configuration"},
            #         "long_name": {"type": "char", "data": "Digital Filter Settings"},
            #         # "units": {"type": "char", "data": "count"},
            #         # "valid_min": {"type": "int", "data": 1},
            #         # "valid_max": {"type": "int", "data": 255},
            #         # "step_increment": {"type": "int", "data": 1},
            #         "default_value": {"type": "char", "data": "2.17872e-1, 1.26719, -6.02159e-1, 1.27174e-1, -1.00721e-2"},
            #         # "default_value": {"type": "char", "data": "4.9223749853, 1.26719, -17.3683786754, 2.45694773252, -4.73788064044"},
            #     },
            # },
            # "hsp": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "configuration"},
            #         "long_name": {"type": "char", "data": "Case Heater Setpoint"},
            #         "units": {"type": "char", "data": "degrees_C"},
            #         "valid_min": {"type": "int", "data": 0},
            #         "valid_max": {"type": "int", "data": 50},
            #         "step_increment": {"type": "int", "data": 0.1},
            #         # "default_value": {"type": "float", "data": },
            #     },
            # },
            # "stbl": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "configuration"},
            #         "long_name": {"type": "char", "data": "Case Temperature Window"},
            #         "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         "step_increment": {"type": "int", "data": 0.1},
            #         # "default_value": {"type": "float", "data": },
            #     },
            # },
            # "Tc": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "calibration"},
            #         "long_name": {"type": "char", "data": "Case Temperature Calibration"},
            #         # "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         # "step_increment": {"type": "int", "data": 0.1},
            #         "default_value": {"type": "char", "data": "1.039004e-3, 2.376432e-4, 0, 1.6161e-7"},
            #         # "default_value": {"type": "char", "data": "-0.175694307104, 2.45981192217, 0, -2.60698473703"},
            #     },
            # },
            # "Ta": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "calibration"},
            #         "long_name": {"type": "char", "data": "Inlet Flow Temperature Calibration"},
            #         # "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         # "step_increment": {"type": "int", "data": 0.1},
            #         "default_value": {"type": "char", "data": "1.039004e-3, 2.376432e-4, 0, 1.6161e-7"},
            #         # "default_value": {"type": "char", "data": "-0.175694307104, 2.45981192217, 0, -2.60698473703"},
            #     },
            # },
            # "flow": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "calibration"},
            #         "long_name": {"type": "char", "data": "Flow Calibration"},
            #         # "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         # "step_increment": {"type": "int", "data": 0.1},
            #         "default_value": {"type": "char", "data": "-8.03700e-01,1.20400e+00,-4.99400e-01,9.74000e-02"},
            #         # "default_value": {"type": "char", "data": "-22.8468310553, 3.27281132146, -14.5750994513, 24.4760650092"},
            #     },
            # },
            # "pid": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "calibration"},
            #         "long_name": {"type": "char", "data": "PID Gain"},
            #         # "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         # "step_increment": {"type": "int", "data": 0.1},
            #         # "default_value": {"type": "char", "data": "250, 5, 1"},
            #     },
            # },
            # "led": {
            #     "type": "float",
            #     "shape": ["time"],
            #     "attributes": {
            #         "variable_type": {"type": "string", "data": "setting"},
            #         "tags": {"type": "string", "data": "calibration"},
            #         "long_name": {"type": "char", "data": "LED mode and intensity for each color"},
            #         # "units": {"type": "char", "data": "degrees_C"},
            #         # "valid_min": {"type": "int", "data": 0},
            #         # "valid_max": {"type": "int", "data": 50},
            #         # "step_increment": {"type": "int", "data": 0.1},
            #         "default_value": {"type": "char", "data": "1, 100, 100, 100"},
            #     },
            # },
        },
    }

    def __init__(self, config=None, **kwargs):
        super(TAP2901, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_data_buffer = asyncio.Queue()

        self.sensor_definition_file = "Brechtel_TAP2901_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

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
        super(TAP2901, self).configure()

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
                        "baudrate": 57600,
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
                "tap2901.configure", extra={"interfaces": conf["interfaces"]}
            )

        # TODO change settings for new sensor definition
        '''
        The new settings are part [variables] now so this is a bit of a hack to use the existing structure
        with the new format.
        '''
        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        # for name, setting in TAP2901.metadata["settings"].items():
        for name, setting in settings_def["variables"].items():
        
            requested = setting["attributes"]["default_value"]["data"]
            if "settings" in config and name in config["settings"]:
                requested = config["settings"][name]

            self.settings.set_setting(name, requested=requested)

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            # settings=TAP2901.metadata["settings"],
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
        await super(TAP2901, self).handle_interface_data(message)

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
        start_command = "show\r"
        # stop_command = "show\r"
        stop_command = "hide\r"

        need_start = True
        # need_start = False
        start_requested = False
        # wait to see if data is already streaming
        await asyncio.sleep(2)
        # # if self.collecting:
        # await self.interface_send_data(data={"data": stop_command})
        # await asyncio.sleep(2)
        # self.collecting = True
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
                # variables = [
                #     "time",
                #     "temperature",
                #     "rh",
                #     "pressure",
                #     "wind_speed",
                #     "wind_direction",
                # ]
                # variables = list(self.config.variables.keys())
                variables = list(self.config.metadata.variables.keys())
                # print(f"variables: \n{variables}\n{variables2}")
                variables.remove("time")
                # variables2.remove("time")
                print(f"variables: \n{variables}")

                print(f"include metadata: {self.include_metadata}")
                record = self.build_data_record(meta=self.include_metadata)
                print(f"default_parse: data: {data}, record: {record}")
                self.include_metadata = False
                try:
                    record["timestamp"] = data.data["timestamp"]
                    record["variables"]["time"]["data"] = data.data["timestamp"]
                    parts = data.data["data"].split(",")

                    if len(parts) < 49:
                        return None
                    for index, name in enumerate(variables):
                        if name in record["variables"]:
                            # instvar = self.config.variables[name]
                            instvar = self.config.metadata.variables[name]
                            vartype = instvar.type
                            if instvar.type == "string":
                                vartype = "str"
                            if 'intensity' in name:
                                intensity = struct.unpack('!f', bytes.fromhex(parts[index].strip()))[0]
                            else:
                                intensity = parts[index].strip()
                            try:
                                # record["variables"][name]["data"] = eval(vartype)(parts[index].strip())
                                record["variables"][name]["data"] = round(eval(vartype)(intensity), 2)
                            except ValueError:
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
    logger = logging.getLogger(f"Brechtel::TAP2901::{sn}")

    logger.debug("Starting TAP2901")
    inst = TAP2901()
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