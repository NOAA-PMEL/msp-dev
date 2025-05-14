import asyncio
import signal
import sys
import os
import logging
import logging.config
import yaml
import time

from envds.core import envdsLogger
from envds.daq.interface import Interface, InterfaceConfig #, InterfacePath
from envds.daq.event import DAQEvent
from envds.daq.client import DAQClient, DAQClientConfig, _StreamClient
from envds.exceptions import envdsRunTransitionException, envdsRunWaitException, envdsRunErrorException


from pydantic import BaseModel
from labjack import ljm


class Labjack(Interface):
    """docstring for T7."""

    metadata = {
        "attributes": {
            "type": {"type": "char", "data": "Labjack"},
            "name": {"type": "char", "data": "Labjack T"},
            "host": {"type": "char", "data": "localhost"},
            "description": {
                "type": "char",
                "data": "Labjack Analog and Digital DAQ Module",
            },
            "tags": {"type": "char", "data": "testing, labjack, analog, digital, ethernet, TCP"},
        },
        "paths": {
            "I2C-1": {
                "attributes": {
                    # "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    # "client_class": {"type": "string", "data": "TCPClient"},
                    # "client_module": {"type": "string", "data": "Labjack"},
                    # "client_class": {"type": "string", "data": "LabjackClient"},
                    "host": {"type": "string", "data": "localhost"},
                    # "port": {"type": "int", "data": 4001},
                    "method": {"type": "string", "data": "I2C"},
                    "CS": {"type": "int", "data": None},
                    "CLK": {"type": "int", "data": None},
                    "slave_add": {"type": "int", "data": None},
                },
                "data": [],
            }
        }
    }

    def __init__(self, config=None, **kwargs):
        super(Labjack, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        #self.default_client_module = "envds.daq.clients.tcp_client"
        #self.default_client_class = "TCPClient"

        # self.default_client_module = "Labjack"
        # self.default_client_class = "LabjackClient"

        self.data_loop_task = None

    def configure(self):
        super(Labjack, self).configure()

        try:
            # get config from file
            try:
                with open("/app/config/interface.conf", "r") as f:
                    conf = yaml.safe_load(f)
            except FileNotFoundError:
                conf = {"uid": "UNKNOWN", "paths": {}}

            # add hosts to each path if not present
            try:
                host = conf["host"]
            except KeyError as e:
                self.logger.debug("no host - default to localhost")
                host = "localhost"
            
            path_map = dict()
            for name, path in conf["paths"].items():
                if "host" not in path:
                    path["host"] = host

                param_config = dict()
                for parameter, value in path:
                    param_config[parameter] = value

                path_map[name] = param_config
            
            atts = Labjack.metadata["attributes"]
            
            self.config = InterfaceConfig(
                type=atts["type"]["data"],
                name=atts["name"]["data"],
                uid=conf["uid"],
                paths=path_map
            )

            self.host = host
            self.path_map = path_map

            # self.logger.debug("conf", extra={"data": conf})

            # atts = Labjack.metadata["attributes"]

            # path_map = dict()
            # for name, val in Labjack.metadata["paths"].items():
            #     # set path host from interface attributes
            #     if "host" in atts:
            #         val["attributes"]["host"]["data"] = atts["host"]

            #     client_config = val

            #     # override values from yaml config
            #     if "paths" in conf and name in conf["paths"]:
            #         self.logger.debug("yaml conf", extra={"id": name, "conf['paths']": conf['paths'], })
            #         for attname, attval in conf["paths"][name].items():
            #             self.logger.debug("config paths", extra={"id": name, "attname": attname, "attval": attval})
            #             client_config["attributes"][attname]["data"] = attval
            #     self.logger.debug("config paths", extra={"client_config": client_config})
                    
            #     path_map[name] = {
            #         "client_id": name,
            #         "client": None,
            #         "client_config": client_config,
            #         # "recv_handler": self.recv_data_loop(name),
            #         "recv_task": None,
            #     }

            # self.config = InterfaceConfig(
            #     type=atts["type"]["data"],
            #     name=atts["name"]["data"],
            #     uid=conf["uid"],
            #     paths=path_map
            # )

            # self.logger.debug(
            #     "configure",
            #     extra={"conf": conf, "self.config": self.config},
            # )

        except Exception as e:
            self.logger.debug("USCDR301:configure", extra={"error": e})


    def open_conn(self):
        handle = ljm.openS("ANY", "ANY", self.host)
        return handle

    def i2c_data(self, data: dict):
        handle = self.open_conn()
        I2C_path = self.path_map["I2C-1"]

        ljm.eWriteName(handle, "I2C_SDA_DIONUM", I2C_path['CS'])  # CS is FIO2
        ljm.eWriteName(handle, "I2C_SCL_DIONUM", I2C_path['CLK'])  # CLK is FIO3
        ljm.eWriteName(handle, "I2C_SPEED_THROTTLE", I2C_path['frequency']) # CLK frequency approx 100 kHz
        ljm.eWriteName(handle, "I2C_OPTIONS", I2C_path['options'])
        ljm.eWriteName(handle, "I2C_SLAVE_ADDRESS", I2C_path['slave_add']) # default address is 0x28 (40 decimal)

        ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", I2C_path['num_write_bytes'])
        ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", 0)

        ljm.eWriteNameByteArray(handle, "I2C_DATA_TX", I2C_path['num_write_bytes'], [I2C_path['command']])
        ljm.eWriteName(handle, "I2C_GO", 1)
        time.sleep(0.5)
        ljm.eWriteName(handle, "I2C_NUM_BYTES_TX", 0)
        ljm.eWriteName(handle, "I2C_NUM_BYTES_RX", I2C_path['num_read_bytes'])
        ljm.eWriteName(handle, "I2C_GO", 1)

        dataRead = ljm.eReadNameByteArray(handle, "I2C_DATA_RX", I2C_path['num_read_bytes'])
