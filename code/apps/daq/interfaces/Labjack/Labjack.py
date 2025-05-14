import asyncio
import signal
import sys
import os
import logging
import logging.config
import yaml

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
            for name, path in conf["paths"].items():
                if "host" not in path:
                    path["host"] = host

            self.logger.debug("conf", extra={"data": conf})

            atts = Labjack.metadata["attributes"]

            path_map = dict()
            for name, val in Labjack.metadata["paths"].items():
                # set path host from interface attributes
                if "host" in atts:
                    val["attributes"]["host"]["data"] = atts["host"]

                client_config = val

                # override values from yaml config
                if "paths" in conf and name in conf["paths"]:
                    self.logger.debug("yaml conf", extra={"id": name, "conf['paths']": conf['paths'], })
                    for attname, attval in conf["paths"][name].items():
                        self.logger.debug("config paths", extra={"id": name, "attname": attname, "attval": attval})
                        client_config["attributes"][attname]["data"] = attval
                self.logger.debug("config paths", extra={"client_config": client_config})
                    
                path_map[name] = {
                    "client_id": name,
                    "client": None,
                    "client_config": client_config,
                    # "recv_handler": self.recv_data_loop(name),
                    "recv_task": None,
                }

            self.config = InterfaceConfig(
                type=atts["type"]["data"],
                name=atts["name"]["data"],
                uid=conf["uid"],
                paths=path_map
            )

            self.logger.debug(
                "configure",
                extra={"conf": conf, "self.config": self.config},
            )
        except Exception as e:
            self.logger.debug("USCDR301:configure", extra={"error": e})
 
    def i2c_data(self, data: dict):

