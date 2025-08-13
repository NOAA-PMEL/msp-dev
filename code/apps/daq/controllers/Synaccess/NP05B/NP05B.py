import asyncio
import signal
import sys
import os
import logging
import logging.config
import yaml
import traceback
from envds.core import envdsLogger
from envds.daq.controller import Controller, ControllerConfig, ControllerMetadata
from envds.daq.event import DAQEvent
from aiomqtt import Client
from pydantic import BaseModel
import json


task_list = []

class NP05B(Controller):
    """docstring for ShellyPro3."""

    metadata = {
        "attributes": {
            "type": {"type": "char", "data": "Synaccess"},
            "name": {"type": "char", "data": "NP05B"},
            "host": {"type": "char", "data": "localhost"},
            "description": {
                "type": "char",
                "data": "Synaccess 5 outlet switch PDU",
            },
            "tags": {"type": "char", "data": "testing, Synaccess, PDU, outlet, serial, tcp, ethernet, sensor"},
            "variable_types": {
                "type": "string",
                "data": ""
            },
            "serial_number": {
                "type": "string",
                "data": ""
            },
            "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
            "client_class": {"type": "string", "data": "TCPClient"},
            "client_host": {"type": "string", "data": 'localhost'},
            "client_port": {"type": "int", "data": 1883},
            # "subscriptions": {"type": "string", "data": None}
        },
        "dimensions": {
            "time": 0
        },
        "variables": {
            "outlet_1_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attribute": {
                    "variable_type": {
                        "type": "string",
                        "data": "setting"
                    },
                    "outlet": {
                        "type": "int",
                        "data": 1
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Outlet 1 Power"
                    },
                    "units": {
                        "type": "char",
                        "data": "count"
                    },
                    "valid_min": {
                        "type": "int",
                        "data": 0
                    },
                    "valid_max": {
                        "type": "int",
                        "data": 1
                    },
                    "step_increment": {
                        "type": "int",
                        "data": 1
                    },
                    "default_value": {
                        "type": "int",
                        "data": 1
                    }
                }
            },
            "outlet_2_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attribute": {
                    "variable_type": {
                        "type": "string",
                        "data": "setting"
                    },
                    "outlet": {
                        "type": "int",
                        "data": 2
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Outlet 2 Power"
                    },
                    "units": {
                        "type": "char",
                        "data": "count"
                    },
                    "valid_min": {
                        "type": "int",
                        "data": 0
                    },
                    "valid_max": {
                        "type": "int",
                        "data": 1
                    },
                    "step_increment": {
                        "type": "int",
                        "data": 1
                    },
                    "default_value": {
                        "type": "int",
                        "data": 1
                    }
                }
            },
            "outlet_3_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attribute": {
                    "variable_type": {
                        "type": "string",
                        "data": "setting"
                    },
                    "outlet": {
                        "type": "int",
                        "data": 3
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Outlet 3 Power"
                    },
                    "units": {
                        "type": "char",
                        "data": "count"
                    },
                    "valid_min": {
                        "type": "int",
                        "data": 0
                    },
                    "valid_max": {
                        "type": "int",
                        "data": 1
                    },
                    "step_increment": {
                        "type": "int",
                        "data": 1
                    },
                    "default_value": {
                        "type": "int",
                        "data": 1
                    }
                }
            },
            "outlet_4_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attribute": {
                    "variable_type": {
                        "type": "string",
                        "data": "setting"
                    },
                    "outlet": {
                        "type": "int",
                        "data": 4
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Outlet 4 Power"
                    },
                    "units": {
                        "type": "char",
                        "data": "count"
                    },
                    "valid_min": {
                        "type": "int",
                        "data": 0
                    },
                    "valid_max": {
                        "type": "int",
                        "data": 1
                    },
                    "step_increment": {
                        "type": "int",
                        "data": 1
                    },
                    "default_value": {
                        "type": "int",
                        "data": 1
                    }
                }
            },
            "outlet_5_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attribute": {
                    "variable_type": {
                        "type": "string",
                        "data": "setting"
                    },
                    "outlet": {
                        "type": "int",
                        "data": 5
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Outlet 5 Power"
                    },
                    "units": {
                        "type": "char",
                        "data": "count"
                    },
                    "valid_min": {
                        "type": "int",
                        "data": 0
                    },
                    "valid_max": {
                        "type": "int",
                        "data": 1
                    },
                    "step_increment": {
                        "type": "int",
                        "data": 1
                    },
                    "default_value": {
                        "type": "int",
                        "data": 1
                    }
                }
            }
        }
    }

    def __init__(self, config=None, **kwargs):
        super(NP05B, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_client_module = "envds.daq.clients.tcp_client"
        self.default_client_class = "TCPClient"
        self.default_client_host = "localhost"
        self.default_client_port = 1883

        self.controller_id_prefix = "NP05B"

        self.data_loop_task = None
        self.enable_task_list.append(self.recv_data_loop())
        self.enable_task_list.append(self.get_status_loop())

        self.controller_definition_file = "Synaccess_NP05B_controller_definition.json"

        try:            
            with open(self.controller_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("controller_definition not found. Exiting")            
            sys.exit(1)


    def configure(self):

        super(NP05B, self).configure()

        try:
            try:
                with open("/app/config/controller.conf", "r") as f:
                    conf = yaml.safe_load(f)
            except FileNotFoundError:
                conf = {"uid": "UNKNOWN", "paths": {}}
            
            self.logger.debug("configure", extra={"conf": conf})

            host = conf.get("host", "localhost")
            port = conf.get("port", 80)
            client_module = conf.get("client_module", self.default_client_module)
            client_class = conf.get("client_class", self.default_client_class)
            client_host = conf.get("client_host", self.default_client_host)
            client_port = conf.get("client_port", self.default_client_port)
            self.controller_id_prefix = conf.get("controller_id_prefix", "NP05B")
            # client_subscriptions_list = conf.get("client_subscriptions", "")
            # if client_subscriptions_list == "":
            #     client_subscriptions = []
            # else:
            #     client_subscriptions = client_subscriptions_list.split(",")

            # status_sub = f"{self.controller_id_prefix}/status/#"
            # if status_sub not in client_subscriptions:
            #     client_subscriptions.append(status_sub)

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

            self.logger.debug("conf", extra={"data": conf})

            attrs = self.metadata["attributes"]

            # override default metadata attributes with config values
            for name, val in conf["attributes"].items():
                if name in attrs:
                    attrs[name]["data"] = val

            settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
            # for name, setting in self.metadata["settings"].items():
            for name, setting in settings_def["variables"].items():
            
                requested = setting["attributes"]["default_value"]["data"]
                if "settings" in config and name in config["settings"]:
                    requested = config["settings"][name]

                self.settings.add_setting(name, requested=requested)

            meta = ControllerMetadata(
                attributes=self.metadata["attributes"],
                dimensions=self.metadata["dimensions"],
                variables=self.metadata["variables"],
                settings=settings_def["variables"]
            )

            self.config = ControllerConfig(
                make=self.metadata["attributes"]["make"]["data"],
                model=self.metadata["attributes"]["model"]["data"],
                serial_number=self.metadata["attributes"]["serial_number"]["data"],
                format_version=self.metadata["attributes"]["format_version"]["data"],
                metadata=meta,
                host=host,
                port=port,
                daq_id=self.core_settings.namespace_prefix # this is a hack for now
            )

            # TODO build client config 
            self.client_config = {
                "client_module": client_module,
                "client_class": client_class,
                "properties": {
                    "host": client_host,
                    "port": client_port,
                    "device-interface-properties": {
                        "read-properties": {
                                "read-method": "readline",  # readline, read-until, readbytes, readbinary
                                # "read-terminator": "\r",  # only used for read_until
                                "decode-errors": "strict",
                                "send-method": "ascii",
                            },
                        # "subscriptions": client_subscriptions,
                    }
                }
            }

            print(f"self.config: {self.config}")

            self.logger.debug(
                "configure",
                extra={"conf": conf, "self.config": self.config},
            )


        except Exception as e:
            self.logger.debug("NP05B:configure", extra={"error": e})
            print(traceback.format_exc())
    

    async def get_status_loop(self):
        while True:
            for outlet in range(0,5):
                get_status_cmd = "$A5\r"
                self.logger.debug("get_status_loop", extra={"payload": get_status_cmd})
                await self.send_data(get_status_cmd)
            await asyncio.sleep(5)


    async def set_outlet_power(self, outlet, state):
        if isinstance(state, str):
            if state.lower() in ["on", "yes"]:
                state = 1
            else:
                state = 0

            if state:
                cmd = 1
            else:
                cmd = 0
            
            data = f"pset {outlet} {cmd}\r"
            self.logger.debug("set_outlet_power", extra={"payload", data})
            await self.send_data(data)

    
    async def recv_data_loop(self):
        while True:
            try:
                data = await self.client_recv_buffer.get()
                self.logger.debu("recv_data_loop", extra={"recv_data": data})
                if data:
                    if '$A0,' in data:
                        try:
                            status_data = status_data["data"].replace('$A0,', '').replace('$A5\r\n', '')
                            status_list = [int(digit) for digit in str(status_data)]
                            self.logger.debug("recv_data_loop", extra={"status_data": status_list})
                            for outlet_status in status_list:
                                name = f"outlet_{status_list.index(outlet_status)}_power"
                                self.settings.set_actual(name=name, actual=outlet_status)
                                self.logger.debug("recv_data_loop", extra={"setting_name": name, "actual": outlet_status, "settings": self.settings.get_settings()})

                        except (KeyError, Exception) as e:
                            self.logger.error("recv_data_loop", extra={"error": e})
                        
                    await asyncio.sleep(0.01)

            except (KeyError, Exception) as e:
                self.logger.error("recv_data_loop", extra={"error": e})
                await asyncio.sleep(0.1)           


    async def wait_for_ok(self, timeout=0):
        pass


    async def send_data(self, data):
        try:
            self.logger.debug("send_data", extra={"payload": data})
            if self.client:
                self.logger.debug("send_data", extra={"payload": data})
                await self.client.send_to_client(data)
        
        except Exception:
            pass
    

    async def settings_check(self):
        await super().settings_check()

        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):

                    try:
                        setting = self.settings.get_setting(name)
                        if name in ["outlet_1_power", "outlet_2_power", "outlet_3_power", "outlet_4_power", "outlet_5_power"]:
                            outlet = self.metadata["variables"][name]["attributes"]["outlet"]["data"]
                            await self.set_outlet_power(outlet, setting["requested"])

                        self.logger.debug(
                            "settings_check - set setting",
                            extra={
                                "setting-name": name,
                                "setting": self.settings.get_setting(name)
                            }
                        )
                    except Exception as e:
                        self.logger.errot("settings_check", extra={"reason": e})


class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"


async def test_task():
    while True:
        await asyncio.sleep(1)
        # print("daq test_task...")
        logger = logging.getLogger("envds.info")
        logger.info("NP05B_test_task", extra={"test": "NP05B task"})


async def shutdown(interface):
    print("shutting down")

    if interface:
        await interface.shutdown()

    for task in task_list:
        print(f"cancel: {task}")
        task.cancel()


async def main(server_config: ServerConfig = None):
    # uiconfig = UIConfig(**config)
    if server_config is None:
        server_config = ServerConfig()
    print(server_config)

    # print("starting mock1 test task")

    # test = envdsBase()
    # task_list.append(asyncio.create_task(test_task()))

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger("interface::NP05B")

    # test = envdsBase()
    # task_list.append(asyncio.create_task(test_task()))

    iface = NP05B()
    iface.run()
    # task_list.append(asyncio.create_task(iface.run()))
    # await asyncio.sleep(2)
    iface.enable()
    logger.debug("Starting Synaccess 5 Outlet PDU Controller")


    event_loop = asyncio.get_event_loop()
    global do_run 
    do_run = True
    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        logger.debug("NP05B.run", extra={"do_run": do_run})
        await asyncio.sleep(1)


    print("starting shutdown...")
    # await iface.shutdown()
    await shutdown(iface)
    # await asyncio.sleep(2)
    print("done.")


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