import asyncio
import json
import signal
import sys
import os
import logging
import logging.config
import yaml
import traceback
from envds.core import envdsLogger
from envds.daq.controller import Controller, ControllerMetadata, ControllerConfig #, InterfacePath
from envds.daq.event import DAQEvent
from aiomqtt import Client
from pydantic import BaseModel


task_list = []

class ShellyPro3(Controller):
    """docstring for ShellyPro3."""

    metadata = {
        "attributes": {
            "make": {"type": "char", "data": "Shelly"},
            "model": {"type": "char", "data": "ShellyPro3"},
            "host": {"type": "char", "data": "localhost"},
            "description": {
                "type": "char",
                "data": "Shelly Pro 3 Smart Switch",
            },
            "tags": {"type": "char", "data": "testing, Shelly, ShellyPro3, serial, tcp, ethernet, sensor"},
            "format_version": {
                "type": "char",
                "data": "1.0.0"
            },
            "variable_types": {
                "type": "string",
                "data": "main, setting"
            },
            "serial_number": {
                "type": "string",
                "data": ""
            },
            "client_module": {"type": "string", "data": "envds.daq.clients.mqtt_client"},
            "client_class": {"type": "string", "data": "MQTT_Client"},
            "client_host": {"type": "string", "data": 'mqtt.default'},
            "client_port": {"type": "int", "data": 1883},
            "subscriptions": {"type": "string", "data": None}
        },
        "dimensions": {
            "time": 0
        },
        "variables": {
            "temperature": {
                "type": "float",
                "shape": [
                    "time"
                ],
                "attributes": {
                    "variable_type": {
                        "type": "string",
                        "data": "main"
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Temperature"
                    },
                    "units": {
                        "type": "char",
                        "data": "degree_C"
                    }
                }
            },
            "channel_1_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attributes": {
                    "variable_type": {
                        "type": "string", 
                        "data": "setting"
                    },
                    "channel": {
                        "type": "int",
                        "data": 1
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Channel 1 Power"
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
            "channel_2_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attributes": {
                    "variable_type": {
                        "type": "string", 
                        "data": "setting"
                    },
                    "channel": {
                        "type": "int",
                        "data": 2
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Channel 2 Power"
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
            "channel_3_power": {
                "type": "int",
                "shape": [
                    "time"
                ],
                "attributes": {
                    "variable_type": {
                        "type": "string", 
                        "data": "setting"
                    },
                    "channel": {
                        "type": "int",
                        "data": 3
                    },
                    "long_name": {
                        "type": "char",
                        "data": "Channel 3 Power"
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
        }
    }

    def __init__(self, config=None, **kwargs):
        super(ShellyPro3, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_client_module = "envds.daq.clients.mqtt_client"
        self.default_client_class = "MQTT_Client"
        self.default_client_host = "mqtt.default"
        self.default_client_port = 1883

        self.controller_id_prefix = "shellypro3"
        

        self.data_loop_task = None
        # self.enable_task_list.append(self.deal_with_data())

        # TODO change to external json definition - this is placeholder
        # self.metadata = ShellyPro3.metadata
        self.controller_definition_file = "Shelly_ShellyPro3_controller_definition.json"

        try:            
            with open(self.controller_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("controller_definition not found. Exiting")            
            sys.exit(1)

    def configure(self):

        super(ShellyPro3, self).configure()

        try:
            try:
                with open("/app/config/controller.conf", "r") as f:
                    conf = yaml.safe_load(f)
            except FileNotFoundError:
                conf = {"uid": "UNKNOWN", "paths": {}}

            self.logger.debug("configure", extra={"conf": conf})

            # TODO 

            # # add hosts to each path if not present
            # try:
            #     host = conf["host"]
            # except KeyError as e:
            #     self.logger.debug("no host - default to localhost")
            #     host = "localhost"
            # print('paths', conf["paths"].items())
            # for name, path in conf["paths"].items():
            #     if "host" not in path:
            #         path["host"] = host

            host = conf.get("host", "localhost")
            port = conf.get("port", 80)
            client_module = conf.get("client_module", self.default_client_module)
            client_class = conf.get("client_class", self.default_client_class)
            client_host = conf.get("client_host", self.default_client_host)
            client_port = conf.get("client_port", self.default_client_port)
            self.controller_id_prefix = conf.get("controller_id_prefix", "shellypro3")
            client_subscriptions_list = conf.get("client_subscriptions", "")
            if client_subscriptions_list == "":
                client_subscriptions = []
            else:
                client_subscriptions = client_subscriptions_list.split(",")

            status_sub = f"{self.controller_id_prefix}/status/#"
            if status_sub not in client_subscriptions:
                client_subscriptions.append(status_sub)


            self.logger.debug("conf", extra={"data": conf})

            attrs = ShellyPro3.metadata["attributes"]

            # override default metadata attributes with config values
            for name, val in conf["attributes"].items():
                if name in attrs:
                    attrs[name]["data"] = val

            # path_map = dict()
            # for name, val in ShellyPro3.metadata["paths"].items():


            #     if "client_module" not in val["attributes"]:
            #         val["attributes"]["client_module"]["data"] = self.default_client_module
            #     if "client_class" not in val["attributes"]:
            #         val["attributes"]["client_class"]["data"] = self.default_client_class

            #     # set path host from controller attributes
            #     if "host" in attrs:
            #         val["attributes"]["host"]["data"] = attrs["host"]

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
            #         "client_module": val["attributes"]["client_module"]["data"],
            #         "client_class": val["attributes"]["client_class"]["data"],
            #         # "data_buffer": asyncio.Queue(),
            #         "recv_handler": self.recv_data_loop(name),
            #         "recv_task": None,
            #     }

            # self.config = ControllerConfig(
            #     type=attrs["type"]["data"],
            #     name=attrs["name"]["data"],
            #     uid=conf["uid"],
            #     paths=path_map
            # )

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
                # settings=self.metadata["settings"],
                settings=settings_def["variables"]
            )

            self.config = ControllerConfig(
                make=self.metadata["attributes"]["make"]["data"],
                model=self.metadata["attributes"]["model"]["data"],
                serial_number=self.metadata["attributes"]["serial_number"]["data"],
                metadata=meta,
                host=host,
                port=port,
                # interfaces=conf["interfaces"],
                # daq_id=conf["daq_id"],
                daq_id=self.core_settings.namespace_prefix # this is a hack for now
            )

            # TODO build client config 
            self.client_config = {
                "client_module": client_module,
                "client_class": client_class,
                "properties": {
                    "host": client_host,
                    "port": client_port,
                    "subscriptions": client_subscriptions,
                }
            }

            print(f"self.config: {self.config}")

            self.logger.debug(
                "configure",
                extra={"conf": conf, "self.config": self.config},
            )
        except Exception as e:
            self.logger.debug("ShellyPro3:configure", extra={"error": e})
            print(traceback.format_exc())

    async def get_status_loop(self):
        #TODO get status for each channel every N seconds and update settings based on response
        pass

        while True:
            for channel in range(0,3):

                data = {
                    "path": f"{self.controller_id_prefix}/command/switch:{channel}",
                    "message": "status_update"
                }

                await self.send_data(data)
            await asyncio.sleep(5)

    async def set_channel_power(self, channel, state):
        if isinstance(state, str):
            if state.lower() in ["on", "yes"]:
                state = 1
            else:
                state = 0 
        
        if state:
            cmd = "on"
        else:
            cmd = "off"
        data = {
            "path": f"{self.controller_id_prefix}/command/switch:{channel}",
            "message": cmd
        }
        await self.send_data(data)
        

    async def deal_with_data(self, client, data):
        if data['data']['device'] == 'shelly':
            toggle_topic = 'shellypro3/command/switch:'
            channel = data['data']['channel']
            toggle_topic = toggle_topic + str(channel)
            complete_message = {'topic': toggle_topic, 'message': data['data']['message']}
            try:
                await self.send_data(client, complete_message)
            except Exception as e:
                self.logger.error("deal with data error", extra={"error": e})
                await asyncio.sleep(1)
        else:
            pass

    async def recv_data_loop(self, client_id: str):
        while True:
            try:
                data = await self.client_recv_buffer.get()
                status = json.loads(data)

                # the only data coming from Shelly should be status
                if "id" in status:
                    channel = status["id"]
                    output = status["output"]
                    if channel == 0:
                        temperature = status["temperature"]["tC"]
                        record = self.build_data_record(meta=False)

                        # channel 0 temperature data record
                        if record:
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


                    # update actual state of channel output
                    name = f"channel_{channel}_power"
                    self.settings.set_actual(name=name, actual=int(output))
                await asyncio.sleep(0.01)

            except (KeyError, Exception) as e:
                self.logger.error("recv_data_loop", extra={"error": e})
                await asyncio.sleep(1)           

    async def wait_for_ok(self, timeout=0):
        pass

    # async def send_data(self, event: DAQEvent):
    #         print(f"here:1 {event}")
    #         try:
    #             print(f"send_data:1 - {event}")
    #             client_id = event["path_id"]
    #             client = self.client_map[client_id]["client"]
    #             data = event.data["data"]

    #             await client.send(data)
    #         except KeyError:
    #             pass
    async def send_data(self, data):
            try:
                if self.client:
                    await self.client.send_to_client(data)
            except Exception:
                pass

    async def settings_check(self):
        await super().settings_check()

        if not self.settings.get_health():  # something has changed
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    
                    # set channel power
                    if setting := self.settings.get_setting(name):
                        if name in ["channel_1_power", "channel_2_power", "channel_3_power"]:
                            ch = self.metadata["variables"][name]["attributes"]["channel"]["data"]
                            self.set_channel_power(ch, setting["requested"])


                    self.logger.debug(
                        "settings_check - set setting",
                        extra={
                            "setting-name": name,
                            "setting": self.settings.get_setting(name),
                        },
                    )


class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"


async def test_task():
    while True:
        await asyncio.sleep(1)
        # print("daq test_task...")
        logger = logging.getLogger("envds.info")
        logger.info("USCDR301_test_task", extra={"test": "USCDR301 task"})


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
    logger = logging.getLogger("interface::USCDR301")

    # test = envdsBase()
    # task_list.append(asyncio.create_task(test_task()))

    iface = ShellyPro3()
    iface.run()
    # task_list.append(asyncio.create_task(iface.run()))
    # await asyncio.sleep(2)
    iface.enable()
    logger.debug("Starting Shelly Pro 3 Controller")


    event_loop = asyncio.get_event_loop()
    global do_run 
    do_run = True
    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        logger.debug("ShellyPro3.run", extra={"do_run": do_run})
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