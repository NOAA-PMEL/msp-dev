import asyncio
import signal
import sys
import os
import logging
import logging.config
import yaml
import traceback
from envds.core import envdsLogger
from envds.daq.controller import Controller, ControllerConfig #, InterfacePath
from envds.daq.event import DAQEvent
from aiomqtt import Client
from pydantic import BaseModel


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
        },
        "paths": {
            "port-1": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": 'localhost'},
                    "port": {"type": "int", "data": 23},
                    "device-interface-properties": {
                        "read-properties": {
                            "read-method": "readline",  # readline, read-until, readbytes, readbinary
                            # "read-terminator": "\r",  # only used for read_until
                            "decode-errors": "strict",
                            "send-method": "ascii",
                        }
                    }
                },
                "data": [],
            },
            "port-2": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.mqtt_client"},
                    "client_class": {"type": "string", "data": "MQTT_Client"},
                    "host": {"type": "string", "data": 'mqtt.default'},
                    "port": {"type": "int", "data": 1883},
                    "subscriptions": {"type": "string", "data": None}
                },
                "data": [],
            }
        }
    }

    def __init__(self, config=None, **kwargs):
        super(NP05B, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_client_module = "envds.daq.clients.tcp_client"
        self.default_client_class = "TCPClient"

        self.data_loop_task = None
        # asyncio.create_task(self.monitor_outlet_status())
        self.enable_task_list.append(self.monitor_outlet_status())

    def configure(self):

        super(NP05B, self).configure()

        try:
            try:
                with open("/app/config/controller.conf", "r") as f:
                    conf = yaml.safe_load(f)
            except FileNotFoundError:
                conf = {"uid": "UNKNOWN", "paths": {}}

            # add hosts to each path if not present
            try:
                host = conf["host"]
            except KeyError as e:
                self.logger.debug("no host - default to localhost")
                host = "localhost"
            print('paths', conf["paths"].items())
            for name, path in conf["paths"].items():
                if "host" not in path:
                    path["host"] = host

            self.logger.debug("conf", extra={"data": conf})

            atts = NP05B.metadata["attributes"]

            path_map = dict()
            for name, val in NP05B.metadata["paths"].items():


                if "client_module" not in val["attributes"]:
                    val["attributes"]["client_module"]["data"] = self.default_client_module
                if "client_class" not in val["attributes"]:
                    val["attributes"]["client_class"]["data"] = self.default_client_class

                # set path host from controller attributes
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
                    "client_module": val["attributes"]["client_module"]["data"],
                    "client_class": val["attributes"]["client_class"]["data"],
                    # "data_buffer": asyncio.Queue(),
                    "recv_handler": self.recv_data_loop(name),
                    "recv_task": None,
                }

            self.config = ControllerConfig(
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
            self.logger.debug("NP05B:configure", extra={"error": e})
            print(traceback.format_exc())

    async def monitor_outlet_status(self):
        while True:
            try:
                tcp_client = self.client_map["port-1"]["client"]
                get_status_command = {'data': "$A5\r"}
                await self.send_data(tcp_client, get_status_command)
                status_data = await tcp_client.recv()
                print("STATUS DATA                                 ", status_data)
                if '$A0,' in status_data["data"]:
                    final_status = {}
                    status_data = status_data["data"].replace('$A0,', '').replace('$A5\r\n', '')
                    print("STATUS DATA                                 ", status_data)
                    # for outlet_status in status_data:
                    #     final_status[outlet] = outlet_status
                    # await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error("get status error", extra={"error": e})
                await asyncio.sleep(1)

    async def deal_with_data(self, client, data):
        try:
            if data['data']['device'] == 'pdu':
                outlet = data['data']['outlet']
                if data['data']['message'] == 'on':
                    print('Driver thinks outlet is commanded on', data)
                    command = {'data': f"pset {outlet} 1\r"}
                    print('COMMAND', command)
                if data['data']['message'] == 'off':
                    command = {'data': f"pset {outlet} 0\r"}
                try:
                    tcp_client = self.client_map["port-1"]["client"]
                    await self.send_data(tcp_client, command)
                except Exception as e:
                    self.logger.error("deal with data error", extra={"error": e})
                    await asyncio.sleep(1)
        except Exception as e:
                    self.logger.error("deal with data error", extra={"error": e})
                    await asyncio.sleep(1)
    

    async def recv_data_loop(self, client_id: str):
        while True:
            try:
                client = self.client_map[client_id]["client"]
                print("Client ID in recv_data_loop:", client_id)
                if client:
                    self.logger.debug("recv_data_loop", extra={"client": client})
                    data = await client.recv()
                    self.logger.debug("recv_data", extra={"client_id": client_id, "data": data}) 
                    await self.update_recv_data(client_id=client_id, data=data)
                    await self.deal_with_data(client, data)

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
    async def send_data(self, client, data):
            try:
                await client.send(data)
            except KeyError:
                pass


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