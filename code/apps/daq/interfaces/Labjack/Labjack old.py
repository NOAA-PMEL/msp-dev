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


task_list = []


class _LabjackClient(_StreamClient):
    """docstring for _LabjackClient."""

    def __init__(self, config=None):
        super().__init__(config)
        self.logger.debug("_LabjackClient.init")
        self.connected = False
        self.keep_connected = False
        asyncio.create_task(self.connection_monitor())
        self.host = ""
        self.port = 0
        self.handle = None
        self.reconnect_delay = 1
        self.logger.debug("_LabjackClient.init done")

        if "host" in self.config.properties:
            self.host = self.config.properties["host"]["data"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]["data"]

    def configure(self):
        super().configure()
        self.logger.debug("_LabjackClient: configure", extra={"config": self.config.properties})

        if "host" in self.config.properties:
            self.host = self.config.properties["host"]["data"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]["data"]
        self.logger.debug("_LabjackClient: configure", extra={"host": self.host, "port": self.port})
    
    async def connection_monitor(self):
        while True:
            try:
                while self.keep_connected:
                    if self.connection_state == self.DISCONNECTED:
                        self.connection_state = self.CONNECTING
                        try:
                            self.handle = ljm.openS("ANY", "ANY", self.host)
                            self.connection_state = self.CONNECTED
                        except(asyncio.TimeoutError, ConnectionRefusedError, Exception) as e:
                            self.logger.error("_LabjackClient connection error", extra={"error":e})
                            self.handle = None
                            self.connection_state = self.DISCONNECTED
                    await asyncio.sleep(self.reconnect_delay)
                
                await asyncio.sleep(self.reconnect_delay)
            except Exception as e:
                self.logger.error("connection_monitor error", extra={"error": e})

    async def do_enable(self):
        try:
            await super().do_enable()
        # except envdsRunTransitionException:
        except (envdsRunTransitionException, envdsRunErrorException, envdsRunWaitException) as e:
            raise e
        print("_LabjackClient.enable")
        # simulate connect delay
        await asyncio.sleep(1)
        self.keep_connected = True


class LabjackClient(DAQClient):
    """docstring for Labjack_Client."""
    
    def __init__(self, config=None):
        super(LabjackClient, self).__init__(config=config)
        self.logger.debug("LabjackClient.init")
        self.client_class = "_LabjackClient"
        self.logger.debug("LabjackClient.init", extra={"config": config})
        self.config = config
    
    async def recv_from_client(self, message_info):
        if True:
            try:
                self.address_name = message_info["address_name"]
                self.num_bytes = message_info["num_bytes"]
                data = ljm.eReadNameByteArray(self.handle, self.address_name, self.num_bytes)
                return data
            except Exception as e:
                self.logger.error("recv_from_client", extra={"e": e})
        return None
    
    async def send_to_client(self, data):
        try:
            self.address_name = data["address_name"]
            self.message = data["data"]
            ljm.eWriteName(self.handle, self.address_name, self.message)
        except Exception as e:
            self.logger.error("send_to_client error", extra={"error": e})



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
            "port-1": {
                "attributes": {
                    # "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    # "client_class": {"type": "string", "data": "TCPClient"},
                    "client_module": {"type": "string", "data": "Labjack"},
                    "client_class": {"type": "string", "data": "LabjackClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "port": {"type": "int", "data": 4001},
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

        self.default_client_module = "Labjack"
        self.default_client_class = "LabjackClient"

        self.data_loop_task = None

    def configure(self):

        # print("configure:1")
        super(Labjack, self).configure()

        try:
            # get config from file
            # print("configure:2")
            try:
                # print("configure:3")
                with open("/app/config/interface.conf", "r") as f:
                    conf = yaml.safe_load(f)
                # print("configure:4")
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

            # print("configure:5")
            self.logger.debug("conf", extra={"data": conf})

            atts = Labjack.metadata["attributes"]

            # print("configure:7")
            path_map = dict()
            for name, val in Labjack.metadata["paths"].items():
                # path_map[name] = InterfacePath(name=name, path=val["data"])
                # print("configure:8")

                if "client_module" not in val["attributes"]:
                    val["attributes"]["client_module"]["data"] = self.default_client_module
                if "client_class" not in val["attributes"]:
                    val["attributes"]["client_class"]["data"] = self.default_client_class
                # print("configure:9")

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
                # print("configure:10")
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
            # print("configure:11")

            self.config = InterfaceConfig(
                type=atts["type"]["data"],
                name=atts["name"]["data"],
                uid=conf["uid"],
                paths=path_map
            )
            # print(f"self.config: {self.config}")

            self.logger.debug(
                "configure",
                extra={"conf": conf, "self.config": self.config},
            )
        except Exception as e:
            self.logger.debug("USCDR301:configure", extra={"error": e})
 
        
    async def recv_data_loop(self, client_id: str):
        
        # self.logger.debug("recv_data_loop", extra={"client_id": client_id})
        while True:
            try:
                # client = self.config.paths[client_id]["client"]
                client = self.client_map[client_id]["client"]
                # while client is not None:
                if client:
                    self.logger.debug("recv_data_loop", extra={"client": client})
                    data = await client.recv_from_client()
                    # data = await client.recv()
                    self.logger.debug("recv_data", extra={"client_id": client_id, "data": data})

                    await self.update_recv_data(client_id=client_id, data=data)
                    # await asyncio.sleep(self.min_recv_delay)
                else:
                    await asyncio.sleep(1)
            except (KeyError, Exception) as e:
                self.logger.error("recv_data_loop", extra={"error": e})
                await asyncio.sleep(1)

            # await asyncio.sleep(self.min_recv_delay)
            await asyncio.sleep(0.1)

    async def wait_for_ok(self, timeout=0):
        pass

    async def send_data(self, event: DAQEvent):
            print(f"here:1 {event}")
            try:
                print(f"send_data:1 - {event}")
                client_id = event["path_id"]
                client = self.client_map[client_id]["client"]
                data = event.data["data"]

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

    iface = USCDR301()
    iface.run()
    # task_list.append(asyncio.create_task(iface.run()))
    # await asyncio.sleep(2)
    iface.enable()
    logger.debug("Starting US Converters Interface")

    # remove fastapi ----
    # # get config from file
    # uid = "9999"
    # try:
    #     with open("/app/config/interface.conf", "r") as f:
    #         conf = yaml.safe_load(f)
    #         try:
    #             uid = conf["uid"]
    #         except KeyError:
    #             pass
    # except FileNotFoundError:
    #     pass

    # root_path=f"/envds/interface/system/Mock/{uid}"
    # # print(f"root_path: {root_path}")

    # config = uvicorn.Config(
    #     "main:app",
    #     host=server_config.host,
    #     port=server_config.port,
    #     log_level=server_config.log_level,
    #     root_path=root_path,
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
        logger.debug("USCDR301.run", extra={"do_run": do_run})
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