import asyncio
import json
import signal
import sys
import os
import logging
import logging.config
import yaml
from envds.core import envdsLogger
from envds.daq.interface import Interface, InterfaceConfig #, InterfacePath
from envds.daq.event import DAQEvent

from pydantic import BaseModel

from labjack import ljm

task_list = []

class Tx(Interface):
    """docstring for Tx."""


    metadata = {
        "attributes": {
            "type": {"type": "char", "data": "LabJack"},
            "name": {"type": "char", "data": "Tx"}, # T4, T7
            "host": {"type": "char", "data": "localhost"},
            "description": {
                "type": "char",
                "data": "US converters serial to ethernet server",
            },
            "tags": {"type": "char", "data": "testing, LabJack, Tx, T4, T7, serial, tcp, ethernet, interface"},
        },
        "path_types": {
            "voltage_in": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "channel": {"type": "string", "data": "FIO0"}
                }
            },
            "voltage_out": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "channel": {"type": "string", "data": "FIO0"}
                }
            },
            "pwm": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "channel": {"type": "string", "data": "FIO0"},
                    "clock_roll_value": {"type": "int", "data": 3200}
                }
            },
            "counter": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "channel": {"type": "string", "data": "FIO0"}
                }
            },
            "i2c": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "sda_channel": {"type": "int", "data": 2},
                    "scl_channel": {"type": "int", "data": 3},
                    "speed_throttle": {"type": "int", "data": 65516}
                }
            },
            "spi": {
                "attributes": {
                    "client_module": {"type": "string", "data": "envds.daq.clients.tcp_client"},
                    "client_class": {"type": "string", "data": "TCPClient"},
                    "host": {"type": "string", "data": "localhost"},
                    "cs_channel": {"type": "int", "data": 2},
                    "clk_channel": {"type": "int", "data": 3},
                    "miso_channel": {"type": "int", "data": 2},
                    "mosi_channel": {"type": "int", "data": 3},
                    "mode": {"type": "int", "data": 1},
                    "options": {"type": "int", "data": 0},
                    "speed_throttle": {"type": "int", "data": 65516}
                }
            },
        }    
    }

    def __init__(self, config=None, **kwargs):
        super(Tx, self).__init__(config=config, **kwargs)
        self.data_task = None
        self.data_rate = 1

        self.default_client_module = "labjack_client"
        self.default_client_class = "LabJackClient"

        self.interface_definition_file = "LabJack_Tx_interface_definition.json"
        try:            
            with open(self.interface_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("interface_config not found. Exiting")            
            sys.exit(1)

        self.host = "localhost"
        # self.labjack = None

        # self.run_tasks.append(self.connection_monitor())
        self.data_loop_task = None
        self.recv_data_buffers = dict()

    def configure(self):

        # print("configure:1")
        super(Tx, self).configure()
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

            self.logger.debug("configure", extra={"conf": conf})

            # add hosts to each path if not present
            try:
                host = conf["attributes"]["host"]
            except KeyError as e:
                self.logger.debug("no host - default to localhost")
                host = "localhost"
            self.host = host
            for name, path in conf["paths"].items():
                if "host" not in path:
                    path["host"] = host

            # print("configure:5")
            self.logger.debug("conf", extra={"data": conf})

            # atts = Tx.metadata["attributes"]
            attrs = self.metadata["attributes"]
            path_types = self.metadata["path_types"]

            self.logger.debug("configure", extra={"attrs": attrs, "path_types": path_types})
            # print("configure:7")

            # override default metadata attributes with config values
            for name, val in conf["attributes"].items():
                if name in attrs:
                    attrs[name]["data"] = val
        
            print("here:1")
            path_map = dict()
            for name, val in conf["paths"].items():
                client_config = dict()
                # skip path if we don't know what type
                print("here:2")
                try:
                    path_defaults = path_types[val["path_type"]]
                    client_config["attributes"] = path_defaults["attributes"].copy()
                    print("here:3")

                    for path_att_name, path_att in val.items():
                        if path_att_name == "path_type":
                            continue
                        if path_att_name in client_config["attributes"]:
                            client_config["attributes"][path_att_name]["data"] = path_att
                    print("here:4")

                    if client_config["attributes"]["host"]["data"] == "":
                        client_config["attributes"]["host"]["data"] = attrs["host"]
                    
                    client_config["attributes"]["path_type"] = {"data": val["path_type"]}

                    print("here:5")
                except KeyError as e:
                    self.logger.error("configuration: unknown or missing path type", extra={"path": name})
                    continue



            # for name, val in Tx.metadata["paths"].items():
            #     # path_map[name] = InterfacePath(name=name, path=val["data"])
            #     # print("configure:8")

            #     if "client_module" not in val["attributes"]:
            #         val["attributes"]["client_module"]["data"] = self.default_client_module
            #     if "client_class" not in val["attributes"]:
            #         val["attributes"]["client_class"]["data"] = self.default_client_class
            #     # print("configure:9")

            #     # set path host from interface attributes
            #     if "host" in attrs:
            #         val["attributes"]["host"]["data"] = attrs["host"]

            #     client_config = val
            #     # override values from yaml config
            #     if "paths" in conf and name in conf["paths"]:
            #         self.logger.debug("yaml conf", extra={"id": name, "conf['paths']": conf['paths'], })
            #         for attname, attval in conf["paths"][name].items():
            #             self.logger.debug("config paths", extra={"id": name, "attname": attname, "attval": attval})
            #             client_config["attributes"][attname]["data"] = attval
            #     # print("configure:10")
                self.logger.debug("config paths", extra={"client_config": client_config})
                
                self.recv_data_buffers[name] = asyncio.Queue()
                path_map[name] = {
                    "client_id": name,
                    "client": None,
                    "client_config": client_config,
                    "client_module": client_config["attributes"]["client_module"]["data"],
                    "client_class": client_config["attributes"]["client_class"]["data"],
                    # "data_buffer": self.recv_data_buffers[name],
                    "recv_handler": self.recv_data_loop(name),
                    "recv_task": None,
                }
            # print("configure:11")

            self.config = InterfaceConfig(
                type=attrs["type"]["data"],
                name=attrs["name"]["data"],
                uid=conf["uid"],
                paths=path_map
            )
            # print(f"self.config: {self.config}")

            self.logger.debug(
                "configure",
                extra={"conf": conf, "self.config": self.config},
            )


            # TODO Loop through paths and check for "unpolled" ops and start a task to 
            # handle
            
        except Exception as e:
            self.logger.debug("Tx:configure", extra={"error": e})
 
    # async def connection_monitor(self):
    #     while True:
    #         try:
    #             if not self.labjack:
    #                 self.labjack = ljm.openS("ANY", "ANY", self.host)
    #         except Exception as e:
    #             self.logger.error("connection_monitor", extra={"reason": e})
    #             self.labjack = None
    #         await asyncio.sleep(5)

    async def recv_data_loop(self, client_id: str):
        
        # self.logger.debug("recv_data_loop", extra={"client_id": client_id})
        while True:
            try:
                # client = self.config.paths[client_id]["client"]
                client = self.client_map[client_id]["client"]
                # data_buffer = self.client_map[client_id]["data_buffer"]
                # while client is not None:
                # if data_buffer:
                if client:
                    self.logger.debug("recv_data_loop", extra={"client": client})
                    data = await client.recv()
                    # data = await data_buffer.get()
                    self.logger.debug("recv_data", extra={"client_id": client_id, "data": data})

                    await self.update_recv_data(client_id=client_id, data=data)
                    # await asyncio.sleep(self.min_recv_delay)
                else:
                    await asyncio.sleep(1)
            except (KeyError, Exception) as e:
                self.logger.error("recv_data_loop", extra={"error": e})
                await asyncio.sleep(1)

            # await asyncio.sleep(self.min_recv_delay)
            await asyncio.sleep(0.0001)

    async def wait_for_ok(self, timeout=0):
        pass

    async def send_data(self, event: DAQEvent):
            print(f"here:1 {event}")
            try:
                print(f"send_data:1 - {event}")
                client_id = event["path_id"]
                client = self.client_map[client_id]["client"]
                client_config = self.client_map[client_id]["client_config"]
                path_type = client_config["attributes"]["path_type"]["data"]

                data = event.data["data"]
                self.logger.debug("send_data", extra={"client.send.data": data})
                await client.send(data)
                # if path_type == "AtoD":
                #     pass
                # elif path_type == "DtoA":
                #     pass
                # elif path_type == "pwm":
                #     pass
                # elif path_type == "counter":
                #     pass
                # elif path_type == "i2c":
                #     await self.send_i2c(client_id, data)
                # elif path_type == "spi":
                #     pass


                # await client.send(data)
            except KeyError as e:
                self.logger.error("send_data", extra={"reason": e})
                pass

    # def hex_to_int(self, hex_val):
    #         if "Ox" not in hex_val:
    #             hex_val = f"0x{hex_val}"
    #         return int(hex_val, 16) 
        
    # async def send_i2c(self, client_id, data):

    #     try:
    #         client_config = self.client_map[client_id]["client_config"]
    #         data_buffer = self.client_map[client_id]["data_buffer"]
    #         # get i2c commands
    #         i2c_write = data["i2c-write"]
    #         # i2c_read = data["i2c-read"]
            
    #         address = self.hex_to_int(i2c_write["address"])
    #         write_data = [self.hex_to_int(hex_val) for hex_val in i2c_write["data"]]

    #         ljm.eWriteName(self.labjack, "I2C_SDA_DIONUM", client_config["attributes"]["sda_channel"]["data"])  # CS is FIO2
    #         ljm.eWriteName(self.labjack, "I2C_SCL_DIONUM", client_config["attributes"]["scl_channel"]["data"])  # CLK is FIO3
    #         ljm.eWriteName(self.labjack, "I2C_SPEED_THROTTLE", client_config["attributes"]["speed_throttle"]["data"]) # CLK frequency approx 100 kHz
    #         ljm.eWriteName(self.labjack, "I2C_OPTIONS", 0)
    #         ljm.eWriteName(self.labjack, "I2C_SLAVE_ADDRESS", address) # default address is 0x28 (40 decimal)

    #         ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", len(write_data))
    #         ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", 0)

    #         ljm.eWriteNameByteArray(self.labjack, "I2C_DATA_TX", len(write_data), write_data)
    #         #ljm.eWriteNameArray(self.labjack, "I2C_DATA_TX", 1, [0x00])
    #         #ljm.eWriteName(self.labjack, "I2C_DATA_TX", 0x00)
    #         ljm.eWriteName(self.labjack, "I2C_GO", 1)

    #         i2c_read = data["i2c-read"]
    #         await asyncio.sleep(i2c_read.get("delay-ms",500)/1000) # does this have to be 0.5?

    #         address = self.hex_to_int(i2c_read["address"])
    #         read_bytes = i2c_read["read-length"]

    #         #dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", 4)
    #         ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", 0)
    #         ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", read_bytes)
    #         ljm.eWriteName(self.labjack, "I2C_GO", 1)

    #         dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", read_bytes)
    #         if dataRead:
    #             output = {
    #                 "input-data": data,
    #                 "data": dataRead 
    #             }
    #             await data_buffer.put(output)

    #     except Exception as e:
    #         self.logger.error("send_i2c")

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"


async def test_task():
    while True:
        await asyncio.sleep(1)
        # print("daq test_task...")
        logger = logging.getLogger("envds.info")
        logger.info("Tx_test_task", extra={"test": "Tx task"})


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
    logger = logging.getLogger("interface::Tx")

    # test = envdsBase()
    # task_list.append(asyncio.create_task(test_task()))

    iface = Tx()
    iface.run()
    # task_list.append(asyncio.create_task(iface.run()))
    # await asyncio.sleep(2)
    iface.enable()
    logger.debug("Starting LabJack Tx Interface")

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
        logger.debug("Tx.run", extra={"do_run": do_run})
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