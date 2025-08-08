import asyncio
import binascii
from envds.daq.client import DAQClient, DAQClientConfig, _StreamClient
from envds.core import envdsStatus
from envds.exceptions import envdsRunTransitionException, envdsRunWaitException, envdsRunErrorException
import random

from envds.util.util import time_to_next

from labjack import ljm

class _TCPClient(_StreamClient):
    """docstring for _TCPClient."""

    def __init__(self, config=None):
        # self.logger.debug("_TCPClient.init before super")
        super().__init__(config)

        self.logger.debug("_TCPClient.init")
        # self.mock_data_type = "1D"
        # print("_MockClient.init")
        self.connected = False
        self.keep_connected = False
        # self.keep_connected = True
        # self.run_task_list.append(self.connection_monitor())
        # self.enable_task_list.append(asyncio.create_task(asyncio.sleep(1)))
        asyncio.create_task(self.connection_monitor())
        # self.host = "192.168.86.38"
        # self.port = 24
        self.host = ""
        self.port = 0
        self.reconnect_delay = 1
        self.logger.debug("_TCPClient.init done")
        if "host" in self.config.properties:
            self.host = self.config.properties["host"]["data"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]["data"]

        # self.send_method = send_method
        # self.read_method = read_method
        # self.read_terminator = read_terminator
        # self.read_num_bytes = read_num_bytes
        # self.decode_errors = decode_errors

        # self.return_packet_bytes = deque(maxlen=25000)

    def configure(self):
        super().configure()
        # parse self.config
        self.logger.debug("_TCPClient: configure", extra={"config": self.config.properties})
        # if "attributes" in self.config.properties:
        if "host" in self.config.properties:
            self.host = self.config.properties["host"]["data"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]["data"]
        self.logger.debug("_TCPClient: configure", extra={"host": self.host, "port": self.port})


    async def connection_monitor(self):

        while True:
            print("SELF.CONNECTION STATE", self.connection_state)
            print("SELF.KEEP CONNECTED", self.keep_connected)
            try:
                while self.keep_connected:

                    if self.connection_state == self.DISCONNECTED:
                        # connect
                        self.connection_state = self.CONNECTING
                        print("SELF.CONNECTION STATE", self.connection_state)
                        try:
                            self.logger.debug("open_connection", extra={"host": self.host, "port": self.port})
                            self.reader, self.writer = await asyncio.open_connection(
                                host=self.host,
                                port=self.port
                            )
                            self.logger.debug("_TCPClient: connect", extra={"host": self.host, "port": self.port} )
                            self.connection_state = self.CONNECTED
                        except (asyncio.TimeoutError, ConnectionRefusedError, Exception) as e:
                            self.logger.error("_TCPClient connection error", extra={"error": e})
                            self.reader = None
                            self.writer = None
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
        print("_TCPlient.enable")
        # simulate connect delay
        await asyncio.sleep(1)
        self.keep_connected = True
        
        # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.TRUE)


# async def do_run(self):
#     try:
#         self.client = getattr(self, "_MockClient")()
#         # self.client = self._MockClient
#         self.logger.debug("do_run", extra={"client": self.client})
#     except Exception as e:
#         self.logger.error("do_run", extra={"error": e})
#     await super().do_run()

# class TCPClient(DAQClient):
#     """docstring for TCPClient."""

#     def __init__(self, config: DAQClientConfig=None, **kwargs):
#         # print("mock_client: 1")
#         super(TCPClient, self).__init__(config=config)
#         # print("mock_client: 2")
#         self.logger.debug("TCPClient.init")
#         self.client_class = "_TCPClient"
#         self.logger.debug("TCPClient.init", extra={"config": config})

#         # TODO: set uid here? or let caller do it?
#         self.config = config
#         # print("mock_client: 3")

#         # self.mock_type = "1D"  # 2D, i2c
#         self.read_method = "readline"
#         self.send_method = "ascii"
#         self.decode_errors = "strict"
#         self.read_terminator = "\r"
#         # print("mock_client: 4")
#         self.logger.debug("TCPClient.init", extra={'config': self.config})

#         # self.enable_task_list.append(self.recv_loop())

#         # self.logger.debug("init", extra={})
#         # try:
#         #     self.client = _MockClient()
#         # except Exception as e:
#         #     self.logger.error("init client error", extra={"error": e})
#         #     self.client = None
#         # self.logger.debug("init", extra={"client": self.client})


#     async def recv_from_client(self):
#         # print("recv_from_client:1")
#         # if self.enabled():
#         if True:
#             try:
#                 print(f"recv_from_client:1 props {self.config.properties}")
#                 props = self.config.properties["device-interface-properties"]["read-properties"]
                        
#                 print(f"recv_from_client:1.1 - sip {self.config.properties['device-interface-properties']}")
#                 print(f"recv_from_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
#                 print(f"recv_from_client:1.3 - rm {self.config.properties['device-interface-properties']['read-properties']['read-method']}")
#                 # read_method = props.get("read-method", self.read_method)
#                 # decode_errors = props.get("decode-errors", self.decode_errors)

#                 read_method = self.read_method
#                 if "read-method" in self.config.properties['device-interface-properties']['read-properties']:
#                     read_method = self.config.properties['device-interface-properties']['read-properties']['read-method']
#                 decode_errors = self.decode_errors
#                 if "decode-errors" in self.config.properties['device-interface-properties']['read-properties']:
#                     decode_errors = self.config.properties['device-interface-properties']['read-properties']['decode-errors']

#                 print(f"recv_from_client:2 -- readmethod={read_method}")
#                 if read_method == "readline":
#                     # print("recv_from_client:3")
#                     data = await self.client.readline()
#                     print(f"recv_from_client:4 {data}")

#                 elif read_method == "readuntil":
#                     print(f"recv_from_client:3 - read until {props.get('read-terminator',self.read_terminator)}")
#                     data = await self.client.readuntil(
#                         terminator=props.get("read-terminator",self.read_terminator), 
#                         decode_errors=decode_errors) 
#                     print(f"recv_from_client: 3.1 {data}")

#                 elif read_method == "readbytes":
#                     data = await self.client.read(
#                         num_bytes=props.get("read-num-bytes",1),
#                         decode_errors=decode_errors
#                     )
#                 elif read_method == "readbinary":
#                     self.logger.debug("recv_from_client:5")
#                     ret_packet_size = await self.client.get_return_packet_size()
#                     self.logger.debug("recv_from_client:5.1", extra={"ret_size": ret_packet_size})
#                     data = await self.client.readbinary(
#                         num_bytes=ret_packet_size,
#                         decode_errors=decode_errors
#                     )
#                     self.logger.debug("recv_from_client:5.2", extra={"bin data": data})
#                 return data
#             except Exception as e:
#                 self.logger.error("recv_from_client", extra={"e": e})
#         # print("recv_from_client:5")
#         return None

#     async def send_to_client(self, data):
#         try:
            
#             # send_method = data.get("send-method", self.send_method)

#             print(f"send_to_client:1 props {self.config.properties}")
#             props = self.config.properties["device-interface-properties"]["read-properties"]

#             print(f"send_to_client:1.1 - sip {self.config.properties['device-interface-properties']}")
#             print(f"send_to_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
#             print(f"send_to_client:1.3 - sm {self.config.properties['device-interface-properties']['read-properties']['send-method']}")
#             # read_method = props.get("read-method", self.read_method)
#             # decode_errors = props.get("decode-errors", self.decode_errors)

#             send_method = self.send_method
#             if "send-method" in self.config.properties['device-interface-properties']['read-properties']:
#                 send_method = self.config.properties['device-interface-properties']['read-properties']['send-method']


#             self.logger.debug("send_to_client", extra={"send_method": send_method, "data": data})
#             if send_method == "binary":
#                 # if num of expected bytes not supplied, fail
#                 try:
#                     read_num_bytes = data["read-num-bytes"]
#                     self.client.return_packet_bytes.append(
#                         data["read-num-bytes"]
#                     )

#                     # convert back to bytes
#                     await self.client.writebinary(data["data"])
#                     # await self.client.writebinary(binary_data)
#                 except KeyError:
#                     self.logger.error("binary write failed - read-num-bytes not specified")
#                     return
#             else:
#                 try:
#                     await self.client.write(data["data"])
#                 except KeyError:
#                     self.logger.error("write failed - data not specified")
#                     return

#             self.logger.debug(
#                 "send_to_client",
#                 extra={
#                     "send_method": send_method,
#                     "data": data["data"]
#                 },
#             )
#         except Exception as e:
#             self.logger.error("send_to_client error", extra={"error": e})

class LabJackClient(DAQClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig=None, **kwargs):
        # print("mock_client: 1")
        super(LabJackClient, self).__init__(config=config)
        # print("mock_client: 2")
        self.logger.debug("self.LabJackClient.init")
        # self.client_class = "_self.LabJackClient"
        self.logger.debug("self.LabJackClient.init", extra={"config": config})

        # TODO: set uid here? or let caller do it?
        self.config = config
        # print("mock_client: 3")

        # # self.mock_type = "1D"  # 2D, i2c
        # self.read_method = "readline"
        # self.send_method = "ascii"
        # self.decode_errors = "strict"
        # self.read_terminator = "\r"
        # # print("mock_client: 4")
        self.logger.debug("self.LabJackClient.init", extra={'config': self.config})
        self.data_buffer = asyncio.Queue()
        # self.enable_task_list.append(self.recv_loop())

        # self.logger.debug("init", extra={})
        # try:
        #     self.client = _MockClient()
        # except Exception as e:
        #     self.logger.error("init client error", extra={"error": e})
        #     self.client = None
        # self.logger.debug("init", extra={"client": self.client})

    def hex_to_int(self, hex_val):
            if "Ox" not in hex_val:
                hex_val = f"0x{hex_val}"
            return int(hex_val, 16) 

    async def recv_from_client(self):
        pass

    async def send_to_client(self, data):
        pass

class I2CClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig=None, **kwargs):
        # print("mock_client: 1")
        super(I2CClient, self).__init__(config=config)



    async def recv_from_client(self):
        # print("recv_from_client:1")
        # if self.enabled():
        if True:
            try:

                data = await self.data_buffer.get()
                return data

                # print(f"recv_from_client:1 props {self.config.properties}")
                # props = self.config.properties["device-interface-properties"]["read-properties"]
                        
                # print(f"recv_from_client:1.1 - sip {self.config.properties['device-interface-properties']}")
                # print(f"recv_from_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
                # print(f"recv_from_client:1.3 - rm {self.config.properties['device-interface-properties']['read-properties']['read-method']}")
                # # read_method = props.get("read-method", self.read_method)
                # # decode_errors = props.get("decode-errors", self.decode_errors)

                # read_method = self.read_method
                # if "read-method" in self.config.properties['device-interface-properties']['read-properties']:
                #     read_method = self.config.properties['device-interface-properties']['read-properties']['read-method']
                # decode_errors = self.decode_errors
                # if "decode-errors" in self.config.properties['device-interface-properties']['read-properties']:
                #     decode_errors = self.config.properties['device-interface-properties']['read-properties']['decode-errors']

                # print(f"recv_from_client:2 -- readmethod={read_method}")
                # if read_method == "readline":
                #     # print("recv_from_client:3")
                #     data = await self.client.readline()
                #     print(f"recv_from_client:4 {data}")

                # elif read_method == "readuntil":
                #     print(f"recv_from_client:3 - read until {props.get('read-terminator',self.read_terminator)}")
                #     data = await self.client.readuntil(
                #         terminator=props.get("read-terminator",self.read_terminator), 
                #         decode_errors=decode_errors) 
                #     print(f"recv_from_client: 3.1 {data}")

                # elif read_method == "readbytes":
                #     data = await self.client.read(
                #         num_bytes=props.get("read-num-bytes",1),
                #         decode_errors=decode_errors
                #     )
                # elif read_method == "readbinary":
                #     self.logger.debug("recv_from_client:5")
                #     ret_packet_size = await self.client.get_return_packet_size()
                #     self.logger.debug("recv_from_client:5.1", extra={"ret_size": ret_packet_size})
                #     data = await self.client.readbinary(
                #         num_bytes=ret_packet_size,
                #         decode_errors=decode_errors
                #     )
                #     self.logger.debug("recv_from_client:5.2", extra={"bin data": data})
                # return data
            except Exception as e:
                self.logger.error("recv_from_client", extra={"e": e})
        # print("recv_from_client:5")
        return None

    async def send_to_client(self, data):
        try:
            client_config = self.client_map[client_id]["client_config"]
            data_buffer = self.client_map[client_id]["data_buffer"]
            # get i2c commands
            i2c_write = data["i2c-write"]
            # i2c_read = data["i2c-read"]
            
            address = self.hex_to_int(i2c_write["address"])
            write_data = [self.hex_to_int(hex_val) for hex_val in i2c_write["data"]]

            ljm.eWriteName(self.labjack, "I2C_SDA_DIONUM", client_config["attributes"]["sda_channel"]["data"])  # CS is FIO2
            ljm.eWriteName(self.labjack, "I2C_SCL_DIONUM", client_config["attributes"]["scl_channel"]["data"])  # CLK is FIO3
            ljm.eWriteName(self.labjack, "I2C_SPEED_THROTTLE", client_config["attributes"]["speed_throttle"]["data"]) # CLK frequency approx 100 kHz
            ljm.eWriteName(self.labjack, "I2C_OPTIONS", 0)
            ljm.eWriteName(self.labjack, "I2C_SLAVE_ADDRESS", address) # default address is 0x28 (40 decimal)

            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", len(write_data))
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", 0)

            ljm.eWriteNameByteArray(self.labjack, "I2C_DATA_TX", len(write_data), write_data)
            #ljm.eWriteNameArray(self.labjack, "I2C_DATA_TX", 1, [0x00])
            #ljm.eWriteName(self.labjack, "I2C_DATA_TX", 0x00)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            i2c_read = data["i2c-read"]
            await asyncio.sleep(i2c_read.get("delay-ms",500)/1000) # does this have to be 0.5?

            address = self.hex_to_int(i2c_read["address"])
            read_bytes = i2c_read["read-length"]

            #dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", 4)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", 0)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", read_bytes)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", read_bytes)
            if dataRead:
                output = {
                    "input-data": data,
                    "data": dataRead 
                }
                await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_i2c")

        # try:
            
        #     # send_method = data.get("send-method", self.send_method)

        #     print(f"send_to_client:1 props {self.config.properties}")
        #     props = self.config.properties["device-interface-properties"]["read-properties"]

        #     print(f"send_to_client:1.1 - sip {self.config.properties['device-interface-properties']}")
        #     print(f"send_to_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
        #     print(f"send_to_client:1.3 - sm {self.config.properties['device-interface-properties']['read-properties']['send-method']}")
        #     # read_method = props.get("read-method", self.read_method)
        #     # decode_errors = props.get("decode-errors", self.decode_errors)

        #     send_method = self.send_method
        #     if "send-method" in self.config.properties['device-interface-properties']['read-properties']:
        #         send_method = self.config.properties['device-interface-properties']['read-properties']['send-method']


        #     self.logger.debug("send_to_client", extra={"send_method": send_method, "data": data})
        #     if send_method == "binary":
        #         # if num of expected bytes not supplied, fail
        #         try:
        #             read_num_bytes = data["read-num-bytes"]
        #             self.client.return_packet_bytes.append(
        #                 data["read-num-bytes"]
        #             )

        #             # convert back to bytes
        #             await self.client.writebinary(data["data"])
        #             # await self.client.writebinary(binary_data)
        #         except KeyError:
        #             self.logger.error("binary write failed - read-num-bytes not specified")
        #             return
        #     else:
        #         try:
        #             await self.client.write(data["data"])
        #         except KeyError:
        #             self.logger.error("write failed - data not specified")
        #             return

        #     self.logger.debug(
        #         "send_to_client",
        #         extra={
        #             "send_method": send_method,
        #             "data": data["data"]
        #         },
        #     )
        # except Exception as e:
        #     self.logger.error("send_to_client error", extra={"error": e})
