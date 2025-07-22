from aiomqtt import Client
import asyncio
from envds.daq.client import DAQClient, DAQClientConfig, _BaseClient
from envds.exceptions import envdsRunTransitionException, envdsRunWaitException, envdsRunErrorException
import traceback
import json


class _MQTT_Client(_BaseClient):
    """docstring for _MQTT_Client."""
    def __init__(self, config=None):
        super().__init__(config)

        self.logger.debug("_MQTT_Client.init")
        self.connected = False
        # self.keep_connected = False
        self.keep_connected = True
        self.host = ""
        self.port = 0
        self.reconnect_delay = 1
        self.recv_buffer_local = asyncio.Queue()
        # self.topics = []
        if "host" in self.config.properties:
            self.host = self.config.properties["host"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]
        if "subscriptions" in self.config.properties:
            self.topics = self.config.properties["subscriptions"]
    
    def configure(self):
        super().configure()
        self.logger.debug("_MQTT_Client: configure", extra={"config": self.config.properties})
        if "host" in self.config.properties:
            self.host = self.config.properties["host"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]
        if "subscriptions" in self.config.properties:
            self.subscriptions = self.config.properties["subscriptions"]
        asyncio.create_task(self.connection_monitor())

    async def connection_monitor(self):
        while True:
            try:
                while self.keep_connected:

                    # if self.connection_state == self.DISCONNECTED:
                    # if self.connected == False:
                        # connect
                        # self.connection_state = self.CONNECTING
                    try:
                        self.logger.debug("open_connection", extra={"host": self.host, "port": self.port})
                        async with Client(self.host, self.port) as self.mqttclient:
                            for topic in self.subscriptions:
                                await self.mqttclient.subscribe(topic)
                            async for message in self.mqttclient.messages:
                                payload = message.payload.decode("utf-8")
                                self.logger.debug("payload receieved:", extra={"payload": payload})
                                await self.recv_buffer_local.put(payload)
                        self.logger.debug("_TCPClient: connect", extra={"host": self.host, "port": self.port} )
                        # self.connection_state = self.CONNECTED
                    except (asyncio.TimeoutError, ConnectionRefusedError, Exception) as e:
                        self.logger.error("_TCPClient connection error", extra={"error": e})
                        self.mqttclient = None
                        # self.connection_state = self.DISCONNECTED

                await asyncio.sleep(self.reconnect_delay)            
            except Exception as e:
                self.logger.error("connection_monitor error", extra={"error": e})
    
    async def read(self):
        return await self.recv_buffer_local.get()
    
    async def publish(self, data):
        try:
            topic = data['path']
            if topic[0] == "/":
                topic = topic[1:]

            message = data['message']
            self.logger.debug("MQTTClient publish", extra={"topic": topic, "payload": message})
            await self.mqttclient.publish(topic, payload=message)
        except Exception as e:
            self.logger.error("mqtt client publish error", extra={"error": e})


    async def do_enable(self):
        try:
            await super().do_enable()
        # except envdsRunTransitionException:
        except (envdsRunTransitionException, envdsRunErrorException, envdsRunWaitException) as e:
            raise e
        print("_MQTT_Client.enable")
        # simulate connect delay
        await asyncio.sleep(1)
        self.keep_connected = True


class MQTT_Client(DAQClient):
    """docstring for MQTT_Client."""

    def __init__(self, config: DAQClientConfig=None, **kwargs):
        super(MQTT_Client, self).__init__(config=config)
        self.logger.debug("MQTT_Client.init")
        self.client_class = "_MQTT_Client"
        self.logger.debug("MQTT_Client.init", extra={"config": config})
        self.config = config
        self.logger.debug("MQTT_Client.init", extra={'config': self.config})
        if "host" in self.config.properties:
            self.host = self.config.properties["host"]
        if "port" in self.config.properties:
            self.port = self.config.properties["port"]
        if "subscriptions" in self.config.properties:
            self.subscriptions = self.config.properties["subscriptions"]
    
    async def recv_from_client(self):
        if True:
            try:
                print("host:", self.host, "port", self.port)
                data = await self.client.read()
                data = json.loads(data)
                return data
            except Exception as e:
                self.logger.error("recv_from_client error", extra={"e": e})
        return None

    async def send_to_client(self, data):
        try:
            self.logger.debug("MQTT_Client:send_to_client", extra={"payload": data})
            await self.client.publish(data)

        except Exception as e:
            self.logger.error("send_to_client error", extra={"error": e})




# class _MQTT_Client(_BaseClient):
#     """docstring for _MQTT_Client."""
#     def __init__(self, config=None):
#         super().__init__(config)

#         self.logger.debug("_MQTT_Client.init")
#         self.connected = False
#         # self.keep_connected = False
#         self.keep_connected = True
#         self.host = ""
#         self.port = 0
#         self.reconnect_delay = 1
#         self.recv_buffer_local = asyncio.Queue()
#         # self.topics = []
#         if "host" in self.config.properties:
#             self.host = self.config.properties["host"]["data"]
#         if "port" in self.config.properties:
#             self.port = self.config.properties["port"]["data"]
#         if "subscriptions" in self.config.properties:
#             self.topics = self.config.properties["subscriptions"]["data"]
    
#     def configure(self):
#         super().configure()
#         self.logger.debug("_MQTT_Client: configure", extra={"config": self.config.properties})
#         if "host" in self.config.properties:
#             self.host = self.config.properties["host"]["data"]
#         if "port" in self.config.properties:
#             self.port = self.config.properties["port"]["data"]
#         if "subscriptions" in self.config.properties:
#             self.subscriptions = self.config.properties["subscriptions"]["data"]
#         asyncio.create_task(self.connection_monitor())

#     async def connection_monitor(self):
#         while True:
#             try:
#                 while self.keep_connected:

#                     # if self.connection_state == self.DISCONNECTED:
#                     # if self.connected == False:
#                         # connect
#                         # self.connection_state = self.CONNECTING
#                     try:
#                         self.logger.debug("open_connection", extra={"host": self.host, "port": self.port})
#                         async with Client(self.host, self.port) as self.mqttclient:
#                             for topic in self.subscriptions:
#                                 print('TOPIC', topic)
#                                 await self.mqttclient.subscribe(topic)
#                             print(f"here:2 {self.mqttclient}")
#                             async for message in self.mqttclient.messages:
#                                 payload = message.payload
#                                 print('payload received:', payload)
#                                 await self.recv_buffer_local.put(payload)
#                         self.logger.debug("_TCPClient: connect", extra={"host": self.host, "port": self.port} )
#                         # self.connection_state = self.CONNECTED
#                     except (asyncio.TimeoutError, ConnectionRefusedError, Exception) as e:
#                         self.logger.error("_TCPClient connection error", extra={"error": e})
#                         self.mqttclient = None
#                         # self.connection_state = self.DISCONNECTED
#                 await asyncio.sleep(self.reconnect_delay)

#                 await asyncio.sleep(self.reconnect_delay)            
#             except Exception as e:
#                 self.logger.error("connection_monitor error", extra={"error": e})
    
#     async def read(self):
#         return await self.recv_buffer_local.get()
    
#     # async def recv_data_loop(self):
#     #     while True:
#     #         try:
#     #             print(f"here:1", {self.host}, {self.port})
#     #             async with Client(self.host, self.port) as self.mqttclient:
#     #                 print(f"here:2 {self.mqttclient}")
#     #                 self.connected = True
#     #                 await self.mqttclient.subscribe("websocket_topic")
#     #                 async for message in self.mqttclient.messages:
#     #                     payload = message.payload
#     #                     print('payload received:', payload)
#     #                     await self.recv_buffer.put(payload)
#     #                     await asyncio.sleep(.01)
#     #             print("connection closed")
#     #             await asyncio.sleep(1)
#     #         except Exception as e:
#     #             print("error: {e}")

#     #         await asyncio.sleep(1)

#     # async def send_data_loop(self):
#     #     while True:
#     #         try:
#     #             if self.connected and self.host and self.port > 0:
#     #                 payload = await self.send_buffer.get()
#     #                 await self.mqttclient.publish("test_topic", payload=payload)
#     #                 print("send message to mqtt")
#     #             await asyncio.sleep(.1)
#     #         except Exception as e:
#     #             print("error: {e}")
#     #         await asyncio.sleep(.1)

#     # async def read(self):
#     #     return await self.recv_buffer.get()
    
#     # async def write(self, data: str):
#     #     await self.send_buffer.put(data)

#     async def do_enable(self):
#         try:
#             await super().do_enable()
#         # except envdsRunTransitionException:
#         except (envdsRunTransitionException, envdsRunErrorException, envdsRunWaitException) as e:
#             raise e
#         print("_MQTT_Client.enable")
#         # simulate connect delay
#         await asyncio.sleep(1)
#         self.keep_connected = True


# class MQTT_Client(DAQClient):
#     """docstring for MQTT_Client."""

#     def __init__(self, config: DAQClientConfig=None, **kwargs):
#         # print("mock_client: 1")
#         super(MQTT_Client, self).__init__(config=config)
#         # print("mock_client: 2")
#         self.logger.debug("MQTT_Client.init")
#         self.client_class = "_MQTT_Client"
#         self.logger.debug("MQTT_Client.init", extra={"config": config})
#         self.config = config
#         self.logger.debug("MQTT_Client.init", extra={'config': self.config})
#         if "host" in self.config.properties:
#             self.host = self.config.properties["host"]["data"]
#         if "port" in self.config.properties:
#             self.port = self.config.properties["port"]["data"]
    
#     async def recv_from_client(self):
#         if True:
#             try:
#                 print("host:", self.host, "port", self.port)
#                 data = await self.client.read()
#                 print('data in bottom loop', data)
#                 # async with Client(self.host, self.port) as self.mqttclient:
#                 #     await self.mqttclient.subscribe("websocket_topic")
#                 #     async for message in self.mqttclient.messages:
#                 #         payload = message.payload
#                 #         print('payload received:', payload)
#                 return data
#             except Exception as e:
#                 self.logger.error("recv_from_client error", extra={"e": e})
#         return None

#     async def send_to_client(self, data):
#         try:
#             payload = 'test message'
#             await self.mqttclient.publish("test_topic", payload=payload)

#         except Exception as e:
#             self.logger.error("send_to_client error", extra={"error": e})
