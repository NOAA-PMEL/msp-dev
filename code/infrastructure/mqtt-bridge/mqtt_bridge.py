# import uuid
from ulid import ULID
import logging
import asyncio
# import os

import httpx
from logfmter import Logfmter
from pydantic import BaseSettings, Field
# from asyncio_mqtt import Client, MqttError
# from aiomqtt import Client, MqttError
from aiomqtt import Client, MqttError, TLSParameters
import ssl

from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

class MQTTBridgeSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080

    local_mqtt_broker: str = 'mosquitto.default'
    local_mqtt_port: int = 1883
    local_broker_type: str = 'mosquitto'
    local_mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' #['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    local_mqtt_client_id: str = Field(str(ULID()))
    local_validation_required: bool = False

    remote_mqtt_broker: str = 'mosquitto.default'
    remote_mqtt_port: int = 1883
    remote_broker_type: str = 'mosquitto' # aws
    remote_mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' #['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    remote_mqtt_client_id: str = Field(str(ULID()))
    remote_validation_required: bool = False

    class Config:
        env_prefix = 'MQTT_BRIDGE_'
        case_sensitive = False

    # class Config:
    #     env_prefix = 'KN_MQTT_'
    #     case_sensitive = False


class MQTTBridge():
    def __init__(self, config=None):
        
        # self.logger = logging.getLogger(self.__class__.__name__)
        # self.logger.debug("TestClass instantiated")
        L.debug("init MQTTBridge")
        
        if config is None:
            config = MQTTBridgeSettings()
        self.config = config
        self.to_local_mqtt_buffer = asyncio.Queue()
        self.to_remote_mqtt_buffer = asyncio.Queue()

        asyncio.create_task(self.send_to_remote_mqtt_loop())
        asyncio.create_task(self.send_to_local_mqtt_loop())
        asyncio.create_task(self.get_from_remote_mqtt_loop())
        asyncio.create_task(self.get_from_local_mqtt_loop())

        self.local_client = None
        self.remote_client = None

    async def send_to_local_mqtt(self, ce: CloudEvent):
        await self.to_local_mqtt_buffer.put(ce)

    async def send_to_remote_mqtt(self, ce: CloudEvent):
        await self.to_remote_mqtt_buffer.put(ce)

    def ssl_alpn(self, client_type:str="local"):
        IoT_protocol_name = "x-amzn-mqtt-ca"
        # aws_iot_endpoint = "AWS_IoT_ENDPOINT_HERE" # <random>.iot.<region>.amazonaws.com
        # aws_iot_endpoint = config.mqtt_broker # <random>.iot.<region>.amazonaws.com
        # url = "https://{}".format(aws_iot_endpoint)

        if client_type == "local": 
            ca = "/certs/local/aws/rootCA.crt" 
            cert = "/certs/local/aws/cert.crt"
            private = "/certs/local/aws/private.key"
        elif client_type == "remote": 
            ca = "/certs/remote/aws/rootCA.crt" 
            cert = "/certs/remote/aws/cert.crt"
            private = "/certs/remote/aws/private.key"
        else:
            return None
        
        try:
            #debug print opnessl version
            L.info("open ssl version:{}".format(ssl.OPENSSL_VERSION))
            ssl_context = ssl.create_default_context()
            ssl_context.set_alpn_protocols([IoT_protocol_name])
            ssl_context.load_verify_locations(cafile=ca)
            ssl_context.load_cert_chain(certfile=cert, keyfile=private)
            L.info(f"ssl_context: {ssl_context}")
            return  ssl_context
        except Exception as e:
            print("exception ssl_alpn()")
            raise e
        
    def get_mqtt_client(self, client_type:str="local"):

        try:
            tls_context = None
            L.info("get_mqtt_client", extra={"client_type": client_type})
            if client_type == "local": 
                broker = self.config.local_mqtt_broker
                port = self.config.local_mqtt_port
                client_id = self.config.local_mqtt_client_id
                if self.config.local_validation_required:
                    if self.config.local_broker_type == "aws":
                        tls_context = self.ssl_alpn(client_type=client_type)
            elif client_type == "remote": 
                broker = self.config.remote_mqtt_broker
                port = self.config.remote_mqtt_port
                client_id = self.config.remote_mqtt_client_id
                L.info("get_mqtt_client", extra={"broker": broker, "port": port, "identifier": client_id, "tls_context": tls_context})
                if self.config.remote_validation_required:
                    if self.config.remote_broker_type == "aws":
                        tls_context = self.ssl_alpn(client_type=client_type)
                        L.info("get_mqtt_client", extra={"broker": broker, "port": port, "identifier": client_id, "tls_context": tls_context})


            L.info("get_mqtt_client", extra={"broker": broker, "port": port, "identifier": client_id, "tls_context": tls_context})
            client = Client(broker, port=port, tls_context=tls_context, identifier=client_id)
        except Exception as e:
            L.error("get_mqtt_client", extra={"reason": e})
            client = None

        return client

    # try:
    #     # client = Client(config.mqtt_broker, port=config.mqtt_port, tls_context=ssl_alpn())
    #     client = Client(config.mqtt_broker, port=config.mqtt_port, tls_context=ssl_alpn(), identifier=config.mqtt_client_id)#, timeout=10)
    #     # client = Client(config.mqtt_broker, port=443, tls_context=ssl_alpn(), identifier='mqtt-bridge')#, timeout=10)
    #     # client = Client(config.mqtt_broker, port=443, tls_params=tls_params)
    #     L.info(f"client: {client}, {config.mqtt_broker}")
    # except Exception as e:
    #     L.info(f"client: {e}, {config.mqtt_broker}")
    
    # interval = 5
    # while True:
    #     L.info("here:1")
    #     try:
    #         L.info("here:2")
    #         # await client.publish("sdk/test/python", "test-message")
    #         L.info("here:2.1")
    #         async with client:

    async def send_to_remote_mqtt_loop(self):

        reconnect = 5
        while True:
            try:
                # L.debug("listen", extra={"config": self.config})
                # async with Client(self.conwfig.mqtt_broker, port=self.config.mqtt_port) as self.client:
                while True:
                    ce = await self.to_remote_mqtt_buffer.get()
                    L.debug("ce", extra={"ce": ce})
                    while not self.remote_client:
                        L.info("waiting for mqtt client")
                        await asyncio.sleep(reconnect)
                    try:
                        destpath = ce["destpath"]
                        L.debug(destpath)
                        await self.remote_client.publish(destpath, payload=to_json(ce))
                    except Exception as e:
                        L.error("send_to_mqtt", extra={"reason": e})    
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('remote_mqtt_') }
                )
                await asyncio.sleep(reconnect)
            # finally:
            #     await asyncio.sleep(reconnect)

    async def get_from_local_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                L.debug("listen local", extra={"config": self.config})
                # client_id=str(ULID())
                # async with Client(self.config.local_mqtt_broker, port=self.config.local_mqtt_port,identifier=client_id) as self.local_client:
                async with self.get_mqtt_client(client_type="local") as self.local_client:
                    # for topic in self.config.mqtt_topic_subscriptions.split("\n"):
                    for topic in self.config.local_mqtt_topic_subscriptions.split(","):
                        # print(f"run - topic: {topic.strip()}")
                        # L.debug("run", extra={"topic": topic})
                        if topic.strip():
                            L.debug("subscribe", extra={"topic": topic.strip()})
                            await self.local_client.subscribe(f"$share/mqtt_bridge/{topic.strip()}")

                        # await client.subscribe(config.mqtt_topic_subscription, qos=2)
                    # async with client.messages() as messages:
                    async for message in self.local_client.messages: #() as messages:

                        ce = from_json(message.payload)
                        topic = message.topic.value
                        ce["sourcepath"] = topic
                        
                        try:
                            L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
                            await self.send_to_remote_mqtt(ce)
                        except Exception as e:
                            L.error("Error sending to remote broker", extra={"reason": e})
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('local_mqtt_') }
                )
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    async def send_to_local_mqtt_loop(self):

        reconnect = 5
        while True:
            try:
                # L.debug("listen", extra={"config": self.config})
                # async with Client(self.conwfig.mqtt_broker, port=self.config.mqtt_port) as self.client:
                while True:
                    ce = await self.to_local_mqtt_buffer.get()
                    L.debug("ce", extra={"ce": ce})
                    while not self.local_client:
                        L.info("waiting for mqtt client")
                        await asyncio.sleep(reconnect)
                    try:
                        destpath = ce["destpath"]
                        L.debug(destpath)
                        await self.local_client.publish(destpath, payload=to_json(ce))
                    except Exception as e:
                        L.error("send_to_mqtt", extra={"reason": e})    
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('local_mqtt_') }
                )
                await asyncio.sleep(reconnect)
            # finally:
            #     await asyncio.sleep(reconnect)

    async def get_from_remote_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                L.debug("listen remote", extra={"config": self.config})
                # client_id=str(ULID())
                # async with Client(self.config.local_mqtt_broker, port=self.config.local_mqtt_port,identifier=client_id) as self.local_client:
                async with self.get_mqtt_client(client_type="remote") as self.remote_client:
                    # for topic in self.config.mqtt_topic_subscriptions.split("\n"):
                    for topic in self.config.remote_mqtt_topic_subscriptions.split(","):
                        # print(f"run - topic: {topic.strip()}")
                        # L.debug("run", extra={"topic": topic})
                        if topic.strip():
                            L.debug("subscribe", extra={"topic": topic.strip()})
                            await self.remote_client.subscribe(f"$share/mqtt_bridge/{topic.strip()}")

                        # await client.subscribe(config.mqtt_topic_subscription, qos=2)
                    # async with client.messages() as messages:
                    async for message in self.remote_client.messages: #() as messages:

                        ce = from_json(message.payload)
                        topic = message.topic.value
                        ce["sourcepath"] = topic
                        
                        try:
                            L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
                            await self.send_to_local_mqtt(ce)
                        except Exception as e:
                            L.error("Error sending to local broker", extra={"reason": e})
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('remote_mqtt_') }
                )
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    # async def send_to_knbroker(self, ce: CloudEvent):
    #     print("send ", ce)
    #     await self.to_knbroker_buffer.put(ce)

    # async def get_client(self):
    #     # create a new client for each request
    #     async with httpx.AsyncClient() as client:
    #         # yield the client to the endpoint function
    #         L.debug("get_client")
    #         yield client
    #         # close the client when the request is done

    # def open_http_client(self):
    #     # create a new client for each request
    #     L.debug("open_http_client")
    #     self.http_client = httpx.AsyncClient()
    #         # # yield the client to the endpoint function
    #         # yield client
    #         # # close the client when the request is done

    # async def close_http_client(self):
    #     await self.http_client.aclose()
    #     self.http_client = None

    # async def send_to_knbroker_loop(self): #, template):
    #     # client = None
    #     while True:
    #         try:
    #             if not self.http_client:
    #                 self.open_http_client()

    #             ce = await self.to_knbroker_buffer.get()
    #             print(f"to_broker Qsize {self.to_knbroker_buffer.qsize()}")
    #             # print(ce)
    #             # continue
    #             # TODO discuss validation requirements
    #             if self.config.validation_required:
    #                 # wrap in verification cloud event
    #                 pass

    #             L.debug(ce)#, extra=template)
    #             try:
    #                 timeout = httpx.Timeout(5.0, read=0.5)
    #                 ce["datacontenttype"] = "application/json"
    #                 attrs = {
    #                     # "type": "envds.controller.control.request",
    #                     "type": ce["type"],
    #                     "source": ce["source"],
    #                     "id": ce["id"],
    #                     "datacontenttype": "application/json",
    #                 }

    #                 if "destpath" in ce:
    #                     attrs["destpath"] = ce["destpath"]
    #                 if "sourcepath" in ce:
    #                     attrs["sourcepath"] = ce["sourcepath"]
    #                 ce = CloudEvent(attributes=attrs, data=ce.data)
    #                 # ce["destpath"] = "test/path"
    #                 headers, body = to_structured(ce)
    #                 L.debug("send_to_knbroker", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
    #                 # send to knative kafkabroker
    #                 # async with httpx.AsyncClient() as client:
    #                 #     r = await client.post(
    #                 #         self.config.knative_broker,
    #                 #         # "http://broker-ingress.knative-eventing.svc.cluster.local/mspbase02-system/default",
    #                 #         headers=headers,
    #                 #         data=body,
    #                 #         timeout=timeout
    #                 #     )

    #                 #     L.info("adapter send", extra={"verifier-request": r.request.content})#, "status-code": r.status_code})
    #                 #     r.raise_for_status()
    #                 # async with self.get_client() as client:
    #                 r = await self.http_client.post(
    #                     self.config.knative_broker,
    #                     # "http://broker-ingress.knative-eventing.svc.cluster.local/mspbase02-system/default",
    #                     headers=headers,
    #                     data=body,
    #                     timeout=timeout
    #                 )
                    
    #                 L.info("adapter send", extra={"verifier-request": r.request.content})#, "status-code": r.status_code})
    #                 r.raise_for_status()


    #             except InvalidStructuredJSON:
    #                 L.error(f"INVALID MSG: {ce}")
    #             except httpx.TimeoutException:
    #                 pass
    #             except httpx.HTTPError as e:
    #                 L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
    #         except Exception as e:
    #             print("error", e)
    #         await asyncio.sleep(0.0001)


async def shutdown():
    print("shutting down")
    # for task in task_list:
    #     print(f"cancel: {task}")
    #     task.cancel()

async def main(config):
    # asyncio.create_task(send_to_knbroker_loop())

    uvconfig = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        # log_level=server_config.log_level,
        # root_path="/msp/datastore",
        # log_config=dict_config,
    )
    print
    server = uvicorn.Server(uvconfig)
    # test = logging.getLogger()
    # test.info("test")
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")


if __name__ == "__main__":
    # app.run(debug=config.debug, host=config.host, port=config.port)
    # app.run()
    config = MQTTBridgeSettings()
    print(config)
    asyncio.run(main(config))
