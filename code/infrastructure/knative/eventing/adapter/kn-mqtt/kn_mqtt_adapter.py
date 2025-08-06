# import uuid
from ulid import ULID
import logging
import asyncio
# import os

import httpx
from logfmter import Logfmter
from pydantic import BaseSettings, Field
# from asyncio_mqtt import Client, MqttError
from aiomqtt import Client, MqttError

from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


class KNMQTTAdapterSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' #['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mqtt_client_id: str = Field(str(ULID()))
    knative_broker: str = 'http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default'
    validation_required: bool = False

    class Config:
        env_prefix = 'KN_MQTT_'
        case_sensitive = False

    # class Config:
    #     env_prefix = 'KN_MQTT_'
    #     case_sensitive = False

# async def send_to_knbroker_loop():
#     reconnect = 3
#     while True:
#         try:
#             L.debug("listen", extra={"config": config})
#             async with Client(config.mqtt_broker, port=config.mqtt_port) as client:
#                 for topic in config.mqtt_topic_subscriptions.split("\n"):
#                     # print(f"run - topic: {topic.strip()}")
#                     # L.debug("run", extra={"topic": topic})
#                     if topic.strip():
#                         L.debug("subscribe", extra={"topic": topic.strip()})
#                         await client.subscribe(topic.strip())

#                     # await client.subscribe(config.mqtt_topic_subscription, qos=2)
#                 # async with client.messages() as messages:
#                 async for message in client.messages: #() as messages:


#                     # # async for message in messages:
#                     # ce = from_json(message.payload)

#                     # print(f"publish message: {message.payload}")

#                     ce = from_json(message.payload)
#                     topic = message.topic.value
#                     ce["sourcepath"] = topic
                    
#                     try:
#                         L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
#                         await send_to_knbroker_loop(ce)
#                     except Exception as e:
#                         L.error("Error sending to knbroker", extra={"reason": e})
#         except MqttError as error:
#             L.error(
#                 f'{error}. Trying again in {reconnect} seconds',
#                 extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
#             )
#         finally:
#             await asyncio.sleep(reconnect)

# async def send_to_knbroker_loop(ce: CloudEvent): #, template):
        
#         # TODO discuss validation requirements
#         if config.validation_required:
#             # wrap in verification cloud event
#             pass

#         # attributes = {
#         #         "type": msg_type,
#         #         "source": "uasdaq.dashboard",
#         #         "id": str(ULID()),
#         #         "datacontenttype": "application/json; charset=utf-8",
#         #     }

#         # Always log the message
#         L.debug(ce)#, extra=template)
#         try:
#             timeout = httpx.Timeout(5.0, read=0.1)
#             # ce["datacontenttype"] = "application/json"
#             ce["destpath"] = "test/path"
#             headers, body = to_structured(ce)
#             # L.debug("send_to_knbroker", extra={"broker": config.knative_broker, "h": headers, "b": body})
#             # send to knative kafkabroker
#             async with httpx.AsyncClient() as client:
#                 r = await client.post(
#                     config.knative_broker,
#                     headers=headers,
#                     data=body,
#                     timeout=timeout
#                 )
#                 # L.info("adapter send", extra={"verifier-request": r.request.content})#, "status-code": r.status_code})
#                 r.raise_for_status()
#         except InvalidStructuredJSON:
#             L.error(f"INVALID MSG: {ce}")
#         except httpx.TimeoutException:
#             pass
#         except httpx.HTTPError as e:
#             L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")


class KNMQTTClient():
    def __init__(self, config=None):
        
        # self.logger = logging.getLogger(self.__class__.__name__)
        # self.logger.debug("TestClass instantiated")
        L.debug("init KNMQTTClient")
        
        if config is None:
            config = KNMQTTAdapterSettings()
        self.config = config
        self.to_mqtt_buffer = asyncio.Queue()
        self.to_knbroker_buffer = asyncio.Queue()

        asyncio.create_task(self.send_to_mqtt_loop())
        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.send_to_knbroker_loop())

        self.client = None


    async def send_to_mqtt(self, ce: CloudEvent):
        await self.to_mqtt_buffer.put(ce)

    async def send_to_mqtt_loop(self):

        reconnect = 5
        while True:
            try:
                # L.debug("listen", extra={"config": self.config})
                # async with Client(self.conwfig.mqtt_broker, port=self.config.mqtt_port) as self.client:
                while True:
                    ce = await self.to_mqtt_buffer.get()
                    L.debug("ce", extra={"ce": ce})
                    while not self.client:
                        L.info("waiting for mqtt client")
                        await asyncio.sleep(reconnect)
                    try:
                        destpath = ce["destpath"]
                        L.debug(destpath)
                        await self.client.publish(destpath, payload=to_json(ce))
                    except Exception as e:
                        L.error("send_to_mqtt", extra={"reason": e})    
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('mqtt_') }
                )
                await asyncio.sleep(reconnect)
            # finally:
            #     await asyncio.sleep(reconnect)

    async def get_from_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                L.debug("listen", extra={"config": self.config})
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as self.client:
                    for topic in self.config.mqtt_topic_subscriptions.split("\n"):
                        # print(f"run - topic: {topic.strip()}")
                        # L.debug("run", extra={"topic": topic})
                        if topic.strip():
                            L.debug("subscribe", extra={"topic": topic.strip()})
                            await self.client.subscribe(topic.strip())

                        # await client.subscribe(config.mqtt_topic_subscription, qos=2)
                    # async with client.messages() as messages:
                    async for message in self.client.messages: #() as messages:

                        ce = from_json(message.payload)
                        topic = message.topic.value
                        ce["sourcepath"] = topic
                        
                        try:
                            L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
                            await self.send_to_knbroker(ce)
                        except Exception as e:
                            L.error("Error sending to knbroker", extra={"reason": e})
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('mqtt_') }
                )
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    async def send_to_knbroker(self, ce: CloudEvent):
        print("send ", ce)
        await self.to_knbroker_buffer.put(ce)

    async def send_to_knbroker_loop(self): #, template):
        while True:
            ce = await self.to_knbroker_buffer.get()
            print(type(ce))
            # print(ce)
            # continue
            try:
                # TODO discuss validation requirements
                if self.config.validation_required:
                    # wrap in verification cloud event
                    pass

                L.debug(ce)#, extra=template)
                try:
                    timeout = httpx.Timeout(5.0, read=0.1)
                    ce["datacontenttype"] = "application/json"
                    attrs = {
                        # "type": "envds.controller.control.request",
                        "type": ce["type"],
                        "source": ce["source"],
                        "id": ce["id"],
                        "datacontenttype": "application/json",
                    }

                    if "destpath" in ce:
                        attrs["destpath"] = ce["destpath"]
                    if "sourcepath" in ce:
                        attrs["sourcepath"] = ce["sourcepath"]
                    ce = CloudEvent(attributes=attrs, data=ce.data)
                    # ce["destpath"] = "test/path"
                    headers, body = to_structured(ce)
                    L.debug("send_to_knbroker", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
                    # send to knative kafkabroker
                    async with httpx.AsyncClient() as client:
                        r = await client.post(
                            self.config.knative_broker,
                            # "http://broker-ingress.knative-eventing.svc.cluster.local/mspbase02-system/default",
                            headers=headers,
                            data=body,
                            timeout=timeout
                        )
                        L.info("adapter send", extra={"verifier-request": r.request.content})#, "status-code": r.status_code})
                        r.raise_for_status()
                except InvalidStructuredJSON:
                    L.error(f"INVALID MSG: {ce}")
                except httpx.TimeoutException:
                    pass
                except httpx.HTTPError as e:
                    L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
            except Exception as e:
                print("error", e)
            await asyncio.sleep(0.0001)


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
    config = KNMQTTAdapterSettings()
    print(config)
    asyncio.run(main(config))
