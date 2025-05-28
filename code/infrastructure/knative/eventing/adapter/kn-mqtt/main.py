import asyncio
from datetime import datetime, timezone
import json
import logging
import socket

from fastapi import FastAPI  # , APIRouter
from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

# from cloudevents.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http

from cloudevents.pydantic import CloudEvent

# from typing import Union
import httpx
from logfmter import Logfmter
from pydantic import BaseModel, BaseSettings, Field, List

from ulid import ULID
from aiomqtt import Client, MqttError

import envds.message.client as emc

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class Settings(BaseSettings):
    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: List[str] = ['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mqtt_client_id: str = Field(str(ULID()))
    knative_broker: str = 'http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default'
    validation_required: bool = False
    dry_run: bool = False

    class Config:
        env_prefix = 'KN_MQTT_'
        case_sensitive = False


config = Settings()
print(f"main: {config}")
# from apis.router import api_router

app = FastAPI()

origins = ["*"]  # dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class KNMQTTClient():
    def __init__(self, config):

        if config is None:
            config = Settings()
        self.config = config

        self.run = False
        self.client = None
        self.connected = False
        self.to_kn_buffer = asyncio.Queue()
        self.to_mqtt_buffer = asyncio.Queue()

        asyncio.create_task(self.send_to_knbroker_loop())
        asyncio.create_task(self.send_to_mqtt_loop())


    def configure(self):
        pass

    def run(self):
        self.client = emc.MessageClientManager.create(
            config=emc.MQTTClientConfig(
                type="mqtt",
                config={"hostname": self.config.mqtt_broker, "port": self.config.mqtt_port}
            )
        )
        for topic in self.config.mqtt_topic_subscriptions:
            self.client.subscribe(topic)

        self.run = True

    # listen for mqtt messages
    async def send_to_knbroker_loop(self):

        while True:
            ce = await self.client.get()
            L.debug("listen", extra={"mqtt": ce})
            try:
                await self.send_to_knbroker(ce)
            except Exception as e:
                L.error("Error sending to knbroker", extra={"reason": e})

        # reconnect = 3
        # while True:
        #     try:
        #         L.debug("listen", extra={"config": self.config})
        #         async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as self.client:
        #             for topic in config.mqtt_topic_subscriptions:
        #                 await self.client.subscribe(topic, qos=2)
        #             self.connected = True
        #             async for message in self.client.messages:
        #                 ce = message.payload.decode()
        #                 topic = message.topic
        #                 ce["source_path"] = topic
        #                 try:
        #                     L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
        #                     await self.send_to_knbroker(ce)
        #                 except Exception as e:
        #                     L.error("Error sending to knbroker")
        #     except MqttError as error:
        #         self.connected = False
        #         L.error(
        #             f'{error}. Trying again in {reconnect} seconds',
        #             extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
        #         )
        #     finally:
        #         await asyncio.sleep(reconnect)

                
            # async with self.client.messages() as messages:

            #     L.info(
            #         "Subscribed",
            #         extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
            #     )

            #     # template = {
            #     #     'topic_filter': config.mqtt_topic_filter,
            #     #     'subscription': config.mqtt_topic_subscription
            #     # }
            #     async for message in messages:
            #         print(f"publish message: {message}")
            #     # await asyncio.sleep(1)
            #     await self.send_to_knbroker(message)#, template)

    # async def recv_loop(self):
    #     while True:
    #         message = await self.recv_buffer.get()
    #         await self.send_to_knbroker(message)

    async def send_to_knbroker(self, ce: CloudEvent): #, template):
            
            if self.config.validation_required:
                # wrap in verification cloud event
                pass

            # try:
            #     # payload = message.payload.decode()
            #     data = from_json(ce)
            # except InvalidStructuredJSON:
            #     L.error(f"INVALID MSG: {ce}")#, extra=template)

            # Always log the message
            L.info(ce)#, extra=template)
            # Send the messages on to Broker if we aren't in a dry_run
            if config.dry_run is False:
                try:
                    headers, body = to_structured(ce)
                    # send to knative kafkabroker
                    async with httpx.AsyncClient() as client:
                        r = await client.post(
                            config.knative_broker,
                            headers=headers,
                            data=body.decode()
                        )
                        r.raise_for_status()
                except InvalidStructuredJSON:
                    L.error(f"INVALID MSG: {ce}")
                except httpx.HTTPError as e:
                    L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")

    async def send_to_mqtt(self, ce: CloudEvent):
        await self.to_mqtt_buffer.put(ce)

    async def send_to_mqtt_loop(self):
        # put the data back on MQTT broker
        # TODO this function will have to infer? the topic
        while True:
            ce = await self.to_mqtt_buffer.get()
            try:
                dest_path = ce["dest_path"]
                await self.client.send(ce)
            except Exception as e:
                L.error("send_to_mqtt", extra={"reason": e})    


    # def run(self):
    #     self.run = True
    
    # def shutdown(self):
    #     self.run = False

    # async def run_loop(self):
    #     L.info("Starting", extra=config.dict())
    #     reconnect = 3
    #     while self.run:
    #         try:
    #             await self.listen()
    #         except MqttError as error:
    #             L.error(
    #                 f'{error}. Trying again in {reconnect} seconds',
    #                 extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
    #             )
    #         finally:
    #             await asyncio.sleep(reconnect)


# router = APIRouter()
# home_router = APIRouter()

# @home_router.get("/")
# async def home():
#     return {"message": "Hello World"}

# home_router.include_router(api_router)
# router.include_router(home_router)#, prefix="/envds/home")

# app.include_router(api_router)#, prefix="/envds/home")
# app.include_router(router)

# @app.on_event("startup")
# async def start_system():
#     print("starting system")


# @app.on_event("shutdown")
# async def start_system():
#     print("stopping system")

# class DeviceDataSearch(BaseModel):

#     start_time: str | None = None
#     end_time: str | None = None
#     custom: dict | None = None

adapter = KNMQTTClient(config)
adapter.run()

@app.get("/")
async def root():
    return {"message": "Hello World from kn-mqtt"}


# @app.post("/ce")
# async def handle_ce(ce: CloudEvent):
#     # print(ce.data)
#     # print(from_http(ce))
#     # header, data = from_http(ce)
#     print(f"type: {ce['type']}, source: {ce['source']}, data: {ce.data}, id: {ce['id']}")
#     print(f"attributes: {ce}")
#     # event = from_http(ce.headers, ce.get_data)
#     # print(event)


# @app.get("/device/data/request/{device_id}")
# async def get_device_data(device_id: str, search_opts: DeviceDataSearch):

#     return None


#Accept data from Knative system and publish to MQTT broker
@app.post("/mqtt/send")
async def mqtt_send(ce: CloudEvent):
    await adapter.send_to_mqtt(ce)


# @app.post("/data/update")
# async def data_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

# @app.post("/settings/update")
# async def settings_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

# @app.post("/status/update")
# async def status_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

# @app.post("/event/update")
# async def status_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200
