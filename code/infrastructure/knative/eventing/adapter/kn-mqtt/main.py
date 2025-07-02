# import asyncio
from datetime import datetime, timezone
# import json
import logging
# import socket

from fastapi import FastAPI, Request, status  # , APIRouter
# from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
# from cloudevents.http import CloudEvent, from_http, from_json, to_json
# from cloudevents.conversion import to_structured # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

# from cloudevents.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http

# from cloudevents.pydantic import CloudEvent

# from typing import Union
# import httpx
from logfmter import Logfmter

# from typing import Any, Dict, List
# from pydantic import BaseModel, BaseSettings, Field

# from ulid import ULID
# from aiomqtt import Client, MqttError

from kn_mqtt_adapter import KNMQTTClient, KNMQTTAdapterSettings

# import envds.message.client as emc
# from envds.message.client import MessageClientManager, MessageClientConfig

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)



# class Settings(BaseSettings):
#     mqtt_broker: str = 'mosquitto.default'
#     mqtt_port: int = 1883
#     # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
#     mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' #['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
#     # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
#     mqtt_client_id: str = Field(str(ULID()))
#     knative_broker: str = 'http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default'
#     validation_required: bool = False
#     dry_run: bool = False

#     class Config:
#         env_prefix = 'KN_MQTT_'
#         case_sensitive = False


config = KNMQTTAdapterSettings()
print(f"main: {config}")

app = FastAPI()

# origins = ["*"]  # dev
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# class KNMQTTClient():
#     def __init__(self, config=None):

#         L.debug("init KNMQTTClient")
#         if config is None:
#             config = Settings()
#         self.config = config
#         self.to_mqtt_buffer = asyncio.Queue()

#         asyncio.create_task(self.send_to_mqtt_loop())

#     async def send_to_mqtt(self, ce: CloudEvent):
#         await self.to_mqtt_buffer.put(ce)

#     async def send_to_mqtt_loop(self):

#         reconnect = 3
#         while True:
#             try:
#                 L.debug("listen", extra={"config": config})
#                 async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
#                     while True:
#                         ce = await self.to_mqtt_buffer.get()
#                         L.debug("ce", extra={"ce": ce})
#                         try:
#                             destpath = ce["destpath"]
#                             L.debug(destpath)
#                             await client.publish(destpath, payload=to_json(ce))
#                         except Exception as e:
#                             L.error("send_to_mqtt", extra={"reason": e})    
#             except MqttError as error:
#                 L.error(
#                     f'{error}. Trying again in {reconnect} seconds',
#                     extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
#                 )
#             finally:
#                 await asyncio.sleep(reconnect)


adapter = KNMQTTClient(config)

@app.get("/")
async def root():
    L.debug("message: Hello World from kn-mqtt-adapter")
    return "ok", 200



#Accept data from Knative system and publish to MQTT broker
@app.post("/mqtt/send", status_code=status.HTTP_202_ACCEPTED)
async def mqtt_send(request: Request):
    try:
        ce = await request.json()
        print(ce)
        # ce = from_http(request.headers, await request.body())
        # L.debug(request.headers)
        # L.debug("mqtt_send", extra={"ce": ce, "destpath": ce["destpath"]})
        await adapter.send_to_mqtt(ce)
    except Exception as e:
        # L.error("send", extra={"reason": e})
        pass

    return {"message": "OK"}
    # return "",204
