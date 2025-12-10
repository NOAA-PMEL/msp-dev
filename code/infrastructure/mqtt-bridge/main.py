# import asyncio
from datetime import datetime, timezone
# import json
import logging
# import socket

from fastapi import FastAPI, Request, status, Response  # , APIRouter
# from fastapi.middleware.cors import CORSMiddleware

from cloudevents.http import from_http
# from cloudevents.http import CloudEvent, from_http, from_json
# from cloudevents.conversion import to_structured, to_json # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON
# from cloudevents.pydantic import CloudEvent

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

from mqtt_bridge import MQTTBridge, MQTTBridgeSettings

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


config = MQTTBridgeSettings()
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



bridge = MQTTBridge(config)

@app.get("/")
async def root():
    L.debug("message: Hello World from mqtt-bridge")
    return "ok", 200



#Accept data from Knative system and publish to MQTT broker
# @app.post("/mqtt/send/", status_code=status.HTTP_202_ACCEPTED)
@app.post("/mqtt/send/local")
async def mqtt_send_local(request: Request):
    try:
        # L.debug("mqtt/send")
        # return Response(status_code=status.HTTP_204_NO_CONTENT)
        ce = from_http(request.headers, await request.body())
        # L.debug(request.headers)
        L.debug("mqtt_send", extra={"ce": ce, "destpath": ce["destpath"]})
        # ce = from_http(request.headers, await request.body())
        # L.debug(request.headers)
        # L.debug("mqtt_send", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await bridge.send_to_local_mqtt(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        L.debug("mqtt sent")
    except Exception as e:
        L.error("mqtt_send", extra={"reason": e})
        # pass
        # # return {"message": "OK"}
        # return "",204
        return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/mqtt/send/remote")
async def mqtt_send_remote(request: Request):
    try:
        # L.debug("mqtt/send")
        # return Response(status_code=status.HTTP_204_NO_CONTENT)
        ce = from_http(request.headers, await request.body())
        # L.debug(request.headers)
        L.debug("mqtt_send", extra={"ce": ce, "destpath": ce["destpath"]})
        # ce = from_http(request.headers, await request.body())
        # L.debug(request.headers)
        # L.debug("mqtt_send", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await bridge.send_to_remote_mqtt(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        L.debug("mqtt sent")
    except Exception as e:
        L.error("mqtt_send", extra={"reason": e})
        # pass
        # # return {"message": "OK"}
        # return "",204
        return Response(status_code=status.HTTP_204_NO_CONTENT)
