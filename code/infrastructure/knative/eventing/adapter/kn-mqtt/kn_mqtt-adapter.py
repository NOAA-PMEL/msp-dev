import uuid
from ulid import ULID
import logging
import asyncio
import os

import httpx
from logfmter import Logfmter
from pydantic import BaseSettings, Field
# from asyncio_mqtt import Client, MqttError
from aiomqtt import Client, MqttError
from cloudevents.http import to_structured, from_json
from cloudevents.exceptions import InvalidStructuredJSON

import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    # mqtt_broker: str = 'localhost'
    # mqtt_broker: str = 'mosquitto.default'
    # # mqtt_port: int = 1883
    # mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    # mqtt_topic_subscription: str = 'aws-id/#'
    # # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    # mqtt_client_id: str = Field(str(ULID()))
    # knative_broker: str = 'http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default'
    # dry_run: bool = False

    class Config:
        env_prefix = 'KN_MQTT_'
        case_sensitive = False


# class KNMQTTClient():
#     def __init__(self):

#         self.run = False

#     async def listen(self):
#         async with Client(config.mqtt_broker, port=config.mqtt_port) as client:
#             await client.subscribe(config.mqtt_topic_subscription, qos=2)
#             async with client.messages() as messages:

#                 L.info(
#                     "Subscribed",
#                     extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
#                 )

#                 template = {
#                     'topic_filter': config.mqtt_topic_filter,
#                     'subscription': config.mqtt_topic_subscription
#                 }
#                 async for message in messages:
#                     print(f"publish message: {message}")
#                 await asyncio.sleep(1)
#                 # await publish_messages(messages, template)



#     async def publish_messages(self, messages, template):
#         async for message in messages:
#             try:
#                 payload = message.payload.decode()
#                 data = from_json(payload)
#             except InvalidStructuredJSON:
#                 L.error(f"INVALID MSG: {payload}", extra=template)

#             # Always log the message
#             L.info(data, extra=template)
#             # Send the messages on to Kafka if we aren't in a dry_run
#             if config.dry_run is False:
#                 try:
#                     headers, body = to_structured(data)
#                     # send to knative kafkabroker
#                     async with httpx.AsyncClient() as client:
#                         r = await client.post(
#                             config.knative_broker,
#                             headers=headers,
#                             data=body.decode()
#                         )
#                         r.raise_for_status()
#                 except InvalidStructuredJSON:
#                     L.error(f"INVALID MSG: {payload}")
#                 except httpx.HTTPError as e:
#                     L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")

#     def run(self):
#         self.run = True
    
#     def shutdown(self):
#         self.run = False

#     async def run_loop(self):
#         L.info("Starting", extra=config.dict())
#         reconnect = 3
#         while self.run:
#             try:
#                 await self.listen()
#             except MqttError as error:
#                 L.error(
#                     f'{error}. Trying again in {reconnect} seconds',
#                     extra={ k: v for k, v in config.dict().items() if k.lower().startswith('mqtt_') }
#                 )
#             finally:
#                 await asyncio.sleep(reconnect)

async def shutdown():
    print("shutting down")
    # for task in task_list:
    #     print(f"cancel: {task}")
    #     task.cancel()

async def main(config):
    uvconfig = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        # log_level=server_config.log_level,
        root_path="/msp/datastore",
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
    config = Settings()
    print(config)
    asyncio.run(main(config))
