import asyncio
import json
import logging

# import math
import sys
from time import sleep

# from typing import List

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

# import httpx
from logfmter import Logfmter

from pydantic import BaseSettings, BaseModel, Field
from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

# from datetime import datetime, timedelta, timezone
from envds.util.util import (
    # get_datetime_string,
    # get_datetime,
    # datetime_to_string,
    # get_datetime_with_delta,
    # string_to_timestamp,
    # timestamp_to_string,
    time_to_next,
)

# from envds.daq.event import DAQEvent
from envds.daq.types import DAQEventType as det
from envds.sampling.types import SamplingEventType as sampet
from envds.core import envdsBase
import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class ERDDAPUtilConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True
    # knative_broker: str = (
    #     "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    # )
    # mongodb_user_name: str = ""
    # mongodb_user_password: str = ""
    # mongodb_connection: str = (
    #     "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-1.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-2.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/data?replicaSet=uasdaq-mongodb&ssl=false"
    # )
    # erddap_http_connection: str = (
    #     "http://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # erddap_https_connection: str = (
    #     "https://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # # erddap_author: str = "fake_author"

    daq_id: str | None = None
    # data_base_path: str | None = None
    # logs_base_path: str | None = None
    # save_interval: int = 60
    # file_interval: str = "day"

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: str = (
        "envds/+/+/+/data/#"  # ['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    )
    # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mqtt_client_id: str = Field(str(ULID()))

    knative_broker: str | None = None

    class Config:
        env_prefix = "ERDDAP_"
        case_sensitive = False



class ERDDAPUtil:
    """docstring for ERDDAPUtil."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("ERDDAPUtil instantiated")
        self.logger.setLevel(logging.DEBUG)

        # self.file_map = {"data": dict(), "logs": dict()}
        self.ID_DELIM = envdsBase.ID_DELIM
        self.config = ERDDAPUtilConfig()
        self.configure()

        self.save_buffer = asyncio.Queue()
        # asyncio.create_task(self.get_from_mqtt_loop())
        # asyncio.create_task(self.handle_save_buffer())

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})

    # async def get_from_mqtt_loop(self):
    #     reconnect = 10
    #     while True:
    #         try:
    #             L.debug("listen", extra={"config": self.config})
    #             client_id = str(ULID())
    #             async with Client(
    #                 self.config.mqtt_broker,
    #                 port=self.config.mqtt_port,
    #                 identifier=client_id,
    #             ) as self.client:
    #                 # for topic in self.config.mqtt_topic_subscriptions.split("\n"):
    #                 for topic in self.config.mqtt_topic_subscriptions.split(","):
    #                     # print(f"run - topic: {topic.strip()}")
    #                     # L.debug("run", extra={"topic": topic})
    #                     if topic.strip():
    #                         L.debug("subscribe", extra={"topic": topic.strip()})
    #                         await self.client.subscribe(
    #                             f"$share/datastore/{topic.strip()}"
    #                         )

    #                     # await client.subscribe(config.mqtt_topic_subscription, qos=2)
    #                 # async with client.messages() as messages:
    #                 async for message in self.client.messages:  # () as messages:

    #                     try:
    #                         ce = from_json(message.payload)
    #                         topic = message.topic.value
    #                         ce["sourcepath"] = topic
    #                         await self.save(ce)
    #                         L.debug(
    #                             "get_from_mqtt_loop",
    #                             extra={"cetype": ce["type"], "topic": topic},
    #                         )
    #                     except Exception as e:
    #                         L.error("get_from_mqtt_loop", extra={"reason": e})
    #                     # try:
    #                     #     L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
    #                     #     await self.send_to_knbroker(ce)
    #                     # except Exception as e:
    #                     #     L.error("Error sending to knbroker", extra={"reason": e})
    #         except MqttError as error:
    #             L.error(
    #                 f"{error}. Trying again in {reconnect} seconds",
    #                 extra={
    #                     k: v
    #                     for k, v in self.config.dict().items()
    #                     if k.lower().startswith("mqtt_")
    #                 },
    #             )
    #             await asyncio.sleep(reconnect)
    #         finally:
    #             await asyncio.sleep(0.0001)

    # async def save(self, ce):
    #     await self.save_buffer.put(ce)

    # async def handle_save_buffer(self):
    #     while True:
    #         try:
    #             ce = await self.save_buffer.get()

    #             if ce["type"] in [
    #                 # "envds.data.update",
    #                 det.data_update(),
    #                 det.controller_data_update(),
    #             ]:
    #                 # await self.device_data_update(ce)
    #                 await self.data_save(ce)
    #             # elif ce["type"] in [det.controller_data_update()]: #"envds.controller.data.update":
    #             #     await self.controller_data_update(ce)
    #             elif ce["type"] in [
    #                 sampet.sampling_condition_status_update(),
    #                 sampet.sampling_state_status_update(),
    #                 sampet.sampling_mode_status_update(),
    #             ]:
    #                 await self.logs_save(ce)
    #         except Exception as e:
    #             L.error("handle_save_buffer", extra={"reason": e})

    #         await asyncio.sleep(0.0001)
    #         self.save_buffer.task_done()

    # async def data_save(self, ce: CloudEvent):
    #     try:
    #         src = ce["source"]
    #         self.logger.debug("data_save", extra={"src": src, "ce": ce})
    #         if src not in self.file_map["data"]:
    #             parts = src.split(".")
    #             # device_name = parts[-1].split(self.ID_DELIM)
    #             if ce["type"] in [
    #                 # "envds.data.update",
    #                 det.data_update(),
    #                 # det.sensor_data_update(),
    #             ]:
    #                 device_name = parts[-1].split(self.ID_DELIM)
    #                 file_path = os.path.join("/data", "device", *device_name)
    #             elif ce["type"] in [det.controller_data_update()]:
    #                 controller_name = parts[-1].split(self.ID_DELIM)
    #                 file_path = os.path.join("/data", "controller", *controller_name)
    #             else:
    #                 return
                
    #             self.file_map["data"][src] = DataFile(base_path=file_path)

    #         self.logger.debug("data_save", extra={"src": src, "ce": ce})
    #         await self.file_map["data"][src].write(ce)
    #     except Exception as e:
    #         self.logger.error("data_save", extra={"reason": e})

    # async def logs_save(self, ce: CloudEvent):
    #     try:
    #         src = ce["source"]
    #         self.logger.debug("logs_save", extra={"src": src, "ce": ce})
    #         if src not in self.file_map["logs"]:
    #             parts = src.split(".")
    #             log_name = parts[1:]
    #             # # device_name = parts[-1].split(self.ID_DELIM)
    #             # if ce["type"] in [
    #             #     "envds.data.update",
    #             #     det.device_data_update(),
    #             #     det.sensor_data_update(),
    #             # ]:
    #             #     device_name = parts[-1].split(self.ID_DELIM)
    #             # if ce["type"] == sampet.sampling_condition_status_update():
    #             #     file_type = "conditions"
    #             # elif ce["type"] == sampet.sampling_state_status_update():
    #             #     file_type = "states"
    #             # elif ce["type"] == sampet.sampling_state_status_update():
    #             #     file_type = "modes"
    #             # else:
    #             #     return

    #             file_path = os.path.join("/logs", *log_name)
                
    #             self.file_map["logs"][src] = LogFile(base_path=file_path)
            
    #         self.logger.debug("logs_save", extra={"src": src, "ce": ce})
    #         await self.file_map["logs"][src].write(ce)
    #     except Exception as e:
    #         self.logger.error("log_update", extra={"reason": e})

async def shutdown():
    print("shutting down")
    # for task in task_list:
    #     print(f"cancel: {task}")
    #     task.cancel()


async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        # log_level=server_config.log_level,
        root_path="/msp/data",
        # log_config=dict_config,
    )

    server = uvicorn.Server(config)
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
    config = ERDDAPUtilConfig()
    # asyncio.run(main(config))

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
