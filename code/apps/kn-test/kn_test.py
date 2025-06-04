import asyncio
from datetime import datetime
import json
import logging
import math
import os
from time import sleep
import time
from ulid import ULID
from pathlib import Path

# import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
# from typing import Dict
from fastapi import FastAPI, Request
from pydantic import BaseSettings, Field
from cloudevents.http import CloudEvent, from_http
from cloudevents.conversion import to_structured
from cloudevents.exceptions import InvalidStructuredJSON

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


# class Settings(BaseSettings):
#     host: str = '0.0.0.0'
#     port: int = 8787
#     debug: bool = False
#     url: str = 'https://localhost:8444/erddap/tabledap'
#     author: str = 'super_secret_author'
#     dry_run: bool = False

#     class Config:
#         env_prefix = 'IOT_ERDDAP_INSERT_'
#         case_sensitive = False


# class Settings(BaseSettings):
#     host: str = "0.0.0.0"
#     port: int = 8080
#     debug: bool = False
#     knative_broker: str = (
#         "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/msp-system/default"
#     )
#     dry_run: bool = False

#     class Config:
#         env_prefix = "KN_TEST_"
#         case_sensitive = False

def time_to_next(sec: float) -> float:
    now = time.time()
    delta = sec - (math.fmod(now, sec))
    return delta

class Tester():
    def __init__(self):
        asyncio.create_task(self.send_message_loop())


    async def send_message_loop(self):
        while True:
            
            data = {"T": 23.5, "RH": 44.3}
            L.info("send_message_loop")# extra={"payload": payload})
            attributes = {
                "type": "envds.controller.control.request",
                "source": "test.sender",
                "id": str(ULID()),
                "datacontenttype": "application/json; charset=utf-8",
            }
            
            ce = CloudEvent(attributes=attributes, data=data)
            ce["destpath"] = "/to/the/moon"
            try:
                headers, body = to_structured(ce)
                # send to knative kafkabroker
                L.debug("here:1")
                timeout = httpx.Timeout(5.0, read=0.5)
                with httpx.Client() as client:
                    r = client.post(
                        "http://broker-ingress.knative-eventing.svc.cluster.local/msp/default", headers=headers, data=body, timeout=timeout
                        # "http://kn-mqtt-adapter.msp-system/mqtt/send", headers=headers, data=body, timeout=timeout
                        # "http://localhost:8080", headers=headers, data=body
                        # config.knative_broker, headers=headers, data=body.decode()
                    )
                    L.info("verifier send", extra={"verifier-request": r.request.content})#, "status-code": r.status_code})
                    r.raise_for_status()
            except InvalidStructuredJSON:
                L.error(f"INVALID MSG: {ce}")
            except httpx.HTTPError as e:
                L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")

            await asyncio.sleep(1)

# class Archiver():
#     def __init__(self):
#         self.base_path = "/data"
#         self.data_buffer = asyncio.Queue()
#         self.file = None
#         self.save_interval = 60
#         self.save_now = True

#         self.task_list = []
#         self.task_list.append(asyncio.create_task(self.save_file_loop()))
#         self.task_list.append(asyncio.create_task(self.write_data_loop()))

#     def archive_data(self, data: dict):
#         self.data_buffer.put_nowait(data)

#     async def write_data_loop(self):
#         while True:
#             data = await self.data_buffer.get()

#             if data:
#                 self.open()
#                 if self.file:
#                     json.dump(data, self.file)           

#                     if self.save_now:
#                         self.file.flush()
#                         if self.save_interval > 0:
#                             self.save_now = False

#     async def save_file_loop(self):

#         while True:
#             if self.save_interval > 0:
#                 await asyncio.sleep(time_to_next(self.save_interval))
#                 self.save_now = True
#             else:
#                 self.save_now = True
#                 await asyncio.sleep(1)

#     def open(self):

#         fname = datetime.utcnow().strftime("%Y-%m-%d")
#         fname += ".jsonl"

#         # check if current file is already open
#         if (
#             self.file is not None
#             and not self.file.closed
#             and os.path.basename(self.file.name) == fname
#         ):
#             return
        
#         try:
#             # print(f"base_path: {self.base_path}")
#             if not os.path.exists(self.base_path):
#                 os.makedirs(self.base_path, exist_ok=True)
#         except OSError as e:
#             self.logger.error("OSError", extra={"error": e})
#             # print(f'OSError: {e}')
#             self.file = None
#             return
#         # print(f"self.file: before")
#         self.file = open(
#             # self.base_path+fname,
#             os.path.join(self.base_path, fname),
#             mode="a",
#         )

#     def close(self):

#         for t in self.task_list:
#             t.cancel()

#         if self.file:
#             try:
#                 self.file.flush()
#                 self.file.close()
#                 self.file = None
#             except ValueError:
#                 pass


# app = Flask(__name__)
app = FastAPI()
test = Tester()
# config = Settings()
# archiver = Archiver()


@app.get("/")
async def test_message():
    L.debug("here:get")
    return "ok", 200
# @app.route("/", methods=["POST"])
# def archive_message():

@app.post("/test")#, status_code=204)
async def test_message(request: Request):

    try:
        L.debug("here")
        # print(f"Request: {request.body()}")
        data = await request.json()
        # data = from_http(request.headers(), request.body())
        print(f"data: {data}")
    except Exception as e:
        L.error("test", extra={"reason": e})
    return "", 204
    # return "OK"
    # try:
    #     ce = from_http(headers=request.headers, data=request.get_data())
    #     # to support local testing...
    #     if isinstance(ce.data, str):
    #         ce.data = json.loads(ce.data)
    # except InvalidStructuredJSON:
    #     L.error("not a valid cloudevent")
    #     return "not a valid cloudevent", 400

    # # message = ce.data
    # archiver.archive_data(data)




    # parts = Path(ce["source"]).parts
    # L.info(
    #     "handle sensor",
    #     extra={"ce-source": ce["source"], "ce-type": ce["type"], "ce-data": ce.data},
    # )

    # parts = ce.data["source-path"].split("/")
    # acct = parts[0]
    # sampling_system_id = parts[1]
    # data_format = parts[2]

    # payload = ce.data["payload"]

    # # deal with each source as needed. Archive after decrypting if 
    # #   necessary?
    # if data_format in standard_data_formats:
    #     # if sensor is registered, send sensor data
    #     sensor = from_dict(payload)
    #     L.info("standard data update", extra={"sensor": sensor})
    #     # else regsister sensor

    #     # also, archive original message
    #     archive_message(ce.data)

    # else:

    #     custom1_sensor_data = "sensor.custom1-data.update"
    #     custom2_sensor_data = "sensor.custom2-data.update"
    #     msg_source = "uasdaq.custom.source"

    #     return "not implemented"

    # try:
    #     headers, body = to_structured(sensor)
    #     # send to knative kafkabroker
    #     with httpx.Client() as client:
    #         r = client.post(
    #             config.knative_broker,
    #             headers=headers,
    #             data=body,
    #             # config.knative_broker, headers=headers, data=body.decode()
    #         )
    #         L.info("sensor handler sent", extra={"sensor-request": r.request.content})
    #         r.raise_for_status()
    # except InvalidStructuredJSON:
    #     L.error(f"INVALID MSG: {ce}")
    # except httpx.HTTPError as e:
    #     L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")

    # return "ok", 200


if __name__ == "__main__":
    asyncio.run(app.run(debug=True, host="0.0.0.0", port=8080))
    # app.run()
