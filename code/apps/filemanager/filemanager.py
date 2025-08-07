import asyncio
import json
import logging
# import math
import sys
from time import sleep
# from typing import List

# import numpy as np
# from ulid import ULID
from pathlib import Path
import os

# import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings
from cloudevents.http import CloudEvent

# from cloudevents.http.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

# from datetime import datetime, timedelta, timezone
from envds.util.util import (
    # get_datetime_string,
    # get_datetime,
    # datetime_to_string,
    # get_datetime_with_delta,
    # string_to_timestamp,
    # timestamp_to_string,
    time_to_next
)

# from envds.daq.event import DAQEvent
from envds.daq.types import DAQEventType as det

import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

class FilemanagerConfig(BaseSettings):
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
    base_path: str | None = None
    save_interval: int = 60
    file_interval: str = "day"

    knative_broker: str | None = None
    class Config:
        env_prefix = "FILEMANAGER_"
        case_sensitive = False

class DataFile:
    def __init__(
        self,
        base_path="/data",
        save_interval=60,
        file_interval="day",
        # config=None,
    ):

        self.logger = logging.getLogger(self.__class__.__name__)

        self.base_path = base_path

        # unless specified, flush file every 60 sec
        self.save_interval = save_interval

        # allow for cases where we want hour files
        #   options: 'day', 'hour'
        self.file_interval = file_interval

        # if config:
        #     self.setup(config)

        # if self.base_path[-1] != '/':
        #     self.base_path += '/'

        self.save_now = True
        # if save_interval == 0:
        #     self.save_now = True

        self.current_file_name = ""

        self.data_buffer = asyncio.Queue()

        self.task_list = []
        # self.loop = asyncio.get_event_loop()

        self.file = None

        self.open()

    async def write(self, data_event: CloudEvent):
        # add message to queue and return
        # print(f'write: {data}')
        # print(f"write: {data_event}")
        await self.data_buffer.put(data_event.data)
        qsize = self.data_buffer.qsize()
        if qsize > 5:
            self.logger.warning("write buffer filling up", extra={"qsize": qsize})

    async def __write(self):

        while True:

            data = await self.data_buffer.get()
            # print(f'datafile.__write: {data}')

            try:
                dts = data["variables"]["time"]["data"]
                d_and_t = dts.split("T")
                ymd = d_and_t[0]
                hour = d_and_t[1].split(":")[0]
                # print(f"__write: {dts}, {ymd}, {hour}")
                self.__open(ymd, hour=hour)
                if not self.file:
                    return

                json.dump(data, self.file)
                self.file.write("\n")

                if self.save_now:
                    self.file.flush()
                    if self.save_interval > 0:
                        self.save_now = False

            except KeyError:
                pass

            # if data and ('DATA' in data):
            #     d_and_t = data['DATA']['DATETIME'].split('T')
            #     ymd = d_and_t[0]
            #     hour = d_and_t[1].split(':')[0]

            #     self.__open(ymd, hour=hour)

            #     if not self.file:
            #         return

            #     json.dump(data, self.file)
            #     self.file.write('\n')

            #     if self.save_now:
            #         self.file.flush()
            #         if self.save_interval > 0:
            #             self.save_now = False

    def __open(self, ymd, hour=None):

        fname = ymd
        if self.file_interval == "hour":
            fname += "_" + hour
        fname += ".jsonl"

        # print(f"__open: {self.file}")
        if (
            self.file is not None
            and not self.file.closed
            and os.path.basename(self.file.name) == fname
        ):
            return

        # TODO: change to raise error so __write can catch it
        try:
            # print(f"base_path: {self.base_path}")
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path, exist_ok=True)
        except OSError as e:
            self.logger.error("OSError", extra={"error": e})
            # print(f'OSError: {e}')
            self.file = None
            return
        # print(f"self.file: before")
        self.file = open(
            # self.base_path+fname,
            os.path.join(self.base_path, fname),
            mode="a",
        )
        self.logger.debug(
            "_open",
            extra={"file": self.file, "base_path": self.base_path, "fname": fname},
        )
        # print(f"open: {self.file}, {self.base_path}, {fname}")

    def open(self):
        self.logger.debug("DataFile.open")
        self.task_list.append(asyncio.create_task(self.save_file_loop()))
        self.task_list.append(asyncio.create_task(self.__write()))

    def close(self):

        for t in self.task_list:
            t.cancel()

        if self.file:
            try:
                self.file.flush()
                self.file.close()
                self.file = None
            except ValueError:
                self.logger.info("file already closed")
                # print("file already closed")

    async def save_file_loop(self):

        while True:
            if self.save_interval > 0:
                await asyncio.sleep(time_to_next(self.save_interval))
                self.save_now = True
            else:
                self.save_now = True
                await asyncio.sleep(1)


class Filemanager:
    """docstring for Filemanager."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Filemanager instantiated")
        self.logger.setLevel(logging.DEBUG)

        self.file_map = dict()

        self.config = FilemanagerConfig()
        self.configure()

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})

    async def data_save(self, ce, data_type="device"):
        try:
            src = ce["source"]
            if src not in self.file_map:
                parts = src.split(".")
                device_name = parts[-1].split(self.ID_DELIM)
                file_path = os.path.join("/data", data_type, *device_name)

                self.file_map[src] = DataFile(base_path=file_path)
            await self.file_map[src].write(ce)
        except Exception as e:
            self.logger.error("data_save", extra={"reason": e})
               

    # async def send_event(self, ce):
    #     try:
    #         self.logger.debug(ce)#, extra=template)
    #         try:
    #             timeout = httpx.Timeout(5.0, read=0.1)
    #             headers, body = to_structured(ce)
    #             self.logger.debug("send_event", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
    #             # send to knative broker
    #             async with httpx.AsyncClient() as client:
    #                 r = await client.post(
    #                     self.config.knative_broker,
    #                     headers=headers,
    #                     data=body,
    #                     timeout=timeout
    #                 )
    #                 r.raise_for_status()
    #         except InvalidStructuredJSON:
    #             self.logger.error(f"INVALID MSG: {ce}")
    #         except httpx.TimeoutException:
    #             pass
    #         except httpx.HTTPError as e:
    #             self.logger.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
    #     except Exception as e:
    #         print("error", e)
    #     await asyncio.sleep(0.01)


    # def find_one(self):  # , database: str, collection: str, query: dict):
    #     # self.connect()
    #     # if self.client:
    #     #     db = self.client[database]
    #     #     db_collection = db[collection]
    #     #     result = db_collection.find_one(query)
    #     #     if result:
    #     #         update = {"last_update": datetime.now(tz=timezone.utc)}
    #     #         db_client.update_one(database, collection, result, update)
    #     #     return result
    #     return None

    # def insert_one(self):  # , database: str, collection: str, document: dict):
    #     # self.connect()
    #     # if self.client:
    #     #     db = self.client[database]
    #     #     device_defs = db[collection]
    #     #     result = device_defs.insert_one(document)
    #     #     return result
    #     return None

    # def update_one(self):
    #     #     self,
    #     #     database: str,
    #     #     collection: str,
    #     #     document: dict,
    #     #     update: dict,
    #     #     filter: dict = None,
    #     #     upsert=False,
    #     # ):
    #     # self.connect()
    #     # if self.client:
    #     #     db = self.client[database]
    #     #     device = db[collection]
    #     #     if filter is None:
    #     #         filter = document
    #     #     set_update = {"$set": update}
    #     #     if upsert:
    #     #         set_update["$setOnInsert"] = document
    #     #     result = device.update_one(filter=filter, update=set_update, upsert=upsert)
    #     #     return result
    #     return None

    # async def device_data_update(self, ce: CloudEvent):

    #     try:
    #         # database = "data"
    #         # collection = "device"
    #         # self.logger.debug("data_device_update", extra={"ce": ce})
    #         database = "data"
    #         collection = "device"
    #         attributes = ce.data["attributes"]
    #         dimensions = ce.data["dimensions"]
    #         variables = ce.data["variables"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]

    #         # TODO fix serial number in magic data record, tmp workaround for now
    #         # serial_number = attributes["serial_number"]

    #         format_version = attributes["format_version"]["data"]
    #         parts = format_version.split(".")
    #         self.logger.debug(f"parts: {parts}, {format_version}")
    #         erddap_version = f"v{parts[0]}"
    #         device_id = "::".join([make, model, serial_number])
    #         self.logger.debug("device_data_update", extra={"device_id": device_id})
    #         timestamp = string_to_timestamp(ce.data["timestamp"]) # change to an actual timestamp

    #         self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

    #         doc = {
    #             # "_id": id,
    #             "make": make,
    #             "model": model,
    #             "serial_number": serial_number,
    #             "version": erddap_version,
    #             "timestamp": timestamp,
    #             "attributes": attributes,
    #             "dimensions": dimensions,
    #             "variables": variables,
    #             # "last_update": datetime.now(tz=timezone.utc),
    #         }

    #         request = DataUpdate(
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             timestamp=timestamp,
    #             attributes=attributes,
    #             dimensions=dimensions,
    #             variables=variables,
    #         )

    #         self.logger.debug("device_data_update", extra={"request": request})
    #         # request = DatastoreRequest(
    #         #     database="data", collection="device", request=update
    #         # )
    #         # self.logger.debug("device_data_update", extra={"device-doc": doc})
    #         # filter = {
    #         #     "make": make,
    #         #     "model": model,
    #         #     "version": erddap_version,
    #         #     "serial_number": serial_number,
    #         #     "timestamp": timestamp,
    #         # }
    #         await self.db_client.device_data_update(
    #             database=database,
    #             collection=collection,
    #             request=request,
    #             ttl=self.config.db_data_ttl,
    #         )
    #         # await self.db_client.device_data_update(
    #         #     document=doc, ttl=self.config.db_data_ttl
    #         # )
    #         # result = self.db_client.update_one(
    #         #     database="data",
    #         #     collection="device",
    #         #     filter=filter,
    #         #     # update=update,
    #         #     document=doc,
    #         #     # upsert=True,
    #         #     ttl=self.config.db_data_ttl
    #         # )
    #         # L.info("device_data_update result", extra={"result": result})

    #     except Exception as e:
    #         L.error("device_data_update", extra={"reason": e})
    #     pass

    # # async def device_data_get(self, query: DataStoreQuery):
    # async def device_data_get(self, query: DataRequest):

    #     # fill in useful values based on user request

    #     # why do we need to do this?
    #     # # if not make,model,serial_number try to build from device_id
    #     # self.logger.debug("device_data_get", extra={"query": query})
    #     # if not query.make or not query.model or not query.serial_number:
    #     #     if not query.device_id:
    #     #         self.logger.debug("device_data_get:1", extra={"query": query})
    #     #         return {"results": []}
    #     #     parts = query.device_id.split("::")
    #     #     query.make = parts[0]
    #     #     query.model = parts[1]
    #     #     query.serial_number = parts[2]
    #     #     self.logger.debug("device_data_get:2", extra={"query": query})
    #     # else:
    #     #     query.device_id = "::".join([query.make,query.model,query.serial_number])

    #     self.logger.debug("device_data_get:3", extra={"query": query})
    #     if query.start_time:
    #         query.start_timestamp = string_to_timestamp(query.start_time)

    #     if query.end_time:
    #         query.end_timestamp = string_to_timestamp(query.end_time)


    #     if query.last_n_seconds:
    #         # this overrides explicit start,end times
    #         start_dt = get_datetime_with_delta(-(query.last_n_seconds))
    #         # current_time = get_datetime()
    #         # start_dt = current_time - timedelta(seconds=query.last_n_seconds)
    #         query.start_timestamp = start_dt.timestamp()
    #         query.end_timestamp = None

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         self.logger.debug("device_data_get:4", extra={"query": query})
    #         return await self.db_client.device_data_get(query)

    #     return {"results": []}

    # async def device_definition_registry_update(self, ce: CloudEvent):

    #     try:
    #         for definition_type, device_def in ce.data.items():
    #             make = device_def["attributes"]["make"]["data"]
    #             model = device_def["attributes"]["model"]["data"]
    #             format_version = device_def["attributes"]["format_version"]["data"]
    #             parts = format_version.split(".")
    #             version = f"v{parts[0]}"
    #             valid_time = device_def.get("valid_time", "2020-01-01T00:00:00Z")

    #             if definition_type == "device-definition":
    #                 database = "registry"
    #                 collection = "device-definition"
    #                 attributes = device_def["attributes"]
    #                 dimensions = device_def["dimensions"]
    #                 variables = device_def["variables"]

    #                 if "device_type" in device_def["attributes"]:
    #                     device_type = device_def["attributes"]["device_type"]["data"]
    #                 else:
    #                     # default for backward compatibility
    #                     device_def["attributes"]["device_type"] = {
    #                         "type": "string",
    #                         "data": "sensor"
    #                     }
    #                     device_type = "sensor"

    #                 device_definition_id = "::".join([make,model,format_version])
    #                 request = DeviceDefinitionUpdate(
    #                     device_definition_id=device_definition_id,
    #                     make=make,
    #                     model=model,
    #                     version=format_version,
    #                     device_type=device_type,
    #                     valid_time=valid_time,
    #                     attributes=attributes,
    #                     dimensions=dimensions,
    #                     variables=variables,
    #                 )

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="device-definition",
    #                 #     request=update
    #                 # )

    #         self.logger.debug(
    #             "device_definition_registry_update", extra={"request": request}
    #         )
    #         if self.db_client:
    #             result = await self.db_client.device_definition_registry_update(
    #                 database=database,
    #                 collection=collection,
    #                 request=request,
    #                 ttl=self.config.db_reg_device_definition_ttl,
    #             )
    #             if result:
    #                 self.logger.debug("configure", extra={"self.config": self.config})
    #                 ack = DAQEvent.create_device_definition_registry_ack(
    #                     source=f"envds.{self.config.daq_id}.datastore",
    #                     data={"device-definition": {"make": make, "model":model, "version": format_version}}

    #                 )
    #                 # f"envds/{self.core_settings.namespace_prefix}/device/registry/ack"
    #                 ack["destpath"] = f"envds/{self.config.daq_id}/device/registry/ack"
    #                 await self.send_event(ack)

    #     except Exception as e:
    #         self.logger.error("device_definition_registry_update", extra={"reason": e})
    #     pass

    # async def device_definition_registry_get(self, query: DeviceDefinitionRequest) -> dict:
        
    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         return await self.db_client.device_definition_registry_get(query)
        
    #     return {"results": []}

    # async def device_instance_registry_update(self, ce: CloudEvent):

    #     try:
    #         self.logger.debug("device_instance_registry_update", extra={"ce": ce})
    #         for instance_type, instance_reg in ce.data.items():
    #             request = None
    #             self.logger.debug("device_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
    #             try:
    #                 # device_id = instance_reg.get("device_id", None)
    #                 make = instance_reg["make"]
    #                 model = instance_reg["model"]
    #                 serial_number = instance_reg["serial_number"]
    #                 format_version = instance_reg["format_version"]
    #                 parts = format_version.split(".")
    #                 version = f"v{parts[0]}"

    #                 if make is None or model is None or serial_number is None:
    #                     # if "device_id" in instance_reg and instance_reg["device_id"] is not None:
    #                         # parts = instance_reg["device_id"].split("::")
    #                         # make = parts[0]
    #                         # model = parts[1]
    #                         # serial_number = parts[2]
    #                     self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
    #                     return
                    
    #                 # if device_id is None:
    #                 device_id = "::".join([make, model, serial_number])

    #                 if instance_type == "device-instance":
    #                     database = "registry"
    #                     collection = "device-instance"
    #                     attributes = instance_reg["attributes"]

    #                     if "device_type" in instance_reg["attributes"]:
    #                         device_type = instance_reg["attributes"]["device_type"]["data"]
    #                     else:
    #                         # default for backward compatibility
    #                         device_type = "sensor"

    #                     request = DeviceInstanceUpdate(
    #                         device_id=device_id,
    #                         make=make,
    #                         model=model,
    #                         serial_number=serial_number,
    #                         version=format_version,
    #                         device_type=device_type,
    #                         attributes=attributes,
    #                     )

    #             except (KeyError, IndexError) as e:
    #                     self.logger.error("datastore:device_instance_registry_update", extra={"reason": e})
    #                     continue

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="device-instance",
    #                 #     request=update,
    #                 # )

    #             self.logger.debug("datastore:device_instance_registry_update", extra={"request": request, "db_client": self.db_client})
    #             if self.db_client and request:
    #                 await self.db_client.device_instance_registry_update(
    #                     database=database,
    #                     collection=collection,
    #                     request=request,
    #                     ttl=self.config.db_reg_device_instance_ttl,
    #                 )

    #     except Exception as e:
    #         L.error("device_instance_registry_update", extra={"reason": e})
    #     pass

    # async def device_instance_registry_get(self, query: DeviceInstanceRequest) -> dict:
        
    #     # TODO add in logic to get/sync from erddap if available?
    #     if self.db_client:
    #         return await self.db_client.device_instance_registry_get(query)
        
    #     return {"results": []}

    # # TODO Add controller_data_update
    # async def controller_data_update(self, ce: CloudEvent):

    #     try:
    #         # database = "data"
    #         # collection = "device"
    #         # self.logger.debug("data_device_update", extra={"ce": ce})
    #         database = "data"
    #         collection = "controller"
    #         attributes = ce.data["attributes"]
    #         dimensions = ce.data["dimensions"]
    #         variables = ce.data["variables"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]

    #         # TODO fix serial number in magic data record, tmp workaround for now
    #         # serial_number = attributes["serial_number"]

    #         format_version = attributes["format_version"]["data"]
    #         parts = format_version.split(".")
    #         self.logger.debug(f"parts: {parts}, {format_version}")
    #         erddap_version = f"v{parts[0]}"
    #         controller_id = "::".join([make, model, serial_number])
    #         self.logger.debug("controller_data_update", extra={"device_id": controller_id})
    #         timestamp = string_to_timestamp(ce.data["timestamp"]) # change to an actual timestamp

    #         self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

    #         doc = {
    #             # "_id": id,
    #             "make": make,
    #             "model": model,
    #             "serial_number": serial_number,
    #             "version": erddap_version,
    #             "timestamp": timestamp,
    #             "attributes": attributes,
    #             "dimensions": dimensions,
    #             "variables": variables,
    #             # "last_update": datetime.now(tz=timezone.utc),
    #         }

    #         request = ControllerDataUpdate(
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             timestamp=timestamp,
    #             attributes=attributes,
    #             dimensions=dimensions,
    #             variables=variables,
    #         )

    #         self.logger.debug("controller_data_update", extra={"request": request})
    #         # request = DatastoreRequest(
    #         #     database="data", collection="device", request=update
    #         # )
    #         # self.logger.debug("device_data_update", extra={"device-doc": doc})
    #         # filter = {
    #         #     "make": make,
    #         #     "model": model,
    #         #     "version": erddap_version,
    #         #     "serial_number": serial_number,
    #         #     "timestamp": timestamp,
    #         # }
    #         await self.db_client.controller_data_update(
    #             database=database,
    #             collection=collection,
    #             request=request,
    #             ttl=self.config.db_data_ttl,
    #         )
    #         # await self.db_client.device_data_update(
    #         #     document=doc, ttl=self.config.db_data_ttl
    #         # )
    #         # result = self.db_client.update_one(
    #         #     database="data",
    #         #     collection="device",
    #         #     filter=filter,
    #         #     # update=update,
    #         #     document=doc,
    #         #     # upsert=True,
    #         #     ttl=self.config.db_data_ttl
    #         # )
    #         # L.info("device_data_update result", extra={"result": result})

    #     except Exception as e:
    #         L.error("device_data_update", extra={"reason": e})
    #     pass


    # async def controller_data_get(self, query: DataRequest):

    #     # fill in useful values based on user request

    #     # why do we need to do this?
    #     # # if not make,model,serial_number try to build from controller_id
    #     # self.logger.debug("controller_data_get", extra={"query": query})
    #     # if not query.make or not query.model or not query.serial_number:
    #     #     if not query.controller_id:
    #     #         self.logger.debug("controller_data_get:1", extra={"query": query})
    #     #         return {"results": []}
    #     #     parts = query.controller_id.split("::")
    #     #     query.make = parts[0]
    #     #     query.model = parts[1]
    #     #     query.serial_number = parts[2]
    #     #     self.logger.debug("controller_data_get:2", extra={"query": query})
    #     # else:
    #     #     query.controller_id = "::".join([query.make,query.model,query.serial_number])

    #     self.logger.debug("controller_data_get:3", extra={"query": query})
    #     if query.start_time:
    #         query.start_timestamp = string_to_timestamp(query.start_time)

    #     if query.end_time:
    #         query.end_timestamp = string_to_timestamp(query.end_time)


    #     if query.last_n_seconds:
    #         # this overrides explicit start,end times
    #         start_dt = get_datetime_with_delta(-(query.last_n_seconds))
    #         # current_time = get_datetime()
    #         # start_dt = current_time - timedelta(seconds=query.last_n_seconds)
    #         query.start_timestamp = start_dt.timestamp()
    #         query.end_timestamp = None

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         self.logger.debug("controller_data_get:4", extra={"query": query})
    #         return await self.db_client.controller_data_get(query)

    #     return {"results": []}

    # async def controller_definition_registry_update(self, ce: CloudEvent):

    #     try:
    #         for definition_type, controller_def in ce.data.items():
    #             make = controller_def["attributes"]["make"]["data"]
    #             model = controller_def["attributes"]["model"]["data"]
    #             format_version = controller_def["attributes"]["format_version"]["data"]
    #             parts = format_version.split(".")
    #             version = f"v{parts[0]}"
    #             valid_time = controller_def.get("valid_time", "2020-01-01T00:00:00Z")

    #             if definition_type == "controller-definition":
    #                 database = "registry"
    #                 collection = "controller-definition"
    #                 attributes = controller_def["attributes"]
    #                 dimensions = controller_def["dimensions"]
    #                 variables = controller_def["variables"]

    #                 # if "controller_type" in controller_def["attributes"]:
    #                 #     controller_type = controller_def["attributes"]["controller_type"]["data"]
    #                 # else:
    #                 #     # default for backward compatibility
    #                 #     controller_def["attributes"]["controller_type"] = {
    #                 #         "type": "string",
    #                 #         "data": "sensor"
    #                 #     }
    #                 #     controller_type = "sensor"

    #                 controller_definition_id = "::".join([make,model,format_version])
    #                 request = ControllerDefinitionUpdate(
    #                     controller_definition_id=controller_definition_id,
    #                     make=make,
    #                     model=model,
    #                     version=format_version,
    #                     # controller_type=controller_type,
    #                     valid_time=valid_time,
    #                     attributes=attributes,
    #                     dimensions=dimensions,
    #                     variables=variables,
    #                 )

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="controller-definition",
    #                 #     request=update
    #                 # )

    #         self.logger.debug(
    #             "controller_definition_registry_update", extra={"request": request}
    #         )
    #         if self.db_client:
    #             result = await self.db_client.controller_definition_registry_update(
    #                 database=database,
    #                 collection=collection,
    #                 request=request,
    #                 ttl=self.config.db_reg_controller_definition_ttl,
    #                 # ttl=self.config.db_reg_device_definition_ttl
    #             )
    #             if result:
    #                 self.logger.debug("configure", extra={"self.config": self.config})
    #                 ack = DAQEvent.create_controller_definition_registry_ack(
    #                     source=f"envds.{self.config.daq_id}.datastore",
    #                     data={"controller-definition": {"make": make, "model":model, "version": format_version}}

    #                 )
    #                 # f"envds/{self.core_settings.namespace_prefix}/controller/registry/ack"
    #                 ack["destpath"] = f"envds/{self.config.daq_id}/controller/registry/ack"
    #                 await self.send_event(ack)

    #     except Exception as e:
    #         self.logger.error("controller_definition_registry_update", extra={"reason": e})
    #     pass

    # async def controller_definition_registry_get(self, query: ControllerDefinitionRequest) -> dict:
        
    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         return await self.db_client.controller_definition_registry_get(query)
        
    #     return {"results": []}

    # async def controller_instance_registry_update(self, ce: CloudEvent):

    #     try:
    #         self.logger.debug("controller_instance_registry_update", extra={"ce": ce})
    #         for instance_type, instance_reg in ce.data.items():
    #             request = None
    #             self.logger.debug("controller_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
    #             try:
    #                 # controller_id = instance_reg.get("controller_id", None)
    #                 make = instance_reg["make"]
    #                 model = instance_reg["model"]
    #                 serial_number = instance_reg["serial_number"]
    #                 format_version = instance_reg["format_version"]
    #                 parts = format_version.split(".")
    #                 version = f"v{parts[0]}"

    #                 if make is None or model is None or serial_number is None:
    #                     # if "controller_id" in instance_reg and instance_reg["controller_id"] is not None:
    #                         # parts = instance_reg["controller_id"].split("::")
    #                         # make = parts[0]
    #                         # model = parts[1]
    #                         # serial_number = parts[2]
    #                     self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
    #                     return
                    
    #                 # if controller_id is None:
    #                 controller_id = "::".join([make, model, serial_number])

    #                 if instance_type == "controller-instance":
    #                     database = "registry"
    #                     collection = "controller-instance"
    #                     attributes = instance_reg["attributes"]

    #                     # if "controller_type" in instance_reg["attributes"]:
    #                     #     controller_type = instance_reg["attributes"]["controller_type"]["data"]
    #                     # else:
    #                     #     # default for backward compatibility
    #                     #     controller_type = "sensor"

    #                     request = ControllerInstanceUpdate(
    #                         controller_id=controller_id,
    #                         make=make,
    #                         model=model,
    #                         serial_number=serial_number,
    #                         version=format_version,
    #                         # controller_type=controller_type,
    #                         attributes=attributes,
    #                     )

    #             except (KeyError, IndexError) as e:
    #                     self.logger.error("datastore:controller_instance_registry_update", extra={"reason": e})
    #                     continue

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="controller-instance",
    #                 #     request=update,
    #                 # )

    #             self.logger.debug("datastore:controller_instance_registry_update", extra={"request": request, "db_client": self.db_client})
    #             if self.db_client and request:
    #                 await self.db_client.controller_instance_registry_update(
    #                     database=database,
    #                     collection=collection,
    #                     request=request,
    #                     ttl=self.config.db_reg_controller_instance_ttl,
    #                 )

    #     except Exception as e:
    #         L.error("controller_instance_registry_update", extra={"reason": e})
    #     pass

    # async def controller_instance_registry_get(self, query: ControllerInstanceRequest) -> dict:
        
    #     # TODO add in logic to get/sync from erddap if available?
    #     if self.db_client:
    #         return await self.db_client.controller_instance_registry_get(query)
        
    #     return {"results": []}

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
        root_path="/msp/filemanager",
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
    config = FilemanagerConfig()
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
