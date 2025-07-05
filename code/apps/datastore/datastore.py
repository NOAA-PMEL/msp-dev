import asyncio
import json
import logging
import math
import sys
from time import sleep
from typing import List

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings, BaseModel
from cloudevents.http import CloudEvent, from_http

# from cloudevents.http.conversion import from_http
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timedelta, timezone
from envds.util.util import (
    get_datetime_string,
    get_datetime,
    datetime_to_string,
    get_datetime_with_delta,
)

from envds.daq.event import DAQEvent
import envds.daq.types as det

# import pymongo

import uvicorn

from datastore_requests import (
    DataStoreQuery,
    DataUpdate,
    DataRequest,
    DeviceDefinitionUpdate,
    DeviceDefinitionRequest,
    DeviceInstanceUpdate,
    DeviceInstanceRequest,
    DatastoreRequest,
)
from db_client import DBClientManager, DBClientConfig

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


# test


class DatastoreConfig(BaseSettings):
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

    # TODO fix ns prefix
    daq_id: str | None = None

    db_client_type: str | None = None
    db_client_connection: str | None = None
    db_client_hostname: str | None = None
    db_client_port: str | None = None
    db_client_username: str | None = None
    db_client_password: str | None = None

    db_data_ttl: int = 600  # seconds
    db_reg_device_definition_ttl: int = 0  # permanent
    db_reg_device_instance_ttl: int = 600  # seconds

    erddap_enable: bool = False
    erddap_http_connection: str | None = None
    erddap_http_connection: str | None = None
    erddap_author: str = "fake_author"

    knative_broker: str | None = None
    class Config:
        env_prefix = "DATASTORE_"
        case_sensitive = False


class Datastore:
    """docstring for TestClass."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
        self.logger.setLevel(logging.DEBUG)
        self.db_client = None
        self.erddap_client = None
        self.config = DatastoreConfig()
        self.configure()

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})
        db_client_config = DBClientConfig(
            type=self.config.db_client_type,
            config={
                "connection": self.config.db_client_connection,
                "hostname": self.config.db_client_hostname,
                "port": self.config.db_client_port,
                "username": self.config.db_client_username,
                "password": self.config.db_client_password,
            },
        )
        self.logger.debug("configure", extra={"db_client_config": db_client_config})
        self.db_client = DBClientManager.create(db_client_config)

        if self.config.erddap_enable:
            # setup erddap client
            pass

    async def send_event(self, ce):
        try:
            self.logger.debug(ce)#, extra=template)
            try:
                timeout = httpx.Timeout(5.0, read=0.1)
                headers, body = to_structured(ce)
                self.logger.debug("send_event", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
                # send to knative broker
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        self.config.knative_broker,
                        headers=headers,
                        data=body,
                        timeout=timeout
                    )
                    r.raise_for_status()
            except InvalidStructuredJSON:
                self.logger.error(f"INVALID MSG: {ce}")
            except httpx.TimeoutException:
                pass
            except httpx.HTTPError as e:
                self.logger.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
        except Exception as e:
            print("error", e)
        await asyncio.sleep(0.01)


    def find_one(self):  # , database: str, collection: str, query: dict):
        # self.connect()
        # if self.client:
        #     db = self.client[database]
        #     db_collection = db[collection]
        #     result = db_collection.find_one(query)
        #     if result:
        #         update = {"last_update": datetime.now(tz=timezone.utc)}
        #         db_client.update_one(database, collection, result, update)
        #     return result
        return None

    def insert_one(self):  # , database: str, collection: str, document: dict):
        # self.connect()
        # if self.client:
        #     db = self.client[database]
        #     device_defs = db[collection]
        #     result = device_defs.insert_one(document)
        #     return result
        return None

    # def update_one(self, database: str, collection: str, document: dict, update: dict, upsert=False):
    #     self.connect()
    #     if self.client:
    #         db = self.client[database]
    #         device_defs = db[collection]
    #         set_update = {"$set": update}
    #         result = device_defs.update_one(document, set_update, upsert)
    #         return result
    #     return None

    def update_one(self):
        #     self,
        #     database: str,
        #     collection: str,
        #     document: dict,
        #     update: dict,
        #     filter: dict = None,
        #     upsert=False,
        # ):
        # self.connect()
        # if self.client:
        #     db = self.client[database]
        #     device = db[collection]
        #     if filter is None:
        #         filter = document
        #     set_update = {"$set": update}
        #     if upsert:
        #         set_update["$setOnInsert"] = document
        #     result = device.update_one(filter=filter, update=set_update, upsert=upsert)
        #     return result
        return None

    async def device_data_update(self, ce: CloudEvent):

        try:
            # database = "data"
            # collection = "device"
            # self.logger.debug("data_device_update", extra={"ce": ce})
            database = "data"
            collection = "device"
            attributes = ce.data["attributes"]
            dimensions = ce.data["dimensions"]
            variables = ce.data["variables"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]

            # TODO fix serial number in magic data record, tmp workaround for now
            # serial_number = attributes["serial_number"]

            format_version = attributes["format_version"]["data"]
            parts = format_version.split(".")
            self.logger.debug(f"parts: {parts}, {format_version}")
            erddap_version = f"v{parts[0]}"
            device_id = "::".join([make, model, serial_number])
            timestamp = ce.data["timestamp"]

            doc = {
                # "_id": id,
                "make": make,
                "model": model,
                "serial_number": serial_number,
                "version": erddap_version,
                "timestamp": timestamp,
                "attributes": attributes,
                "dimensions": dimensions,
                "variables": variables,
                # "last_update": datetime.now(tz=timezone.utc),
            }

            request = DataUpdate(
                make=make,
                model=model,
                serial_number=serial_number,
                version=erddap_version,
                timestamp=timestamp,
                attributes=attributes,
                dimensions=dimensions,
                variables=variables,
            )

            # request = DatastoreRequest(
            #     database="data", collection="device", request=update
            # )
            # self.logger.debug("device_data_update", extra={"device-doc": doc})
            # filter = {
            #     "make": make,
            #     "model": model,
            #     "version": erddap_version,
            #     "serial_number": serial_number,
            #     "timestamp": timestamp,
            # }
            await self.db_client.device_data_update(
                database=database,
                collection=collection,
                request=request,
                ttl=self.config.db_data_ttl,
            )
            # await self.db_client.device_data_update(
            #     document=doc, ttl=self.config.db_data_ttl
            # )
            # result = self.db_client.update_one(
            #     database="data",
            #     collection="device",
            #     filter=filter,
            #     # update=update,
            #     document=doc,
            #     # upsert=True,
            #     ttl=self.config.db_data_ttl
            # )
            # L.info("device_data_update result", extra={"result": result})

        except Exception as e:
            L.error("device_data_update", extra={"reason": e})
        pass

    # async def device_data_get(self, query: DataStoreQuery):
    async def device_data_get(self, query: DataRequest):

        # fill in useful values based on user request

        # if not make,model,serial_number try to build from device_id
        if not query.make or not query.model or not query.serial_number:
            if not query.device_id:
                return {}
            parts = query.device_id.split("::")
            query.make = parts[0]
            query.model = parts[1]
            query.serial_number = parts[2]

        if query.last_n_seconds:
            # this overrides explicit start,end times
            start_dt = get_datetime_with_delta(-(query.last_n_seconds))
            # current_time = get_datetime()
            # start_dt = current_time - timedelta(seconds=query.last_n_seconds)
            query.start_time = datetime_to_string(start_dt)
            query.end_time = None

        # TODO add in logic to get/sync from erddap if available
        if self.db_client:
            return await self.db_client.device_data_get(query)

        return {"results": []}

    async def device_definition_registry_update(self, ce: CloudEvent):

        try:
            for definition_type, device_def in ce.data.items():
                make = device_def["attributes"]["make"]["data"]
                model = device_def["attributes"]["model"]["data"]
                format_version = device_def["attributes"]["format_version"]["data"]
                parts = format_version.split(".")
                version = f"v{parts[0]}"
                valid_time = device_def.get("valid_time", "2020-01-01T00:00:00Z")

                if definition_type == "device-definition":
                    database = "registry"
                    collection = "device-definition"
                    attributes = device_def["attributes"]
                    dimensions = device_def["dimensions"]
                    variables = device_def["variables"]

                    if "device_type" in device_def["attributes"]:
                        device_type = device_def["attributes"]["device_type"]["data"]
                    else:
                        # default for backward compatibility
                        device_def["attributes"]["device_type"] = {
                            "type": "string",
                            "data": "sensor"
                        }
                        device_type = "sensor"

                    device_definition_id = "::".join([make,model,format_version])
                    request = DeviceDefinitionUpdate(
                        device_definition_id=device_definition_id,
                        make=make,
                        model=model,
                        version=format_version,
                        device_type=device_type,
                        valid_time=valid_time,
                        attributes=attributes,
                        dimensions=dimensions,
                        variables=variables,
                    )

                    # request = DatastoreRequest(
                    #     database="registry",
                    #     collection="device-definition",
                    #     request=update
                    # )

            self.logger.debug(
                "device_definition_registry_update", extra={"request": request}
            )
            if self.db_client:
                result = await self.db_client.device_definition_registry_update(
                    database=database,
                    collection=collection,
                    request=request,
                    ttl=self.config.db_reg_device_definition_ttl,
                )
                if result:
                    self.logger.debug("configure", extra={"self.config": self.config})
                    ack = DAQEvent.create_device_definition_registry_ack(
                        source=f"envds.{self.config.daq_id}.datastore",
                        data={"device-definition": {"make": make, "model":model, "version": format_version}}

                    )
                    # f"envds/{self.core_settings.namespace_prefix}/device/registry/ack"
                    ack["destpath"] = f"envds/{self.config.daq_id}/device/registry/ack"
                    await self.send_event(ack)

        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": e})
        pass

    async def device_definition_registry_get(self, query: DeviceDefinitionRequest) -> dict:
        
        # TODO add in logic to get/sync from erddap if available
        if self.db_client:
            return await self.db_client.device_definition_registry_get(query)
        
        return {"results": []}

    async def device_instance_registry_update(self, ce: CloudEvent):

        try:
            self.logger.debug("device_instance_registry_update", extra={"ce": ce})
            for instance_type, instance_reg in ce.data.items():
                request = None
                self.logger.debug("device_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
                try:
                    # device_id = instance_reg.get("device_id", None)
                    make = instance_reg["make"]
                    model = instance_reg["model"]
                    serial_number = instance_reg["serial_number"]
                    format_version = instance_reg["format_version"]
                    parts = format_version.split(".")
                    version = f"v{parts[0]}"

                    if make is None or model is None or serial_number is None:
                        # if "device_id" in instance_reg and instance_reg["device_id"] is not None:
                            # parts = instance_reg["device_id"].split("::")
                            # make = parts[0]
                            # model = parts[1]
                            # serial_number = parts[2]
                        self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
                        return
                    
                    # if device_id is None:
                    device_id = "::".join([make, model, serial_number])

                    if instance_type == "device-instance":
                        database = "registry"
                        collection = "device-instance"
                        attributes = instance_reg["attributes"]

                        if "device_type" in instance_reg["attributes"]:
                            device_type = instance_reg["attributes"]["device_type"]["data"]
                        else:
                            # default for backward compatibility
                            device_type = "sensor"

                        request = DeviceInstanceUpdate(
                            device_id=device_id,
                            make=make,
                            model=model,
                            serial_number=serial_number,
                            version=format_version,
                            device_type=device_type,
                            attributes=attributes,
                        )

                except (KeyError, IndexError) as e:
                        self.logger.error("datastore:device_instance_registry_update", extra={"reason": e})
                        continue

                    # request = DatastoreRequest(
                    #     database="registry",
                    #     collection="device-instance",
                    #     request=update,
                    # )

                self.logger.debug("datastore:device_instance_registry_update", extra={"request": request, "db_client": self.db_client})
                if self.db_client and request:
                    await self.db_client.device_instance_registry_update(
                        database=database,
                        collection=collection,
                        request=request,
                        ttl=self.config.db_reg_device_instance_ttl,
                    )

        except Exception as e:
            L.error("device_instance_registry_update", extra={"reason": e})
        pass

    async def device_instance_registry_get(self, query: DeviceInstanceRequest) -> dict:
        
        # TODO add in logic to get/sync from erddap if available?
        if self.db_client:
            return await self.db_client.device_instance_registry_get(query)
        
        return {"results": []}

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
        root_path="/msp/datastore",
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
    config = DatastoreConfig()
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
