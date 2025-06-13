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
from envds.util.util import get_datetime_string, get_datetime, datetime_to_string, get_datetime_with_delta
# import pymongo

import uvicorn

from datastore_query import DataStoreQuery
from db_client import DBClientManager, DBClientConfig

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


# test

class Settings(BaseSettings):
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
    
    db_client_type: str | None = None
    db_client_connection: str | None = None
    db_client_hostname: str | None = None
    db_client_port: str | None = None
    db_client_username: str | None = None
    db_client_password: str | None = None

    db_data_ttl: int = 600 # seconds
    db_reg_device_instance_ttl: int = 600 # seconds

    erddap_enable: bool = False
    erddap_http_connection: str | None = None
    erddap_http_connection: str | None = None
    erddap_author: str = "fake_author"

    class Config:
        env_prefix = "DATASTORE_"
        case_sensitive = False

class Datastore():
    """docstring for TestClass."""
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
        self.db_client = None
        self.erddap_client = None
        self.config = Settings()
        self.configure()

    def configure(self):
        # set clients
        
        db_client_config = DBClientConfig(
            type=self.config.db_client_type,
            config={
                "connection": self.config.db_client_connection,
                "hostname": self.config.db_client_connection,
                "port": self.config.db_client_port,
                "username": self.config.client_username,
                "password": self.config.client_password,
            }
        )
        self.db_client = DBClientManager(db_client_config)

        if self.config.erddap_enable:
            # setup erddap client
            pass


    def find_one(self):#, database: str, collection: str, query: dict):
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

    def insert_one(self):#, database: str, collection: str, document: dict):
        # self.connect()
        # if self.client:
        #     db = self.client[database]
        #     sensor_defs = db[collection]
        #     result = sensor_defs.insert_one(document)
        #     return result
        return None

    # def update_one(self, database: str, collection: str, document: dict, update: dict, upsert=False):
    #     self.connect()
    #     if self.client:
    #         db = self.client[database]
    #         sensor_defs = db[collection]
    #         set_update = {"$set": update}
    #         result = sensor_defs.update_one(document, set_update, upsert)
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
        #     sensor = db[collection]
        #     if filter is None:
        #         filter = document
        #     set_update = {"$set": update}
        #     if upsert:
        #         set_update["$setOnInsert"] = document
        #     result = sensor.update_one(filter=filter, update=set_update, upsert=upsert)
        #     return result
        return None
    
    async def data_sensor_update(self, ce: CloudEvent):
        
        try:
            # database = "data"
            # collection = "sensor"
            attributes = ce.data["attributes"]
            dimensions = ce.data["dimensions"]
            variables = ce.data["variables"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]
            format_version = attributes["format_version"]["data"]
            parts = format_version.split(".")
            erddap_version = f"v{parts[0]}"
            sensor_id = "::".join([make, model, serial_number])
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

            # filter = {
            #     "make": make,
            #     "model": model,
            #     "version": erddap_version,
            #     "serial_number": serial_number,
            #     "timestamp": timestamp,
            # }

            await self.db_client.sensor_data_update(
                document=doc,
                ttl=self.config.db_data_ttl
            )
            # result = self.db_client.update_one(
            #     database="data",
            #     collection="sensor",
            #     filter=filter,
            #     # update=update,
            #     document=doc,
            #     # upsert=True,
            #     ttl=self.config.db_data_ttl
            # )
            L.info("sensor_data_update result", extra={"result": result})

        except Exception as e:
            L.error("sensor_data_update", extra={"reason": e})
        pass

    async def sensor_data_get(self, query: DataStoreQuery):

        # fill in useful values based on user request

        # if not make,model,serial_number try to build from sensor_id
        if not query.make or not query.model or not query.serial_number:
            if not query.serial_number:
                return {}
            parts = query.serial_number.split("::")
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

        return await self.db_client.sensor_data_get(query)
    
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
    config = Settings()
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

