import asyncio
import json
import logging
import math
import sys
from time import sleep

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings
from cloudevents.http import CloudEvent, from_http

# from cloudevents.http.conversion import from_http
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone
# import pymongo

import uvicorn

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
    db_client_username: str | None = None
    db_client_password: str | None = None

    erddap_client: bool = False
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

    def configure(self):
        # set clients
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

