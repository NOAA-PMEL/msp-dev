import importlib
import json
import logging
import math
from time import sleep

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

from pydantic import BaseModel, BaseSettings
from cloudevents.http import CloudEvent, from_http

# from cloudevents.http.conversion import from_http
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone
from datastore_requests import (
    DataStoreQuery,
    DataUpdate,
    DataRequest,
    DeviceDefinitionUpdate,
    DeviceDefinitionRequest,
    DeviceInstanceUpdate,
    DeviceInstanceRequest,
    DatastoreRequest,
    ControllerDefinitionUpdate,
    ControllerDefinitionRequest,
    ControllerDataRequest,
    ControllerDataUpdate,
    ControllerInstanceRequest,
    ControllerInstanceUpdate
)

class DBClientConfig(BaseModel):
    type: str | None = "redis"
    # config: dict | None = {"hostname": "localhost", "port": 1883}
    config: dict | None = {
        "hostname": "", 
        "port": None,
        "username": "",
        "password": ""
    }


class DBClientManager:
    """MessageClientManager.

    Factory class to create MessageClients
    """

    @staticmethod
    def create(config: DBClientConfig = None):
        if config is None:
            config = DBClientConfig()

        if config.type == "redis":

            client_mod = "redis_client"
            client_class = "redis_class"
            mod_ = importlib.import_module("redis_client")
            # print(f"mod_: {"redis_client"}")
            client = getattr(mod_, "RedisClient")(config)
            print(f"client: {client}, {config}")
            return client
            # return RedisClient(config)
            # pass
        elif config.type == "mongoDB":
            # return mongoDBClient
            return None
            pass
        else:
            print("unknown messageclient reqest")
            return None



class DBClient:
    def __init__(self, config: DBClientConfig) -> None:
        # self.db_type = db_type
        # self.config = config
        if config is None:
            config = DBClientConfig()
        self.config = config.config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(config, self.config)
        # self.connection = connection

    def connect(self):
        pass
        # if self.db_type == "mongodb":
        #     self.connect_mongo()
        # return self.client

    # def connect_mongo(self):
    #     if not self.client:
    #         try:
    #             self.client = pymongo.MongoClient(
    #                 self.connection,
    #                 # tls=True,
    #                 # tlsAllowInvalidCertificates=True
    #             )
    #         except pymongo.errors.ConnectionError:
    #             self.client = None
    #         L.info("mongo client", extra={"connection": self.connection, "client": self.client})
    #     # return self.client

    def find_one(self, database: str, collection: str, query: dict):
        self.connect()
        if self.client:
            db = self.client[database]
            db_collection = db[collection]
            result = db_collection.find_one(query)
            if result:
                update = {"last_update": datetime.now(tz=timezone.utc)}
                self.client.update_one(database, collection, result, update)
            return result
        return None

    def insert_one(self, database: str, collection: str, document: dict):
        self.connect()
        if self.client:
            db = self.client[database]
            device_defs = db[collection]
            result = device_defs.insert_one(document)
            return result
        return None

    async def device_data_update(
        self,
        database: str,
        collection: str,
        # document: dict,
        request: DataUpdate,
        # update: dict,
        # filter: dict = None,
        # upsert=False,
        ttl: int = 300
    ):
        return None

    async def device_data_get(self, query: DataRequest):
        return None

    async def device_definition_registry_update(
        self,
        database: str,
        collection: str,
        request: DeviceDefinitionUpdate,
        ttl: int = 0
    ) -> bool:
        return False

    async def device_definition_registry_get(
            self,
            request: DeviceDefinitionRequest
    ) -> dict:
        return {"results": []}

    async def device_instance_registry_update(
        self,
        database: str,
        collection: str,
        request: DeviceInstanceUpdate,
        ttl: int = 0
    ) -> bool:
        return False

    async def device_instance_registry_get(
        self,
        request: DeviceInstanceRequest
    ) -> dict:
        return {"results": []}

    async def controller_data_update(
        self,
        database: str,
        collection: str,
        # document: dict,
        request: DataUpdate,
        # update: dict,
        # filter: dict = None,
        # upsert=False,
        ttl: int = 300
    ):
        return None

    async def controller_data_get(self, query: DataRequest):
        return None

    async def controller_definition_registry_update(
        self,
        database: str,
        collection: str,
        request: ControllerDefinitionUpdate,
        ttl: int = 0
    ) -> bool:
        return False

    async def controller_definition_registry_get(
            self,
            request: ControllerDefinitionRequest
    ) -> dict:
        return {"results": []}

    async def controller_instance_registry_update(
        self,
        database: str,
        collection: str,
        request: ControllerInstanceUpdate,
        ttl: int = 0
    ) -> bool:
        return False

    async def controller_instance_registry_get(
        self,
        request: ControllerInstanceRequest
    ) -> dict:
        return {"results": []}

