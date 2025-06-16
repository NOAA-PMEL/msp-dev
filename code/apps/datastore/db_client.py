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
from datastore_query import DataStoreQuery

class DBClientConfig(BaseModel):
    type: str | None = "redis"
    # config: dict | None = {"hostname": "localhost", "port": 1883}
    config: dict | None = {
        "hostname": "mosquitto.default", 
        "port": 1883,
        "username": "unknown",
        "password": "unknown"
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
            # return mqtt client
            mod_ = importlib.import_module("redis_client")
            print(f"mod_: {"redis_client"}")
            client = getattr(mod_, "RedisClient")(config)
            print(f"client: {client}")
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
        self.config = config
        if config is None:
            self.config = DBClientConfig()
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
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
            sensor_defs = db[collection]
            result = sensor_defs.insert_one(document)
            return result
        return None

    async def data_sensor_update(
        self,
        # database: str,
        # collection: str,
        document: dict,
        # update: dict,
        # filter: dict = None,
        # upsert=False,
        ttl: int = 300
    ):
        return None

    async def data_sensor_get(self, query: DataStoreQuery):
        return None

    # def update_one(
    #     self,
    #     database: str,
    #     collection: str,
    #     document: dict,
    #     # update: dict,
    #     filter: dict = None,
    #     # upsert=False,
    #     ttl: int = 300
    # ):
    #     pass
    #     # self.connect()
    #     # if self.client:
    #     #     db = self.client[database]
    #     #     sensor = db[collection]
    #     #     if filter is None:
    #     #         filter = document
    #     #     set_update = {"$set": update}
    #     #     if upsert:
    #     #         set_update["$setOnInsert"] = document
    #     #     result = sensor.update_one(filter=filter, update=set_update, upsert=upsert)
    #     #     return result
    #     return None
