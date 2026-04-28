import importlib
import json
import logging
import math
from time import sleep

from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

from pydantic import BaseModel, BaseSettings
from cloudevents.http import CloudEvent, from_http
from cloudevents.conversion import to_structured 
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
    ControllerInstanceUpdate,
    VariableMapDefinitionRequest,
    VariableMapDefinitionUpdate,
    VariableSetDefinitionRequest,
    VariableSetDefinitionUpdate,
    VariableSetDataRequest,
    VariableSetDataUpdate,
)

class DBClientConfig(BaseModel):
    type: str | None = "redis"
    config: dict | None = {
        "hostname": "", 
        "port": None,
        "username": "",
        "password": "",
        "clear_db": False
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
            client = getattr(mod_, "RedisClient")(config)
            print(f"client: {client}, {config}")
            return client
        elif config.type == "mongoDB":
            return None
        else:
            print("unknown messageclient reqest")
            return None


class DBClient:
    def __init__(self, config: DBClientConfig) -> None:
        if config is None:
            config = DBClientConfig()
        self.config = config.config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(config, self.config)

    def connect(self):
        pass

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

    # ---------------------------------------------------------
    # DEVICES
    # ---------------------------------------------------------
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300):
        return None

    async def device_data_get(self, query: DataRequest):
        return None

    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0) -> bool:
        return False

    async def device_definition_registry_get_ids(self) -> dict:
        return {"results": []}

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        return {"results": []}

    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 0) -> bool:
        return False

    async def device_instance_registry_get_ids(self) -> dict:
        return {"results": []}

    async def device_instance_registry_get(self, request: DeviceInstanceRequest) -> dict:
        return {"results": []}

    # ---------------------------------------------------------
    # CONTROLLERS
    # ---------------------------------------------------------
    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300):
        return None

    async def controller_data_get(self, query: ControllerDataRequest):
        return None

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0) -> bool:
        return False

    async def controller_definition_registry_get_ids(self) -> dict:
        return {"results": []}

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        return {"results": []}

    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 0) -> bool:
        return False

    async def controller_instance_registry_get_ids(self) -> dict:
        return {"results": []}

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest) -> dict:
        return {"results": []}

    # ---------------------------------------------------------
    # VARIABLE MAPS & SETS
    # ---------------------------------------------------------
    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0) -> bool:
        return False

    async def variablemap_definition_registry_get_ids(self) -> dict:
        return {"results": []}

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        return {"results": []}

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool:
        return False

    async def variableset_definition_registry_get_ids(self) -> dict:
        return {"results": []}

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        return {"results": []}

    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 0) -> bool:
        return False

    async def variableset_data_get(self, request: VariableSetDataRequest) -> dict:
        return {"results": []}

    # ---------------------------------------------------------
    # SAMPLING DEFINITIONS (DYNAMIC)
    # ---------------------------------------------------------
    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        return {"results": []}

    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool:
        return False
    
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        return {"results": []}
    
    