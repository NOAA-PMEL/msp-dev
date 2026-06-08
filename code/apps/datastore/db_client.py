import importlib
import json
import logging
import math
import time
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
    VariableSetInstanceRequest,
    VariableSetInstanceUpdate,
)

class DBClientConfig(BaseModel):
    type: str | None = "redis"
    config: dict | None = {
        "hostname": "", 
        "port": None,
        "username": "",
        "password": "",
        "clear_db": False,
        "db_data_ttl": 600,
        "erddap_enable": False,
        "erddap_http_connection": None,
        "erddap_author": None,
        "log_level": "INFO"
    }


class DBClient:
    """Base class for Database Clients."""
    def __init__(self, config: DBClientConfig) -> None:
        if config is None:
            config = DBClientConfig()
        self.config = config.config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        level_str = self.config.get("log_level", "INFO").upper()
        self.logger.setLevel(level_str)
        self.logger.debug("DBClient initialized", extra={"config": self.config})

    def connect(self):
        pass

    def find_one(self, database: str, collection: str, query: dict):
        pass

    def insert_one(self, database: str, collection: str, document: dict):
        pass

    # Method stubs required by the Datastore interface
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300): return None
    async def device_data_get(self, query: DataRequest): return None
    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0) -> bool: return False
    async def device_definition_registry_get_ids(self) -> dict: return {"results": []}
    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict: return {"results": []}
    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 0) -> bool: return False
    async def device_instance_registry_get_ids(self) -> dict: return {"results": []}
    async def device_instance_registry_get(self, request: DeviceInstanceRequest) -> dict: return {"results": []}
    
    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300): return None
    async def controller_data_get(self, query: ControllerDataRequest): return None
    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0) -> bool: return False
    async def controller_definition_registry_get_ids(self) -> dict: return {"results": []}
    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict: return {"results": []}
    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 0) -> bool: return False
    async def controller_instance_registry_get_ids(self) -> dict: return {"results": []}
    async def controller_instance_registry_get(self, request: ControllerInstanceRequest) -> dict: return {"results": []}

    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0) -> bool: return False
    async def variablemap_definition_registry_get_ids(self) -> dict: return {"results": []}
    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict: return {"results": []}
    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool: return False
    async def variableset_definition_registry_get_ids(self) -> dict: return {"results": []}
    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict: return {"results": []}
    
    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 0) -> bool: return False
    async def variableset_data_get(self, request: VariableSetDataRequest) -> dict: return {"results": []}
    
    async def sampling_definition_registry_get_ids(self, resource: str) -> dict: return {"results": []}
    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool: return False
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict: return {"results": []}
    
    async def variableset_instance_registry_update(self, database: str, collection: str, request: VariableSetInstanceUpdate, ttl: int = 0) -> bool: return False
    async def variableset_instance_registry_get_ids(self) -> dict: return {"results": []}
    async def variableset_instance_registry_get(self, request: VariableSetInstanceRequest) -> dict: return {"results": []}
    
    async def project_definition_registry_get_ids(self) -> dict: return {"results": []}
    async def platform_definition_registry_get_ids(self) -> dict: return {"results": []}


class CompositeDBClient(DBClient):
    """
    Federated router. Handles the tiered storage architecture:
    - Live Telemetry (< TTL): Routes to Redis
    - Historical Telemetry (> TTL): Routes to ERDDAP
    - Definitions: Uses Redis as a Read-Through cache for ERDDAP
    """
    def __init__(self, config: DBClientConfig):
        super().__init__(config)
        
        import redis_client
        self.redis = redis_client.RedisClient(config)
        
        self.live_window_seconds = self.config.get("db_data_ttl", 600)

        if self.config.get("erddap_enable"):
            import erddap_client
            self.erddap = erddap_client.ErddapClient(config)
            self.logger.info("Composite Router: ERDDAP historical backend enabled.")
        else:
            self.erddap = None
            self.logger.info("Composite Router: ERDDAP disabled. Operating in Edge/Cache-only mode.")

    async def build_indexes(self):
        """Pass through index building to Redis."""
        if hasattr(self.redis, "build_indexes"):
            await self.redis.build_indexes()

    # ---------------------------------------------------------
    # TELEMETRY ROUTING (Federated)
    # ---------------------------------------------------------
    def _is_live_query(self, start_timestamp: float = None, end_timestamp: float = None) -> bool:
        now = time.time()
        
        if not start_timestamp and not end_timestamp:
            return True
            
        safe_redis_window = self.live_window_seconds - 60
        
        if start_timestamp and (now - start_timestamp) <= safe_redis_window:
            return True
            
        return False

    async def device_data_get(self, request: DataRequest) -> dict:
        if getattr(request, "force_archive", False) and self.erddap:
            self.logger.debug("Routing device_data_get to ERDDAP (Forced Archive Mode)")
            return await self.erddap.device_data_get(request)

        if not self.erddap or self._is_live_query(request.start_timestamp, request.end_timestamp):
            self.logger.debug("Routing device_data_get to REDIS (Live Window or Edge-Only Mode)")
            return await self.redis.device_data_get(request)
        else:
            self.logger.debug("Routing device_data_get to ERDDAP (Historical Window)")
            return await self.erddap.device_data_get(request)

    async def controller_data_get(self, request: ControllerDataRequest) -> dict:
        if getattr(request, "force_archive", False) and self.erddap and hasattr(self.erddap, "controller_data_get"):
            self.logger.debug("Routing controller_data_get to ERDDAP (Forced Archive Mode)")
            return await self.erddap.controller_data_get(request)

        if not self.erddap or self._is_live_query(request.start_timestamp, request.end_timestamp):
            self.logger.debug("Routing controller_data_get to REDIS")
            return await self.redis.controller_data_get(request)
        else:
            self.logger.debug("Routing controller_data_get to ERDDAP")
            if hasattr(self.erddap, "controller_data_get"):
                return await self.erddap.controller_data_get(request)
            return {"results": []}

    async def variableset_data_get(self, request: VariableSetDataRequest) -> dict:
        if getattr(request, "force_archive", False) and self.erddap:
            self.logger.debug("Routing variableset_data_get to ERDDAP (Forced Archive Mode)")
            return await self.erddap.variableset_data_get(request)

        if not self.erddap or self._is_live_query(request.start_timestamp, request.end_timestamp):
            self.logger.debug("Routing variableset_data_get to REDIS")
            return await self.redis.variableset_data_get(request)
        else:
            self.logger.debug("Routing variableset_data_get to ERDDAP")
            return await self.erddap.variableset_data_get(request)

    # ---------------------------------------------------------
    # DEFINITIONS (Read-Through Cache)
    # ---------------------------------------------------------
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        result = await self.redis.sampling_definition_registry_get(resource, query)
        if not result.get("results"):
            if self.erddap:
                self.logger.info(f"Cache miss for {resource} '{query.get('name')}'. Fetching from ERDDAP...")
                result = await self.erddap.sampling_definition_registry_get(resource, query)
                if result.get("results"):
                    for definition in result["results"]:
                        await self.redis.sampling_definition_registry_update(
                            resource=resource,
                            database="registry",
                            collection=f"{resource}-definition",
                            request=definition,
                            ttl=3600
                        )
        return result

    # ---------------------------------------------------------
    # WRITE PASS-THROUGHS 
    # ---------------------------------------------------------
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300):
        return await self.redis.device_data_update(database, collection, request, ttl)

    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300):
        return await self.redis.controller_data_update(database, collection, request, ttl)

    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 0) -> bool:
        return await self.redis.variableset_data_update(database, collection, request, ttl)

    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool:
        return await self.redis.sampling_definition_registry_update(resource, database, collection, request, ttl)

    # ---------------------------------------------------------
    # FEDERATED DEFINITION PASS-THROUGHS TO REDIS
    # ---------------------------------------------------------
    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0) -> bool:
        return await self.redis.device_definition_registry_update(database, collection, request, ttl)
        
    async def device_definition_registry_get_ids(self) -> dict:
        return await self.redis.device_definition_registry_get_ids()
        
    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        result = await self.redis.device_definition_registry_get(request)
        if not result.get("results") and self.erddap and hasattr(self.erddap, "device_definition_registry_get"):
            self.logger.info("Cache miss for device definition. Fetching from ERDDAP...")
            result = await self.erddap.device_definition_registry_get(request)
            if result.get("results"):
                for definition in result["results"]:
                    await self.redis.device_definition_registry_update(
                        database="registry", 
                        collection="device-definition", 
                        request=definition, 
                        ttl=3600
                    )
        return result

    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 0) -> bool:
        return await self.redis.device_instance_registry_update(database, collection, request, ttl)
        
    async def device_instance_registry_get_ids(self) -> dict:
        return await self.redis.device_instance_registry_get_ids()
        
    async def device_instance_registry_get(self, request: DeviceInstanceRequest) -> dict:
        return await self.redis.device_instance_registry_get(request)

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0) -> bool:
        return await self.redis.controller_definition_registry_update(database, collection, request, ttl)
        
    async def controller_definition_registry_get_ids(self) -> dict:
        return await self.redis.controller_definition_registry_get_ids()
        
    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        result = await self.redis.controller_definition_registry_get(request)
        if not result.get("results") and self.erddap and hasattr(self.erddap, "controller_definition_registry_get"):
            self.logger.info("Cache miss for controller definition. Fetching from ERDDAP...")
            result = await self.erddap.controller_definition_registry_get(request)
            if result.get("results"):
                for definition in result["results"]:
                    await self.redis.controller_definition_registry_update(
                        database="registry", 
                        collection="controller-definition", 
                        request=definition, 
                        ttl=3600
                    )
        return result

    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 0) -> bool:
        return await self.redis.controller_instance_registry_update(database, collection, request, ttl)
        
    async def controller_instance_registry_get_ids(self) -> dict:
        return await self.redis.controller_instance_registry_get_ids()
        
    async def controller_instance_registry_get(self, request: ControllerInstanceRequest) -> dict:
        return await self.redis.controller_instance_registry_get(request)

    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0) -> bool:
        return await self.redis.variablemap_definition_registry_update(database, collection, request, ttl)
        
    async def variablemap_definition_registry_get_ids(self) -> dict:
        return await self.redis.variablemap_definition_registry_get_ids()
        
    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        result = await self.redis.variablemap_definition_registry_get(request)
        if not result.get("results") and self.erddap and hasattr(self.erddap, "variablemap_definition_registry_get"):
            self.logger.info("Cache miss for variablemap definition. Fetching from ERDDAP...")
            result = await self.erddap.variablemap_definition_registry_get(request)
            if result.get("results"):
                for definition in result["results"]:
                    await self.redis.variablemap_definition_registry_update(
                        database="registry", 
                        collection="variablemap-definition", 
                        request=definition, 
                        ttl=3600
                    )
        return result

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool:
        return await self.redis.variableset_definition_registry_update(database, collection, request, ttl)
        
    async def variableset_definition_registry_get_ids(self) -> dict:
        return await self.redis.variableset_definition_registry_get_ids()
        
    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        result = await self.redis.variableset_definition_registry_get(request)
        if not result.get("results") and self.erddap and hasattr(self.erddap, "variableset_definition_registry_get"):
            self.logger.info("Cache miss for variableset definition. Fetching from ERDDAP...")
            result = await self.erddap.variableset_definition_registry_get(request)
            if result.get("results"):
                for definition in result["results"]:
                    await self.redis.variableset_definition_registry_update(
                        database="registry", 
                        collection="variableset-definition", 
                        request=definition, 
                        ttl=3600
                    )
        return result

    async def variableset_instance_registry_update(self, database: str, collection: str, request: VariableSetInstanceUpdate, ttl: int = 0) -> bool:
        return await self.redis.variableset_instance_registry_update(database, collection, request, ttl)
        
    async def variableset_instance_registry_get_ids(self) -> dict:
        return await self.redis.variableset_instance_registry_get_ids()
        
    async def variableset_instance_registry_get(self, request: VariableSetInstanceRequest) -> dict:
        return await self.redis.variableset_instance_registry_get(request)

    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        return await self.redis.sampling_definition_registry_get_ids(resource)
    async def project_definition_registry_get_ids(self) -> dict:
        return await self.redis.project_definition_registry_get_ids()
    async def platform_definition_registry_get_ids(self) -> dict:
        return await self.redis.platform_definition_registry_get_ids()

class DBClientManager:
    """Factory class to create Database Clients"""

    @staticmethod
    def create(config: DBClientConfig = None):
        if config is None:
            config = DBClientConfig()

        if config.type == "redis":
            client = CompositeDBClient(config)
            print(f"client: {client}, {config}")
            return client
        elif config.type == "mongoDB":
            return None
        else:
            print("unknown dbclient reqest")
            return None