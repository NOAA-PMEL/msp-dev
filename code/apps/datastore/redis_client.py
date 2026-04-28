import asyncio
import json
import redis.asyncio as redis
from redis.commands.json.path import Path
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.query import Query

from db_client import DBClient, DBClientConfig
from datastore_requests import (
    DataUpdate, DataRequest, DeviceDefinitionUpdate,
    DeviceDefinitionRequest, DeviceInstanceUpdate, DeviceInstanceRequest,
    ControllerInstanceUpdate, ControllerInstanceRequest,
    ControllerDataRequest, ControllerDataUpdate, ControllerDefinitionRequest,
    ControllerDefinitionUpdate, VariableSetDataUpdate, VariableSetDataRequest,
    VariableSetDefinitionUpdate, VariableSetDefinitionRequest,
    VariableMapDefinitionRequest, VariableMapDefinitionUpdate
)

class RedisClient(DBClient):
    def __init__(self, config: DBClientConfig):
        super(RedisClient, self).__init__(config)
        self.data_device_index_name = "idx:data-device"
        self.registry_device_definition_index_name = "idx:registry-device-definition"
        self.registry_device_instance_index_name = "idx:registry-device-instance"
        self.data_controller_index_name = "idx:data-controller"
        self.registry_controller_definition_index_name = "idx:registry-controller-definition"
        self.registry_controller_instance_index_name = "idx:registry-controller-instance"
        self.registry_variablemap_definition_index_name = "idx:registry-variablemap-definition"
        self.registry_variableset_definition_index_name = "idx:registry-variableset-definition"

    def connect(self):
        if not self.client:
            try:
                if self.config["port"] is None:
                    self.config["port"] = 6379
                if self.config["password"]:
                    self.client = redis.Redis(host=self.config["hostname"], port=self.config["port"], password=self.config["password"])
                else:
                    self.client = redis.Redis(host=self.config["hostname"], port=self.config["port"])
            except Exception as e:
                self.logger.error("redis connect", extra={"reason": e})
                self.client = None

    async def build_indexes(self):
        self.connect()
        if self.config.get("clear_db"):
            await self.client.flushall()

        try:
            # Create indexes for search-based retrieval
            # Device Indexes
            try:
                await self.client.ft(self.data_device_index_name).info()
            except Exception:
                schema = (
                    TagField("$.record.device_id", as_name="device_id"),
                    TagField("$.record.make", as_name="make"),
                    TagField("$.record.model", as_name="model"),
                    TagField("$.record.serial_number", as_name="serial_number"),
                    TagField("$.record.version", as_name="version"),
                    NumericField("$.record.timestamp", as_name="timestamp")
                )
                definition = IndexDefinition(prefix=["data:device:"], index_type=IndexType.JSON)
                await self.client.ft(self.data_device_index_name).create_index(schema, definition=definition)

            try:
                await self.client.ft(self.registry_device_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.device_definition_id", as_name="device_definition_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.version", as_name="version"),
                    TextField("$.registration.device_type", as_name="device_type"),
                )
                definition = IndexDefinition(prefix=["registry:device-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_device_definition_index_name).create_index(schema, definition=definition)

            # Controller Indexes
            try:
                await self.client.ft(self.data_controller_index_name).info()
            except Exception:
                schema = (
                    TagField("$.record.controller_id", as_name="controller_id"),
                    TagField("$.record.make", as_name="make"),
                    TagField("$.record.model", as_name="model"),
                    TagField("$.record.serial_number", as_name="serial_number"),
                    TagField("$.record.version", as_name="version"),
                    NumericField("$.record.timestamp", as_name="timestamp")
                )
                definition = IndexDefinition(prefix=["data:controller:"], index_type=IndexType.JSON)
                await self.client.ft(self.data_controller_index_name).create_index(schema, definition=definition)

            try:
                await self.client.ft(self.registry_controller_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.controller_definition_id", as_name="controller_definition_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.version", as_name="version"),
                )
                definition = IndexDefinition(prefix=["registry:controller-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_controller_definition_index_name).create_index(schema, definition=definition)

            # Sampling/Variable Indexes
            for resource in ["platform", "project", "systemmode", "samplingmode", "samplingstate", "samplingcondition", "action"]:
                index_name = f"idx:registry-{resource}-definition"
                prefix = f"registry:{resource}-definition:"
                try:
                    await self.client.ft(index_name).info()
                except Exception:
                    schema = (TagField("$.registration.metadata.name", as_name="name"),)
                    definition = IndexDefinition(prefix=[prefix], index_type=IndexType.JSON)
                    await self.client.ft(index_name).create_index(schema, definition=definition)

        except Exception as e:
            self.logger.error("build_indexes", extra={"reason": e})

    def escape_query(self, query: str) -> str:
        special = [",",".","<",">","{","}","[","]","'",":",";","!","@","#","$","%","^","&","*","(",")","-","+","=","~"]
        escaped = ""
        for ch in query:
            if ch in special:
                escaped += "\\"
            escaped += ch
        return escaped 

    def _parse_docs_sync(self, documents):
        """
        Helper to parse JSON in a background worker thread.
        Reverted to standard json.loads to support NaN values in telemetry.
        """
        res = []
        for doc in documents:
            try:
                if doc.json:
                    record = json.loads(doc.json)
                    payload = record.get("record") or record.get("registration")
                    if payload:
                        res.append(payload)
            except Exception:
                continue
        return res

    # --- DEVICES ---
    async def device_data_get(self, request: DataRequest):
        await super(RedisClient, self).device_data_get(request)
        max_results = 10000
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = (await self.client.ft(self.data_device_index_name).search(q)).docs
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        """Removed 'Fast Path' direct lookup to ensure RediSearch fallback."""
        query_args = []
        if request.device_definition_id:
            query_args.append(f"@device_definition_id:{{{self.escape_query(request.device_definition_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(q)).docs
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    async def device_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:device-definition:*"):
                id = key.decode('utf-8').replace("registry:device-definition:", "")
                ids.append(id)
            return {"results": ids}
        except Exception as e:
            self.logger.error("device_definition_registry_get_ids", extra={"reason": e})
            return {"results": []}

    # --- CONTROLLERS ---
    async def controller_data_get(self, request: ControllerDataRequest):
        await super(RedisClient, self).controller_data_get(request)
        max_results = 10000
        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = (await self.client.ft(self.data_controller_index_name).search(q)).docs
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        """FIXED: standardized to use RediSearch and background parsing helper."""
        query_args = []
        if request.controller_definition_id:
            query_args.append(f"@controller_definition_id:{{{self.escape_query(request.controller_definition_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_controller_definition_index_name).search(q)).docs
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    async def controller_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:controller-definition:*"):
                id = key.decode('utf-8').replace("registry:controller-definition:", "")
                ids.append(id)
            return {"results": ids}
        except Exception as e:
            self.logger.error("controller_definition_registry_get_ids", extra={"reason": e})
            return {"results": []}

    # --- SAMPLING RESOURCES ---
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        query_args = []
        if "name" in query and query["name"]:
            query_args.append(f"@name:{{{self.escape_query(query['name'])}}}")

        qstring = " ".join(query_args) if query_args else "*"
        index_name = f"idx:registry-{resource}-definition"
        
        try:
            q = Query(qstring)
            docs = (await self.client.ft(index_name).search(q)).docs
            results = await asyncio.to_thread(self._parse_docs_sync, docs)
            return {"results": results}
        except Exception as e:
            self.logger.error(f"redis_client: {resource}_get error", extra={"reason": e})
            return {"results": []}

    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        ids = []
        prefix = f"registry:{resource}-definition:"
        async for key in self.client.scan_iter(f"{prefix}*"):
            ids.append(key.decode('utf-8').replace(prefix, ""))
        return {"results": ids}

    # --- UPDATES ---
    async def device_data_update(self, database, collection, request, ttl=300):
        self.connect()
        document = request.dict()
        device_id = "::".join([document["make"], document["model"], document["serial_number"]])
        key = f"{database}:{collection}:{device_id}:{document['timestamp']}"
        await self.client.json().set(key, "$", {"record": document})
        await self.client.expire(key, ttl)

    async def device_definition_registry_update(self, database, collection, request, ttl=0):
        self.connect()
        document = request.dict()
        id = "::".join([request.make, request.model, request.version])
        key = f"{database}:{collection}:{id}"
        await self.client.json().set(key, "$", {"registration": document})
        if ttl > 0:
            await self.client.expire(key, ttl)
        return True
