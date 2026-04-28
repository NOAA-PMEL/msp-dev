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
            # Device registry index
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

            # Instance registry index (Active Sensors)
            try:
                await self.client.ft(self.registry_device_instance_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.device_id", as_name="device_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.serial_number", as_name="serial_number"),
                )
                definition = IndexDefinition(prefix=["registry:device-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_device_instance_index_name).create_index(schema, definition=definition)

            # Controller Indexes
            try:
                await self.client.ft(self.registry_controller_definition_index_name).info()
            except Exception:
                schema = (TagField("$.registration.controller_definition_id", as_name="controller_definition_id"),)
                definition = IndexDefinition(prefix=["registry:controller-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_controller_definition_index_name).create_index(schema, definition=definition)

            try:
                await self.client.ft(self.registry_controller_instance_index_name).info()
            except Exception:
                schema = (TagField("$.registration.controller_id", as_name="controller_id"),)
                definition = IndexDefinition(prefix=["registry:controller-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_controller_instance_index_name).create_index(schema, definition=definition)

            # Variable registry indexes
            try:
                await self.client.ft(self.registry_variablemap_definition_index_name).info()
            except Exception:
                schema = (TagField("$.registration.variablemap_definition_id", as_name="variablemap_definition_id"),)
                definition = IndexDefinition(prefix=["registry:variablemap-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_variablemap_definition_index_name).create_index(schema, definition=definition)

            try:
                await self.client.ft(self.registry_variableset_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.variableset_definition_id", as_name="variableset_definition_id"),
                    TagField("$.registration.variablemap_definition_id", as_name="variablemap_definition_id"),
                )
                definition = IndexDefinition(prefix=["registry:variableset-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_variableset_definition_index_name).create_index(schema, definition=definition)

            # Variableset Telemetry Data Index
            try:
                await self.client.ft("idx:data-variableset").info()
            except Exception:
                schema = (
                    TagField("$.record.variableset_id", as_name="variableset_id"),
                    NumericField("$.record.timestamp", as_name="timestamp"),
                )
                definition = IndexDefinition(prefix=["data:variableset:"], index_type=IndexType.JSON)
                await self.client.ft("idx:data-variableset").create_index(schema, definition=definition)

            try:
                await self.client.ft(self.registry_variableset_instance_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.variableset_id", as_name="variableset_id"),
                    TagField("$.registration.variablemap_id", as_name="variablemap_id"),
                    TagField("$.registration.variableset", as_name="variableset"),
                )
                definition = IndexDefinition(prefix=["registry:variableset-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_variableset_instance_index_name).create_index(schema, definition=definition)

            # Sampling Generic Resource Indexes
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
            if ch in special: escaped += "\\"
            escaped += ch
        return escaped 

    def _parse_docs_sync(self, documents):
        """Helper to parse JSON in worker thread to support NaN values safely."""
        res = []
        for doc in documents:
            try:
                if doc.json:
                    record = json.loads(doc.json)
                    payload = record.get("record") or record.get("registration")
                    if payload: res.append(payload)
            except Exception:
                continue
        return res

    # -------------------------------------------------------------------------------------
    # VARIABLE SET & MAP FUNCTIONS
    # -------------------------------------------------------------------------------------
    async def variableset_definition_registry_get_ids(self) -> dict:
        prefix = "registry:variableset-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        query_args = []
        if request.variableset_definition_id: 
            query_args.append(f"@variableset_definition_id:{{{self.escape_query(request.variableset_definition_id)}}}")
        if request.variablemap_definition_id:
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(request.variablemap_definition_id)}}}")
        qstring = " ".join(query_args) if query_args else "*"
        docs = (await self.client.ft(self.registry_variableset_definition_index_name).search(Query(qstring))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool:
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variableset_definition_id.replace(':', '')}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def variableset_data_get(self, request: VariableSetDataRequest):
        query_args = []
        if request.variableset_id: query_args.append(f"@variableset_id:{{{self.escape_query(request.variableset_id)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 10000).sort_by("timestamp")
        docs = (await self.client.ft("idx:data-variableset").search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variableset_id}:{request.timestamp}"
        result = await self.client.json().set(key, "$", {"record": document})
        if result: await self.client.expire(key, ttl)
        return result

    # -------------------------------------------------------------------------------------
    # VARIABLE SET INSTANCE (Active Variablesets)
    # -------------------------------------------------------------------------------------
    async def variableset_instance_registry_get_ids(self) -> dict:
        prefix = "registry:variableset-instance:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def variableset_instance_registry_get(self, request: VariableSetInstanceRequest) -> dict:
        query_args = []
        if request.variableset_id: 
            query_args.append(f"@variableset_id:{{{self.escape_query(request.variableset_id)}}}")
        if request.variablemap_id:
            query_args.append(f"@variablemap_id:{{{self.escape_query(request.variablemap_id)}}}")
        if request.variableset:
            query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
            
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 10000)
        docs = (await self.client.ft(self.registry_variableset_instance_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variableset_instance_registry_update(self, database: str, collection: str, request: VariableSetInstanceUpdate, ttl: int = 300) -> bool:
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variableset_id}"
        result = await self.client.json().set(key, "$", {"registration": document})
        if result and ttl > 0:
            await self.client.expire(key, ttl)
        return result
    
    async def variablemap_definition_registry_get_ids(self) -> dict:
        prefix = "registry:variablemap-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        query_args = []
        if request.variablemap_definition_id:
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(request.variablemap_definition_id)}}}")
        qstring = " ".join(query_args) if query_args else "*"
        docs = (await self.client.ft(self.registry_variablemap_definition_index_name).search(Query(qstring))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variablemap_definition_id.replace(':', '')}"
        return await self.client.json().set(key, "$", {"registration": document})

    # -------------------------------------------------------------------------------------
    # DEVICES
    # -------------------------------------------------------------------------------------
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['device_id']}:{document['timestamp']}"
        result = await self.client.json().set(key, "$", {"record": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def device_data_get(self, request: DataRequest):
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        q = Query(" ".join(query_args) if query_args else "*").paging(0, 10000).sort_by("timestamp")
        docs = (await self.client.ft(self.data_device_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['make']}::{document['model']}::{document['version']}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def device_definition_registry_get_ids(self) -> dict:
        prefix = "registry:device-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest):
        query_args = []
        if request.device_definition_id: query_args.append(f"@device_definition_id:{{{self.escape_query(request.device_definition_id)}}}")
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(Query(" ".join(query_args) if query_args else "*"))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['make']}::{document['model']}::{document['serial_number']}"
        result = await self.client.json().set(key, "$", {"registration": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def device_instance_registry_get_ids(self) -> dict:
        prefix = "registry:device-instance:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def device_instance_registry_get(self, request: DeviceInstanceRequest):
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        docs = (await self.client.ft(self.registry_device_instance_index_name).search(Query(" ".join(query_args) if query_args else "*").paging(0, 50))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    # -------------------------------------------------------------------------------------
    # CONTROLLERS
    # -------------------------------------------------------------------------------------
    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['controller_id']}:{document['timestamp']}"
        result = await self.client.json().set(key, "$", {"record": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def controller_data_get(self, request: ControllerDataRequest):
        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        docs = (await self.client.ft(self.data_controller_index_name).search(Query(" ".join(query_args) if query_args else "*").paging(0, 10000).sort_by("timestamp"))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['make']}::{document['model']}::{document['version']}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def controller_definition_registry_get_ids(self) -> dict:
        prefix = "registry:controller-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest):
        query_args = []
        if request.controller_definition_id: query_args.append(f"@controller_definition_id:{{{self.escape_query(request.controller_definition_id)}}}")
        docs = (await self.client.ft(self.registry_controller_definition_index_name).search(Query(" ".join(query_args) if query_args else "*"))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['make']}::{document['model']}::{document['serial_number']}"
        result = await self.client.json().set(key, "$", {"registration": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def controller_instance_registry_get_ids(self) -> dict:
        prefix = "registry:controller-instance:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest):
        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        docs = (await self.client.ft(self.registry_controller_instance_index_name).search(Query(" ".join(query_args) if query_args else "*"))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    # -------------------------------------------------------------------------------------
    # SAMPLING DEFINITIONS (DYNAMIC)
    # -------------------------------------------------------------------------------------
    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        prefix = f"registry:{resource}-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool:
        self.connect()
        id = f"{request['metadata']['name']}::{request['metadata'].get('valid_config_time', '2020').replace(':', '')}"
        return await self.client.json().set(f"{database}:{collection}:{id}", "$", {"registration": request})
    
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        query_args = []
        if "name" in query and query["name"]: query_args.append(f"@name:{{{self.escape_query(query['name'])}}}")
        docs = (await self.client.ft(f"idx:registry-{resource}-definition").search(Query(" ".join(query_args) if query_args else "*"))).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}
    