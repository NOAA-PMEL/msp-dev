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
    VariableMapDefinitionRequest, VariableMapDefinitionUpdate,
    VariableSetInstanceRequest, VariableSetInstanceUpdate
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
        self.registry_variableset_instance_index_name = "idx:registry-variableset-instance"

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
            # Device Data Index
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

            # Device Definition Index
            try:
                await self.client.ft(self.registry_device_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.device_definition_id", as_name="device_definition_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.version", as_name="version"),
                    TagField("$.registration.device_type", as_name="device_type"),
                )
                definition = IndexDefinition(prefix=["registry:device-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_device_definition_index_name).create_index(schema, definition=definition)

            # Device Instance Index (Active Sensors)
            try:
                await self.client.ft(self.registry_device_instance_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.device_id", as_name="device_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.serial_number", as_name="serial_number"),
                    TagField("$.registration.version", as_name="version"),
                    TagField("$.registration.device_type", as_name="device_type"),
                )
                definition = IndexDefinition(prefix=["registry:device-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_device_instance_index_name).create_index(schema, definition=definition)

            # Controller Data Index
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

            # Controller Definition Index
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

            # Controller Instance Index
            try:
                await self.client.ft(self.registry_controller_instance_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.controller_id", as_name="controller_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.serial_number", as_name="serial_number"),
                    TagField("$.registration.version", as_name="version"),
                )
                definition = IndexDefinition(prefix=["registry:controller-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_controller_instance_index_name).create_index(schema, definition=definition)

            # Variable Map Definition Index
            try:
                await self.client.ft(self.registry_variablemap_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.variablemap_definition_id", as_name="variablemap_definition_id"),
                    TagField("$.registration.variablemap_type", as_name="variablemap_type"),
                    TagField("$.registration.variablemap_type_id", as_name="variablemap_type_id"),
                    TagField("$.registration.variablemap", as_name="variablemap"),
                    TagField("$.registration.valid_config_time", as_name="valid_config_time"),
                )
                definition = IndexDefinition(prefix=["registry:variablemap-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_variablemap_definition_index_name).create_index(schema, definition=definition)

            # Variable Set Definition Index
            try:
                await self.client.ft(self.registry_variableset_definition_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.variableset_definition_id", as_name="variableset_definition_id"),
                    TagField("$.registration.variablemap_definition_id", as_name="variablemap_definition_id"),
                    TagField("$.registration.variableset", as_name="variableset"),
                    TagField("$.registration.index_type", as_name="index_type"),
                    TagField("$.registration.index_value", as_name="index_value"),
                )
                definition = IndexDefinition(prefix=["registry:variableset-definition:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_variableset_definition_index_name).create_index(schema, definition=definition)

            # Variable Set Telemetry Data Index
            try:
                await self.client.ft("idx:data-variableset").info()
            except Exception:
                schema = (
                    TagField("$.record.variableset_id", as_name="variableset_id"),
                    TagField("$.record.variablemap_id", as_name="variablemap_id"),
                    TagField("$.record.variableset", as_name="variableset"),
                    NumericField("$.record.timestamp", as_name="timestamp"),
                )
                definition = IndexDefinition(prefix=["data:variableset:"], index_type=IndexType.JSON)
                await self.client.ft("idx:data-variableset").create_index(schema, definition=definition)

            # Variable Set Instance Index
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
        for ch in str(query):
            if ch in special: escaped += "\\"
            escaped += ch
        return escaped 

    def _parse_docs_sync(self, documents):
        """Helper to parse JSON from RediSearch result documents."""
        res = []
        for doc in documents:
            try:
                # FIXED: Access the '$' attribute using getattr because 
                # '$' is not a valid Python identifier for direct doc.$ access.
                data = None
                if hasattr(doc, "$"):
                    data = getattr(doc, "$")
                elif hasattr(doc, "json"):
                    data = doc.json
                elif isinstance(doc, dict) and "$" in doc:
                    data = doc["$"]
                
                if data:
                    # Handle both single JSON objects and list results from JSON paths
                    if isinstance(data, list):
                        # RediSearch RETURN $ often returns a list of results
                        record = json.loads(data[0]) if isinstance(data[0], str) else data[0]
                    else:
                        record = json.loads(data)
                        
                    # Extract payload from either telemetry 'record' or registry 'registration'
                    payload = record.get("record") or record.get("registration") or record
                    if payload:
                        res.append(payload)
            except Exception as e:
                self.logger.error("parse_docs_sync_error", extra={"reason": str(e)})
                continue
        return res
    
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
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=10000).sort_by("timestamp").return_fields("$")
        docs = (await self.client.ft(self.data_device_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        id_str = "::".join([request.make, request.model, request.version])
        key = f"{database}:{collection}:{id_str}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def device_definition_registry_get_ids(self) -> dict:
        prefix = "registry:device-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest):
        query_args = []
        if request.device_definition_id: query_args.append(f"@device_definition_id:{{{self.escape_query(request.device_definition_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.device_type: query_args.append(f"@device_type:{{{self.escape_query(request.device_type)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['device_id']}"
        result = await self.client.json().set(key, "$", {"registration": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def device_instance_registry_get_ids(self) -> dict:
        prefix = "registry:device-instance:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def device_instance_registry_get(self, request: DeviceInstanceRequest):
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")
        if request.device_type: query_args.append(f"@device_type:{{{self.escape_query(request.device_type)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 100).return_fields("$")
        docs = (await self.client.ft(self.registry_device_instance_index_name).search(q)).docs
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
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=10000).sort_by("timestamp").return_fields("$")
        docs = (await self.client.ft(self.data_controller_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        id_str = "::".join([request.make, request.model, request.version])
        key = f"{database}:{collection}:{id_str}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def controller_definition_registry_get_ids(self) -> dict:
        prefix = "registry:controller-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest):
        query_args = []
        if request.controller_definition_id: query_args.append(f"@controller_definition_id:{{{self.escape_query(request.controller_definition_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_controller_definition_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 300):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{document['controller_id']}"
        result = await self.client.json().set(key, "$", {"registration": document})
        if result: await self.client.expire(key, ttl)
        return result

    async def controller_instance_registry_get_ids(self) -> dict:
        prefix = "registry:controller-instance:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest):
        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_controller_instance_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    # -------------------------------------------------------------------------------------
    # VARIABLE MAPS & SETS
    # -------------------------------------------------------------------------------------
    async def variablemap_definition_registry_get_ids(self) -> dict:
        prefix = "registry:variablemap-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        query_args = []
        if request.variablemap_definition_id:
            redis_id = request.variablemap_definition_id.replace(":", "") if "::" not in request.variablemap_definition_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(request.variablemap_definition_id.split("::"))])
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(redis_id)}}}")
        if request.variablemap: query_args.append(f"@variablemap:{{{self.escape_query(request.variablemap)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_variablemap_definition_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0):
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variablemap_definition_id.replace(':', '')}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def variableset_definition_registry_get_ids(self) -> dict:
        prefix = "registry:variableset-definition:"
        return {"results": [k.decode('utf-8').replace(prefix, "") async for k in self.client.scan_iter(f"{prefix}*")]}

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        query_args = []
        if request.variableset_definition_id: 
            redis_id = request.variableset_definition_id.replace(":", "") if "::" not in request.variableset_definition_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(request.variableset_definition_id.split("::"))])
            query_args.append(f"@variableset_definition_id:{{{self.escape_query(redis_id)}}}")
        if request.variablemap_definition_id:
            redis_id = request.variablemap_definition_id.replace(":", "") if "::" not in request.variablemap_definition_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(request.variablemap_definition_id.split("::"))])
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(redis_id)}}}")
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
        if request.index_type: query_args.append(f"@index_type:{{{self.escape_query(request.index_type)}}}")
        if request.index_value: query_args.append(f"@index_value:{{{self.escape_query(str(request.index_value))}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_variableset_definition_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool:
        self.connect()
        document = request.dict()
        key = f"{database}:{collection}:{request.variableset_definition_id.replace(':', '')}"
        return await self.client.json().set(key, "$", {"registration": document})

    async def variableset_data_get(self, request: VariableSetDataRequest):
        query_args = []
        if request.variableset_id: query_args.append(f"@variableset_id:{{{self.escape_query(request.variableset_id)}}}")
        if request.variablemap_id: query_args.append(f"@variablemap_id:{{{self.escape_query(request.variablemap_id)}}}")
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=10000).sort_by("timestamp").return_fields("$")
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
        if request.variableset_id: query_args.append(f"@variableset_id:{{{self.escape_query(request.variableset_id)}}}")
        if request.variablemap_id: query_args.append(f"@variablemap_id:{{{self.escape_query(request.variablemap_id)}}}")
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
            
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 10000).return_fields("$")
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
        # FAST PATH: If the registrar passes a full ID (name::timestamp), use direct O(1) fetch
        if "name" in query and "::" in query["name"]:
            key = f"registry:{resource}-definition:{query['name']}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception:
                pass

        # SLOW PATH: Search by Name
        query_args = []
        if "name" in query and query["name"]: 
            query_args.append(f"@name:{{{self.escape_query(query['name'])}}}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        
        docs = (await self.client.ft(f"idx:registry-{resource}-definition").search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}