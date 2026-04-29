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
                    self.client = redis.Redis(
                        host=self.config["hostname"], 
                        port=self.config["port"], 
                        password=self.config["password"],
                        decode_responses=True 
                    )
                else:
                    self.client = redis.Redis(
                        host=self.config["hostname"], 
                        port=self.config["port"],
                        decode_responses=True 
                    )
            except Exception as e:
                self.logger.error("redis connect", extra={"reason": str(e)})
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
            self.logger.error("build_indexes", extra={"reason": str(e)})

    # -------------------------------------------------------------------------------------
    # UTILITY HELPERS
    # -------------------------------------------------------------------------------------
    def escape_query(self, query: str) -> str:
        special = [",",".","<",">","{","}","[","]","'",":",";","!","@","#","$","%","^","&","*","(",")","-","+","=","~"]
        escaped = ""
        for ch in str(query):
            if ch in special: escaped += "\\"
            escaped += ch
        return escaped 

    def _parse_docs_sync(self, documents):
        res = []
        for doc in documents:
            try:
                data = None
                if hasattr(doc, "$"):
                    data = getattr(doc, "$")
                elif hasattr(doc, "json"):
                    data = doc.json
                elif isinstance(doc, dict) and "$" in doc:
                    data = doc["$"]
                
                if data:
                    if isinstance(data, list):
                        record = json.loads(data[0]) if isinstance(data[0], str) else data[0]
                    else:
                        record = json.loads(data) if isinstance(data, str) else data
                        
                    payload = record.get("record") or record.get("registration") or record
                    if payload:
                        res.append(payload)
            except Exception as e:
                self.logger.error("parse_docs_sync_error", extra={"reason": str(e)})
                continue
        return res


    async def _get_ids_safely(self, prefix: str) -> dict:
        """Safely scans for IDs regardless of decode_responses settings to prevent crashes."""
        ids = []
        try:
            self.connect()
            async for key in self.client.scan_iter(f"{prefix}*"):
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                ids.append(key.replace(prefix, ""))
        except Exception as e:
            self.logger.error("redis_client:get_ids_safely", extra={"prefix": prefix, "reason": str(e)})
        return {"results": ids}


    # -------------------------------------------------------------------------------------
    # DEVICES
    # -------------------------------------------------------------------------------------
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{document['device_id']}:{document['timestamp']}"
            result = await self.client.json().set(key, "$", {"record": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:device_data_update", extra={"reason": str(e)})
            return False

    async def device_data_get(self, request: DataRequest) -> dict:
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

    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 0) -> bool:
        try:
            self.connect()
            self.logger.debug("redis_client:device_definition_registry_update", extra={"here": str(1)})
            document = request.dict()
            id_str = "::".join([request.make, request.model, request.version])
            key = f"{database}:{collection}:{id_str}"
            self.logger.debug("redis_client:device_definition_registry_update", extra={"k": key})
            result = await self.client.json().set(key, "$", {"registration": document})
            self.logger.debug("redis_client:device_definition_registry_update", extra={"res": result})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:device_definition_registry_update", extra={"reason": str(e)})
            return False

    async def device_definition_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:device-definition:")

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        await super(RedisClient, self).device_definition_registry_get(request)

        # -------------------------------------------------------------------------
        # FAST PATH: O(1) Direct Lookup for Registrar Sync Requests
        # -------------------------------------------------------------------------
        if request.device_definition_id:
            key = f"registry:device-definition:{request.device_definition_id}"
            try:
                # Instant lookup bypassing RediSearch entirely
                doc = await self.client.json().get(key)
                if doc:
                    # Fallback to the flat doc if "registration" wrapper is missing (Legacy)
                    return {"results": [doc.get("registration", doc)]}
            except Exception as e:
                self.logger.error("redis_client fast-path error", extra={"reason": str(e)})
            
            # If exact ID not found, return empty (don't fall back to heavy search)
            return {"results": []}

        # -------------------------------------------------------------------------
        # SLOW PATH: RediSearch Fallback for Dashboard Queries
        # -------------------------------------------------------------------------
        query_args = []
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.device_type:
            query_args.append(f"@device_type:{request.device_type}")
        
        qstring = " ".join(query_args) if query_args else "*"
        self.logger.debug("device_definition_registry_get search", extra={"query": qstring})
        
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    # Support legacy docs that lack the registration wrapper
                    results.append(reg.get("registration", reg))
            except Exception as e:
                self.logger.error("device_definition_registry_get parsing", extra={"reason": str(e)})
                continue
                
        return {"results": results}
    
    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 300) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{document['device_id']}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:device_instance_registry_update", extra={"reason": str(e)})
            return False

    async def device_instance_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:device-instance:")

    async def device_instance_registry_get(self, request: DeviceInstanceRequest) -> dict:
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")
        if request.device_type: query_args.append(f"@device_type:{{{self.escape_query(request.device_type)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 1000).return_fields("$")
        docs = (await self.client.ft(self.registry_device_instance_index_name).search(q)).docs
        return {"results": await asyncio.to_thread(self._parse_docs_sync, docs)}

    # -------------------------------------------------------------------------------------
    # CONTROLLERS
    # -------------------------------------------------------------------------------------
    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{document['controller_id']}:{document['timestamp']}"
            result = await self.client.json().set(key, "$", {"record": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:controller_data_update", extra={"reason": str(e)})
            return False

    async def controller_data_get(self, request: ControllerDataRequest) -> dict:
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

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 0) -> bool:
        try:
            self.connect()
            document = request.dict()
            id_str = "::".join([request.make, request.model, request.version])
            key = f"{database}:{collection}:{id_str}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:controller_definition_registry_update", extra={"reason": str(e)})
            return False

    async def controller_definition_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:controller-definition:")

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        # -------------------------------------------------------------------------
        # FAST PATH: O(1) Direct Lookup for Registrar Sync Requests
        # -------------------------------------------------------------------------
        if request.controller_definition_id:
            key = f"registry:controller-definition:{request.controller_definition_id}"
            try:
                # Instant lookup bypassing RediSearch entirely
                doc = await self.client.json().get(key)
                if doc:
                    # Fallback to the flat doc if "registration" wrapper is missing (Legacy)
                    return {"results": [doc.get("registration", doc)]}
            except Exception as e:
                self.logger.error("redis_client fast-path error", extra={"reason": str(e)})
            
            # If exact ID not found, return empty (don't fall back to heavy search)
            return {"results": []}

        # -------------------------------------------------------------------------
        # SLOW PATH: RediSearch Fallback for Dashboard Queries
        # -------------------------------------------------------------------------
        query_args = []
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        
        qstring = " ".join(query_args) if query_args else "*"
        self.logger.debug("controller_definition_registry_get search", extra={"query": qstring})
        
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_controller_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    # Support legacy docs that lack the registration wrapper
                    results.append(reg.get("registration", reg))
            except Exception as e:
                self.logger.error("controller_definition_registry_get parsing", extra={"reason": str(e)})
                continue
                
        return {"results": results}
    
    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 300) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{document['controller_id']}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:controller_instance_registry_update", extra={"reason": str(e)})
            return False

    async def controller_instance_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:controller-instance:")

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest) -> dict:
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
        return await self._get_ids_safely("registry:variablemap-definition:")

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        redis_id = request.variablemap_definition_id
        if redis_id:
            redis_id = redis_id.replace(":", "") if "::" not in redis_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(redis_id.split("::"))])
        
        # ---------------------------------------------------------
        # FAST PATH: Exact ID Lookup for Registrar Syncs
        # ---------------------------------------------------------
        if redis_id:
            key = f"registry:variablemap-definition:{redis_id}"
            try:
                doc = await self.client.json().get(key)
                if doc:
                    return {"results": [doc.get("registration", doc)]}
            except Exception:
                pass
            return {"results": []} # Prevent RediSearch fallback on direct ID lookups

        # ---------------------------------------------------------
        # SLOW PATH: RediSearch Dashboard Lookups
        # ---------------------------------------------------------
        query_args = []
        if request.variablemap_type: query_args.append(f"@variablemap_type:{{{self.escape_query(request.variablemap_type)}}}")
        if request.variablemap_type_id: query_args.append(f"@variablemap_type_id:{{{self.escape_query(request.variablemap_type_id)}}}")
        if request.variablemap: query_args.append(f"@variablemap:{{{self.escape_query(request.variablemap)}}}")
        if request.valid_config_time: query_args.append(f"@valid_config_time:{{{self.escape_query(request.valid_config_time.replace(':', ''))}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_variablemap_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg.get("registration", reg))
            except Exception:
                continue
        return {"results": results}

    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 0) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{request.variablemap_definition_id.replace(':', '')}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:variablemap_definition_registry_update", extra={"reason": str(e)})
            return False

    async def variableset_definition_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:variableset-definition:")

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        redis_id = request.variableset_definition_id
        if redis_id: 
            redis_id = redis_id.replace(":", "") if "::" not in redis_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(redis_id.split("::"))])
            
        # ---------------------------------------------------------
        # FAST PATH: Exact ID Lookup for Registrar Syncs
        # ---------------------------------------------------------
        if redis_id:
            key = f"registry:variableset-definition:{redis_id}"
            try:
                doc = await self.client.json().get(key)
                if doc:
                    return {"results": [doc.get("registration", doc)]}
            except Exception:
                pass
            return {"results": []}

        # ---------------------------------------------------------
        # SLOW PATH: RediSearch Dashboard Lookups
        # ---------------------------------------------------------
        query_args = []
        if request.variablemap_definition_id:
            vmap_id = request.variablemap_definition_id.replace(":", "") if "::" not in request.variablemap_definition_id else "::".join([p.replace(":", "") if i==2 else p for i, p in enumerate(request.variablemap_definition_id.split("::"))])
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(vmap_id)}}}")
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
        if request.index_type: query_args.append(f"@index_type:{{{self.escape_query(request.index_type)}}}")
        if request.index_value: query_args.append(f"@index_value:{{{self.escape_query(str(request.index_value))}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        docs = (await self.client.ft(self.registry_variableset_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg.get("registration", reg))
            except Exception:
                continue
        return {"results": results}

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 0) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{request.variableset_definition_id.replace(':', '')}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:variableset_definition_registry_update", extra={"reason": str(e)})
            return False

    async def variableset_data_get(self, request: VariableSetDataRequest) -> dict:
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

    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 300) -> bool:
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{request.variableset_id}:{request.timestamp}"
            result = await self.client.json().set(key, "$", {"record": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:variableset_data_update", extra={"reason": str(e)})
            return False

    # -------------------------------------------------------------------------------------
    # VARIABLE SET INSTANCE (Active Variablesets)
    # -------------------------------------------------------------------------------------
    async def variableset_instance_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:variableset-instance:")

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
        try:
            self.connect()
            document = request.dict()
            key = f"{database}:{collection}:{request.variableset_id}"
            result = await self.client.json().set(key, "$", {"registration": document})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error("redis_client:variableset_instance_registry_update", extra={"reason": str(e)})
            return False

    # -------------------------------------------------------------------------------------
    # PROJECT & PLATFORM HELPERS (Legacy Support for main.py)
    # -------------------------------------------------------------------------------------
    async def project_definition_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:project-definition:")

    async def platform_definition_registry_get_ids(self) -> dict:
        return await self._get_ids_safely("registry:platform-definition:")

    # -------------------------------------------------------------------------------------
    # SAMPLING DEFINITIONS (DYNAMIC)
    # -------------------------------------------------------------------------------------
    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        return await self._get_ids_safely(f"registry:{resource}-definition:")

    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool:
        try:
            self.connect()
            name = request.get("metadata", {}).get("name", "unknown")
            valid_time = request.get("metadata", {}).get("valid_config_time", "2020-01-01T00:00:00Z").replace(":", "")
            id = f"{name}::{valid_time}"
            key = f"{database}:{collection}:{id}"
            
            result = await self.client.json().set(key, "$", {"registration": request})
            if result and ttl > 0:
                await self.client.expire(key, ttl)
            return True if result else False
        except Exception as e:
            self.logger.error(f"redis_client:{resource}_definition_registry_update", extra={"reason": str(e)})
            return False
    
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        # ---------------------------------------------------------
        # FAST PATH: Exact ID Lookup for Registrar Syncs
        # ---------------------------------------------------------
        if "name" in query and "::" in query["name"]:
            key = f"registry:{resource}-definition:{query['name']}"
            try:
                doc = await self.client.json().get(key)
                if doc:
                    return {"results": [doc.get("registration", doc)]}
            except Exception:
                pass
            return {"results": []}

        # ---------------------------------------------------------
        # SLOW PATH: RediSearch Dashboard Lookups
        # ---------------------------------------------------------
        query_args = []
        if "name" in query and query["name"]: 
            query_args.append(f"@name:{{{self.escape_query(query['name'])}}}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).return_fields("$")
        
        docs = (await self.client.ft(f"idx:registry-{resource}-definition").search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg.get("registration", reg))
            except Exception:
                continue
        return {"results": results}