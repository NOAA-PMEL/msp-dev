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
            # Index creation logic remains identical to support search queries
            # ... [Omitted for brevity, maintain your existing build_indexes implementation] ...
            pass
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

    # --- SHARED PARSING HELPER ---
    def _parse_docs_sync(self, documents):
        """
        Helper to parse JSON in a background thread.
        Uses standard json.loads to support NaN/Inf values.
        """
        res = []
        for doc in documents:
            try:
                if doc.json:
                    # FIX: Use standard json instead of orjson to support NaN
                    record = json.loads(doc.json)
                    # Support both 'record' (telemetry) and 'registration' (definitions)
                    data_payload = record.get("record") or record.get("registration")
                    if data_payload:
                        res.append(data_payload)
            except Exception:
                continue
        return res

    # --- DATA RETRIEVAL (DEVICES) ---
    async def device_data_get(self, request: DataRequest):
        await super(RedisClient, self).device_data_get(request)
        max_results = 10000
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        
        docs = (await self.client.ft(self.data_device_index_name).search(q)).docs
        # Offload parsing to worker thread to prevent event loop blocking
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    # --- REGISTRY RETRIEVAL (RELIABLE PATH) ---
    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        """
        Reverted to a single RediSearch path. 
        Removed 'Fast Path' direct lookups that caused empty results if ID formatting differed.
        """
        query_args = []
        if request.device_definition_id:
            query_args.append(f"@device_definition_id:{{{self.escape_query(request.device_definition_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.device_type: query_args.append(f"@device_type:{request.device_type}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(q)).docs
        
        results = await asyncio.to_thread(self._parse_docs_sync, docs)
        return {"results": results}

    async def device_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            # Reverted to scan_iter to ensure all keys are found without relying on an external SET
            async for key in self.client.scan_iter("registry:device-definition:*"):
                id = key.decode('utf-8').replace("registry:device-definition:", "")
                ids.append(id)
            return {"results": ids}
        except Exception as e:
            self.logger.error("device_definition_registry_get_ids", extra={"reason": e})
            return {"results": []}

    # --- CONTROLLER AND VARIABLESET GETTERS ---
    # Apply the same logic (asyncio.to_thread with self._parse_docs_sync) 
    # to controller_data_get and variableset_data_get
    async def controller_data_get(self, request: ControllerDataRequest):
        # ... [Similar to device_data_get using asyncio.to_thread] ...
        pass

    async def variableset_data_get(self, request: VariableSetDataRequest):
        # ... [Similar to device_data_get using asyncio.to_thread] ...
        pass

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
        # FIX: Removed the existence check to allow definition updates to overwrite correctly
        result = await self.client.json().set(key, "$", {"registration": document})
        if result and ttl > 0:
            await self.client.expire(key, ttl)
        return result
