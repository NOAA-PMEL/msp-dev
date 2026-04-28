import asyncio
import json
import orjson
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
                self.logger.debug("connect", extra={"self.config": self.config})
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
            # data:device
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

            # registry:device-definition
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

            # registry:device-instance
            try:
                await self.client.ft(self.registry_device_instance_index_name).info()
            except Exception:
                schema = (
                    TagField("$.registration.device_id", as_name="device_id"),
                    TagField("$.registration.make", as_name="make"),
                    TagField("$.registration.model", as_name="model"),
                    TagField("$.registration.serial_number", as_name="serial_number"),
                    TagField("$.registration.version", as_name="version"),
                    TextField("$.registration.device_type", as_name="device_type"),
                )
                definition = IndexDefinition(prefix=["registry:device-instance:"], index_type=IndexType.JSON)
                await self.client.ft(self.registry_device_instance_index_name).create_index(schema, definition=definition)

            # data:controller
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

            # registry:controller-definition
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

            # registry:controller-instance
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

            # registry:variablemap-definition
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

            # registry:variableset-definition
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

            # data:variableset
            try:
                await self.client.ft("idx:data-variableset").info()
            except Exception:
                schema = (
                    TagField("$.record.variableset_id", as_name="variableset_id"),
                    TagField("$.record.variablemap_id", as_name="variablemap_id"),
                    TagField("$.record.variableset", as_name="variableset"),
                    NumericField("$.record.timestamp", as_name="timestamp")
                )
                definition = IndexDefinition(prefix=["data:variableset:"], index_type=IndexType.JSON)
                await self.client.ft("idx:data-variableset").create_index(schema, definition=definition)

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
    
    # -------------------------------------------------------------------------------------
    # DEVICES
    # -------------------------------------------------------------------------------------
    async def device_data_update(self, database: str, collection: str, request: DataUpdate, ttl: int = 300):
        await super(RedisClient, self).device_data_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            timestamp = document["timestamp"]

            device_id = "::".join([make,model,serial_number])
            document["device_id"] = device_id

            key = f"{database}:{collection}:{device_id}:{timestamp}"
            await self.client.json().set(key, "$", {"record": document})
            await self.client.expire(key, ttl)
        except Exception as e:
            self.logger.error("device_data_update", extra={"reason": e})

    async def device_data_get(self, request: DataRequest):
        await super(RedisClient, self).device_data_get(request)
        max_results = 10000

        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = (await self.client.ft(self.data_device_index_name).search(q)).docs

        def parse_docs(documents):
            res = []
            for doc in documents:
                if doc.json:
                    res.append(orjson.loads(doc.json)["record"])
            return res

        try:
            results = await asyncio.to_thread(parse_docs, docs)
        except Exception as e:
            self.logger.error("device_data_get parsing error", extra={"reason": e})
            results = []

        return {"results": results}

    async def device_definition_registry_update(self, database: str, collection: str, request: DeviceDefinitionUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).device_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            id = "::".join([request.make, request.model, request.version])
            key = f"{database}:{collection}:{id}"
            
            # Unconditionally set the document to ensure definition updates are always stored
            result = await self.client.json().set(key, "$", {"registration": document})

            if result and ttl > 0:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": e})
            return False

    async def device_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            # Use scan_iter to ensure we grab all keys regardless of when they were added
            async for key in self.client.scan_iter("registry:device-definition:*"):
                id = key.decode('utf-8').replace("registry:device-definition:", "")
                ids.append(id)
            return {"results": ids}
        except Exception as e:
            self.logger.error("device_definition_registry_get_ids", extra={"reason": e})
            return {"results": []}
        
    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        await super(RedisClient, self).device_definition_registry_get(request)

        # FAST PATH
        if request.device_definition_id:
            key = f"registry:device-definition:{request.device_definition_id}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception as e:
                self.logger.error("redis_client: device direct get error", extra={"reason": e})
            return {"results": []}

        # SLOW PATH
        query_args = []
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.device_type: query_args.append(f"@device_type:{request.device_type}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_device_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue
                
        return {"results": results}

    async def device_instance_registry_update(self, database: str, collection: str, request: DeviceInstanceUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).device_instance_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]

            device_id = "::".join([make,model,serial_number])
            key = f"{database}:{collection}:{device_id}"

            result = await self.client.json().set(key, "$", {"registration": document})
            if result:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("redis_client:device_instance_registry_update", extra={"reason": e})
            return False

    async def device_instance_registry_get(self, request: DeviceInstanceRequest):
        await super(RedisClient, self).device_instance_registry_get(request)
        query_args = []
        if request.device_id: query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")
        if request.device_type: query_args.append(f"@device_type:{request.device_type}")
        
        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(0, 50)
        docs = (await self.client.ft(self.registry_device_instance_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue

        return {"results": results}


    # -------------------------------------------------------------------------------------
    # CONTROLLERS
    # -------------------------------------------------------------------------------------
    async def controller_data_update(self, database: str, collection: str, request: ControllerDataUpdate, ttl: int = 300):
        await super(RedisClient, self).controller_data_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            timestamp = document["timestamp"]

            controller_id = "::".join([make,model,serial_number])
            document["controller_id"] = controller_id

            key = f"{database}:{collection}:{controller_id}:{timestamp}"
            await self.client.json().set(key, "$", {"record": document})
            await self.client.expire(key, ttl)
        except Exception as e:
            self.logger.error("controller_data_update", extra={"reason": e})

    async def controller_data_get(self, request: ControllerDataRequest):
        await super(RedisClient, self).controller_data_get(request)
        max_results = 10000

        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = (await self.client.ft(self.data_controller_index_name).search(q)).docs

        def parse_docs(documents):
            res = []
            for doc in documents:
                if doc.json:
                    res.append(orjson.loads(doc.json)["record"])
            return res

        try:
            results = await asyncio.to_thread(parse_docs, docs)
        except Exception as e:
            self.logger.error("controller_data_get parsing error", extra={"reason": e})
            results = []

        return {"results": results}

    async def controller_definition_registry_update(self, database: str, collection: str, request: ControllerDefinitionUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).controller_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            id = "::".join([request.make, request.model, request.version])
            key = f"{database}:{collection}:{id}"
            
            result = await self.client.json().set(key, "$", {"registration": document})
            
            if result and ttl > 0:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("controller_definition_registry_update", extra={"reason": e})
            return False

    async def controller_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:controller-definition:*"):
                id = key.decode('utf-8').replace("registry:controller-definition:", "")
                ids.append(id)
        except Exception as e:
            self.logger.error("controller_definition_registry_get_ids", extra={"reason": e})
        return {"results": ids}

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        await super(RedisClient, self).controller_definition_registry_get(request)

        # FAST PATH
        if request.controller_definition_id:
            key = f"registry:controller-definition:{request.controller_definition_id}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception as e:
                self.logger.error("redis_client: controller direct get error", extra={"reason": e})
            return {"results": []}

        # SLOW PATH
        query_args = []
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version: query_args.append(f"@version:{{{self.escape_query(request.version)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_controller_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue
                
        return {"results": results}

    async def controller_instance_registry_update(self, database: str, collection: str, request: ControllerInstanceUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).controller_instance_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]

            controller_id = "::".join([make,model,serial_number])
            key = f"{database}:{collection}:{controller_id}"

            result = await self.client.json().set(key, "$", {"registration": document})
            if result:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("redis_client:controller_instance_registry_update", extra={"reason": e})
            return False

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest):
        await super(RedisClient, self).controller_instance_registry_get(request)
        query_args = []
        if request.controller_id: query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        if request.make: query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model: query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number: query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_controller_instance_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue

        return {"results": results}


    # -------------------------------------------------------------------------------------
    # VARIABLEMAP / VARIABLESET
    # -------------------------------------------------------------------------------------
    async def variablemap_definition_registry_get_ids(self) -> dict:
        ids = []
        async for key in self.client.scan_iter("registry:variablemap-definition:*"):
            ids.append(key.decode('utf-8').replace("registry:variablemap-definition:", ""))
        return {"results": ids}
    
    async def variablemap_definition_registry_update(self, database: str, collection: str, request: VariableMapDefinitionUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).variablemap_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()

            if "variablemap_definition_id" in document:
                parts = document["variablemap_definition_id"].split("::")
                if len(parts) > 2:
                    parts[2] = parts[2].replace(":", "")
                    document["variablemap_definition_id"] = "::".join(parts)            

            variable_map_type_id = request.variablemap_type_id
            variablemap = request.variablemap
            valid_config_time = request.valid_config_time

            redis_time = valid_config_time.replace(":", "")
            id = "::".join([variable_map_type_id,variablemap,redis_time])
            key = f"{database}:{collection}:{id}"
            
            result = await self.client.json().set(key, "$", {"registration": document})
            
            if result and ttl > 0:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("redis_client: variablemap_definition_registry_update", extra={"reason": e})
            return False

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        await super(RedisClient, self).variablemap_definition_registry_get(request)

        # FAST PATH
        if request.variablemap_definition_id:
            parts = request.variablemap_definition_id.split("::")
            if len(parts) > 2: parts[2] = parts[2].replace(":", "")
            redis_id = "::".join(parts)
            
            key = f"registry:variablemap-definition:{redis_id}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception:
                pass
            return {"results": []}

        # SLOW PATH
        query_args = []
        if request.variablemap_type: query_args.append(f"@variablemap_type:{{{self.escape_query(request.variablemap_type)}}}")
        if request.variablemap_type_id: query_args.append(f"@variablemap_type_id:{{{self.escape_query(request.variablemap_type_id)}}}")
        if request.variablemap: query_args.append(f"@variablemap:{{{self.escape_query(request.variablemap)}}}")
        if request.valid_config_time:
            redis_time = request.valid_config_time.replace(":", "")
            query_args.append(f"@valid_config_time:{{{self.escape_query(redis_time)}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_variablemap_definition_index_name).search(q)).docs
        
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue
                
        return {"results": results}

    async def variableset_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:variableset-definition:*"):
                id = key.decode('utf-8').replace("registry:variableset-definition:", "")
                ids.append(id)
        except Exception as e:
            self.logger.error("variableset_definition_registry_get_ids", extra={"reason": e})
        return {"results": ids}

    async def variableset_definition_registry_update(self, database: str, collection: str, request: VariableSetDefinitionUpdate, ttl: int = 300) -> bool:
        await super(RedisClient, self).variableset_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()

            if "variableset_definition_id" in document:
                parts = document["variableset_definition_id"].split("::")
                if len(parts) > 2:
                    parts[2] = parts[2].replace(":", "")
                    document["variableset_definition_id"] = "::".join(parts)
                    
            if "variablemap_definition_id" in document:
                parts = document["variablemap_definition_id"].split("::")
                if len(parts) > 2:
                    parts[2] = parts[2].replace(":", "")
                    document["variablemap_definition_id"] = "::".join(parts)

            variablemap_definition_id = document["variablemap_definition_id"]
            parts = variablemap_definition_id.split("::")
            parts[2] = parts[2].replace(":", "")
            redis_id = "::".join(parts)

            variableset = document["variableset"]
            id = "::".join([redis_id,variableset])
            key = f"{database}:{collection}:{id}"
            
            result = await self.client.json().set(key, "$", {"registration": document})
            
            if result and ttl > 0:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error("redis_client: variableset_definition_registry_update", extra={"reason": e})
            return False

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        await super(RedisClient, self).variableset_definition_registry_get(request)

        # FAST PATH
        if request.variableset_definition_id:
            parts = request.variableset_definition_id.split("::")
            if len(parts) > 2: parts[2] = parts[2].replace(":", "")
            redis_id = "::".join(parts)
            
            key = f"registry:variableset-definition:{redis_id}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception:
                pass
            return {"results": []}

        # SLOW PATH
        query_args = []
        if request.variablemap_definition_id:
            parts = request.variablemap_definition_id.split("::")
            if len(parts) > 2: parts[2] = parts[2].replace(":", "")
            redis_id = "::".join(parts)
            query_args.append(f"@variablemap_definition_id:{{{self.escape_query(redis_id)}}}")
            
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
        if request.index_type: query_args.append(f"@index_type:{{{self.escape_query(request.index_type)}}}")
        if request.index_value: query_args.append(f"@index_value:{{{self.escape_query(str(request.index_value))}}}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring)
        docs = (await self.client.ft(self.registry_variableset_definition_index_name).search(q)).docs

        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception:
                continue
                
        return {"results": results}

    async def variableset_data_update(self, database: str, collection: str, request: VariableSetDataUpdate, ttl: int = 300):
        await super(RedisClient, self).variableset_data_update(database, collection, request, ttl)
        try:
            self.connect()
            document = request.dict()
            variableset_id = document["variableset_id"]
            timestamp = document["timestamp"]

            key = f"{database}:{collection}:{variableset_id}:{timestamp}"
            await self.client.json().set(key, "$", {"record": document})
            if ttl > 0:
                await self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("redis_client: variableset_data_update", extra={"reason": e})

    async def variableset_data_get(self, request: VariableSetDataRequest):
        await super(RedisClient, self).variableset_data_get(request)
        max_results = 10000

        query_args = []
        if request.variableset_id: query_args.append(f"@variableset_id:{{{self.escape_query(request.variableset_id)}}}")
        if request.variablemap_id: query_args.append(f"@variablemap_id:{{{self.escape_query(request.variablemap_id)}}}")
        if request.variableset: query_args.append(f"@variableset:{{{self.escape_query(request.variableset)}}}")
        if request.start_timestamp: query_args.append(f"@timestamp >= {request.start_timestamp}")
        if request.end_timestamp: query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args) if query_args else "*"
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = (await self.client.ft("idx:data-variableset").search(q)).docs
        
        def parse_docs(documents):
            res = []
            for doc in documents:
                if doc.json:
                    res.append(orjson.loads(doc.json)["record"])
            return res

        try:
            results = await asyncio.to_thread(parse_docs, docs)
        except Exception as e:
            self.logger.error("variableset_data_get parsing error", extra={"reason": e})
            results = []

        return {"results": results}


    # -------------------------------------------------------------------------------------
    # GENERIC SAMPLING/REGISTRY FALLBACKS
    # -------------------------------------------------------------------------------------
    async def project_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:project-definition:*"):
                id = key.decode('utf-8').replace("registry:project-definition:", "")
                ids.append(id)
        except Exception as e:
            self.logger.error("project_definition_registry_get_ids", extra={"reason": e})
        return {"results": ids}

    async def platform_definition_registry_get_ids(self) -> dict:
        ids = []
        try:
            async for key in self.client.scan_iter("registry:platform-definition:*"):
                ids.append(key.decode('utf-8').replace("registry:platform-definition:", ""))
        except Exception as e:
            self.logger.error("platform_definition_registry_get_ids", extra={"reason": e})
        return {"results": ids}

    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        ids = []
        prefix = f"registry:{resource}-definition:"
        async for key in self.client.scan_iter(f"{prefix}*"):
            ids.append(key.decode('utf-8').replace(prefix, ""))
        return {"results": ids}
    
    async def sampling_definition_registry_update(self, resource: str, database: str, collection: str, request: dict, ttl: int = 0) -> bool:
        try:
            self.connect()
            document = request
            name = document["metadata"]["name"]
            valid_config_time = document["metadata"].get("valid_config_time", "2020-01-01T00:00:00Z")
            
            redis_time = valid_config_time.replace(":", "")
            id = f"{name}::{redis_time}"
            key = f"{database}:{collection}:{id}"
            
            result = await self.client.json().set(key, "$", {"registration": document})
            
            if result and ttl > 0:
                await self.client.expire(key, ttl)

            return result
        except Exception as e:
            self.logger.error(f"redis_client: {resource}_definition_registry_update", extra={"reason": e})
            return False

    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        # FAST PATH
        if "name" in query and "::" in query["name"]:
            redis_id = query["name"]
            key = f"registry:{resource}-definition:{redis_id}"
            try:
                doc = await self.client.json().get(key)
                if doc and "registration" in doc:
                    return {"results": [doc["registration"]]}
            except Exception:
                pass
            return {"results": []}

        # SLOW PATH
        query_args = []
        if "name" in query and query["name"]:
            query_args.append(f"@name:{{{self.escape_query(query['name'])}}}")

        qstring = " ".join(query_args) if query_args else "*"
        index_name = f"idx:registry-{resource}-definition"
        
        try:
            q = Query(qstring)
            docs = (await self.client.ft(index_name).search(q)).docs
            
            results = []
            for doc in docs:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
                    
            return {"results": results}
        except Exception as e:
            self.logger.error(f"redis_client: {resource}_definition_registry_get search error", extra={"reason": e})
            return {"results": []}
        