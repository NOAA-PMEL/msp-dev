import asyncio
import json
import redis
from redis.commands.json.path import Path
# import redis.commands.search.aggregation as aggregations
# import redis.commands.search.reducers as reducers
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.query import NumericFilter, Query

from db_client import DBClient, DBClientConfig
from datastore_requests import (
    DataStoreQuery,
    DataUpdate,
    DataRequest,
    DeviceDefinitionUpdate,
    DeviceDefinitionRequest,
    DeviceInstanceUpdate,
    DeviceInstanceRequest,
    DatastoreRequest,
    ControllerInstanceUpdate,
    ControllerInstanceRequest,
    ControllerDataRequest,
    ControllerDataUpdate,
    ControllerDefinitionRequest,
    ControllerDefinitionUpdate
)

class RedisClient(DBClient):
    """docstring for RedisClient."""
    def __init__(self, config: DBClientConfig):
        super(RedisClient, self).__init__(config)
        self.data_device_index_name = "idx:data-device"
        self.registry_device_definition_index_name = "idx:registry-device-definition"
        self.registry_device_instance_index_name = "idx:registry-device-instance"
        self.data_controller_index_name = "idx:data-controller"
        self.registry_controller_definition_index_name = "idx:registry-controller-definition"
        self.registry_controller_instance_index_name = "idx:registry-controller-instance"
        self.build_indexes()

    def connect(self):
        if not self.client:
            try:
                self.logger.debug("connect", extra={"self.config": self.config})
                if self.config["port"] is None:
                    self.config["port"] = 6379
                self.client = redis.Redis(host=self.config["hostname"], port=self.config["port"])
            except Exception as e:
                self.logger.error("redis connect", extra={"reason": e})
                self.client = None

    def build_indexes(self):

        self.connect()
        
        # data:device
        # index_name = "idx:data-device"
        try:
            self.client.ft(self.data_device_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.data_device_index_name})
            self.client.ft(self.registry_device_definition_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.registry_device_definition_index_name})
            self.client.ft(self.registry_device_instance_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.registry_device_instance_index_name})
            self.client.ft(self.data_controller_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.data_controller_index_name})
            self.client.ft(self.registry_controller_definition_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.registry_controller_definition_index_name})
            self.client.ft(self.registry_controller_instance_index_name).dropindex()
            self.logger.debug("build_index:dropped", extra={"index": self.registry_controller_instance_index_name})
        except Exception as e:
            self.logger.error("build_index", extra={"reason": e})
            pass

        try:
            # data:device
            schema = (
                TagField("$.record.device_id", as_name="device_id"),
                TagField("$.record.make", as_name="make"),
                TagField("$.record.model", as_name="model"),
                TagField("$.record.serial_number", as_name="serial_number"),
                TagField("$.record.version", as_name="version"),
                # TextField("$.record.device_type", as_name="device_type"),
                NumericField("$.record.timestamp", as_name="timestamp")
            )
            definition = IndexDefinition(
                prefix=["data:device:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.data_device_index_name).create_index(schema, definition=definition)

            # registry:device-definition
            schema = (
                TagField("$.registration.device_definition_id", as_name="device_definition_id"),
                TagField("$.registration.make", as_name="make"),
                TagField("$.registration.model", as_name="model"),
                TagField("$.registration.version", as_name="version"),
                TextField("$.registration.device_type", as_name="device_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:device-definition:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_device_definition_index_name).create_index(schema, definition=definition)

            # registry:device-instance
            schema = (
                TagField("$.registration.device_id", as_name="device_id"),
                TagField("$.registration.make", as_name="make"),
                TagField("$.registration.model", as_name="model"),
                TagField("$.registration.serial_number", as_name="serial_number"),
                TagField("$.registration.version", as_name="version"),
                TextField("$.registration.device_type", as_name="device_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:device-instance:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_device_instance_index_name).create_index(schema, definition=definition)

            # data:controller
            schema = (
                TagField("$.record.controller_id", as_name="controller_id"),
                TagField("$.record.make", as_name="make"),
                TagField("$.record.model", as_name="model"),
                TagField("$.record.serial_number", as_name="serial_number"),
                TagField("$.record.version", as_name="version"),
                # TextField("$.record.controller_type", as_name="controller_type"),
                NumericField("$.record.timestamp", as_name="timestamp")
            )
            definition = IndexDefinition(
                prefix=["data:controller:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.data_controller_index_name).create_index(schema, definition=definition)

            # registry:controller-definition
            schema = (
                TagField("$.registration.controller_definition_id", as_name="controller_definition_id"),
                TagField("$.registration.make", as_name="make"),
                TagField("$.registration.model", as_name="model"),
                TagField("$.registration.version", as_name="version"),
                # TextField("$.registration.controller_type", as_name="controller_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:controller-definition:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_controller_definition_index_name).create_index(schema, definition=definition)

            # registry:controller-instance
            schema = (
                TagField("$.registration.controller_id", as_name="controller_id"),
                TagField("$.registration.make", as_name="make"),
                TagField("$.registration.model", as_name="model"),
                TagField("$.registration.serial_number", as_name="serial_number"),
                TagField("$.registration.version", as_name="version"),
                # TextField("$.registration.controller_type", as_name="controller_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:controller-instance:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_controller_instance_index_name).create_index(schema, definition=definition)

        except Exception as e:
            self.logger.error("build_indexes", extra={"reason": e})
    # def check_db(self, database):
    #     if not self.client.json().get(database, "$"):
    #         keys = database.split(":")
    #         self.client.json().set(database, "$", {keys[-1]: {}})

    # def check_collection(self, database, collection):
    #     self.check_db(database=database)
    #     if not self.client.json().get(database, f'$.{collection}'):
    #         keys = database.split(":")
    #         self.client.json().set(database, f"$.{keys[-1]}", {collection: []})

    def escape_query(self, query: str) -> str:
        special = [",",".","<",">","{","}","[","]","'",":",";","!","@","#","$","%","^","&","*","(",")","-","+","=","~"]
        escaped = ""
        for ch in query:
            if ch in special:
                escaped += "\\"
            escaped += ch

        return escaped 
    
    async def device_data_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: DataUpdate,
        ttl: int = 300
    ):
        await super(RedisClient, self).device_data_update(database, collection, request, ttl)
        try:
            self.connect()

            
            # document = {
            #     # "_id": id,
            #     "make": make,
            #     "model": model,
            #     "serial_number": serial_number,
            #     "version": erddap_version,
            #     "timestamp": timestamp,
            #     "attributes": attributes,
            #     "dimensions": dimensions,
            #     "variables": variables,
            #     # "last_update": datetime.now(tz=timezone.utc),
            # }
            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            timestamp = document["timestamp"]
            
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            device_id = "::".join([make,model,serial_number])
            # if not document["device_id"]:
            document["device_id"] = device_id

            key = f"{database}:{collection}:{device_id}:{timestamp}"
            self.logger.debug("redis_client", extra={"key": key, "device-doc": document})
            self.client.json().set(
                key,
                "$",
                {"record": document}
            )
            self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("device_data_update", extra={"reason": e})
            return None
           
    async def device_definition_registry_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: DeviceDefinitionUpdate,
        ttl: int = 300
    ) -> bool:
        await super(RedisClient, self).device_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()

            
            # document = {
            #     # "_id": id,
            #     "make": make,
            #     "model": model,
            #     "serial_number": serial_number,
            #     "version": erddap_version,
            #     "timestamp": timestamp,
            #     "attributes": attributes,
            #     "dimensions": dimensions,
            #     "variables": variables,
            #     # "last_update": datetime.now(tz=timezone.utc),
            # }
            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            # make = document["make"]
            # model = document["model"]
            # version = document["version"]
            
            make = request.make
            model = request.model
            version = request.version
            # document = request.dict().pop("database").pop("collection")
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            id = "::".join([make,model,version])

            key = f"{database}:{collection}:{id}"
            self.logger.debug("redis_client", extra={"key": key, "device-doc": document})
            check_request = DeviceDefinitionRequest(
                make=make,
                model=model,
                version=version
            )
            check_results = await self.device_definition_registry_get(check_request)
            self.logger.debug("device_definition_registry_update", extra={"check": check_results})
            # check = False # tmp
            if check_results["results"]: # check if there are any results
                self.logger.debug("check_results", extra={"results": check_results["results"]})
                result = True
            else:
                result = self.client.json().set(
                    key,
                    "$",
                    {"registration": document}
                )
            if result and ttl > 0:
                self.client.expire(key, ttl)

            self.logger.debug("device_definition_registry_update", extra={"check_request": check_request, "result": result})
            return result
        
        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": e})
            return False

    # async def device_data_get(self, query: DataStoreQuery):
    async def device_data_get(self, request: DataRequest):
        await super(RedisClient, self).device_data_get(request)

        max_results = 10000
        
        # query_args = [f"@make:{{{self.escape_query(query.make)}}}"]
        # query_args.append(f"@model:{{{self.escape_query(query.model)}}}")
        # query_args.append(f"@serial_number:{{{self.escape_query(query.serial_number)}}}")

        query_args = []
        if request.device_id:
            query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")


        # if request.version:
        #     query_args.append(f"@version:{{{self.escape_query(request.version)}}}")

        if request.start_timestamp:
            query_args.append(f"@timestamp >= {request.start_timestamp}")
        
        if request.end_timestamp:
            query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args)
        self.logger.debug("device_data_get", extra={"query_string": qstring})
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = self.client.ft(self.data_device_index_name).search(q).docs
        results = []
        for doc in docs:
            try:
                if doc.json:
                    record = json.loads(doc.json)
                    results.append(record["record"])
            except Exception as e:
                self.logger.error("device_data_get", extra={"reason": e})
                continue

        return {"results": results}

    async def device_definition_registry_get(
            self,
            request: DeviceDefinitionRequest
    ) -> dict:
        await super(RedisClient, self).device_definition_registry_get(request)

        query_args = []
        if request.device_definition_id:
            query_args.append(f"@device_definition_id:{{{self.escape_query(request.device_definition_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")

        # if request.version:
        #     query_args.append(f"@version:{request.version}")

        if request.device_type:
            query_args.append(f"@device_type:{request.device_type}")
        
        if query_args:
            qstring = " ".join(query_args)
        else:
            qstring = "*"
        self.logger.debug("device_definition_registry_get", extra={"query_string": qstring})
        q = Query(qstring)#.sort_by("version", asc=False)
        docs = self.client.ft(self.registry_device_definition_index_name).search(q).docs
        self.logger.debug("device_definition_registry_get", extra={"docs": docs})
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception as e:
                self.logger.error("device_definition_registry_get", extra={"reason": e})
                continue
        self.logger.debug("device_definition_registry_get", extra={"results": results})
        return {"results": results}

    async def device_instance_registry_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: DeviceInstanceUpdate,
        ttl: int = 300
    ) -> bool:
        await super(RedisClient, self).device_instance_registry_update(database, collection, request, ttl)
        try:
            self.connect()

            
            # document = {
            #     # "_id": id,
            #     "make": make,
            #     "model": model,
            #     "serial_number": serial_number,
            #     "version": erddap_version,
            #     "timestamp": timestamp,
            #     "attributes": attributes,
            #     "dimensions": dimensions,
            #     "variables": variables,
            #     # "last_update": datetime.now(tz=timezone.utc),
            # }
            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            version = document["version"]
            
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            device_id = "::".join([make,model,serial_number])

            key = f"{database}:{collection}:{device_id}"
            self.logger.debug("redis_client", extra={"key": key, "device-doc": document})

            # check_request = DeviceInstanceRequest(
            #     make=make,
            #     model=model,
            #     serial_number=serial_number,
            #     version=version
            # )
            # check_results = await self.device_instance_registry_get(check_request)
            # self.logger.debug("device_instance_registry_update", extra={"check": check_results})
            # # check = False # tmp
            # if check_results["results"]: # check if there are any results
            #     self.logger.debug("check_results", extra={"results": check_results["results"]})
            #     result = True
            # else:
            #     result = self.client.json().set(
            #         key,
            #         "$",
            #         {"registration": document}
            #     )

            # update device instance every time to keep up to date
            result = self.client.json().set(
                key,
                "$",
                {"registration": document}
            )
            if result:
                self.client.expire(key, ttl)

            self.logger.debug("device_instance_registry_update", extra={"check_request": check_request, "result": result})
            return result
        
        except Exception as e:
            self.logger.error("redis_client:device_instance_registry_update", extra={"reason": e})
            return False

    async def device_instance_registry_get(self, request: DeviceInstanceRequest):
        await super(RedisClient, self).device_instance_registry_get(request)

        query_args = []
        if request.device_id:
            query_args.append(f"@device_id:{{{self.escape_query(request.device_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number:
            query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")

        # if request.version:
        #     query_args.append(f"@version:{request.version}")

        if request.device_type:
            query_args.append(f"@device_type:{request.device_type}")
        
        # qstring = " ".join(query_args)
        if query_args:
            qstring = " ".join(query_args)
        else:
            qstring = "*"
        self.logger.debug("device_instance_registry_get", extra={"query_string": qstring})
        q = Query(qstring)#.sort_by("version", asc=False)
        docs = self.client.ft(self.registry_device_instance_index_name).search(q).docs
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception as e:
                self.logger.error("device_instance_registry_get", extra={"reason": e})
                continue

        return {"results": results}

    async def controller_data_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: ControllerDataUpdate,
        ttl: int = 300
    ):
        await super(RedisClient, self).controller_data_update(database, collection, request, ttl)
        try:
            self.connect()

            
            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            timestamp = document["timestamp"]

            controller_id = "::".join([make,model,serial_number])
            # if not document["controller_id"]:
            document["controller_id"] = controller_id

            key = f"{database}:{collection}:{controller_id}:{timestamp}"
            self.logger.debug("redis_client", extra={"key": key, "controller-doc": document})
            self.client.json().set(
                key,
                "$",
                {"record": document}
            )
            self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("controller_data_update", extra={"reason": e})
            return None
           
    async def controller_definition_registry_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: ControllerDefinitionUpdate,
        ttl: int = 300
    ) -> bool:
        await super(RedisClient, self).controller_definition_registry_update(database, collection, request, ttl)
        try:
            self.connect()

            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            
            make = request.make
            model = request.model
            version = request.version

            id = "::".join([make,model,version])

            key = f"{database}:{collection}:{id}"
            self.logger.debug("redis_client", extra={"key": key, "controller-doc": document})
            check_request = ControllerDefinitionRequest(
                make=make,
                model=model,
                version=version
            )
            check_results = await self.controller_definition_registry_get(check_request)
            self.logger.debug("controller_definition_registry_update", extra={"check": check_results})
            # check = False # tmp
            if check_results["results"]: # check if there are any results
                self.logger.debug("check_results", extra={"results": check_results["results"]})
                result = True
            else:
                result = self.client.json().set(
                    key,
                    "$",
                    {"registration": document}
                )
            if result and ttl > 0:
                self.client.expire(key, ttl)

            self.logger.debug("controller_definition_registry_update", extra={"check_request": check_request, "result": result})
            return result
        
        except Exception as e:
            self.logger.error("controller_definition_registry_update", extra={"reason": e})
            return False

    # async def controller_data_get(self, query: DataStoreQuery):
    async def controller_data_get(self, request: ControllerDataRequest):
        await super(RedisClient, self).controller_data_get(request)

        max_results = 10000

        query_args = []
        if request.controller_id:
            query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")


        if request.start_timestamp:
            query_args.append(f"@timestamp >= {request.start_timestamp}")
        
        if request.end_timestamp:
            query_args.append(f"@timestamp < {request.end_timestamp}")

        qstring = " ".join(query_args)
        self.logger.debug("controller_data_get", extra={"query_string": qstring})
        q = Query(qstring).paging(offset=0, num=max_results).sort_by("timestamp")
        docs = self.client.ft(self.data_controller_index_name).search(q).docs
        results = []
        for doc in docs:
            try:
                if doc.json:
                    record = json.loads(doc.json)
                    results.append(record["record"])
            except Exception as e:
                self.logger.error("controller_data_get", extra={"reason": e})
                continue

        return {"results": results}

    async def controller_definition_registry_get(
            self,
            request: ControllerDefinitionRequest
    ) -> dict:
        await super(RedisClient, self).controller_definition_registry_get(request)

        query_args = []
        if request.controller_definition_id:
            query_args.append(f"@controller_definition_id:{{{self.escape_query(request.controller_definition_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.version:
            query_args.append(f"@version:{{{self.escape_query(request.version)}}}")

        if query_args:
            qstring = " ".join(query_args)
        else:
            qstring = "*"
        self.logger.debug("controller_definition_registry_get", extra={"query_string": qstring})
        q = Query(qstring)#.sort_by("version", asc=False)
        docs = self.client.ft(self.registry_controller_definition_index_name).search(q).docs
        self.logger.debug("controller_definition_registry_get", extra={"docs": docs})
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception as e:
                self.logger.error("controller_definition_registry_get", extra={"reason": e})
                continue
        self.logger.debug("controller_definition_registry_get", extra={"results": results})
        return {"results": results}

    async def controller_instance_registry_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: ControllerInstanceUpdate,
        ttl: int = 300
    ) -> bool:
        await super(RedisClient, self).controller_instance_registry_update(database, collection, request, ttl)
        try:
            self.connect()

            self.logger.debug("redis_client", extra={"update-doc": request, "ttl": ttl})
            document = request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            version = document["version"]

            controller_id = "::".join([make,model,serial_number])

            key = f"{database}:{collection}:{controller_id}"
            self.logger.debug("redis_client", extra={"key": key, "controller-doc": document})

            # check_request = ControllerInstanceRequest(
            #     make=make,
            #     model=model,
            #     serial_number=serial_number,
            #     version=version
            # )
            # check_results = await self.controller_instance_registry_get(check_request)
            # self.logger.debug("controller_instance_registry_update", extra={"check": check_results})
            # # check = False # tmp
            # if check_results["results"]: # check if there are any results
            #     self.logger.debug("check_results", extra={"results": check_results["results"]})
            #     result = True
            # else:
            #     result = self.client.json().set(
            #         key,
            #         "$",
            #         {"registration": document}
            #     )

            # update instance every time to keep up to date
            result = self.client.json().set(
                key,
                "$",
                {"registration": document}
            )
            if result:
                self.client.expire(key, ttl)

            self.logger.debug("controller_instance_registry_update", extra={"check_request": check_request, "result": result})
            return result
        
        except Exception as e:
            self.logger.error("redis_client:controller_instance_registry_update", extra={"reason": e})
            return False

    async def controller_instance_registry_get(self, request: ControllerInstanceRequest):
        await super(RedisClient, self).controller_instance_registry_get(request)

        query_args = []
        if request.controller_id:
            query_args.append(f"@controller_id:{{{self.escape_query(request.controller_id)}}}")
        if request.make:
            query_args.append(f"@make:{{{self.escape_query(request.make)}}}")
        if request.model:
            query_args.append(f"@model:{{{self.escape_query(request.model)}}}")
        if request.serial_number:
            query_args.append(f"@serial_number:{{{self.escape_query(request.serial_number)}}}")

        if query_args:
            qstring = " ".join(query_args)
        else:
            qstring = "*"
        self.logger.debug("controller_instance_registry_get", extra={"query_string": qstring})
        q = Query(qstring)#.sort_by("version", asc=False)
        docs = self.client.ft(self.registry_controller_instance_index_name).search(q).docs
        results = []
        for doc in docs:
            try:
                if doc.json:
                    reg = json.loads(doc.json)
                    results.append(reg["registration"])
            except Exception as e:
                self.logger.error("controller_instance_registry_get", extra={"reason": e})
                continue

        return {"results": results}
