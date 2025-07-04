import asyncio
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
    DatastoreRequest
)

class RedisClient(DBClient):
    """docstring for RedisClient."""
    def __init__(self, config: DBClientConfig):
        super(RedisClient, self).__init__(config)
        self.data_device_index_name = "idx:data-device"
        self.registry_device_definition_index_name = "idx:registry-device-definition"
        self.registry_device_instance_index_name = "idx:registry-device-instance"
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
        except Exception as e:
            self.logger.error("build_index", extra={"reason": e})
            pass

        try:
            # data:device
            schema = (
                TextField("$.record.make", as_name="make"),
                TextField("$.record.model", as_name="model"),
                TextField("$.record.serial_number", as_name="serial_number"),
                TextField("$.record.version", as_name="version"),
                # TextField("$.record.device_type", as_name="device_type"),
                TextField("$.record.timestamp", as_name="timestamp")
            )
            definition = IndexDefinition(
                prefix=["data:device:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.data_device_index_name).create_index(schema, definition=definition)

            # registry:device-definition
            schema = (
                TextField("$.registration.make", as_name="make"),
                TextField("$.registration.model", as_name="model"),
                TextField("$.registration.version", as_name="version"),
                TextField("$.registration.device_type", as_name="device_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:device-definition:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_device_definition_index_name).create_index(schema, definition=definition)

            # registry:device-instance
            schema = (
                TextField("$.registration.make", as_name="make"),
                TextField("$.registration.model", as_name="model"),
                TextField("$.registration.serial_number", as_name="serial_number"),
                TextField("$.registration.device_type", as_name="device_type"),
            )
            definition = IndexDefinition(
                prefix=["registry:device-instance:"],
                index_type=IndexType.JSON
            )
            self.client.ft(self.registry_device_instance_index_name).create_index(schema, definition=definition)

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
        
    async def device_data_get(self, query: DataStoreQuery):
        await super(RedisClient, self).device_data_get(query)

        query_args = [f"@make:{query.make}"]
        query_args.append(f"@model:{query.model}")
        query_args.append(f"@serial_number:{query.serial_number}")

        if query.version:
            query_args.append(f"@version:{query.version}")

        if query.start_time:
            query_args.append(f"@timestamp >= {query.start_time}")
        
        if query.end_time:
            query_args.append(f"@timestamp < {query.end_time}")

        qstring = " ".join(query_args)
        self.logger.debug("device_data_get", extra={"query_string": qstring})
        q = Query(qstring).sort_by("timestamp")
        result = self.client.ft(self.data_device_index_name).search(q).docs

        return result
    
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
            result = self.client.json().set(
                key,
                "$",
                {"registration": document}
            )
            self.client.expire(key, ttl)
            return result
        
        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": e})
            return False

    # async def device_data_get(self, query: DataStoreQuery):
    async def device_data_get(self, query: DataRequest):
        await super(RedisClient, self).device_data_get(query)

        query_args = [f"@make:{query.make}"]
        query_args.append(f"@model:{query.model}")
        query_args.append(f"@serial_number:{query.serial_number}")

        if query.version:
            query_args.append(f"@version:{query.version}")

        if query.start_time:
            query_args.append(f"@timestamp >= {query.start_time}")
        
        if query.end_time:
            query_args.append(f"@timestamp < {query.end_time}")

        qstring = " ".join(query_args)
        self.logger.debug("device_data_get", extra={"query_string": qstring})
        q = Query(qstring).sort_by("timestamp")
        result = self.client.ft(self.data_device_index_name).search(q).docs

        return {"result": result}

    async def device_definition_registry_get(
            self,
            request: DeviceDefinitionRequest
    ) -> dict:
        
        return {}

    async def device_instance_registry_update(
        self,
        # document: dict,
        database: str,
        collection: str,
        request: DeviceInstanceUpdate,
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

            check_request = DeviceInstanceRequest(
                make=make,
                model=model,
                serial_number=serial_number,
                version=version
            )
            check = await self.device_instance_registry_get(check_request)
            self.logger.debug("device_instance_registry_update", extra={"check": check})
            check = False # tmp
            if check:
                result = True
            else:
                result = self.client.json().set(
                    key,
                    "$",
                    {"registration": document}
                )
            if result:
                self.client.expire(key, ttl)

            self.logger.debug("device_instance_registry_update", extra={"check_request": check_request, "check": check, "result": result})
            return result
        
        except Exception as e:
            self.logger.error("device_instance_registry_update", extra={"reason": e})
            return False

    async def device_instance_registry_get(self, request: DeviceInstanceRequest):
        await super(RedisClient, self).device_instance_registry_get(request)

        query_args = []
        if request.make:
            query_args.append(f"@make:{request.make}")
        if request.model:
            query_args.append(f"@model:{request.model}")
        if request.serial_number:
            query_args.append(f"@serial_number:{request.serial_number}")

        # if request.version:
        #     query_args.append(f"@version:{request.version}")

        if request.device_type:
            query_args.append(f"@device_type:{request.device_type}")
        
        qstring = " ".join(query_args)
        self.logger.debug("device_instance_registry_get", extra={"query_string": qstring})
        q = Query(qstring)#.sort_by("version", asc=False)
        result = self.client.ft(self.registry_device_instance_index_name).search(q).docs.json

        return {"result": result}
