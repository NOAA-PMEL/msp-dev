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
        self.data_sensor_index_name = "idx:data-sensor"
        self.registry_sensor_definition_index_name = "idx:registry-sensor-definition"
    
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
        
        # data:sensor
        # index_name = "idx:data-sensor"
        try:
            self.client.ft(self.data_sensor_index_name).dropindex()
        except:
            pass

        schema = (
            TextField("$.record.make", as_name="make"),
            TextField("$.record.model", as_name="model"),
            TextField("$.record.serial_number", as_name="serial_number"),
            TextField("$.record.version", as_name="version"),
            TextField("$.record.timestamp", as_name="timestamp")
        )
        definition = IndexDefinition(
            prefix=["data:sensor:"],
            index_type=IndexType.JSON
        )
        self.client.ft(self.data_sensor_index_name).create_index(schema, definition=definition)


    # def check_db(self, database):
    #     if not self.client.json().get(database, "$"):
    #         keys = database.split(":")
    #         self.client.json().set(database, "$", {keys[-1]: {}})

    # def check_collection(self, database, collection):
    #     self.check_db(database=database)
    #     if not self.client.json().get(database, f'$.{collection}'):
    #         keys = database.split(":")
    #         self.client.json().set(database, f"$.{keys[-1]}", {collection: []})

    async def sensor_data_update(
        self,
        # document: dict,
        request: DatastoreRequest,
        ttl: int = 300
    ):
        await super(RedisClient, self).sensor_data_update(request, ttl)
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
            document = request.request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            timestamp = document["timestamp"]
            
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            sensor_id = "::".join([make,model,serial_number])

            key = f"{request.database}:{request.collection}:{sensor_id}:{timestamp}"
            self.logger.debug("redis_client", extra={"key": key, "sensor-doc": document})
            self.client.json().set(
                key,
                "$",
                {"record": document}
            )
            self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("sensor_data_update", extra={"reason": e})
            return None
        
    async def sensor_data_get(self, query: DataStoreQuery):
        await super(RedisClient, self).sensor_data_get(query)

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
        self.logger.debug("sensor_data_get", extra={"query_string": qstring})
        q = Query(qstring).sort_by("timestamp")
        result = self.client.ft(self.data_sensor_index_name).search(q).docs

        return result
    
    async def device_definition_registry_update(
        self,
        # document: dict,
        request: DatastoreRequest,
        ttl: int = 300
    ):
        await super(RedisClient, self).device_definition_registry_update(request, ttl)
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
            document = request.request.dict()
            make = document["make"]
            model = document["model"]
            version = document["version"]
            
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            id = "::".join([make,model,version])

            key = f"{request.database}:{request.collection}:{id}"
            self.logger.debug("redis_client", extra={"key": key, "sensor-doc": document})
            self.client.json().set(
                key,
                "$",
                {"registration": document}
            )
            self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("sensor_data_update", extra={"reason": e})
            return None

    async def device_instance_registry_update(
        self,
        # document: dict,
        request: DatastoreRequest,
        ttl: int = 300
    ):
        await super(RedisClient, self).device_definition_registry_update(request, ttl)
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
            document = request.request.dict()
            make = document["make"]
            model = document["model"]
            serial_number = document["serial_number"]
            version = document["version"]
            
            # make = request.request.make
            # model = request.request.model
            # serial_number = request.request.serial_number
            # timestamp = request.request.timestamp

            device_id = "::".join([make,model,serial_number])

            key = f"{request.database}:{request.collection}:{device_id}:{version}"
            self.logger.debug("redis_client", extra={"key": key, "sensor-doc": document})
            self.client.json().set(
                key,
                "$",
                {"registration": document}
            )
            self.client.expire(key, ttl)

        except Exception as e:
            self.logger.error("sensor_data_update", extra={"reason": e})
            return None
